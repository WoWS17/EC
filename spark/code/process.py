from elasticsearch import Elasticsearch
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, ArrayType
import openai
import os
import stat
import json
from elasticsearch import Elasticsearch
import pandas as pd

sc = SparkContext(appName="MedScan")
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

kafkaServer="kafkaServer:9092"
topic = "ner"

idx = 0
ELASTIC_INDEX = "ner_idx"
ELASTIC_HOST = "http://elasticsearch:9200"


SYSTEM_PROMPT = "\
                Sei un intelligente sistema che estrae delle caratteristiche da un testo.\
                Ti fornirò quali caratteristiche devi estrarre, il testo da cui estrarle e il formato di output.\
                "
USER_PROMPT_1 = "Hai chiaro il tuo ruolo?"
ASSISTANT_PROMPT_1 = "Certo, sono pronto ad aiutarti con il tuo compito. Ti prego di fornirmi le informazioni necessarie per iniziare."
GUIDELINES_PROMPT = (
"Formato Output:\n"
"{{\n"
    "\"Emiscroto destro\" : {{\n"
        "\"Testicolo\" : {{\n"
            "\"Sede anatomica\" : \"\",\n"
            "\"Volume\" : \"\",\n"
            "\"Dimensioni\" : \"\",\n"
            "\"Ecostruttura : \"\"\n"
        "}},\n"
        "\"Epididimo\" : {{\n"
            "\"Aspetto ecografico\" : \"\"\n"
        "}},\n"
        "\"ECD\" : {{\n"
            "\"Canale inguinale\" : \"\"\n"
        "}}\n"
    "}},\n"
    "\n"
    "\"Emiscroto sinistro\" : {{\n"
        "\"Testicolo\" : {{\n"
            "\"Sede anatomica\" : \"\",\n"
            "\"Volume\" : \"\",\n"
            "\"Dimensioni\" : \"\",\n"
            "\"Ecostruttura : \"\"\n"
        "}},\n"
        "\"Epididimo\" : {{\n"
            "\"Aspetto ecografico\" : \"\"\n"
        "}},\n"
        "\"ECD\" : {{\n"
            "\"Canale inguinale\" : \"\"\n"
        "}}\n"
    "}}\n"
"}}\n"
"Spiegazione caratteristiche :\n"
"\"Sede anatomica\" può assumere solo i valori \"Fisiologica\" o \"Non fisiologica\" o \"\"\n"
"\"Volume\" deve contenere il valore specificato nel referto. Non includere l'unità di misura e altri simboli come < o >. Se non è presente un valore univoco non inserire nulla. In italiano per indicare le cifre decimali usiamo la virgola; Sostituisci la virgola con il punto perchè devo lavorare con la notazione inglese.\n"
"\"Dimensioni\" può assumere solo i valori \"Normali per età\" o \"Non normali per età\" o \"\"\n"
"\"Ecostruttura\" può assumere solo i valori \"Omogenea\" o \"Disomogenea\" o \"\"\n"
"\"Aspetto ecografico\" può assumere solo i valori \"Normale\" o \"Anormale\" o \"\"\n"
"\"Canale inguinale\" può assumere solo i valori \"Normale\" o \"Anormale\" o \"\"\n"
"Nota che le stesse caratteristiche sono ripetute sia per l'emiscroto destro che per l'emiscroto sinistro, non fare confusione tra le due parti."
"Testo : {}\n"
"Output : "
)

DOCUMENTS = []

ENGINE = "ecos-extract"

def setOpenAIConf():
    openai.api_type = "azure"
    openai.api_base = os.getenv("AZURE_OPENAI_ENDPOINT")
    openai.api_version = "2023-07-01-preview"
    openai.api_key = os.getenv("AZURE_OPENAI_KEY")

def create_elastic_index():
    es = Elasticsearch(hosts = ELASTIC_HOST)
    response = es.indices.create(index=ELASTIC_INDEX, ignore=400)

    if 'acknowledged' in response:
        if response['acknowledged'] == True:
            print("INDEX MAPPING SUCCESS FOR INDEX:", response['index'])
    return es
    
def requestToChatGPT(user_prompt):
   setOpenAIConf()
   return openai.ChatCompletion.create(
      engine=ENGINE,
      messages = [
        {"role":"system","content":SYSTEM_PROMPT},
        {"role":"user","content":USER_PROMPT_1},
        {"role": "assistant", "content": ASSISTANT_PROMPT_1},
        {"role": "user", "content": user_prompt}
        ],
      temperature=0,
      max_tokens=800,
      top_p=0.95,
      frequency_penalty=0,
      presence_penalty=0,
      stop=None)

def sendToEs(batch_df: DataFrame, batch_id: int):
  for row in batch_df.rdd.collect():
    user_prompt = GUIDELINES_PROMPT.format(row['message'])
    #print(row['message'])

    try: # Gestiso quando i server di OpenAI non sono in grado di consumare la mia richiesta 
        response = requestToChatGPT(user_prompt)
    except openai.error.APIConnectionError:
        continue 
       
    response = response['choices'][0]['message']['content']
    print(response)

    try:
        doc = json.loads(response.rsplit('}', 1)[0] + '}')
        doc["Emiscroto destro"]["Testicolo"]["Volume"] = float(doc["Emiscroto destro"]["Testicolo"]["Volume"])
        doc["Emiscroto sinistro"]["Testicolo"]["Volume"] = float(doc["Emiscroto sinistro"]["Testicolo"]["Volume"])
    except (json.JSONDecodeError, ValueError):
        continue
    
    doc["Timestamp"] = row["timestamp"]

    global idx
    idx = idx + 1
    resp = es.index(index=ELASTIC_INDEX, id=idx, document=doc)
    print(resp['result'])
    
    # Making Excel dataset...

    # Crea una nuova struttura dati con le chiavi del dizionario come tuple
    reform = {(outerKey, innerKey, innerInnerKey): values
            for outerKey, innerDict in doc.items() 
            for innerKey, innerInnerDict in innerDict.items() 
            for innerInnerKey, values in innerInnerDict.items()}

    doc = pd.DataFrame(reform, index=[0])
    DOCUMENTS.append(doc)

es = create_elastic_index()

# Read from Kafka
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafkaServer) \
  .option("subscribe", topic) \
  .load()
  # DataFrameReader.load() returns a DataFrame

# Percorso del file di segnalazione nel volume condiviso
signal_file_path = "/shared-data/spark-ready"
# Crea il file di segnalazione
os.mknod(signal_file_path)
# Imposta i permessi del file per consentire a logstash di cancellarlo
os.chmod(signal_file_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

# Define the schema of the JSON data from Kafka
json_schema = StructType() \
    .add("tags", ArrayType(StringType())) \
    .add("message", StringType()) \
    .add("process", StructType()
         .add("exit_code", StringType())
         .add("command_line", StringType())) \
    .add("@version", StringType()) \
    .add("@timestamp", StringType()) \
    .add("host", StructType()
         .add("name", StringType())) \
    .add("event", StructType()
         .add("original", StringType()))

df2 = df.select(from_json(col("value").cast("string"), json_schema).alias("data"))
df3 = df2.select(col("data.@timestamp").alias("timestamp"), col("data.message").alias("message"))
# df3 ha 2 colonne: timestamp e message

# Write to Elastic Search
df3.writeStream \
    .foreachBatch(sendToEs) \
    .start() \
    .awaitTermination()

result = pd.concat(DOCUMENTS, axis=0, ignore_index=True)
result.to_excel("Dataset.xlsx")

'''df3.writeStream \
  .format("console") \
  .start() \
  .awaitTermination()'''
