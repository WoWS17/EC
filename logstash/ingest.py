import os
from glob import glob
from striprtf.striprtf import rtf_to_text
import time 

separator = "---"

# Percorso del file di segnalazione nel volume condiviso
signal_file_path = "/shared-data/spark-ready"
# Intervallo di polling in secondi
polling_interval = 3

# Verifica se il file di segnalazione esiste
while not os.path.exists(signal_file_path):
    # Se il file non esiste, attendi l'intervallo di polling e riprova
    time.sleep(polling_interval)

os.remove(signal_file_path)

time.sleep(5)

# Dir dove stanno i files
directory_path = "/eco-inguine"

# Get a list of all .rtf files in the directory
rtf_files = glob(os.path.join(directory_path, "*.rtf"))

for file_path in rtf_files:
    with open(file_path, "r") as file:
        text = rtf_to_text(file.read())
        print(text)
        print(separator)