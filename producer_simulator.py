import time
import json
import os
from kafka import KafkaProducer

# Je configure mes constantes : où est le serveur (Adresse externe du broker) et comment s'appelle la "boîte aux lettres"
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'iot_sensors_raw'
DATA_DIR = '/home/abdeldpro/cours/Esther_brief/Analyse_flux_data_kafka/data/input'
SLEEP_TIME_SECONDS = 0.5 

# Je transforme mes dictionnaires Python en texte JSON, puis en octets: UTF-8 (format universel pour que Kafka comprenne).
def json_serializer(data):
    return json.dumps(data).encode('utf-8')
# Imagine que tu es un traducteur officiel dans une ambassade.
# Tu reçois un message codé sur un papier (le fichier .json).
# Tu le décodes pour comprendre le sens (tu en fais un dictionnaire dans ta tête).
# On te demande de le transmettre à une autre ambassade par télégraphe :
# Tu le rédiges proprement (tu le transformes en texte JSON).
# Tu appuies sur le bouton du télégraphe pour envoyer des bips électriques (tu le transformes en UTF-8/Octets).


# J'initialise ma connexion avec le serveur Kafka et j'automatise ma fonction de sérialisation
def run_producer_simulator():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=json_serializer
        )
    except Exception as e:
        print(f"ERREUR DE CONNEXION KAFKA : Est-ce que le broker est démarré sur {KAFKA_BROKER} ?")
        print(f"Détails: {e}")
        return # Arrête le script si la connexion échoue

    # Je boucle sur mes fichiers de 4 à 103
    for i in range(4, 104): 
        filename = f"sensor_data_{i:02d}.json"
        file_path = os.path.join(DATA_DIR, filename)
        
        print(f"\n--- [PRODUCTEUR] Tentative d'envoi des données de : {filename} ---")

        try:
            with open(file_path, 'r') as f:
                # Itération sur chaque ligne avec le format JSONL : Chaque ligne de mon fichier est un enregistrement JSON indépendant.
                for line in f:
                    record = json.loads(line.strip())
                    
                    # Envoi du record à Kafka. J'envoie la donnée au "quai de déchargement" (topic)
                    producer.send(KAFKA_TOPIC, value=record) # 'producer' est maintenant défini
                    print(f"   -> Envoyé à {KAFKA_TOPIC}: {record['device_id']} | {record['value']} {record['unit']}")
                    
                    # Je marque une petite pause pour simuler un débit réaliste
                    time.sleep(SLEEP_TIME_SECONDS)
                    
        except FileNotFoundError:
            print(f"ERREUR : Le fichier {file_path} est manquant. Vérifiez le chemin.")
        except json.JSONDecodeError:
            print(f"ERREUR : Une ligne dans {file_path} n'est pas un JSON valide. (JSONDecodeError)")
        except Exception as e:
            print(f"ERREUR INCONNUE lors du traitement de {filename}: {e}")

    # Je m'assure que tous les messages en attente sont bien partis avant de couper
    producer.flush() 
    print("\n--- [PRODUCTEUR] Tous les fichiers ont été traités. Arrêt du simulateur. ---")

# Instanciation : Point d'entrée du script (pour lancer la fonction)
if __name__ == '__main__':
    run_producer_simulator()