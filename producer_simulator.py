import time
import json
import os
from kafka import KafkaProducer

# Configuration (Adresse externe du broker)
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'iot_sensors_raw'

# Chemin du REPERTOIRE contenant les fichiers (modifié)
DATA_DIR = '/home/abdeldpro/cours/Esther_brief/Analyse_flux_data_kafka/data/input'
SLEEP_TIME_SECONDS = 0.5 

def json_serializer(data):
    """Serialize le dictionnaire Python en JSON encodé en UTF-8."""
    return json.dumps(data).encode('utf-8')

# Définition de la fonction principale du producteur
def run_producer_simulator():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=json_serializer
        )
    except Exception as e:
        print(f"!!! ERREUR DE CONNEXION KAFKA : Assurez-vous que le broker est démarré sur {KAFKA_BROKER}")
        print(f"Détails: {e}")
        return # Arrête le script si la connexion échoue

    # Boucle de traitement des fichiers (index 4 à 103)
    for i in range(4, 104): 
        filename = f"sensor_data_{i:02d}.json"
        file_path = os.path.join(DATA_DIR, filename)
        
        print(f"\n--- [PRODUCTEUR] Tentative d'envoi des données de : {filename} ---")

        try:
            with open(file_path, 'r') as f:
                # Itération sur chaque ligne (format JSONL)
                for line in f:
                    record = json.loads(line.strip())
                    
                    # Envoi du record à Kafka
                    producer.send(KAFKA_TOPIC, value=record) # 'producer' est maintenant défini
                    print(f"   -> Envoyé à {KAFKA_TOPIC}: {record['device_id']} | {record['value']} {record['unit']}")
                    
                    time.sleep(SLEEP_TIME_SECONDS)
                    
        except FileNotFoundError:
            print(f"   !!! ERREUR : Le fichier {file_path} est manquant. Vérifiez le chemin.")
        except json.JSONDecodeError:
            print(f"   !!! ERREUR : Une ligne dans {file_path} n'est pas un JSON valide. (JSONDecodeError)")
        except Exception as e:
            print(f"   !!! ERREUR INCONNUE lors du traitement de {filename}: {e}")

    # Forcer l'envoi des messages restants dans le buffer
    producer.flush() 
    print("\n--- [PRODUCTEUR] Tous les fichiers ont été traités. Arrêt du simulateur. ---")

# Point d'entrée du script (pour lancer la fonction)
if __name__ == '__main__':
    run_producer_simulator()