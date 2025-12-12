"""
Exemple d'utilisation de la classe KafkaProducerClass

Ce fichier montre comment utiliser la classe Producer Kafka pour envoyer
des messages au topic 'derco'.
"""

from business.kafka_producer.kafka_producer import KafkaProducerClass


def example_single_message():
    """Exemple d'envoi d'un message unique"""
    print("=== Exemple message unique ===")
    
    # Création du producer
    producer = KafkaProducerClass("mon_producer_1")
    
    # Message à envoyer
    message = {
        "id": "12345",
        "nom": "Test Message",
        "timestamp": "2024-01-01T12:00:00Z",
        "donnees": {
            "temperature": 23.5,
            "humidite": 65,
            "statut": "ok"
        }
    }
    
    # Envoi du message
    success = producer.send_message(message)
    print(f"Envoi réussi: {success}")
    
    # Fermeture propre
    producer.close()


def example_batch_messages():
    """Exemple d'envoi de plusieurs messages"""
    print("\n=== Exemple messages en lot ===")
    
    # Utilisation du context manager pour une gestion automatique
    with KafkaProducerClass("mon_producer_2") as producer:
        # Liste de messages
        messages = [
            {
                "id": f"msg_{i}",
                "type": "sensor_data",
                "valeur": i * 10,
                "timestamp": "2024-01-01T12:00:00Z"
            }
            for i in range(5)
        ]
        
        # Envoi en lot
        success_count, failed_count = producer.send_batch_messages(messages)
        print(f"Résultats: {success_count} succès, {failed_count} échecs")


def example_integration_loop():
    """
    Exemple d'intégration dans une boucle d'itération
    comme demandé dans les requirements
    """
    print("\n=== Exemple intégration en boucle ===")
    
    producer = KafkaProducerClass("producer_loop")
    
    try:
        # Simulation d'une boucle de traitement de données
        data_to_send = [
            {"id": 1, "action": "create", "data": "some_data_1"},
            {"id": 2, "action": "update", "data": "some_data_2"},
            {"id": 3, "action": "delete", "data": "some_data_3"},
        ]
        
        for data in data_to_send:
            # Traitement des données...
            processed_data = {
                "original_id": data["id"],
                "processed_action": data["action"].upper(),
                "payload": data["data"],
                "processed_at": "2024-01-01T12:00:00Z"
            }
            
            # Envoi au topic 'derco'
            success = producer.send_message(processed_data)
            
            if success:
                print(f"Message ID {data['id']} envoyé avec succès")
            else:
                print(f"Échec d'envoi du message ID {data['id']}")
                
    except Exception as e:
        print(f"Erreur dans la boucle: {str(e)}")
    finally:
        producer.close()


if __name__ == "__main__":
    print("Démonstration de la classe Kafka Producer")
    print("==========================================")
    
    # Exemples d'utilisation
    example_single_message()
    example_batch_messages()
    example_integration_loop()
    
    print("\nDémonstration terminée!")