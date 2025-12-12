import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

from utils.utils import Utils


class KafkaProducerClass:
    topic = 'derco'
    hosts = [host for host in str(Utils.load_properties()['KAFKA_SERVERS']).split(",")]
    client_id = None

    def __init__(self, _client_id=None):
        if _client_id:
            self.client_id = _client_id
        else:
            self.client_id = 'kafka_producer_default'
        self.producer = self.init_producer()

    def init_producer(self):
        producer = None
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.hosts,
                client_id=self.client_id,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                retries=3,
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432,
                compression_type='gzip'
            )
        except Exception as e:
            print(f"Error initializing Kafka producer: {str(e)}")
            return None
        print(f'Kafka producer initialized with client_id: {self.client_id}')
        return producer

    def send_message(self, message_dict):
        """
        Envoie un message dictionnaire au topic 'derco'
        
        Args:
            message_dict (dict): Dictionnaire à envoyer
            
        Returns:
            bool: True si envoyé avec succès, False sinon
        """
        if not isinstance(message_dict, dict):
            print(f"Error: message must be a dictionary, got {type(message_dict)}")
            return False
            
        if not self.producer:
            print("Error: Producer not initialized")
            return False
            
        try:
            # Envoi asynchrone du message
            future = self.producer.send(self.topic, value=message_dict)
            
            # On peut attendre la confirmation si nécessaire, mais pour la performance
            # on peut laisser l'envoi asynchrone
            # record_metadata = future.get(timeout=10)
            # print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition}")
            
            print(f"Message sent to topic '{self.topic}' successfully")
            return True
            
        except KafkaError as e:
            print(f"Error sending message to Kafka: {str(e)}")
            return False
        except Exception as e:
            print(f"Unexpected error when sending message: {str(e)}")
            return False

    def send_batch_messages(self, messages_list):
        """
        Envoie une liste de messages dictionnaires au topic 'derco'
        
        Args:
            messages_list (list): Liste de dictionnaires à envoyer
            
        Returns:
            tuple: (success_count, failed_count)
        """
        if not isinstance(messages_list, list):
            print(f"Error: messages_list must be a list, got {type(messages_list)}")
            return (0, 0)
            
        success_count = 0
        failed_count = 0
        
        for message in messages_list:
            if self.send_message(message):
                success_count += 1
            else:
                failed_count += 1
                
        print(f"Batch send completed: {success_count} success, {failed_count} failed")
        return (success_count, failed_count)

    def flush(self):
        """
        Force l'envoi de tous les messages en attente
        """
        if self.producer:
            try:
                self.producer.flush()
                print("All pending messages flushed successfully")
            except Exception as e:
                print(f"Error flushing producer: {str(e)}")

    def close(self):
        """
        Ferme proprement le producer
        """
        if self.producer:
            try:
                self.flush()  # Flush avant de fermer
                self.producer.close()
                print("Kafka producer closed successfully")
            except Exception as e:
                print(f"Error closing producer: {str(e)}")

    def __enter__(self):
        """Support du context manager"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Support du context manager"""
        self.close()