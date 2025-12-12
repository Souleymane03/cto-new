# Kafka Producer Class

Cette classe Producer Kafka permet d'envoyer des messages au topic 'derco' en suivant les mêmes conventions que le consumer existant.

## Fonctionnalités

- ✅ Envoi de messages dictionnaires au topic 'derco'
- ✅ Sérialisation JSON automatique
- ✅ Gestion des erreurs robuste
- ✅ Utilisation des mêmes KAFKA_SERVERS que le consumer
- ✅ Support de l'envoi en lot
- ✅ Support des context managers
- ✅ Configuration optimisée pour la performance

## Utilisation

### Import
```python
from business.kafka_producer.kafka_producer import KafkaProducerClass
```

### Message unique
```python
producer = KafkaProducerClass("mon_producer")
message = {
    "id": "12345",
    "nom": "Test Message",
    "donnees": {"temperature": 23.5}
}
success = producer.send_message(message)
producer.close()
```

### Utilisation avec context manager
```python
with KafkaProducerClass("mon_producer") as producer:
    message = {"id": "1", "action": "create"}
    producer.send_message(message)
```

### Envoi en lot
```python
messages = [
    {"id": 1, "type": "sensor"},
    {"id": 2, "type": "sensor"}
]
producer = KafkaProducerClass()
success_count, failed_count = producer.send_batch_messages(messages)
producer.close()
```

### Intégration dans une boucle
```python
producer = KafkaProducerClass()

for data in data_list:
    processed_data = {
        "id": data["id"],
        "processed": True,
        "payload": data
    }
    success = producer.send_message(processed_data)

producer.close()
```

## Configuration

La classe utilise les mêmes configurations Kafka que le consumer :
- `KAFKA_SERVERS` via `Utils.load_properties()`
- Configuration optimisée : batch_size, linger_ms, compression, retries

## Fichiers

- `kafka_producer.py` : Classe principale Producer
- `__init__.py` : Module initialization
- `example_usage.py` : Exemples d'utilisation
- `README.md` : Documentation