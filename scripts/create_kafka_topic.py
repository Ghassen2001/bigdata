# scripts/create_kafka_topic.py

from kafka.admin import KafkaAdminClient, NewTopic

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"  # Utilisé dans Docker
TOPIC_NAME = "csv-data-topic"
NUM_PARTITIONS = 1
REPLICATION_FACTOR = 1

def create_topic():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='create-topic-client'
        )

        topic = NewTopic(
            name=TOPIC_NAME,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR
        )

        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"✅ Topic '{TOPIC_NAME}' créé avec succès.")
    except Exception as e:
        import traceback
        print(f"⚠️ Erreur lors de la création du topic : {e}")
        traceback.print_exc()

if __name__ == "__main__":
    create_topic()
