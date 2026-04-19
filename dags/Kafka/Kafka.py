import os
import json
import sys



class Kafka():


    def connect(self, topic_name: str = None):
        if topic_name is None:
            print("We need the topic name")
            return
        topic_name = topic_name
        bootstrap_servers = Variable.get("KAFKA_BOOTSTRAP_SERVERS", default_var=os.getenv("KAFKA_BOOTSTRAP_SERVERS")).split(",")
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            print(f"Topic '{topic_name}' created successfully.")
        except TopicAlreadyExistsError:
            print("Topic already exists")
        except Exception as e:
            print(f"Could not verify/create topic (may not have connection): {e}")
        return

    def load_to_kafka(self, df):
        records = df.to_dict(orient='records')
        for record in records:
            producer.send(topic_name, value=record)
        producer.flush()
        print(f"Sent {len(records)} registers to Kafka ({topic_name}).")
        return

    def load_to_kafka_an_event(self, event):
        producer.send(topic_name, value=kafka_event)
        producer.flush()
        return
