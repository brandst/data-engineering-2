from kafka import KafkaProducer
import json
from tqdm import tqdm  # Import tqdm for progress tracking

def kafka_python_producer_sync(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8'))
    # print("Sending " + msg)
    producer.flush(timeout=60)

def success(metadata):
    print(metadata.topic)

def error(exception):
    print(exception)

def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8')).add_callback(success).add_errback(error)
    producer.flush()

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='34.31.254.144:9092')  # use your VM's external IP Here!
    # value_serializer = lambda v: json.dumps(v).encode('utf-8')
    path = "C:/Users/thier/OneDrive/Documenten/JADS/Year 2/Data Engineering/data-engineering-2/data/imdb_Movie_Reviews_with_Overall_Rating.json"
    
    with open(path) as f:  # change this path to the path on your laptop
        json_data = json.load(f)

    # Process each JSON object in the file with a tqdm progress bar
    for data in tqdm(json_data, desc="Sending data to Kafka"):
        kafka_python_producer_sync(producer, json.dumps(data), 'movie_reviews')
    f.close()
