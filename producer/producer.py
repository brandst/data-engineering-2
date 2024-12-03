from kafka import KafkaProducer
import json

def kafka_python_producer_sync(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8'))
    print("Sending " + msg)
    producer.flush(timeout=60)


def success(metadata):
    print(metadata.topic)


def error(exception):
    print(exception)


def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8')).add_callback(success).add_errback(error)
    producer.flush()


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='34.123.5.62:9092')  # use your VM's external IP Here!
    # value_serializer = lambda v: json.dumps(v).encode('utf-8')
    path = "C:\Users\thier\OneDrive\Documenten\JADS\Year 2\Data Engineering\data-engineering-2\data\imdb_Movie_Reviews.json"
    with open(path) as f:   # change this path to the path in your laptop
        json_data = json.load(f)

    # Process each JSON object in the file
    for data in json_data:
        print('Is this a JSON object? ', data)
        kafka_python_producer_sync(producer, json.dumps(data), 'movie_reviews')
    f.close()
