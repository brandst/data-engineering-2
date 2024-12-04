from kafka.admin import KafkaAdminClient, NewTopic


def delete_topics(admin):
    admin.delete_topics(topics=['movie_reviews'])


def create_topics(admin, topic_list):
    admin.create_topics(new_topics=topic_list, validate_only=False)


if __name__ == '__main__':
    admin_client = KafkaAdminClient(bootstrap_servers="34.31.254.144:9092",
                                    client_id='Assignment2')  # use your VM's external IP Here!
    delete_topics(admin_client)
    topic_list = [NewTopic(name="movie_reviews", num_partitions=1, replication_factor=1)]
    create_topics(admin_client, topic_list)
