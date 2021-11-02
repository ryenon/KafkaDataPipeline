import uuid
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

#Creation topic
topic = 'dlq-lcc-p2dmy'

#Initialisation du producer
p = Producer({
    'bootstrap.servers':'pkc-lq8v7.eu-central-1.aws.confluent.cloud:9092',
    'sasl.mechanism':'PLAIN',
    'security.protocol':'SASL_SSL',
    'sasl.username':'4EBLOD2P2ZE6LJCR',
    'sasl.password':'Gh91NvbUHGWtyGGYbjf6Q2lCdyS8p1ZcNdQi4eFmdu0KuooQbll2xsZ0ajsxYljq',
})



def acked(err, msg):
    if err is not None:
        print("Echec message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produit : %s", % (str(msg)))

p.produce(topic, key="key", value="value", callback=acked)
p.poll(10)

#Initialisation du consumer
c = Consumer({
    'bootstrap.servers': 'pkc-lq8v7.eu-central-1.aws.confluent.cloud:9092',
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '4EBLOD2P2ZE6LJCR',
    'sasl.password': 'Gh91NvbUHGWtyGGYbjf6Q2lCdyS8p1ZcNdQi4eFmdu0KuooQbll2xsZ0ajsxYljq',
    'group.id': str(uuid.uuid1()),
})

running = True

def basic_consume_loop(c, topic):
    try:
        c.subscribe(topic)

        while running:
            msg = c.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] fin offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
                msg_count +1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    c.commit(asynchronous=False)
    finally:
        c.close()


