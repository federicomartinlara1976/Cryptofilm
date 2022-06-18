# Funciones
from json import dumps
from json import loads
from kafka import KafkaProducer
from kafka import KafkaConsumer

from core import cfg as c


def getKafkaProducer():
    return KafkaProducer(bootstrap_servers=[c.cfg['kafka.server']],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))


def getKafkaConsumer():
    return KafkaConsumer(cfg.cfg['kafka.topic'],
                         bootstrap_servers=[c.cfg['kafka.server']],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='my-group',
                         value_deserializer=lambda x: loads(x.decode('utf-8')))


def sendToKafka(producer, user):
    producer.send(c.cfg['kafka.topic'], value=user)
