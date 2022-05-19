#!/usr/bin/env python

import funciones as f

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Iniciar Kafka
    consumer = f.getKafkaConsumer()

    # Iniciar mongo
    client = f.getMongoClient()
    database = f.getDb(client)
    collection = f.getCollection(database)

    # Se queda en bucle esperando por mensajes
    f.consumeMessages(consumer, collection)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
