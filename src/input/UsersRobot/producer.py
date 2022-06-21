#!/usr/bin/env python

import sys

sys.path.append('/home/federico/git/Cryptofilm/src')

import funcionesRobot as fr
from core import funcionesKafka as fk

import json

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    nusers = 100

    producer = fk.getKafkaProducer()

    # Cargar los pandas
    pdNombresMasc = fr.loadPandaFrom("../../data/nombres_por_edad_media_hombres.csv", "Orden")
    pdNombresFem = fr.loadPandaFrom("../../data/nombres_por_edad_media_mujeres.csv", "Orden")
    pdApellidos01 = fr.loadPandaFrom("../../data/apellidos_frecuencia01.csv", "Orden")
    pdApellidos02 = fr.loadPandaFrom("../../data/apellidos_frecuencia02.csv", "Orden")
    pdEmails = fr.loadPandaFrom("../../data/emails.csv", "Orden")

    # Unir pdApellidos01 y pdApellidos02
    pdApellidos = fr.concatFrames([pdApellidos01, pdApellidos02])

    usuarios = fr.generateUsers(nusers, pdNombresMasc, pdNombresFem, pdApellidos, pdEmails)

    for user in usuarios:
        fr.sendToKafka(producer, user)
        print("Enviado: " + str(user))

    # with open("../../data/users.json", 'w', newline='', encoding='UTF-8') as outStream:
    #    json.dump(usuarios, outStream, ensure_ascii=False, indent=2)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
