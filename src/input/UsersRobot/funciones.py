# Funciones
import pandas as pd
import random

from calendar import isleap

from core import funcionesKafka as fk
from core import funcionesMongo as fm


def loadPandaFrom(file_csv, indexCol):
    return pd.read_csv(file_csv, index_col=indexCol)


def getRandomInt(min, max):
    return random.randint(min, max)


def getGenderMasc(limit, crit):
    val = getRandomInt(1, limit)

    if val > crit:
        return True
    else:
        return False


def getSampleFromDataframe(dataframe):
    return dataframe.sample()


def getDataString(serie):
    return serie.values.tolist()[0]


def generateEmail(lista, pdEmails):
    nombre = lista[0].lower()
    apellido_1 = lista[1].lower()
    apellido_2 = lista[2].lower()

    # Sacar las tres primeras letras del nombre
    token_1 = nombre[:3].rstrip()

    # Sacar las tres primeras letras de cada apellido
    token_2 = apellido_1[:3].rstrip()
    token_3 = apellido_2[:3].rstrip()

    email = token_1 + token_2 + token_3

    # Escoger números aleatorios (0-9), n veces (1 < n < 6)
    limit = getRandomInt(1, 6)
    numbers = ""
    for i in range(limit):
        numbers += str(getRandomInt(0, 9))

    # Concatenar el mail para tener la cuenta
    dfEmail = getSampleFromDataframe(pdEmails)
    domain = getDataString(dfEmail["email"])

    return email + numbers + '@' + domain


def generarFechaNacimiento():
    minYear = 1930
    maxYear = 2010

    year = getRandomInt(minYear, maxYear)
    mes = getRandomInt(1, 12)
    dia = getRandomInt(1, diasMes(mes, year))

    sYear = str(year)

    sMes = ""
    if mes > 9:
        sMes = str(mes)
    else:
        sMes = "0" + str(mes)

    sDia = ""
    if dia > 9:
        sDia = str(dia)
    else:
        sDia = "0" + str(dia)

    return sDia + "-" + sMes + "-" + sYear


def concatFrames(frames):
    return pd.concat(frames)


def diasMes(mes, anio):
    if mes == 1 or mes == 3 or mes == 5 or mes == 7 or mes == 8 or mes == 10 or mes == 12:
        return 31
    elif mes == 4 or mes == 6 or mes == 9 or mes == 11:
        return 30
    else:
        if isleap(anio):
            return 29
        else:
            return 28


def createUser(nombre, apellido_1, apellido_2, sexo, fechaNacimiento, email):
    usuario = {}
    usuario['nombre'] = nombre
    usuario['apellidos'] = apellido_1 + ' ' + apellido_2
    usuario['sexo'] = sexo
    usuario['fechaNacimiento'] = fechaNacimiento
    usuario['email'] = email

    # Iniciar Kafka
    producer = fk.getKafkaProducer()

    # Enviarlo a Kafka
    fk.sendToKafka(producer, usuario)
    print("Enviado: " + str(usuario))


def insertUsersFromKafka():
    # Iniciar Kafka
    consumer = fk.getKafkaConsumer()

    # Iniciar mongo
    client = fm.getMongoClient()
    database = fm.getDb(client)
    collection = fm.getCollection(database)

    for message in consumer:
        message = message.value
        collection.insert_one(message)

