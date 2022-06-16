# Funciones
import pandas as pd
import random

from calendar import isleap

from core import cfg as c
from core import vars as v


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


def getPostalCode():
    num = getRandomInt(0, 99999)
    if num == 0:
        return "00000"
    elif num < 10:
        return "0000" + str(num)
    elif 10 <= num < 100:
        return "000" + str(num)
    elif 100 <= num < 1000:
        return "00" + str(num)
    elif 1000 <= num < 10000:
        return "0" + str(num)
    else:
        return str(num)


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

    sMes = v.emptyString
    if mes > 9:
        sMes = str(mes)
    else:
        sMes = "0" + str(mes)

    sDia = v.emptyString
    if dia > 9:
        sDia = str(dia)
    else:
        sDia = "0" + str(dia)

    return sYear + "-" + sMes + "-" + sDia


def concatFrames(frames):
    return pd.concat(frames)


def diasMes(mes, anio):
    if mes in [1, 3, 5, 7, 8, 10, 12]:
        return 31
    elif mes in [4, 6, 9, 11]:
        return 30
    else:
        if isleap(anio):
            return 29
        else:
            return 28


def getUser(nombre, apellido_1, apellido_2, sexo, fechaNacimiento, email, postal):
    usuario = {}
    usuario[v.nombreString] = nombre
    usuario[v.apellidosString] = apellido_1 + ' ' + apellido_2
    usuario[v.sexoString] = sexo
    usuario[v.nacimientoString] = fechaNacimiento
    usuario[v.correoString] = email
    usuario[v.postalString] = postal

    return usuario


