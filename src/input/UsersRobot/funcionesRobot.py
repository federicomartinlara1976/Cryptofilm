# Funciones
import pandas as pd
import random

from calendar import isleap

from core import cfg as c
from core import vars as v
from core import utils as f


def loadPandaFrom(file_csv, indexCol):
    return pd.read_csv(file_csv, index_col=indexCol)


def getGenderMasc(limit, crit):
    val = f.getRandomInt(1, limit)

    if val > crit:
        return True
    else:
        return False


def generatePostalCode():
    format = v.emptyString
    num = f.getRandomInt(0, 99999)
    if num == 0:
        format = '00000'
    elif num < 10:
        format = '0000{num}'
    elif 10 <= num < 100:
        format = '000{num}'
    elif 100 <= num < 1000:
        format = '00{num}'
    elif 1000 <= num < 10000:
        format = '0{num}'
    else:
        format = '{num}'

    return format.format(num=str(num))


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
    limit = f.getRandomInt(1, 6)
    numbers = v.emptyString
    for i in range(limit):
        numbers += str(f.getRandomInt(0, 9))

    # Concatenar el mail para tener la cuenta
    dfEmail = getSampleFromDataframe(pdEmails)
    domain = getDataString(dfEmail["email"])

    return email + numbers + '@' + domain


def generateFechaNacimiento():
    minYear = 1930
    maxYear = 2010

    year = f.getRandomInt(minYear, maxYear)
    mes = f.getRandomInt(1, 12)
    dia = f.getRandomInt(1, diasMes(mes, year))

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


def generateUser(nombre, apellido_1, apellido_2, sexo, fechaNacimiento, email, postal):
    usuario = {}
    usuario[v.nombreString] = nombre
    usuario[v.apellidosString] = apellido_1 + ' ' + apellido_2
    usuario[v.sexoString] = sexo
    usuario[v.nacimientoString] = fechaNacimiento
    usuario[v.correoString] = email
    usuario[v.postalString] = postal

    return usuario


def generateUsers(nusers, nombresMasc, nombresFem, apellidos, emails):
    usuarios = []

    for u in range(nusers):

        pdNombre = None
        sexo = v.emptyString

        # Coger un género y decidir el panda de los nombres según el género
        esMasc = getGenderMasc(100, 50)
        if esMasc:
            sexo = "hombre"
            pdNombre = nombresMasc
        else:
            sexo = "mujer"
            pdNombre = nombresFem

        # Coger un nombre
        rNombre = getSampleFromDataframe(pdNombre)
        nombre = getDataString(rNombre["Nombre"])

        # Coger dos apellidos
        dfApellido_1 = getSampleFromDataframe(apellidos)
        apellido_1 = getDataString(dfApellido_1["Apellido"])

        dfApellido_2 = getSampleFromDataframe(apellidos)
        apellido_2 = getDataString(dfApellido_2["Apellido"])

        # Generar el email
        email = generateEmail([nombre, apellido_1, apellido_2], emails)

        fechaNacimiento = generateFechaNacimiento()
        postal = generatePostalCode()

        usuario = generateUser(nombre, apellido_1, apellido_2, sexo, fechaNacimiento, email, postal)
        usuarios.append(usuario)

    return usuarios


