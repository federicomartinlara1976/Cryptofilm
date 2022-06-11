from datetime import datetime
import random
import time
import calendar


# Funciones que nos van a ayudar a limpiar los datos antes de insertarlos
def isEmptyString(s):
    return s.strip() == ""


def isNumber(s):
    return s.strip().isnumeric()


def isDecimal(s):
    try:
        float(s.strip())
        return True
    except ValueError:
        return False


def isNone(s):
    if s is None:
        return True
    else:
        return False


def isReleased(s):
    if s == "Released":
        return False
    else:
        return True


def isEmptylist(s):
    if len(s) > 0:
        return False
    else:
        return True


def isZipcode(s):
    if s < 99999 or s > 10000:
        print("He pasado zipcode")
        print(s)
        return False
    else:
        print("No He pasado zipcode")
        print(s)
        return True


def isNumeric(s):
    if s.isnumeric():
        return False
    else:
        return True


def isDate(s):
    if isinstance(s, datetime):  # isinstance(s, datetime.date),type(s) is datetime.date
        return False
    else:
        print("fecha hoy " + str(datetime.date))
        print("fecha nacimiento " + s)
        return True


def isNotToday(s):
    if s < datetime.today():
        return False
    else:
        return True


def getCurrentTimestamp():
    gmt = time.gmtime()
    ts = calendar.timegm(gmt)
    return ts


def getRandomInt(min, max):
    return random.randint(min, max)
