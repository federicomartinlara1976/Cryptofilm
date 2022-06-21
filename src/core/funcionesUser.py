from core import utils as f
from core import vars as v


def validateUser(user):
    if f.isNone(user[v.correoString]) or f.isEmptyString(user[v.correoString]):
        print("correo incorrecto ")
        return False

    if f.isNone(user[v.nombreString]) or f.isEmptyString(user[v.nombreString]):
        print("Nombre incorrecto " + user[v.correoString])
        return False

    if f.isNone(user[v.apellidosString]) or f.isEmptyString(user[v.apellidosString]):
        print("Apellidos incorrecto " + user[v.correoString])
        return False

    if f.isNone(user[v.nacimientoString]) or f.isDate(user[v.nacimientoString]) or f.isNotToday(
            user[v.nacimientoString]):
        print("NacimientoString incorrecto " + user[v.correoString])
        return False

    if f.isNone(user[v.postalString]) or f.isNumeric(user[v.postalString]) or f.isZipcode(int(user[v.postalString])):
        print("Codigo postal incorrecto " + user[v.correoString])
        return False

    return True


# Escoge un usuario de forma aleatoria
def getSampleUser(db):
    # Usamos el comando aggregate para lanzar un aggregation pipeline de Mongodb
    resultadoAgg = db[v.collectionUsers].aggregate([
        {"$sample": {"size": 1}}
    ])

    # Recorremos el resultado
    for user in resultadoAgg:
        # print(user)
        return user
