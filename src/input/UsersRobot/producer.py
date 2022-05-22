#!/usr/bin/env python

import funciones as f

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    users = 10

    # Cargar los pandas
    pdNombresMasc = f.loadPandaFrom("nombres_por_edad_media_hombres.csv", "Orden")
    pdNombresFem = f.loadPandaFrom("nombres_por_edad_media_mujeres.csv", "Orden")
    pdApellidos01 = f.loadPandaFrom("apellidos_frecuencia01.csv", "Orden")
    pdApellidos02 = f.loadPandaFrom("apellidos_frecuencia02.csv", "Orden")
    pdEmails = f.loadPandaFrom("emails.csv", "Orden")

    # Unir pdApellidos01 y pdApellidos02
    pdApellidos = f.concatFrames([pdApellidos01, pdApellidos02])

    for u in range(users):

        pdNombre = None
        sexo = ""

        # Coger un género y decidir el panda de los nombres según el género
        esMasc = f.getGenderMasc(100, 50)
        if esMasc:
            sexo = "hombre"
            pdNombre = pdNombresMasc
        else:
            sexo = "mujer"
            pdNombre = pdNombresFem

        # Coger un nombre
        rNombre = f.getSampleFromDataframe(pdNombre)
        nombre = f.getDataString(rNombre["Nombre"])

        # Coger dos apellidos
        dfApellido_1 = f.getSampleFromDataframe(pdApellidos)
        apellido_1 = f.getDataString(dfApellido_1["Apellido"])

        dfApellido_2 = f.getSampleFromDataframe(pdApellidos)
        apellido_2 = f.getDataString(dfApellido_2["Apellido"])

        # Generar el email
        email = f.generateEmail([nombre, apellido_1, apellido_2], pdEmails)

        fechaNacimiento = f.generarFechaNacimiento()

        f.createUser(nombre, apellido_1, apellido_2, fechaNacimiento, sexo, email)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
