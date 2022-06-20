#!/usr/bin/env python

import funcionesRobot as fr

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    users = 10

    # Cargar los pandas
    pdNombresMasc = fr.loadPandaFrom("../../data/nombres_por_edad_media_hombres.csv", "Orden")
    pdNombresFem = fr.loadPandaFrom("../../data/nombres_por_edad_media_mujeres.csv", "Orden")
    pdApellidos01 = fr.loadPandaFrom("../../data/apellidos_frecuencia01.csv", "Orden")
    pdApellidos02 = fr.loadPandaFrom("../../data/apellidos_frecuencia02.csv", "Orden")
    pdEmails = fr.loadPandaFrom("../../data/emails.csv", "Orden")

    # Unir pdApellidos01 y pdApellidos02
    pdApellidos = fr.concatFrames([pdApellidos01, pdApellidos02])

    for u in range(users):

        pdNombre = None
        sexo = ""

        # Coger un género y decidir el panda de los nombres según el género
        esMasc = fr.getGenderMasc(100, 50)
        if esMasc:
            sexo = "hombre"
            pdNombre = pdNombresMasc
        else:
            sexo = "mujer"
            pdNombre = pdNombresFem

        # Coger un nombre
        rNombre = fr.getSampleFromDataframe(pdNombre)
        nombre = fr.getDataString(rNombre["Nombre"])

        # Coger dos apellidos
        dfApellido_1 = fr.getSampleFromDataframe(pdApellidos)
        apellido_1 = fr.getDataString(dfApellido_1["Apellido"])

        dfApellido_2 = fr.getSampleFromDataframe(pdApellidos)
        apellido_2 = fr.getDataString(dfApellido_2["Apellido"])

        # Generar el email
        email = fr.generateEmail([nombre, apellido_1, apellido_2], pdEmails)

        fechaNacimiento = fr.generarFechaNacimiento()

        fr.createUser(nombre, apellido_1, apellido_2, fechaNacimiento, sexo, email)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
