from datetime import datetime

from core import funcionesFilmin as ff
from core import funcionesUser as fu
from core import funcionesView as fv
from core import funcionesFollow as fo
from core import vars as v

import json

# A cada procedimiento se le pasa promero el argumento db
# para que se sepa que opera sobre una base de datos mongo


# Procedimiento para insertar películas.
def insertFilms(db, file):
    with open(file, encoding='utf-8') as inStream:
        data = json.load(inStream)

        readMovie = {}

        insertCount = 0
        totalCount = 0

        dataLen = len(data)
        for i in range(0, dataLen):
            readMovie = data[i]
            insertMovie = {}

            insertMovie[v.titleString] = readMovie[v.titleString]
            insertMovie[v.ratingString] = readMovie[v.ratingString]
            insertMovie[v.yearString] = readMovie[v.yearString]
            insertMovie[v.usersratingString] = readMovie[v.usersratingString]
            insertMovie[v.votesString] = readMovie[v.votesString]
            insertMovie[v.metascoreString] = readMovie[v.metascoreString]
            insertMovie[v.countriesString] = readMovie[v.countriesString]
            insertMovie[v.languagesString] = readMovie[v.languagesString]
            insertMovie[v.actorsString] = readMovie[v.actorsString]
            insertMovie[v.genresString] = readMovie[v.genresString]
            insertMovie[v.descriptionString] = readMovie[v.descriptionString]
            directos = readMovie.get(v.directorsString)

            if directos:
                insertMovie[v.directorsString] = readMovie[v.directorsString]
            else:
                insertMovie[v.directorsString] = None
            insertMovie[v.runtimeString] = readMovie[v.runtimeString]

            print("Creada " + insertMovie[v.titleString] + " " + str(totalCount))
            totalCount += 1

            if ff.validateFilm(insertMovie):
                db[v.collectionMovies].insert_one(insertMovie)
                print("Insertada " + insertMovie[v.titleString] + " " + str(insertCount))
                insertCount += 1
            else:
                print("No Insertada " + insertMovie[v.titleString])

    print("Peliculas analizadas: " + str(totalCount) + "/" + str(insertCount) + " movies written.")


# Procedimiento para añadir campo codirector.
def director(db):
    movieObjects = db[v.collectionMovies].find({})

    # Recorremos el resultado
    for movie in movieObjects:

        if movie is not None:

            directors = movie[v.directorsString]

            if directors is not None:

                director = directors[0]
                print(director)

                if len(directors) > 1:
                    codirector = directors[1]
                else:
                    codirector = None
                print(codirector)

                title = movie[v.titleString]
                year = movie[v.yearString]

                print(title)

                output = db[v.collectionMovies].update_one({v.titleString: title, v.yearString: year},
                                                           {'$set': {v.directorString: director,
                                                                     v.codirectorString: codirector}})


# Procedimiento para borrar los directores
def deleteDirectors(db):
    output = db[v.collectionMovies].updateMany({}, {
        "$unset": {v.idString: v.emptyString, v.directorsString: v.emptyString}})


# Procedimiento para limpiar los votos
def cleanVotes(db):
    movieObjects = db[v.collectionMovies].find({})

    for movie in movieObjects:
        # Recorremos el resultado
        title = movie.get(v.titleString)
        print("Tratando película " + title)

        idValue = movie.get(v.idString)
        votes = movie.get(v.votesString)

        if votes != None and isinstance(votes, str):
            votes = int(votes.replace(',', ''))
        elif votes == None:
            votes = 0

        output = db[v.collectionMovies].update_many({v.idString: idValue}, {'$set': {v.votesString: votes}})


# Procedimiento para actualizar los géneros
def genre(db):
    movieObjects = db[v.collectionMovies].find({})

    # Recorremos el resultado
    for movie in movieObjects:

        if movie is not None:

            genres = movie[v.genresString]

            if genres is not None:

                genre = genres[0]
                print(genre)

                if len(genres) > 1:
                    cogenre = genres[1]
                else:
                    cogenre = None
                print(cogenre)

                title = movie[v.titleString]
                year = movie[v.yearString]

                print(title)

                output = db[v.collectionMovies].update_one({v.titleString: title, v.yearString: year},
                                                           {'$set': {v.genreString: genre,
                                                                     v.cogenreString: cogenre}})


# Procedimiento para borrar los géneros
def deleteGenres(db):
    output = db[v.collectionMovies].updateMany({},
                                               {"$unset": {v.idString: v.emptyString, v.genresString: v.emptyString}})


# Procedimiento para insertar los actores
def actor(db):
    movieObjects = db[v.collectionMovies].find({})

    # Recorremos el resultado
    for movie in movieObjects:

        if movie is not None:

            actors = movie[v.actorsString]

            if actors is not None:

                mainactor = actors[0]
                print(mainactor)

                if len(actors) > 1:
                    secactor = actors[1]
                else:
                    cogenre = None
                print(secactor)

                title = movie[v.titleString]
                year = movie[v.yearString]

                print(title)

                output = db[v.collectionMovies].update_one({v.titleString: title, v.yearString: year},
                                                           {'$set': {v.mainactorString: mainactor,
                                                                     v.secactorString: secactor}})


# Procedimiento para borrar los actores
def deleteActors(db):
    output = db[v.collectionMovies].updateMany({},
                                               {"$unset": {v.idString: v.emptyString, v.actorsString: v.emptyString}})


# Procedimiento para insertar los lenguajes
def languages(db):
    movieObjects = db[v.collectionMovies].find({})

    # Recorremos el resultado
    for movie in movieObjects:

        if movie is not None:

            languages = movie[v.languagesString]

            if languages is not None:

                vose = languages[0]
                print(vose)

                if len(languages) > 1:
                    seclanguage = languages[1]
                else:
                    seclanguage = None
                print(seclanguage)

                title = movie[v.titleString]
                year = movie[v.yearString]

                print(title)

                output = db[v.collectionMovies].update_one({v.titleString: title, v.yearString: year},
                                                           {'$set': {v.voseString: vose,
                                                                     v.seclanguageString: seclanguage}})


# Procedimiento que borra los lenguajes
def deleteLanguages(db):
    output = db[v.collectionMovies].updateMany({}, {
        "$unset": {v.idString: v.emptyString, v.languagesString: v.emptyString}})


# Procedimiento para insertar los países
def countries(db):
    movieObjects = db[v.collectionMovies].find({})

    # Recorremos el resultado
    for movie in movieObjects:

        if movie is not None:

            countries = movie[v.countriesString]

            if countries is not None:

                country = countries[0]
                print(country)

                if len(countries) > 1:
                    secountry = countries[1]
                else:
                    secountry = None
                print(secountry)

                title = movie[v.titleString]
                year = movie[v.yearString]

                print(title)

                output = db[v.collectionMovies].update_one({v.titleString: title, v.yearString: year},
                                                           {'$set': {v.countryString: country,
                                                                     v.seccountryString: secountry}})


# Procedimiento para borrar los países
def deleteCountries(db):
    output = db[v.collectionMovies].updateMany({}, {
        "$unset": {v.idString: v.emptyString, v.countriesString: v.emptyString}})


# Procedimiento que inserta usuarios
def insertUsers(db, file):
    with open(file, encoding='utf-8') as inStream:
        data = json.load(inStream)
        print(str(datetime.today()))
        readUser = {}
        writedUser = {}

        insertCount = 0
        totalCount = 0

        dataLen = len(data)
        for i in range(0, dataLen):
            readUser = data[i]

            writedUser[v.nombreString] = readUser[v.nombreString]
            writedUser[v.apellidosString] = readUser[v.apellidosString]
            writedUser[v.correoString] = readUser[v.correoString]
            writedUser[v.nacimientoString] = datetime.fromisoformat(str(readUser[v.nacimientoString]))
            writedUser[v.postalString] = readUser[v.postalString]
            correo = readUser.get(v.correoString)

            if correo:
                writedUser[v.correoString] = readUser[v.correoString]
            else:
                writedUser[v.correoString] = None

            print("Creada " + writedUser[v.nombreString] + " " + str(totalCount))
            totalCount += 1

            if fu.validateUser(writedUser):
                db[v.collectionUsers].insert_one(writedUser)
                print("Insertada " + writedUser[v.correoString] + " " + str(insertCount))
                insertCount += 1
            else:
                print("No Insertada " + writedUser[v.correoString])

        print("Users analizadas: " + str(totalCount) + "/" + str(insertCount) + " users written.")


# Creación de los campos idioma y director
def idiomaypais(db):
    movieObjects = db[v.collectionMovies].find({})

    counter = 0

    # Recorremos el resultado
    for movie in movieObjects:

        if movie is not None:

            countries = movie[v.countriesField]
            languages = movie[v.languagesField]
            title = movie[v.titleField]
            year = movie[v.yearField]

            if countries is not None:
                country = countries[0]
                print(country)

                output = db[v.collectionMovies].update_one({v.titleField: title, v.yearField: year},
                                                           {'$set': {v.countryField: country}})

            if languages is not None:
                language = languages[0]
                print(language)
                output = db[v.collectionMovies].update_one({v.titleField: title, v.yearField: year},
                                                           {'$set': {v.languageField: language}})


def generateMassiveViews(db, nviews):
    for i in range(nviews):
        view = fv.generateView(None)
        fv.insertView(db, view)


def generateMassiveFollows(db, nfollows):
    for i in range(nfollows):
        follow = fo.generateFollow(db, None, None)
        fo.insertFollow(db, follow)