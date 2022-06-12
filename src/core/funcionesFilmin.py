import utils as f
import vars as v


def validateFilm(film):
    if f.isNone(film[v.titleString]) or f.isEmptyString(film[v.titleString]):
        print("Título incorrecto " + film[id])
        return False

    if f.isNone(film[v.ratingString]) or f.isEmptyString(film[v.ratingString]):
        print("Rating incorrecto " + film[v.titleString])
        return False

    if f.isNone(film[v.yearString]) or f.isEmptyString(film[v.yearString]):
        print("Year incorrecto " + film[v.titleString])
        return False

    if f.isNone(film[v.usersratingString]) or f.isEmptyString(film[v.usersratingString]):
        print("RatingUser incorrecto " + film[v.titleString])
        return False

    if f.isNone(film[v.votesString]) or f.isEmptyString(film[v.votesString]):
        print("Votesuser incorrecto " + film[v.titleString])
        return False

    if f.isNone(film[v.metascoreString]) or f.isEmptyString(film[v.metascoreString]):
        print("Metascore incorrecto " + film[v.titleString])
        return False

    if f.isNone(film[v.descriptionString]) or f.isEmptyString(film[v.descriptionString]):
        print("Description incorrecto " + film[v.titleString])
        return False

    if f.isNone(film[v.runtimeString]) or f.isEmptyString(film[v.runtimeString]):
        print("Runtime incorrecto " + film[v.titleString])
        return False

    if f.isNone(film[v.directorsString]) or f.isEmptylist(film[v.directorsString]):
        print("Directors vacio " + film[v.directorsString])
        return False

    if f.isNone(film[v.countriesString]) or f.isEmptylist(film[v.countriesString]):
        print("Countries vacio " + film[v.titleString])
        return False

    if f.isNone(film[v.languagesString]) or f.isEmptylist(film[v.languagesString]):
        print("Languages vacio " + film[v.titleString])
        return False

    if f.isNone(film[v.actorsString]) or f.isEmptylist(film[v.actorsString]):
        print("Actors vacio " + film[v.titleString])
        return False

    if f.isNone(film[v.genresString]) or f.isEmptylist(film[v.genresString]):
        print("Genres vacio " + film[v.titleString])
        return False

    return True


# Escoge una película de forma aleatoria
def getSampleMovie(db):
    # Usamos el comando aggregate para lanzar un aggregation pipeline de Mongodb
    resultadoAgg = db[v.collectionMovies].aggregate([
        {"$sample": {"size": 1}}
    ])

    # Recorremos el resultado
    for movie in resultadoAgg:
        # print(movie)
        return movie


def cleanRuntime(runtime):
    if runtime is not None and isinstance(runtime, str):
        runtime = int(runtime.replace(" min", ''))

    return runtime


def updateRatingFilm(db, view):
    title = view[v.titleString]
    year = view[v.yearString]

    movieObject = db[v.collectionMovies].find_one({v.titleString: title, v.yearString: year})

    newRating = 0
    newVotes = 0
    newScore = int(view[v.scoreString])
    totalVotes = int(movieObject[v.votesString])
    totalRating = float(movieObject[v.usersratingString])

    newVotes = totalVotes + 1
    newRating = ((totalRating * totalVotes) + newScore) / newVotes

    print("Old votes: " + str(totalVotes))
    print("New votes: " + str(newVotes))
    print("Old users rating: " + str(totalRating))
    print("New users rating: " + str(newRating))
    output = db[v.collectionMovies].update_one({v.titleString: title},
                                             {'$set': {v.usersratingString: newRating, v.votesString: newVotes}})

    # Pintamos los documentos modificados
    print('Documentos modificados: ' + str(output.modified_count))


def arreglaVotos(db):
    movies = db[v.collectionMovies].find({})

    for m in movies:
        print(m)

        votes = m[v.votesString]
        title = m[v.titleString]

        votes = int(votes.replace(',', ''))

        output = db[v.collectionMovies].update_one({v.titleString: title}, {'$set': {v.votesString: votes}})

        # Pintamos los documentos modificados
        print('Documentos modificados: ' + str(output.modified_count))


def insertMovieList (movielist):
    #inserto el movielist
    existe = db[v.collectionMoviesList].count_documents({userString: movielist[v.userString], titleString: movielist[v.titleString], yearString: movielist[v.yearString]})
    ##para comprobar si existe ya el follow antes de insertar
    if existe > 0:
        print("La pelicla " + movielist[v.titleString] +" ya está en la lista de " + movielist[v.userString])
    else:
        db[v.collectionMoviesList].insert_one(movielist)