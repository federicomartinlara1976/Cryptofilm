import funcionesUser as fu
import funcionesFilmin as fm
import vars as v

def insertMovieList (db, movielist):
    #inserto el movielist
    existe = db[v.collectionMoviesList].count_documents({userString: movielist[v.userString], titleString: movielist[v.titleString], yearString: movielist[v.yearString]})
    ##para comprobar si existe ya el follow antes de insertar
    if existe > 0:
        print("La pelicula " + movielist[v.titleString] +" ya está en la lista de " + movielist[v.userString])
    else:
        db[v.collectionMoviesList].insert_one(movielist)


def generateMovieList(db, user, movie):
    if user is None:
        user = fu.getSampleUser(db)
        usermail = user.get(correoString)
        print("user " + usermail)
    else:
        userObject = user
        usermail = userObject.get(correoString)
        print("user " + str(usermail))

    if movie is None:
        movie = fm.getSampleMovie(db)
        movietitle = movie.get(v.titleString)
        print("movie " + movietitle)
    else:
        movietitle = movie.get(v.titleString)
        print("movie " + movietitle)

    # Genero el movielist
    movielist = {}
    movielist[userString] = user[v.correoString]
    movielist[titleString] = movie[v.titleString]
    movielist[yearString] = movie[v.yearString]

    return movielist


def generateMovieListView(view):
    # Genero el movielist
    movielist = {}
    movielist[userString] = view[v.userString]
    movielist[titleString] = view[v.titleString]
    movielist[yearString] = view[v.yearString]

    return movielist


def deleteMovieList(db, movielist):
    usermail = movielist[v.userString]
    title = movielist[v.titleString]
    year = movielist[v.yearString]

    moviesonlist = db[v.collectionMoviesList].count_documents(
        {v.userString: usermail, v.titleString: title, v.yearString: year})
    if moviesonlist > 0:
        resultado = db[v.collectionMoviesList].delete_one({v.userString: usermail, v.titleString: title, v.yearString: year})
        deletedDocs = resultado.deleted_count
        print(str(deletedDocs))
        print("Delete done.")
    else:
        print("La pelicula  " + title + " no está en la lista de  " + usermail)


def deleteMovieListForDeleteUser (db, user):

    usermail = user[v.correoString]
    resultado = db[v.collectionMoviesList].delete_many( {v.userString: { "$eq": usermail } } )
    deletedDocs = resultado.deleted_count
    print("User moviesList deleted: " + str(deletedDocs))