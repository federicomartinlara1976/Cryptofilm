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