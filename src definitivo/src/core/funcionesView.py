from core import vars as v
from core import utils as f
from core import funcionesUser as fu
from core import funcionesFilmin as ff
from core import funcionesLike as fl


def generateView(db, user):
    if user is None:
        userObject = fu.getSampleUser(db)
    else:
        userObject = user

    movieObject = ff.getSampleMovie(db)
    # print(str(movieObject))

    # Recogemos los datos de la película
    movieObject = db[v.collectionMovies].find_one({v.titleString: movieObject.get(v.titleString)})

    view = {}

    view[v.userString] = userObject.get(v.correoString)
    view[v.titleString] = movieObject.get(v.titleString)
    view[v.yearString] = movieObject.get(v.yearString)
    view[v.timestampString] = f.getCurrentTimestamp()

    runtime = movieObject.get(v.runtimeString)
    viewingtime = f.getRandomInt(int(ff.cleanRuntime(runtime) * 0.15), int(ff.cleanRuntime(runtime)))

    if viewingtime > ff.cleanRuntime(runtime) * 0.8:
        completed = True
    else:
        completed = False

    view[v.completedString] = completed
    view[v.viewingtimeString] = viewingtime

    if completed:
        view[v.scoreString] = f.getRandomInt(0, 10)

    return view


# Insertar en vista y si le ha gustado inserto en peliculas que gustan
def insertView(db, view):

    db[v.collectionViews].insert_one(view)

    score = view.get(v.scoreString)

    if score :
        ff.updateRatingFilm(db,view)
        if score >= 7:
            like = fl.generateLike(view)
            db[v.collectionLikes].insert_one(like)
