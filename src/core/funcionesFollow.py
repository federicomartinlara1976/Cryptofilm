from core import vars as v
from core import funcionesUser as fu


# Insertar el follow
def insertFollow(db, follow):
    follows = db[v.collectionFollows].count_documents(
        {v.userString: follow[v.userString], v.followString: follow[v.followString]})
    # Para comprobar si existe ya el follow antes de insertar
    if follows > 0:
        print("El follow de " + follow[v.userString] + " a " + follow[v.followString] + " ya existe")
    else:
        db[v.collectionFollows].insert_one(follow)


# Generar un follow
def generateFollow(db, user, follow):
    print("Estoy aqui")
    continua = True
    if user is None:
        user = fu.getSampleUser(db)
        usermail = user.get(v.correoString)
        print("user " + usermail)
    else:
        userObject = user
        usermail = userObject.get(v.correoString)
        print("user " + str(usermail))
    if follow is None:
        while continua:
            follow = fu.getSampleUser(db)
            followmail = follow.get(v.correoString)
            print("follow " + followmail + " y usermail " + usermail)
            if usermail != followmail:
                continua = False
        print("follow " + followmail)
    else:
        followmail = follow.get(v.correoString)
        if usermail != followmail:
            continua = True
            while follow is None or continua:
                follow = fu.getSampleUser(db)
                followmail = follow.get(v.correoString)
                print("follow " + followmail)
                if usermail != followmail:
                    continua = False
        print("follow " + followmail)

    # Genero el follow
    follow = {}
    follow[v.userString] = usermail
    follow[v.followString] = followmail

    return follow


def unfollow(db, user, follow):
    usermail = user[v.correoString]
    followmail = follow[v.correoString]

    resultado = db[v.collectionFollows].delete_one({v.userString: usermail, v.followString: followmail})
    deletedDocs = resultado.deleted_count
    print(str(deletedDocs))

    print("Delete done.")


def unfollowForDelete(db, user):
    usermail = user[v.correoString]
    resultado = db[v.collectionFollows].delete_many({v.userString: {"$eq": usermail}})
    deletedDocs = resultado.deleted_count
    print("User follows deleted: " + str(deletedDocs))

    resultado = db[v.collectionFollows].delete_many({v.followString: {"$eq": usermail}})
    deletedDocs = resultado.deleted_count
    print("User follows deleted: " + str(deletedDocs))
    print("Delete done.")


def generateUnfollow(db, user, follow):
    continua = True
    if user is None:
        user = fu.getSampleUser(db)
        usermail = user.get(v.correoString)
        print("user " + usermail)
    else:
        userObject = user
        usermail = userObject.get(v.correoString)
        print("user " + str(usermail))
    if follow is None:
        while continua:
            follow = fu.getSampleUser(db)
            followmail = follow.get(v.correoString)
            print("follow " + followmail + " y usermail " + usermail)
            if usermail != followmail:
                continua = False
        print("follow " + followmail)
    else:
        followmail = follow.get(v.correoString)
        if usermail != followmail:
            continua = True
            while follow is None or continua:
                follow = fu.getSampleUser(db)
                followmail = follow.get(v.correoString)
                print("follow " + followmail)
                if usermail != followmail:
                    continua = False
        print("follow " + followmail)

    follows = db[v.collectionFollows].count_documents({v.userString: usermail, v.followString: followmail})
    # Para comprobar si existe ya el follow antes de insertar
    print("follows: " + str(follows))
    if follows > 0:
        unfollow(db, user, follow)
    else:
        print("El unfollow de " + usermail + " a " + followmail + " no existe")


def generateUnfollowForDelete(db, user):
    if user is None:
        user = fu.getSampleUser(db)
        usermail = user.get(v.correoString)
        print("user " + usermail)
    else:
        userObject = user
        usermail = userObject.get(v.correoString)
        # print("user "+str(usermail))

    unfollowForDelete(db, user)
