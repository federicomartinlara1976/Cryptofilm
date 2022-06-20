#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pymongo import MongoClient
import pandas as pd
import re
import csv
import json
import random
import time
import calendar
from datetime import datetime
# Importamos cliente de Mongodb para Python
from pymongo import MongoClient

# Conectamos a Mongodb. En nuestro caso, estamos levantando el servidor en local (host es, por tanto, localhost)
# y el puerto será el de por defecto, 27017
client = MongoClient("localhost:27017")
db = client.admin
print("Client admin creado.")

# Imprimimos el resultado del comando serverStatus
serverStatusResult = db.command("serverStatus")
print("Server status:")
print(serverStatusResult)


# In[ ]:


# Conectamos a Mongodb. En nuestro caso, estamos levantando el servidor en local (host es, por tanto, localhost)
# y el puerto será el de por defecto, 27017
client = MongoClient("localhost:27017")
db = client.admin
print("Client admin creado.")

# Imprimimos el resultado del comando serverStatus
serverStatusResult = db.command("serverStatus")
print("Server status:")
print(serverStatusResult)


# In[ ]:


# Definición bases de datos
db = client["db"]
collectionMovies = "movies"
collectionUsers = "users"
collectionViews = "views"
collectionLikes = "likes"
collectionFollows = "follows"
collectionMoviesList = "movieslist"

#Definición de variables

#films
idString = "id"
titleString = "title"
ratingString = "rating"
yearString = "year"
usersratingString = "users_rating"    
votesString = "votes"
metascoreString = "metascore"
countriesString = "countries"
languagesString = "languages"
actorsString = "actors"
genresString = "genres"
descriptionString = "description"
directorsString = "directors"
runtimeString = "runtime"
directorString = "director"
codirectorString = "codirector"
genreString = "genre"
cogenreString = "cogenre"
mainactorString = "mainactor"
secactorString = "secactor"
voseString = "vose"
seclanguageString = "seclanguage"
countryString = "country"
seccountryString = "seccountry"
emptyString = ""

#users

nombreString = "nombre"
apellidosString = "apellidos"
correoString = "correo"
nacimientoString = "nacimiento"
postalString = "postal"

#views
completedString= "completed"
scoreString = "score"
userString = "user"
titleString = "title"
yearString = "year"
timestampString = "timestamp"
newVotesString = "votesFilmin"
newRatingString = "ratingFilmin"
completedRuntimeString = "completedRuntime"
likeString = "like"
viewingtimeString = "viewingtime"

#rating

votesString = "votes"
usersratingString = "users_rating"   

#follow

userString = "user"
followString = "follow"


# In[ ]:


def getSampleMovie():
    # Usamos el comando aggregate para lanzar un aggregation pipeline de Mongodb
    resultadoAgg = db[collectionMovies].aggregate([
            { "$sample":{ "size": 1 } }
        ])

    # Recorremos el resultado
    for movie in resultadoAgg:
        #print(movie)
        return movie


# In[ ]:


def getSampleUser():
    # Usamos el comando aggregate para lanzar un aggregation pipeline de Mongodb
    resultadoAgg = db[collectionUsers].aggregate([
            { "$sample":{ "size": 1 } }
        ])

    # Recorremos el resultado
    for user in resultadoAgg:
        #print(user)
        return user


# In[ ]:


def getCurrentTimestamp():
    gmt = time.gmtime()
    ts = calendar.timegm(gmt)
    return ts


# In[ ]:


def getRandomInt(min, max):
    return random.randint(min, max)


# In[ ]:


def cleanRuntime(runtime):
    if runtime != None and isinstance(runtime, str):
        runtime = int(runtime.replace(" min", ''))

    return runtime


# In[ ]:


def generateView(user, movie):   
    if user is None:
        user = getSampleUser()
        usermail = user.get(correoString)
        #print(usermail)
    else:
        usermail = user.get(correoString)
        #print(str(usermail))
    if movie is None:
        movie = getSampleMovie()
        movietitle = movie.get(titleString)
        print("movie "+ movietitle)
    else:
        movietitle = movie.get(titleString)
        #print("movie "+movietitle) 

    view = {} 
    view[userString] = user.get(correoString)
    view[titleString] = movie.get(titleString)
    view[yearString] = movie.get(yearString)
    view[timestampString] = getCurrentTimestamp()
    
    runtime = movie.get(runtimeString)
    viewingtime = getRandomInt(int(cleanRuntime(runtime) * 0.15), int(cleanRuntime(runtime)))
    
    if viewingtime > cleanRuntime(runtime)* 0.8 :
        completed= True
    else:
        completed= False 

    
    view[completedString] = completed
    view [viewingtimeString] =viewingtime
    
    if completed == True :
        view[scoreString] = getRandomInt(0, 10)         
    
    return view


# In[ ]:


def generateLike(view):   
#Genero el like a partir de la vista
    like = {}
    like[userString] = view[userString]
    like[titleString] = view[titleString]
    like[yearString] = view[yearString]
    
    return like


# In[ ]:


def updateRatingFilm(view):
    
    title =view[titleString]
    year= view[yearString]
    
    movieObject = db[collectionMovies].find_one({titleString: title, yearString: year})
    
    newRating = 0
    newVotes = 0
    newScore = int(view[scoreString])
    totalVotes = int(movieObject[votesString])
    totalRating = float(movieObject[usersratingString])
                
    newVotes = totalVotes + 1
    newRating = ((totalRating * totalVotes) + newScore)/newVotes

    print("Old votes: " + str(totalVotes))    
    print("New votes: " + str(newVotes))
    print("Old users rating: " + str(totalRating))
    print("New users rating: " + str(newRating))
    output = db[collectionMovies].update_one({titleString: title}, {'$set': {usersratingString: newRating, votesString: newVotes }}) 

    # Pintamos los documentos modificados
    print('Documentos modificados: ' + str(output.modified_count))
    


# In[ ]:


def insertView (view):
#inserto en vista y si le ha gustado inserto en peliculas que gustan

    db[collectionViews].insert_one(view)

    score = view.get(scoreString)
    print("score: "+str(score))
    if score :
        movielist = generateMovieListView(view)
        deleteMovieList(movielist)
        updateRatingFilm(view)
        if score >= 7 : 
            like = {}
            like = generateLike(view)
            db[collectionLikes].insert_one(like)


# In[ ]:


def generateMassiveViews(views):   
    for i in range(views):
        view = {}
        view = generateView(None, None)
        insertView(view)
        
generateMassiveViews(10)


# In[ ]:





# In[ ]:


#esta función ya está en el init
def arreglaVotos():

    movies = db[collectionMovies].find({})

    for m in movies:
        print(m)

        votes = m[votesString]
        title = m[titleString]
        
        votes = int(votes.replace(',', ''))

        output = db[collectionMovies].update_one({titleString: title}, {'$set': {votesString: votes}}) 

        # Pintamos los documentos modificados
        print('Documentos modificados: ' + str(output.modified_count))       

arreglaVotos()


# In[ ]:





# In[ ]:


def insertFollow (follow):
#inserto el follow
    follows = db[collectionFollows].count_documents({userString: follow[userString], followString: follow[followString]})
    ##para comprobar si existe ya el follow antes de insertar
    if follows > 0:
        print("El follow de " + follow[userString] +" a " +follow[followString]+" ya existe")
    else:
        db[collectionFollows].insert_one(follow)


# In[ ]:


def generateFollow(user, follow): 
    print("Estoy aqui")
    continua = True
    if user is None:
        user = getSampleUser()
        usermail = user.get(correoString)
        print("user "+usermail)
    else:
        userObject = user
        usermail = userObject.get(correoString)
        print("user "+str(usermail))
    if follow is None:
        while continua :
            follow = getSampleUser()
            followmail = follow.get(correoString)
            print("follow "+followmail+" y usermail "+usermail)
            if usermail != followmail :
                continua = False
        print("follow "+followmail)
    else:
        followmail = follow.get(correoString)
        if usermail != followmail :
            continua = True
            while follow is None or continua:
                follow = getSampleUser()
                followmail = follow.get(correoString)
                print("follow "+followmail)
                if usermail != followmail :
                    continua = False
        print("follow "+followmail)

    #Genero el follow
    follow = {}
    follow[userString] = usermail
    follow[followString] = followmail
    
    return follow


# In[ ]:


def generateMassiveFollows(follows):   
    for i in range(follows):
        follow = {}
        follow = generateFollow(None,None)
        insertFollow(follow)
        
generateMassiveFollows(10)


# In[ ]:


def unfollow(user, follow):
    
    usermail = user[correoString]
    followmail = follow[correoString]
    
    resultado = db[collectionFollows].delete_one({userString: usermail, followString: followmail}) 
    deletedDocs = resultado.deleted_count
    print(str(deletedDocs))

    print("Delete done.")


# In[ ]:


def unfollowForDelete (user):

    usermail = user[correoString]
    resultado = db[collectionFollows].delete_many( {userString: { "$eq": usermail } } )
    deletedDocs = resultado.deleted_count
    print("User follows deleted: "+str(deletedDocs))
    
    resultado = db[collectionFollows].delete_many( {followString: { "$eq": usermail } } )
    deletedDocs = resultado.deleted_count
    print("User follows deleted: "+str(deletedDocs))
    print("Delete done.")


# In[ ]:


def generateUnfollow(user, follow):   
    continua = True
    if user is None:
        user = getSampleUser()
        usermail = user.get(correoString)
        print("user "+usermail)
    else:
        userObject = user
        usermail = userObject.get(correoString)
        print("user "+str(usermail))
    if follow is None:
        while continua :
            follow = getSampleUser()
            followmail = follow.get(correoString)
            print("follow "+followmail+" y usermail "+usermail)
            if usermail != followmail :
                continua = False
        print("follow "+followmail)
    else:
        followmail = follow.get(correoString)
        if usermail != followmail :
            continua = True
            while follow is None or continua:
                follow = getSampleUser()
                followmail = follow.get(correoString)
                print("follow "+followmail)
                if usermail != followmail :
                    continua = False
        print("follow "+followmail) 
    
            
    follows = db[collectionFollows].count_documents({userString: usermail, followString: followmail})
    ##para comprobar si existe ya el follow antes de insertar
    print("follows: "+ str(follows))
    if follows > 0:
        unfollow(user, follow)
    else:
        print("El unfollow de " + usermail +" a " +followmail+" no existe")
        
        
generateUnfollow(None, None)


# In[ ]:


def generateUnfollowForDelete(user):   
    if user is None:
        user = getSampleUser()
        usermail = user.get(correoString)
        print("user "+usermail)
    else:
        userObject = user
        usermail = userObject.get(correoString)
        #print("user "+str(usermail))
    
    unfollowForDelete(user)
        
generateUnfollowForDelete(None)


# In[ ]:


def insertMovieList (movielist):
    #inserto el movielist
    existe = db[collectionMoviesList].count_documents({userString: movielist[userString], titleString: movielist[titleString], yearString: movielist[yearString]})
    ##para comprobar si existe ya el follow antes de insertar
    if existe > 0:
        print("La pelicla " + movielist[titleString] +" ya está en la lista de " + movielist[userString])
    else:
        db[collectionMoviesList].insert_one(movielist)


# In[ ]:


def generateMovieList(user, movie): 
    
    if user is None:
        user = getSampleUser()
        usermail = user.get(correoString)
        print("user "+ usermail)
    else:
        userObject = user
        usermail = userObject.get(correoString)
        print("user "+str(usermail))
        
    if movie is None:
        movie = getSampleMovie()
        movietitle = movie.get(titleString)
        print("movie "+ movietitle)
    else:
        movietitle = movie.get(titleString)
        print("movie "+movietitle)

    #Genero el movielist
    movielist = {}
    movielist[userString] = user[correoString]
    movielist[titleString] = movie[titleString]
    movielist[yearString] = movie[yearString]
    
    return movielist


# In[ ]:


def generateMassiveMovieList(moviesonlist):   
    for i in range(moviesonlist):
        movielist = {}
        movielist = generateMovieList(None,None)
        insertMovieList(movielist)
        
generateMassiveMovieList(10)


# In[ ]:


def generateMovieListView(view): 

    #Genero el movielist
    movielist = {}
    movielist[userString] = view[userString]
    movielist[titleString] = view[titleString]
    movielist[yearString] = view[yearString]
    
    return movielist


# In[ ]:


def deleteMovieList(movielist):
    
    usermail = movielist[userString]
    title = movielist[titleString]
    year = movielist[yearString]
    
    moviesonlist = db[collectionMoviesList].count_documents({userString: usermail, titleString: title, yearString: year})
    if moviesonlist > 0:
        resultado = db[collectionMoviesList].delete_one({userString: usermail, titleString: title, yearString: year}) 
        deletedDocs = resultado.deleted_count
        print(str(deletedDocs))
        print("Delete done.")
    else:
        print("La pelicula  " + title +" no está en la lista de  " +usermail)


# In[ ]:


def deleteMovieListForDeleteUser (user):

    usermail = user[correoString]
    resultado = db[collectionMoviesList].delete_many( {userString: { "$eq": usermail } } )
    deletedDocs = resultado.deleted_count
    print("User moviesList deleted: "+str(deletedDocs))


# In[ ]:


def generateDeleteMovieList(user, movie):   

        movielist = generateMovieList(None,None)
        deleteMovieList(movielist)
        user = getSampleUser()
        deleteMovieListForDeleteUser(user)

        
generateDeleteMovieList(None, None)


# In[ ]:


with open('C:/Users/cmemb/MoviesClean.csv', 'w', newline='', encoding='UTF-8') as outputCsv:
    fieldnames = ['title', 'rating', 'year', 'users_rating', 'votes', 'metascore', 'description', 'runtime', 'codirector', 'director', 'cogenre', 'genre', 'mainactor', 'secactor', 'seclanguage', 'vose', 'country', 'seccountry']
    writer = csv.DictWriter(outputCsv, fieldnames=fieldnames)
    
    movieObjects = db[collectionMovies].find({})
    # Recorremos el resultado
    for movie in movieObjects:   
    
        title = movie[titleString]
        rating = movie[ratingString]
        year = movie[yearString]
        users_rating = movie[usersratingString]
        votes = movie[votesString]
        metascore = movie[metascoreString]
        description = movie[descriptionString]
        runtime = movie[runtimeString]
        codirector = movie[codirectorString]
        director = movie[directorString]
        cogenre = movie[cogenreString]
        genre = movie[genreString]
        mainactor = movie[mainactorString]
        secactor = movie[secactorString]
        seclanguage = movie[seclanguageString]
        vose = movie[voseString]
        country = movie[countryString]
        seccountry = movie[seccountryString]
        
    writer.writerow(line)
        
print("Fichero guardado")


# In[ ]:


with open('C:/Users/cmemb/OneDrive/BIG DATA/Ejercicios/3 Python/movies.json', encoding='utf-8') as inStream, open('C:/Users/cmemb/OneDrive/BIG DATA/Ejercicios/3 Python/moviesFiltered.json', 'w', newline='', encoding='UTF-8') as outStream:
    data = json.load(inStream)
    
    titleString = "title"
    votesString = "votes"
    ratingUsersStringIn = "users_rating"
    ratingUsersString = "ratingUsers"    
    ratingCriticsStringIn = "metascore"
    ratingCriticsString = "ratingCritics"   
    
    movies = []    
    movie = {}
    
    insertCount = 0
    totalCount = 0
    
    dataLen = len(data)
    for i in range(0, dataLen):
        movie = data[i]
        
        title = movie[titleString]
        votes = movie[votesString]
        
        if votes != None and int(getNumber(votes)) >= votesLimit:
            movie[ratingCriticsString] = movie[ratingCriticsStringIn]
            movie[ratingUsersString] = movie[ratingUsersStringIn]
            movie.pop(ratingUsersStringIn)
            movie.pop(ratingCriticsStringIn)
            movies.append(movie)
            
            insertCount += 1
            
        totalCount +=1
    
    json.dump(movies, outStream, ensure_ascii=False, indent=2)
    
print("Json filtered successfully, " + str(insertCount) + "/" + str(totalCount) + " movies written.")

