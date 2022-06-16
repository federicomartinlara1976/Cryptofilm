#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import re
import csv
import json

from datetime import datetime
from datetime import date
# Importamos cliente de Mongodb para Python
from pymongo import MongoClient


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


# In[ ]:


#Funciones que nos van a ayudar a limpiar los datos antes de insertarlos
#Devuelve true si es un string vacío
def isEmptyString(s):
    return (s.strip() == "")
#Devuelve true si es un string numérico
def isNumber(s):
    return (s.strip().isnumeric())
#Devuelve true si es un número con decimales
def isDecimal(s):
    try:
        float(s.strip())
        return True
    except ValueError:
        return False
#Devuelve true si es un objeto con valor null
def isNone(s):
    if s is None :
        return True
    else:
        return False
    
#def isReleased(s):
#    if s== "Released" :
#        return False
#    else:
#        return True

#Devuelve true si una lista es vacía
def isEmptylist(s):
    if len(s) > 0:
        return False
    else:
        return True
#Devuelve true si los códigos postales cumplen unas condiciones
def isZipcode(s):
    if s < 99999 or s > 10000:
        print("He pasado zipcode")
        print(s)
        return False
    else:
        print("No He pasado zipcode")
        print(s)
        return True
    
#Devuelve true si el string que se le pasa no es numérico
def notNumeric(s):
    if s.isnumeric():
        return False
    else:
        return True
#Devuelve true si el string que se le pasa es un formato fecha
def isDate(s):
    if isinstance(s, datetime): #isinstance(s, datetime.date),type(s) is datetime.date
        return False
    else:
        print("fecha hoy " + str(datetime.date))
        print("fecha nacimiento " + s)
        return True
#Devuelve true si el parámetro no coincide con el día actual
def isNotToday(s):
    if s < datetime.today():
        return False
    else:
        return True

    
#Devuelve true al comprobar que los campos de las películas son correctos    
def validateFilm(film):
           
    if isNone(film[titleString]) or isEmptyString(film[titleString]) :
        print("Título incorrecto " + film[id])
        return False

    if isNone(film[ratingString]) or isEmptyString(film[ratingString]) :
        print("Rating incorrecto " + film[titleString])
        return False

    if isNone(film[yearString]) or isEmptyString(film[yearString]) :
        print("Year incorrecto " + film[titleString])
        return False

    if isNone(film[usersratingString]) or isEmptyString(film[usersratingString]) :
        print("RatingUser incorrecto " + film[titleString])
        return False

    if isNone(film[votesString]) or isEmptyString(film[votesString]) :
        print("Votesuser incorrecto " + film[titleString])
        return False

    if isNone(film[metascoreString]) or isEmptyString(film[metascoreString]) :
        print("Metascore incorrecto " + film[titleString])
        return False

    if isNone(film[descriptionString]) or isEmptyString(film[descriptionString]) :
        print("Description incorrecto " + film[titleString])
        return False

    if isNone(film[runtimeString]) or isEmptyString(film[runtimeString]) :
        print("Runtime incorrecto " + film[titleString])
        return False

    if isNone(film[directorsString]) or isEmptylist(film[directorsString]) :
        print("Directors vacio " + film[directorsString])
        return False

    if isNone(film[countriesString]) or isEmptylist(film[countriesString]) :
        print("Countries vacio " + film[titleString])
        return False

    if isNone(film[languagesString]) or isEmptylist(film[languagesString]) :
        print("Languages vacio " + film[titleString])
        return False

    if isNone(film[actorsString]) or isEmptylist(film[actorsString]) :
        print("Actors vacio " + film[titleString])
        return False

    if isNone(film[genresString]) or isEmptylist(film[genresString]) :
        print("Genres vacio " + film[titleString])
        return False
            
    return True             

#Devuelve true al comprobar que los campos de los usuarios son correctos  
def validateUser(user):
    if isNone(user[correoString]) or isEmptyString(user[correoString]) :
        print("correo incorrecto ")
        return False
    
    if isNone(user[nombreString]) or isEmptyString(user[nombreString]) :
        print("Nombre incorrecto " + user[correoString])
        return False
    
    if isNone(user[apellidosString]) or isEmptyString(user[apellidosString]) :
        print("Apellidos incorrecto " + user[correoString])
        return False
    
    if isNone(user[nacimientoString]) or isDate(user[nacimientoString]) or isNotToday(user[nacimientoString]) :
        print("NacimientoString incorrecto " + user[correoString])
        return False
                                                
    if isNone(user[postalString]) or notNumeric(user[postalString]) or isZipcode(int(user[postalString])) :
        print("Codigo postal incorrecto " + user[correoString])
        return False
    
    return True  


# In[ ]:


#voy leyendo las peliculas una a una del fichero json y las que cumplen las condiciones de la función validateFilm las paso a base de datos
with open('movies.json', encoding='utf-8') as inStream:
    data = json.load(inStream)
    
    readMovie = {}
    
    insertCount = 0
    totalCount = 0
    
    #Recorro el fichero completo linea a linea y meto los datos en insertMovie
    dataLen = len(data)
    for i in range(0, dataLen):
        readMovie = data[i]
        insertMovie = {}
        
        insertMovie[titleString] = readMovie[titleString]
        insertMovie[ratingString] = readMovie[ratingString]
        insertMovie[yearString] = readMovie[yearString]
        insertMovie[usersratingString] = readMovie[usersratingString]
        insertMovie[votesString] = readMovie[votesString]
        insertMovie[metascoreString] = readMovie[metascoreString]
        insertMovie[countriesString] = readMovie[countriesString]
        insertMovie[languagesString] = readMovie[languagesString]
        insertMovie[actorsString] = readMovie[actorsString]
        insertMovie[genresString] = readMovie[genresString]
        insertMovie[descriptionString] = readMovie[descriptionString]
        directors = readMovie.get(directorsString)
        
        #Hago las comprobaciones para ver la cohesión de los datos.
        if directors:
            insertMovie[directorsString] = readMovie[directorsString]
        else:
            insertMovie[directorsString]= None
            
        insertMovie[runtimeString] = readMovie[runtimeString]
        
        print("Creada "+insertMovie[titleString] + " "+ str(totalCount))
        totalCount += 1   
             
        #Una vez creada veo si la puedo insertar con la funcion validatefilm
        if validateFilm(insertMovie):
            db[collectionMovies].insert_one(insertMovie)
            print("Insertada "+ insertMovie[titleString] + " "+ str(insertCount))
            insertCount += 1
        else :
            print("No Insertada "+ insertMovie[titleString])
        
print("Peliculas analizadas: " + str(totalCount) + "/" + str(insertCount) + " Peliculas escritas.")


# In[ ]:


#Función que crea los campos director y codirector y borra la lista directors
def director():    
    
    movieObjects = db[collectionMovies].find({})
    
    # Recorremos el resultado
    for movie in movieObjects:   
    
        if movie != None:

            directors = movie[directorsString]

            if directors != None:

                director = directors[0] 
                print(director)

                if len(directors) > 1:
                    codirector = directors[1]
                else:
                    codirector = None
                print(codirector)
                
                title = movie[titleString]
                year = movie[yearString]
                
                print(title)
                
                output = db[collectionMovies].update_one({titleString: title, yearString: year}, {'$set': {directorString: director,                                                                                                    codirectorString: codirector}}) 
    

director()


# In[ ]:


#Funcion que borra la lista de directors
def deleteDirectors():    
    output = db[collectionMovies].updateMany({ },{ "$unset": { idString: emptyString, directorsString: emptyString }})
                
deleteDirectors()


# In[ ]:


def cleanVotes():
    
    movieObjects = db[collectionMovies].find({})

    for movie in movieObjects: 
            # Recorremos el resultado
            title = movie.get(titleString) 
            print("Tratando película " + title)
            
            idValue = movie.get(idString) 
            votes = movie.get(votesString)  

            if votes != None and isinstance(votes, str):
                votes = int(votes.replace(',', ''))
            elif votes == None:
                votes = 0
              
            output = db[collectionMovies].update_many({idString: idValue},{'$set': {votesString: votes}})
    print("Clean movies done.")
    
cleanVotes()


# In[ ]:





# In[ ]:


#Función que crea los campos genre y cogenre y borra la lista genres
def genre():    
    
    movieObjects = db[collectionMovies].find({})
    
    # Recorremos el resultado
    for movie in movieObjects:   
    
        if movie != None:

            genres = movie[genresString]

            if genres != None:

                genre = genres[0] 
                print(genre)

                if len(genres) > 1:
                    cogenre = genres[1]
                else:
                    cogenre = None
                print(cogenre)
                
                title = movie[titleString]
                year = movie[yearString]
                
                print(title)
                
                output = db[collectionMovies].update_one({titleString: title, yearString: year}, {'$set': {genreString: genre,                                                                                                    cogenreString: cogenre}}) 
    

genre()


# In[ ]:


#Funcion que borra la lista de genres
def deleteGenres():    
    output = db[collectionMovies].updateMany({ },{ "$unset": { idString: emptyString, genresString: emptyString }})
                
deleteGenres()


# In[ ]:


#Función que crea los campos mainactor y secactor y borra la lista actors
def actor():    
    
    movieObjects = db[collectionMovies].find({})
    
    # Recorremos el resultado
    for movie in movieObjects:   
    
        if movie != None:

            actors = movie[actorsString]

            if actors != None:

                mainactor = actors[0] 
                print(mainactor)

                if len(actors) > 1:
                    secactor = actors[1]
                else:
                    cogenre = None
                print(secactor)
                
                title = movie[titleString]
                year = movie[yearString]
                
                print(title)
                
                output = db[collectionMovies].update_one({titleString: title, yearString: year}, {'$set': {mainactorString: mainactor,                                                                                                    secactorString: secactor}}) 
    

actor()


# In[ ]:


#Funcion que borra la lista de actors
def deleteActors():    
    output = db[collectionMovies].updateMany({ },{ "$unset": { idString: emptyString, actorsString: emptyString }})
                
deleteActors()


# In[ ]:


#Función que crea los campos vose y seclanguage y borra la lista languages
def languages():    
    
    movieObjects = db[collectionMovies].find({})
    
    # Recorremos el resultado
    for movie in movieObjects:   
    
        if movie != None:

            languages = movie[languagesString]

            if languages != None:

                vose = languages[0] 
                print(vose)

                if len(languages) > 1:
                    seclanguage = languages[1]
                else:
                    seclanguage = None
                print(seclanguage)
                
                title = movie[titleString]
                year = movie[yearString]
                
                print(title)
                
                output = db[collectionMovies].update_one({titleString: title, yearString: year}, {'$set': {voseString: vose,                                                                                                    seclanguageString: seclanguage}}) 
    

languages()


# In[ ]:


#Funcion que borra la lista de languages
def deleteLanguages():    
    output = db[collectionMovies].updateMany({ },{ "$unset": { idString: emptyString, languagesString: emptyString }})
                
deleteLanguages()


# In[ ]:


#Función que crea los campos country y seccountry y borra la lista countries
def countries():    
    
    movieObjects = db[collectionMovies].find({})
    
    # Recorremos el resultado
    for movie in movieObjects:   
    
        if movie != None:

            countries = movie[countriesString]

            if countries != None:

                country = countries[0] 
                print(country)

                if len(countries) > 1:
                    secountry = countries[1]
                else:
                    secountry = None
                print(secountry)
                
                title = movie[titleString]
                year = movie[yearString]
                
                print(title)
                
                output = db[collectionMovies].update_one({titleString: title, yearString: year}, {'$set': {countryString: country,                                                                                                    seccountryString: secountry}}) 
    

countries()


# In[ ]:


#Funcion que borra la lista de countries
def deleteCuntries():    
    output = db[collectionMovies].updateMany({ },{ "$unset": { idString: emptyString, countriesString: emptyString }})
                
deleteCuntries()


# In[ ]:





# In[ ]:


#voy leyendo los usuarios uno a uno del fichero json y las que cumplen los condiciones de la función validateUser los paso a base de datos
with open('users.json', encoding='utf-8') as inStream:
    data = json.load(inStream)
    print(str(datetime.today()))
    readUser = {}
    writedUser = {}

    insertCount = 0
    totalCount = 0

dataLen = len(data)
for i in range(0, dataLen):
    readUser = data[i]
    writedUser = {}
    writedUser[nombreString] = readUser[nombreString]
    writedUser[apellidosString] = readUser[apellidosString]
    writedUser[correoString] = readUser[correoString]
    print("año: "+str(readUser[nacimientoString])[6:10])
    print("mes: "+str(readUser[nacimientoString])[3:5])
    print("dia: "+str(readUser[nacimientoString])[0:2])
    writedUser[nacimientoString] = datetime.fromisoformat(date(int(str(readUser[nacimientoString])[6:10]), int(str(readUser[nacimientoString])[3:5]), int(str(readUser[nacimientoString])[0:2])).isoformat())
    #writedUser[nacimientoString] = datetime.fromisoformat(str(readUser[nacimientoString]))
    writedUser[postalString] = readUser[postalString]
    correo = readUser.get(correoString)

    if correo:
        writedUser[correoString] = readUser[correoString]
    else:
        writedUser[correoString]= None
        
    print("Creada "+ writedUser[nombreString] + " "+ str(totalCount))
    totalCount += 1   
             
    if validateUser(writedUser):
        print("Voy a insertar " )
        print(writedUser[nombreString])
        print(writedUser[apellidosString])
        print(writedUser[correoString] )
        print(writedUser[nacimientoString] )
        print(writedUser[postalString])
        db[collectionUsers].insert_one(writedUser)
        print("Insertada "+ writedUser[correoString] + " "+ str(insertCount))
        insertCount += 1
    else :
        print("No Insertada "+ writedUser[correoString])
        
print("Users analizados: " + str(totalCount) + "/" + str(insertCount) + " users escritos.")


# 

# In[ ]:




