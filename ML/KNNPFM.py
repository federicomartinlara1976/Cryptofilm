#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Se cargan todas las librerias que se van a usar
import pandas as pd
import numpy as np
from scipy import sparse
import base64
import io
import random


# In[2]:


# Lectura de los csv. 
movies = pd.read_csv('moviesTR.csv')
likesClean = pd.read_csv('likesClean.csv')


# In[3]:


# Visualización de la cabecera del csv 'movies' 
movies.head()


# In[4]:


likesClean.head()


# In[5]:


# Se muestra todas la columnas de nuestro dataframe.
movies.columns


# In[6]:


# Se muestra el tipo de cada columna de nuestro dataframe.
movies.dtypes


# In[7]:


# Data cleaning en la columna 'genres'
movies['genres'] = movies['genres'].str.strip('[]').str.replace(' ','').str.replace("'",'')
movies['genres'] = movies['genres'].str.split(',')


# In[8]:


for i,j in zip(movies['genres'],movies.index):
    list2=[]
    list2=i
    list2.sort()
    movies.loc[j,'genres']=str(list2)
movies['genres'] = movies['genres'].str.strip('[]').str.replace(' ','').str.replace("'",'')
movies['genres'] = movies['genres'].str.split(',')


# In[9]:


# Se crea una lista única de los géneros
genreList = []
for index, row in movies.iterrows():
    genres = row["genres"]
    
    for genre in genres:
        if genre not in genreList:
            genreList.append(genre)
genreList[:20] #ahora tenemos una lista única de los géneros.


# In[10]:


# Creamos una función que crea una lista de 1 y 0 con los géneros.

def binaryGenre(genre_list):
    binaryList = []
    
    for genre in genreList:
        if genre in genre_list:
            binaryList.append(1)
        else:
            binaryList.append(0)
    
    return binaryList


# In[ ]:


# Aplicamos la función anteriormente descrita y creamos una nueva columna 'genres_bin' 
# que será una lista de 1 y 0 para cada registro. 

# Un ejemplo del resultado sería para la película Avatar de James Cameron que está catalogada con 4 géneros, 
# el resultado de la columna 'genres_bin' tendra cuatro 1 y el resto de la lista serán todos 0.

movies.iloc[0]


# In[11]:


movies['genres_bin'] = movies['genres'].apply(lambda x: binaryGenre(x))
movies['genres_bin'].head()


# In[12]:


for i,j in zip(movies['cast'],movies.index):
    list2 = []
    list2 = i[:4]
    movies.loc[j,'cast'] = str(list2)
movies['cast'] = movies['cast'].str.strip('[]').str.replace(' ','').str.replace("'",'')
movies['cast'] = movies['cast'].str.split(',')
for i,j in zip(movies['cast'],movies.index):
    list2 = []
    list2 = i
    list2.sort()
    movies.loc[j,'cast'] = str(list2)
movies['cast']=movies['cast'].str.strip('[]').str.replace(' ','').str.replace("'",'')


# In[13]:


castList = []
for index, row in movies.iterrows():
    cast = row["cast"]
    
    for i in cast:
        if i not in castList:
            castList.append(i)
            
print(castList)


# In[14]:


def binaryCast(cast_list):
    binaryList = []
    
    for genre in castList:
        if genre in cast_list:
            binaryList.append(1)
        else:
            binaryList.append(0)
    
    return binaryList


# In[15]:


movies['cast_bin'] = movies['cast'].apply(lambda x: binaryCast(x))
movies['cast_bin'].head()


# In[16]:


# Creamos una función que compruebe el campo director. Si el campo es nulo, devuelve un vacio.
def xstr(s):
    if s is None:
        return ''
    return str(s)
movies['director'] = movies['director'].apply(xstr)


# In[17]:


directorList=[]
for i in movies['director']:
    if i not in directorList:
        directorList.append(i)


# In[18]:


def binaryDirector(director_list):
    binaryList = []  
    for direct in directorList:
        if direct in director_list:
            binaryList.append(1)
        else:
            binaryList.append(0)
    return binaryList


# In[19]:


movies['director_bin'] = movies['director'].apply(lambda x: binaryDirector(x))
movies.head()


# In[20]:


# Ahora vamos a crear una función que utiliza la similitud del coseno.
# Es una medida de similitud entre dos secuencias de número.
# Comparamos varios parámetros que son:
# -genre, score, director

from scipy import spatial

def Similarity(movieId1, movieId2):
    a = movies.iloc[movieId1]
    b = movies.iloc[movieId2]
    
    genresA = a['genres_bin']
    genresB = b['genres_bin']
    
    genreDistance = spatial.distance.cosine(genresA, genresB)
    
    castA = a['cast_bin']
    castB = b['cast_bin']
    castDistance = spatial.distance.cosine(castA, castB)
    
    directorA = a['director_bin']
    directorB = b['director_bin']
    directorDistance = spatial.distance.cosine(directorA, directorB)

    return genreDistance + directorDistance + castDistance 


# In[21]:


Similarity(3,160) #checking similarity between any 2 random movies


# In[ ]:


# La distancia obtenida es de 2.7958758547680684, el cúal es elevado. 
# Cuanto mayor sea la distancia, menos coincidencia tendrán las dos películas que se comparan.


# In[22]:


print(movies.iloc[3])
print(movies.iloc[160])


# In[23]:


new_id = list(range(0,movies.shape[0]))
movies['new_id']=new_id
movies=movies[['original_title','genres','vote_average','genres_bin','cast_bin','new_id','director','director_bin']]
movies.head()


# In[24]:


import operator

# La función que definimos a continuación es el ML usando KNN.

# Todo el código usado está sacado del siguiente enlace:
# https://www.kaggle.com/code/heeraldedhia/movie-ratings-and-recommendation-using-knn/notebook

# El resultado de la función es predecir la puntuación de la película "rating" y recomendarte películas similares 
# a la película introducida en la función.

def predict_score(name):
    #name = input('Enter a movie title: ')
    new_movie = movies[movies['original_title'].str.contains(name)].iloc[0].to_frame().T
    print('Selected Movie: ',new_movie.original_title.values[0])
    def getNeighbors(baseMovie, K):
        distances = []
    
        for index, movie in movies.iterrows():
            if movie['new_id'] != baseMovie['new_id'].values[0]:
                dist = Similarity(baseMovie['new_id'].values[0], movie['new_id'])
                distances.append((movie['new_id'], dist))
    
        distances.sort(key=operator.itemgetter(1))
        neighbors = []
    
        for x in range(K):
            neighbors.append(distances[x])
        return neighbors

    K = 10
    avgRating = 0
    neighbors = getNeighbors(new_movie, K)
    
    print('\nRecommended Movies: \n')
    for neighbor in neighbors:
        avgRating = avgRating+movies.iloc[neighbor[0]][2]  
        print( movies.iloc[neighbor[0]][0]+" | Genres: "+str(movies.iloc[neighbor[0]][1]).strip('[]').replace(' ','')+" | Rating: "+str(movies.iloc[neighbor[0]][2]))
    
    print('\n')
    avgRating = avgRating/K
    print('The predicted rating for %s is: %f' %(new_movie['original_title'].values[0],avgRating))
    print('The actual rating for %s is %f' %(new_movie['original_title'].values[0],new_movie['vote_average']))


# In[25]:


predict_score('The Lord of the Rings')


# In[34]:


test = random.choice(likesClean['title'])

test


# In[35]:


predict_score(test)

