#!/usr/bin/env python
# coding: utf-8

# In[25]:


# Importamos pandas y Scikit Learn, que utilizaremos para
# establecer un modelo de recomendación basado en contenido
import pandas as pd
import numpy as np
import random
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity


# In[26]:


# Lectura de los csv. 
movies = pd.read_csv('movies.csv')

likes = pd.read_csv('likesClean.csv')


# In[27]:


movies


# In[28]:


# Creamos una lista con las columnas que nos parecen interesantes del dataframe
features = ["genre","director","mainactor","country","vose"]


# In[29]:


# Función que, tomando las columnas que nos parecen interesantes, dejen todas las palabras que contienen
# dichas columnas separadas por espacios
# Las listas las pasamos a string y posteriormente reemplazamos corchetes y comas por espacios
def combine_features(row):
    strTotal = str(row["genre"]).replace("[","").replace("]","").replace(","," ").replace("'"," ")
    strTotal += " " + str(row["director"]).replace("[","").replace("]","").replace(","," ").replace("'"," ")
    strTotal += " " + str(row["mainactor"][0:5]).replace("[","").replace("]","").replace(","," ").replace("'"," ")
    strTotal += " " + str(row["country"]).replace("[","").replace("]","").replace(","," ").replace("'"," ")
    strTotal += " " + str(row["vose"]).replace("[","").replace("]","").replace(","," ").replace("'"," ")
    return strTotal


# In[30]:


# Cuando una de estas columnas tenga valor nulo, lo rellenamos con vacío
for feature in features:
    movies[feature] = movies.get(feature).fillna("")
    
# Generamos la nueva columna con las palabras que hemos definido como interesantes
# para establecer el contenido del título
movies["combined_features"] = movies.apply(combine_features,axis=1)

movies


# In[31]:


# Llamamos a la función que cree la matriz de conteo de esa nueva columna
cv = CountVectorizer()
count_matrix = cv.fit_transform(movies["combined_features"])
count_matrix


# In[32]:


# Llamamos a la función que saque las similitudes de coseno
cosine_sim = cosine_similarity(count_matrix)
cosine_sim


# In[33]:


# Creamos dos funciones auxiliares, para sacar el título en base al índice y viceversa
def get_title_from_index(index):
    return movies[movies.index == index]["title"].values[0]

def get_index_from_title(title):
    return movies[movies.title == title]["register"].values[0]


# In[35]:


# En base a un título de entrada, vemos qué recomendaríamos
movie_user_likes = random.choice(likes['title'])

# Cogemos el índice de la película
movie_index = get_index_from_title(movie_user_likes)
# Enumerate devuelve el iterable sobre la lista con las similitudes del título de entrada
similar_movies = list(enumerate(cosine_sim[movie_index]))


# In[36]:


# Ordenamos títulos similares
sorted_similar_movies = sorted(similar_movies, key=lambda x:x[1], reverse=True)[1:]

i=0
print("Top 10 similar movies to " + movie_user_likes + " are:\n")
for element in sorted_similar_movies:
    print(get_title_from_index(element[0]))
    i += 1
    if i > 10:
        break


# In[38]:


# Vemos si tiene sentido la recomendación
moviesOut = movies.loc[(movies.title == 'The Zero Theorem') | (movies.title == 'In Bruges') | (movies.title == 'Shirley Valentine')]
moviesOut

