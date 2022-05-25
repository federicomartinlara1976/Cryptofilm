#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Se cargan todas las librerias que se van a usar
import pandas as pd
import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight')
import seaborn as sns
import numpy as np
import json
import warnings
warnings.filterwarnings('ignore')
import base64
import io
from matplotlib.pyplot import imread
import codecs
from IPython.display import HTML
from scipy import sparse


# In[2]:


# Lectura de los csv. 
movies = pd.read_csv('tmdb_5000_movies.csv')
credits = pd.read_csv('tmdb_5000_credits.csv')


# In[3]:


# Visualización de la cabecera del csv 'movies' 
movies.head()


# In[4]:


# Visualización de la cabecera del csv 'credits'
credits.head()


# In[5]:


# Se cambia el formato json a string de la columna 'genres'
movies['genres'] = movies['genres'].apply(json.loads)
for index,i in zip(movies.index,movies['genres']):
    listGenre = []
    for j in range(len(i)):
        listGenre.append((i[j]['name'])) # la clave "key" 'name' contiene el nombre del género
    movies.loc[index,'genres'] = str(listGenre)

# Se cambia el formato json a string de la columna 'keywords'    
movies['keywords'] = movies['keywords'].apply(json.loads)
for index,i in zip(movies.index,movies['keywords']):
    listKeywords = []
    for j in range(len(i)):
        listKeywords.append((i[j]['name']))
    movies.loc[index,'keywords'] = str(listKeywords)
    
# Se cambia el formato json a string de la columna 'production_companies'
movies['production_companies'] = movies['production_companies'].apply(json.loads)
for index,i in zip(movies.index,movies['production_companies']):
    listProductionCompanies = []
    for j in range(len(i)):
        listProductionCompanies.append((i[j]['name']))
    movies.loc[index,'production_companies'] = str(listProductionCompanies)

# Se cambia el formato json a string de la columna 'cast'
credits['cast'] = credits['cast'].apply(json.loads)
for index,i in zip(credits.index,credits['cast']):
    listCast = []
    for j in range(len(i)):
        listCast.append((i[j]['name']))
    credits.loc[index,'cast'] = str(listCast)

# Se cambia el formato json a string de la columna 'crew'
credits['crew'] = credits['crew'].apply(json.loads)
def director(x):
    for i in x:
        if i['job'] == 'Director':
            return i['name']
credits['crew'] = credits['crew'].apply(director)
credits.rename(columns={'crew':'director'},inplace=True)


# In[6]:


# Visualización de la cabecera del csv 'movies' después de hacer el cambio de formato en algunas columnas
movies.head()


# In[7]:


movies.iloc[25]


# In[8]:


# Ahora mergeamos los dos csv y le decimos que columna del csv 'movies' y que columna del csv 'credits' se hace el merge 
# y también se define el "como"  ||  left_on='id',right_on='movie_id',how='left'

movies = movies.merge(credits,left_on='id',right_on='movie_id',how='left')
movies = movies[['id','original_title','genres','cast','vote_average','director','keywords']]


# In[9]:


movies.iloc[25]


# In[10]:


# Con el comando shape nos dice el tamaño de nuestro dataframe, el que se ha mergeado anteriormente de los dos csv. 
# Tenemos 4803 registros y 20 columnas.

movies.shape


# In[11]:


# Con el comando size nos dice el total de celdas que tiene el dataframe, 
# sería la multiplicación de los registros por las columnas.
movies.size


# In[12]:


# Se muestra todas la columnas de nuestro dataframe.
movies.columns


# In[13]:


# Se muestra el tipo de cada columna de nuestro dataframe.
movies.dtypes


# In[14]:


# Data cleaning en la columna 'genres'
movies['genres'] = movies['genres'].str.strip('[]').str.replace(' ','').str.replace("'",'')
movies['genres'] = movies['genres'].str.split(',')


# In[15]:


# Se genera una gráfica con el top de géneros.

plt.subplots(figsize=(12,10))
genreList = []
for i in movies['genres']:
    genreList.extend(i)
ax = pd.Series(genreList).value_counts()[:10].sort_values(ascending=True).plot.barh(width=0.9,color=sns.color_palette('hls',10))
for i, v in enumerate(pd.Series(genreList).value_counts()[:10].sort_values(ascending=True).values): 
    ax.text(.8, i, v,fontsize=12,color='white',weight='bold')
plt.title('Top Genres')
plt.show()


# In[16]:


for i,j in zip(movies['genres'],movies.index):
    list2=[]
    list2=i
    list2.sort()
    movies.loc[j,'genres']=str(list2)
movies['genres'] = movies['genres'].str.strip('[]').str.replace(' ','').str.replace("'",'')
movies['genres'] = movies['genres'].str.split(',')


# In[19]:


# Se crea una lista única de los géneros
genreList = []
for index, row in movies.iterrows():
    genres = row["genres"]
    
    for genre in genres:
        if genre not in genreList:
            genreList.append(genre)
genreList[:20] #ahora tenemos una lista única de los géneros.


# In[18]:


# Creamos una función que crea una lista de 1 y 0 con los géneros.

def binaryGenre(genre_list):
    binaryList = []
    
    for genre in genreList:
        if genre in genre_list:
            binaryList.append(1)
        else:
            binaryList.append(0)
    
    return binaryList


# In[20]:


# Aplicamos la función anteriormente descrita y creamos una nueva columna 'genres_bin' 
# que será una lista de 1 y 0 para cada registro. 

# Un ejemplo del resultado sería para la película Avatar de James Cameron que está catalogada con 4 géneros, 
# el resultado de la columna 'genres_bin' tendra cuatro 1 y el resto de la lista serán todos 0.

movies.iloc[0]


# In[21]:


movies['genres_bin'] = movies['genres'].apply(lambda x: binaryGenre(x))
movies['genres_bin'].head()


# In[22]:


# Data cleaning en la columna 'cast'
movies['cast'] = movies['cast'].str.strip('[]').str.replace(' ','').str.replace("'",'').str.replace('"','')
movies['cast'] = movies['cast'].str.split(',')


# In[23]:


# Se genera una gráfica con el top de actores con más apariciones.

plt.subplots(figsize=(12,10))
actorList=[]
for i in movies['cast']:
    actorList.extend(i) #lista con los actores
ax=pd.Series(actorList).value_counts()[:15].sort_values(ascending=True).plot.barh(width=0.9,color=sns.color_palette('muted',40))
for i, v in enumerate(pd.Series(actorList).value_counts()[:15].sort_values(ascending=True).values): 
    ax.text(.8, i, v,fontsize=10,color='white',weight='bold')
plt.title('Actors with highest appearance')
plt.show()


# In[25]:


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


# In[26]:


castList = []
for index, row in movies.iterrows():
    cast = row["cast"]
    
    for i in cast:
        if i not in castList:
            castList.append(i)


# In[27]:


def binaryCast(cast_list):
    binaryList = []
    
    for genre in castList:
        if genre in cast_list:
            binaryList.append(1)
        else:
            binaryList.append(0)
    
    return binaryList


# In[28]:


movies['cast_bin'] = movies['cast'].apply(lambda x: binaryCast(x))
movies['cast_bin'].head()


# In[29]:


# Creamos una función que compruebe el campo director. Si el campo es nulo, devuelve un vacio.
def xstr(s):
    if s is None:
        return ''
    return str(s)
movies['director'] = movies['director'].apply(xstr)


# In[30]:


# Se genera una gráfica con el top de director con más películas.

plt.subplots(figsize=(12,10))
ax = movies[movies['director']!=''].director.value_counts()[:10].sort_values(ascending=True).plot.barh(width=0.9,color=sns.color_palette('muted',40))
for i, v in enumerate(movies[movies['director']!=''].director.value_counts()[:10].sort_values(ascending=True).values): 
    ax.text(.5, i, v,fontsize=12,color='white',weight='bold')
plt.title('Directors with highest movies')
plt.show()


# In[31]:


directorList=[]
for i in movies['director']:
    if i not in directorList:
        directorList.append(i)


# In[32]:


#
print(directorList)


# In[33]:


def binaryDirector(director_list):
    binaryList = []  
    for direct in directorList:
        if direct in director_list:
            binaryList.append(1)
        else:
            binaryList.append(0)
    return binaryList


# In[34]:


movies['director_bin'] = movies['director'].apply(lambda x: binaryDirector(x))
movies.head()


# In[35]:


from wordcloud import WordCloud, STOPWORDS
import nltk
from nltk.corpus import stopwords


# In[36]:


plt.subplots(figsize=(12,12))
stop_words = set(stopwords.words('english'))
stop_words.update(',',';','!','?','.','(',')','$','#','+',':','...',' ','')

words=movies['keywords'].dropna().apply(nltk.word_tokenize)
word=[]
for i in words:
    word.extend(i)
word=pd.Series(word)
word=([i for i in word.str.lower() if i not in stop_words])
wc = WordCloud(background_color="black", max_words=2000, stopwords=STOPWORDS, max_font_size= 60,width=1000,height=1000)
wc.generate(" ".join(word))
plt.imshow(wc)
plt.axis('off')
fig=plt.gcf()
fig.set_size_inches(10,10)
plt.show()


# In[ ]:


movies['keywords'] = movies['keywords'].str.strip('[]').str.replace(' ','').str.replace("'",'').str.replace('"','')
movies['keywords'] = movies['keywords'].str.split(',')
for i,j in zip(movies['keywords'],movies.index):
    list2 = []
    list2 = i
    movies.loc[j,'keywords'] = str(list2)
movies['keywords'] = movies['keywords'].str.strip('[]').str.replace(' ','').str.replace("'",'')
movies['keywords'] = movies['keywords'].str.split(',')
for i,j in zip(movies['keywords'],movies.index):
    list2 = []
    list2 = i
    list2.sort()
    movies.loc[j,'keywords'] = str(list2)
movies['keywords'] = movies['keywords'].str.strip('[]').str.replace(' ','').str.replace("'",'')
movies['keywords'] = movies['keywords'].str.split(',')


# In[38]:


wordsList = []
for index, row in movies.iterrows():
    genres = row["keywords"]
    
    for genre in genres:
        if genre not in wordsList:
            wordsList.append(genre)


# In[42]:


def binaryWord(words):
    binaryList = []
    for genre in wordsList:
        if genre in words:
            binaryList.append(1)
        else:
            binaryList.append(0)
    return binaryList


# In[43]:


movies['words_bin'] = movies['keywords'].apply(lambda x: binaryWord(x))
movies = movies[(movies['vote_average']!=0)] #Eliminamos las películas con una puntuación de 0 y el campo director vacío. 
movies = movies[movies['director']!='']


# In[48]:


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
    
    wordsA = a['words_bin']
    wordsB = b['words_bin']
    wordsDistance = spatial.distance.cosine(wordsA, wordsB)
    return genreDistance + directorDistance + castDistance + wordsDistance


# In[49]:


Similarity(3,160) #checking similarity between any 2 random movies


# In[ ]:


# La distancia obtenida es de 1.5686, el cúal es elevado. 
# Cuanto mayor sea la distancia, menos coincidencia tendrán las dos películas que se comparan.


# In[50]:


print(movies.iloc[3])
print(movies.iloc[160])


# In[ ]:


# Como se puede observar las dos películas que se han elegido son:
# -El caballero oscuro: La leyenda renace
# -Cómo entrenar a tu dragón 2
# Son dos películas bastante diferentes. Por lo tanto, la distancia es grande.
# Cuando más cerca de 0 se encuentre el valor mayor será la coincidencia.


# In[53]:


new_id = list(range(0,movies.shape[0]))
movies['new_id']=new_id
movies=movies[['original_title','genres','vote_average','genres_bin','cast_bin','new_id','director','director_bin','words_bin']]
movies.head()


# In[54]:


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


# In[55]:


predict_score('Harry Potter and the Prisoner of Azkaban')

