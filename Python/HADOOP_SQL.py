#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
findspark.find()

import pyspark
findspark.find()


# In[2]:


#Initialize SparkSession and SparkContext
from pyspark.sql import SparkSession

#Create a Spark Session
spark = SparkSession     .builder     .master("local[1]")     .appName("MyFirstJob")     .config("spark.executor.memory", "4g")     .config("spark.cores.max","2")     .enableHiveSupport()     .getOrCreate()

#Get the Spark Context from Spark Session    
sc = spark.sparkContext

print("Spark context init done.")


# In[3]:


data = ["Project","Gutenberg’s","Alice’s","Adventures",
"in","Wonderland","Project","Gutenberg’s","Adventures",
"in","Wonderland","Project","Gutenberg’s"]

rdd=spark.sparkContext.parallelize(data)


# In[4]:


#Hacemos una prueba de que la sesión de Spark funciona

rdd2=rdd.map(lambda x: (x,1))
for element in rdd2.collect():
    print(element)


# In[ ]:


# Leemos el fichero con textFile
date = "20220616"

rddMovies = sc.textFile("./landing/movies/date=" + date + "/movies.csv")
rddFollows = sc.textFile("./landing/movies/date=" + date + "/followsClean.csv")
rddLikes = sc.textFile("./landing/movies/date=" + date + "/likesClean.csv")


listMovies = rddMovies.collect()
for movie in listMovies:
    print(movie)


# In[ ]:


listFollows = rddFollows.collect()
for follow in listFollows:
    print(follow)


# In[ ]:


listLikes = rddLikes.collect()
for like in listLikes:
    print(like)


# In[10]:


def isEmpty(x):
    if (x == None or len(x) == 0):
        return True
    else:
        return False
    
def isNumber(x):
    # Si no se puede castear a float devolvemos 0, que consideraremos un valor no válido
    try:
        return float(x)
    except:
        return 0
    


def validate(x):
    register = x[0]
    user = x[1]
    title = x[2]
    year = x[3]
    score = x[4]
    users_rating = x[5]
    votes = x[6]
    metascore = x[7]
    codirector = x[8]
    director = x[9]
    cogenre = x[10]
    genre = x[11]
    mainactor = x[12]
    secactor = x[13]
    seclanguage = x[14]
    vose = x[15]
    country = x[16]
    seccountry = x[17]


    
    # Serán buenos solamente los elementos del rdd que tienen good
    outputValidation = "good"
    
    if isEmpty(score):
        outputValidation = "badByEmpty"
        x[4] = 0
        
    if outputValidation == "good" and not isNumber(year) or not isNumber(score) or not isNumber(users_rating) or not isNumber(metascore):
        outputValidation = "badByNumber"         
        
    x.append(outputValidation)
    x.append(date)
    
    return x


def castTypes(x):
    x[3] = int(float(x[3]))
    x[4] = float(x[4])
    x[5] = float(x[5])
    x[7] = int(float(x[7]))

    
    return x


# In[ ]:


register
user
title
year
score
users_rating
votes
metascore
codirector
director
cogenre
genre
mainactor
secactor
seclanguage
vose
country
seccountry


# In[11]:


# Leemos el fichero con textFile
date = "20220616"

rddMovies = sc.textFile("./landing/movies/date=" + date + "/movies.csv")
header = rddMovies.first()
rddMovies = rddMovies.filter(lambda x: x != header).repartition(1)

# Reemplazar comas que forman parte de la misma columna
rddMoviesValidated = rddMovies.map(lambda x: x.split(',')).map(validate)
    
rddMoviesOk = rddMoviesValidated.filter(lambda x : x[18] == "good").map(castTypes)
rddMoviesKo = rddMoviesValidated.filter(lambda x : x[18] != "good")
rddMoviesOk.repartition(4).saveAsTextFile("./staging/moviesok/date=" + date + "/")
rddMoviesKo.repartition(4).saveAsTextFile("./staging/moviesko/date=" + date + "/")
rddMoviesKo = rddMoviesKo.map(castTypes)


# In[12]:


listMovies = rddMoviesOk.collect()
for movie in listMovies:
    print(movie)


# In[13]:


from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, FloatType

movieSchema = StructType([  
    StructField('register', StringType(), True),
    StructField('user', StringType(), True),
    StructField('title', StringType(), True),
    StructField('year', IntegerType(), True),
    StructField('score', FloatType(), True),
    StructField('users_rating', FloatType(), True),
    StructField('votes', StringType(), True),
    StructField('metascore', IntegerType(), True),
    StructField('codirector', StringType(), True),
    StructField('director', StringType(), True),
    StructField('cogenre', StringType(), True),
    StructField('genre', StringType(), True),
    StructField('mainactor', StringType(), True),
    StructField('secactor', StringType(), True),
    StructField('seclanguage', StringType(), True),
    StructField('vose', StringType(), True),
    StructField('country', StringType(), True),
    StructField('seccountry', StringType(), True), 
    StructField('validationOut', StringType(), True),
    StructField('date', StringType(), True)
])

dsMovies = spark.createDataFrame(rddMoviesOk, schema = movieSchema).drop('validationOut').drop('register')

dsMovies.printSchema()
dsMovies.show()
dsMovies.repartition(4).write.parquet("./staging/moviesokParquet/", mode = "overwrite", partitionBy = ["date"])


# In[ ]:


from pyspark.sql.functions import col

dsMoviesParquet.registerTempTable("movies")

