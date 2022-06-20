#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
findspark.find()

import pyspark
findspark.find()

import seaborn as sns
import matplotlib.pyplot as plt


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


# In[5]:


# Leemos el fichero con textFile
date = "20220620"

rddMovies = sc.textFile("./landing/movies/date=" + date + "/movies.csv")
rddFollows = sc.textFile("./landing/movies/date=" + date + "/followsClean.csv")
rddLikes = sc.textFile("./landing/movies/date=" + date + "/likesClean.csv")


listMovies = rddMovies.collect()
for movie in listMovies:
    print(movie)


# In[6]:


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
        
    cont = 1
    x.append(outputValidation)
    x.append(date)
    x.append(cont)
    
    return x


def castTypes(x):
    x[3] = int(float(x[3]))
    x[4] = float(x[4])
    x[5] = float(x[5])
    x[7] = int(float(x[7]))
    x[20] = int(x[20])

    
    return x


# In[7]:


# Leemos el fichero con textFile
date = "20220620"

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


# In[8]:


listMovies = rddMoviesOk.collect()
for movie in listMovies:
    print(movie)


# In[9]:


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
    StructField('date', StringType(), True),
    StructField('count', IntegerType(), True)
])

dsMovies = spark.createDataFrame(rddMoviesOk, schema = movieSchema).drop('validationOut').drop('register')

dsMovies.printSchema()
dsMovies.show()
dsMovies.repartition(4).write.parquet("./staging/moviesokParquet/", mode = "overwrite", partitionBy = ["date"])


# In[10]:


movieSchemaKo = StructType([       
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
    StructField('date', StringType(), True),
    StructField('count', IntegerType(), True)
])

dsMoviesKo = spark.createDataFrame(rddMoviesKo, schema = movieSchemaKo).drop('register')
dsMoviesKo.show()
dsMoviesKo.repartition(4).write.parquet("./controls/moviesko/", mode = "overwrite", partitionBy = ["date"])


# In[11]:


dsMoviesParquet = spark.read.parquet("./staging/moviesokParquet/")
listMovies = dsMoviesParquet.collect()
for movie in listMovies:
    print(movie)


# In[12]:


for field in dsMoviesParquet.schema.fields:
    print(field.name +" , "+str(field.dataType))


# In[13]:


from pyspark.sql.functions import col
from pyspark.sql import functions as f

dsMoviesParquet.registerTempTable("movies")

dsMoviesTable = spark.sql("select * FROM movies order by score desc LIMIT 20")

#listMovies = dsMoviesTable.collect()
#for movie in listMovies:
#    print(movie)


# In[14]:


dsMoviesTable.repartition(4).write.partitionBy('date').saveAsTable("movies_processed", format = "parquet", mode = "overwrite", path = "./business/movies_processed/")


# In[15]:


dsMoviesProcessed = spark.read.parquet("./business/movies_processed/")
listMovies = dsMoviesProcessed.collect()
for movie in listMovies:
    print(movie)
    
dsMoviesProcessed.registerTempTable("movies_processed")


# In[ ]:


dsMoviesProcessed = spark.sql("select * from movies_processed")
listMovies = dsMoviesProcessed.collect()
for movie in listMovies:
    print(movie)


# In[16]:


dsMoviesGenre = dsMoviesParquet.groupBy("genre").agg(f.sum("count").alias('Count')).orderBy(col("Count").desc())
dsMoviesActor = dsMoviesParquet.groupBy("mainactor").agg(f.sum("count").alias('Count')).orderBy(col("Count").desc())
dsMoviesDirector = dsMoviesParquet.groupBy("director").agg(f.sum("count").alias('Count')).orderBy(col("Count").desc())


# In[17]:


dsMoviesGenre.show()


# In[18]:


# set plot style: grey grid in the background:
sns.set(style="darkgrid")

moviesGenreDF = dsMoviesGenre.toPandas()

plt.subplots(figsize=(12,10))
sns.barplot(y = "genre", x = "Count", data = moviesGenreDF)
plt.title('Top Genres')
plt.show()


# In[20]:


moviesGenreDF.to_csv("./business/movies_processed/reporting/date=" + date + "/moviesGenre.csv", index = False, header=True)


# In[21]:


dsMoviesActor.show()


# In[23]:


moviesActorDF = dsMoviesActor.toPandas()
moviesActorDF = moviesActorDF[:30]


fig, ax = plt.subplots(figsize=(12,10))
sns.barplot(y = "mainactor", x = "Count", data = moviesActorDF)
plt.title('Top Main Actor')
ax.set_xlim(right=3.5)
plt.show()


# In[24]:


moviesActorDF.to_csv("./business/movies_processed/reporting/date=" + date + "/moviesMainactor.csv", index = False, header=True)


# In[25]:


dsMoviesDirector.show()


# In[26]:


moviesDirectorDF = dsMoviesDirector.toPandas()
moviesDirectorDF = moviesDirectorDF[:30]

plt.subplots(figsize=(12,10))
sns.barplot(y = "director", x = "Count", data = moviesDirectorDF)
plt.title('Top Director')
plt.show()


# In[27]:


moviesDirectorDF.to_csv("./business/movies_processed/reporting/date=" + date + "/moviesDirector.csv", index = False, header=True)

