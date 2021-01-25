#!/usr/bin/env python
# coding: utf-8

# # Data Wrangling with DataFrames Coding Quiz
# 
# Use this Jupyter notebook to find the answers to the quiz in the previous section. There is an answer key in the next part of the lesson.

# In[3]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spark_quiz").getOrCreate()
path = "data/sparkify_log_small.json"
user_log_df = spark.read.json(path)
# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "data/sparkify_log_small.json"
# 4) write code to answer the quiz questions 


# # Question 1
# 
# Which page did user id "" (empty string) NOT visit?

# In[15]:


pages_visited_by_blank_user = user_log_df.filter("userId == ''").select("page").distinct()
all_available_pages = user_log_df.select("page").distinct()
pages_not_visited_by_blank_user = all_available_pages.exceptAll(pages_visited_by_blank_user).sort("page").withColumnRenamed("page", "pages not visited")
pages_not_visited_by_blank_user.show()


# # Question 2 - Reflect
# 
# What type of user does the empty string user id most likely refer to?
# 

# In[23]:


# TODO: use this space to explore the behavior of the user with an empty string
blank_user_activity = user_log_df.filter("userId == ''") .select("firstName", "lastName", "gender", "location", "userAgent", "song")
blank_user_activity.show()


# # Question 3
# 
# How many female users do we have in the data set?

# In[39]:


user_log_df.filter("gender = 'F' and userId != ''").select("userId").dropna().distinct().count()


# # Question 4
# 
# How many songs were played from the most played artist?

# In[63]:


from pyspark.sql.functions import desc

top_artist_row = user_log_df.filter("artist IS NOT NULL and artist != ''")     .groupBy("artist").count().withColumnRenamed("count", "times_played")     .sort(desc("times_played")).first() 
top_artist = top_artist_row['artist']

print(top_artist)

songs_from_top_artist = user_log_df     .filter(user_log_df.artist == top_artist)     .select('song').distinct().count()

print(songs_from_top_artist)


# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
# 
# 

# In[5]:


from pyspark.sql.functions import isnan, count, when, col, desc, udf, col, sort_array, asc, avg
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

function = udf(lambda ishome : int(ishome == 'Home'), IntegerType())

#shouldn't matter if desc or asc.
user_window = Window.partitionBy('userID').orderBy(desc('ts')).rangeBetween(Window.unboundedPreceding, 0)

cusum = user_log_df.filter((user_log_df.page == 'NextSong') | (user_log_df.page == 'Home')) \
.select('userID', 'page', 'ts') \
.withColumn('homevisit', function(col('page'))) \
.withColumn('period', Fsum('homevisit').over(user_window))

# cusum_1138 = cusum.filter('userId == 1138')
# cusum_1138_count = cusum_1138.count()
# cusum_1138.select('userId', 'ts', 'page', 'period').show(cusum_1138_count)
cusum.filter(cusum.page == 'NextSong') \
        .groupBy('userID', 'period') \
        .agg({'period':'count'})  \
        .agg({'count(period)':'avg'}) \
        .show()

