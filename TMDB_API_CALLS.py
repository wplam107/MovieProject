#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import json
import pandas as pd
import numpy as np


# In[ ]:


#### Reponses of movies arranged by user rating desc, year > 2000, vote_count > 100 ####
# function returns a list of movie dictionaries

def tmdb_movies(pages): #integer argument specifying how many pages, each page = 20 results
    responses = [] #blocks of 20 movies as response
    for i in range(1,(pages + 1)):
        r = requests.get("https://api.themoviedb.org/3/discover/movie?api_key=4ae91e0768b7d391f170e698fbd8bf8d&language=en-US&sort_by=vote_average.desc&include_adult=false&include_video=false&page={}&primary_release_date.gte=2000-01-01&vote_count.gte=100&with_original_language=en".format(i))
        responses.append(r)

    movie_dicts = [] #list of movie dictionaries
    for r_block in responses:
        dict_list = r_block.json()['results']
        for movie_dict in dict_list:
            movie_dicts.append(movie_dict)
    return movie_dicts


# In[ ]:


#### Responses for cast and crew by movie id ####
# function returns list of reponses with cast and crew

def cast_crew(movie_dicts): #list of dictionaries of movies argument
    id_list = [ movie['id'] for movie in movie_dicts ]
    cast_crew_rs = []
    for movie_id in id_list:
        r = requests.get("https://api.themoviedb.org/3/movie/{}/credits?api_key=4ae91e0768b7d391f170e698fbd8bf8d".format(movie_id))
        cast_crew_rs.append(r)
    return cast_crew_rs


# In[ ]:


#### Responses for keywords from movie id ####
# function returns list of reponses with movies and keywords

def keywords_of_movies(movie_dicts): #list of dictionaries of movies argument
    id_list = [ movie['id'] for movie in movie_dicts ]
    keyword_rs = []
    for movie_id in id_list:
        r = requests.get("https://api.themoviedb.org/3/movie/{}/keywords?api_key=4ae91e0768b7d391f170e698fbd8bf8d".format(movie_id))
        keyword_rs.append(r)
    return keyword_rs


# In[ ]:


# movie dictionaries
movie_dicts = tmdb_movies(50)
len(movie_dicts)


# In[ ]:


# cast crew responses
cast_crew_rs = cast_crew(movie_dicts)
len(cast_crew_rs)


# In[ ]:


# keyword responses
keyword_rs = keywords_of_movies(movie_dicts)
len(keyword_rs)


# In[ ]:


#### Function returns list of movie dictionaries from list of responses ####

def responses_to_dict(responses):
    rs_to_dicts = [ r.json() for r in responses ]
    return rs_to_dicts


# In[ ]:


# convert cc and keywords responses to cc dicts and keywords dicts
cc_dicts = responses_to_dict(cast_crew_rs)
print(len(cc_dicts))
key_dicts = responses_to_dict(keyword_rs)
print(len(key_dicts))


# In[ ]:


#### Function returns list of casting credit dictionaries {'actor_id':v, 'name':v, 'movie_id':v} ####

def cast_dicts(cc_dicts): #takes in the list of dictionaries of movies w/cast&crew
    all_cast = []
    for cc_dict in cc_dicts:
        for cast in cc_dict['cast']:
            cast_mem = {'credit_id': cast['credit_id'], 'actor_id': cast['id'], 'name': cast['name'], 'movie_id': cc_dict['id']}
            all_cast.append(cast_mem)
    return all_cast


# In[ ]:


#### Function returns list of director credit dictionaries {'director_id':v, 'name':v, 'movie_id':v} ####

def director_dicts(cc_dicts):
    all_dir = []
    for cc_dict in cc_dicts:
        for crew in cc_dict['crew']:
            if crew['job'] == 'Director':
                director = {'director_id': crew['id'], 'name': crew['name'], 'movie_id': cc_dict['id']}
                all_dir.append(director)
    return all_dir


# In[ ]:


#### Function returns list of keyword dictionaries {'movie_id':v, 'keyword':v} for every keyword in every movie ####

def kw_for_mov_dicts(key_dicts):
    all_kw = []
    for key_dict in key_dicts:
        for keyword in key_dict['keywords']:
            kw = {'keyword_id': keyword['id'], 'keyword': keyword['name'], 'movie_id': key_dict['id']}
            all_kw.append(kw)
    return all_kw


# In[ ]:


# creating list of dictionaries for individual cast members and list of dictionaries for individual directors
all_cast = cast_dicts(cc_dicts)
print(len(all_cast))
all_dir = director_dicts(cc_dicts)
print(len(all_dir))


# In[ ]:


# creating list of dictionaries for keywords
all_kw = kw_for_mov_dicts(key_dicts)
len(all_kw)


# In[ ]:


#### Connect to MySQL ####
import mysql.connector
from mysql.connector import errorcode

import config
cnx = mysql.connector.connect(
    host = config.credentials['host'],
    user = config.credentials['user'],
    passwd = config.credentials['passwd']
)
print(cnx)
cursor = cnx.cursor()


# In[ ]:


#### Create MySQL database ####
def create_database(cursor, database):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(database))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

def create_db_w_err_code(db_name):
    try:
        cursor.execute("USE {}".format(db_name))

    except mysql.connector.Error as err:
        print("Database {} does not exists.".format(db_name))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor, db_name)
            print("Database {} created successfully.".format(db_name))
            cnx.database = db_name
        else:
            print(err)
            exit(1)


# In[ ]:


# creating database "Movies_DB"
db_name = 'Movies_DB'
create_db_w_err_code(db_name)


# In[ ]:


#### Connect to Movies_DB database ####
import mysql.connector
from mysql.connector import errorcode

import config
cnx = mysql.connector.connect(
    host = config.credentials['host'],
    user = config.credentials['user'],
    passwd = config.credentials['passwd'],
    database = 'Movies_DB'
)
print(cnx)
cursor = cnx.cursor()


# In[ ]:


#### Create tables ####
def create_tables(TABLES):
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")


# In[ ]:


#### Tables: top_movies, acting_creds, direct_creds, keywords ####
TABLES = {}
TABLES['top_movies'] = (
    """
    CREATE TABLE top_movies
        (
        movie_id INT,
        title TEXT,
        release_date DATE,
        vote_count INT,
        vote_avg FLOAT,
        PRIMARY KEY (movie_id)
        )
        ENGINE=InnoDB
    """
    )

TABLES['acting_creds'] = (
    """
    CREATE TABLE acting_creds
        (
        credit_id varchar(30),
        actor_id INT,
        name TEXT,
        movie_id INT,
        PRIMARY KEY (credit_id, movie_id),
        CONSTRAINT fk_ac FOREIGN KEY (movie_id)
            REFERENCES top_movies (movie_id) ON DELETE CASCADE
        )
        ENGINE=InnoDB
    """
    )

TABLES['direct_creds'] = (
    """
    CREATE TABLE direct_creds
        (
        director_id INT,
        name TEXT,
        movie_id INT,
        PRIMARY KEY (director_id, movie_id),
        CONSTRAINT fk_dc FOREIGN KEY (movie_id)
            REFERENCES top_movies (movie_id) ON DELETE CASCADE
        )
        ENGINE=InnoDB
    """
    )

TABLES['keywords'] = (
    """
    CREATE TABLE keywords
        (
        keyword_id INT,
        keyword TEXT,
        movie_id INT,
        PRIMARY KEY (keyword_id, movie_id),
        CONSTRAINT fk_kw FOREIGN KEY (movie_id)
            REFERENCES top_movies (movie_id) ON DELETE CASCADE
        )
        ENGINE=InnoDB
    """
    )


# In[ ]:


# create tables
create_tables(TABLES)


# In[ ]:


#### Insert top_movies data to database function ####
def insert_top_movies(movie_dicts):
    i = 0
    for movie in movie_dicts:
        cursor.execute(
                    """
                    INSERT INTO Movies_DB.top_movies (movie_id, title, vote_count, vote_avg, release_date)
                    VALUES ("{}", "{}", "{}", "{}", "{}")
                    """
                    .format(movie['id'], movie['title'], movie['vote_count'], movie['vote_average'], movie['release_date'])
                    )
        i += 1
        if i % 100 == 0:
            print('Movie {} inserted.'.format(i))
        
    cnx.commit()


# In[ ]:


# Insert top_movies
insert_top_movies(movie_dicts)


# In[ ]:


#### Convert dicts into dataframe and clean names function ####
def clean_names(all_cast):
    df = pd.DataFrame.from_dict(all_cast)
    for idx, name in enumerate(df['name']):    
        df['name'][idx] = df['name'][idx].replace('"', '')
    return df


# In[ ]:


# Clean actor names
df_cast = clean_names(all_cast)


# In[ ]:


#### Insert actings credits to database from cleaned dataframe function ####
def insert_acting_creds(df):
    i = 0
    for idx, row in df.iterrows():
        cursor.execute(
                    """
                    INSERT INTO Movies_DB.acting_creds (credit_id, actor_id, name, movie_id)
                    VALUES ("{}", "{}", "{}", "{}")
                    """
                    .format(df.iloc[idx]['credit_id'], df.iloc[idx]['actor_id'], df.iloc[idx]['name'], df.iloc[idx]['movie_id'])
                    )
        i += 1
        if i % 1000 == 0:
            print('Acting Credit {} inserted.'.format(i))
    print('Done')
    cnx.commit()


# In[ ]:


# Insert acting credits
insert_acting_creds(df_cast)


# In[ ]:


# cursor.execute("""
# DROP TABLE keywords;
# """)


# In[ ]:


# Clean director names
df_dir = clean_names(all_dir)


# In[ ]:


#### Insert directing credits function ####
def insert_director_creds(df):
    i = 0
    for idx, row in df.iterrows():
        cursor.execute(
                    """
                    INSERT INTO Movies_DB.direct_creds (director_id, name, movie_id)
                    VALUES ("{}", "{}", "{}")
                    """
                    .format(df.iloc[idx]['director_id'], df.iloc[idx]['name'], df.iloc[idx]['movie_id'])
                    )
        i += 1
        if i % 100 == 0:
            print('Director Credit {} inserted.'.format(i))
    print('Done')
    cnx.commit()


# In[ ]:


# Insert directors
insert_director_creds(df_dir)


# In[ ]:


#### Insert keywords function ####
def insert_keywords(all_kw):
    i = 0
    for keyword in all_kw:
        cursor.execute(
                    """
                    INSERT INTO Movies_DB.keywords (keyword_id, keyword, movie_id)
                    VALUES ("{}", "{}", "{}")
                    """
                    .format(keyword['keyword_id'], keyword['keyword'], keyword['movie_id'])
                    )
        i += 1
        if i % 500 == 0:
            print('Keyword {} inserted.'.format(i))
    print('Done')
    
    cnx.commit()


# In[ ]:


# Insert keywords into database
insert_keywords(all_kw)


# ### SQL Queries & Charts

# In[1]:


#### Connect to Movies_DB database ####
import mysql.connector
from mysql.connector import errorcode

import config
cnx = mysql.connector.connect(
    host = config.credentials['host'],
    user = config.credentials['user'],
    passwd = config.credentials['passwd'],
    database = 'Movies_DB'
)
print(cnx)
cursor = cnx.cursor()


# In[2]:


import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np


# In[3]:


q = """
SELECT
    keyword,
    COUNT(keyword)
FROM
    Movies_DB.top_movies top
INNER JOIN
    Movies_DB.MBO bo
ON
    top.title = bo.movies
INNER JOIN
    Movies_DB.keywords kw
ON
    top.movie_id = kw.movie_id
GROUP BY
    keyword
ORDER BY
    COUNT(keyword) DESC
;
"""


# In[4]:


cursor.execute(q)
data = cursor.fetchall()


# In[5]:


keyword_count = pd.DataFrame(data)
keyword_count.rename(columns={0:'Keyword', 1:'Count'}, inplace=True)
keyword_count.info()


# In[6]:


plt.figure(figsize=(20,10))

kw = sns.barplot(x="Keyword", y="Count", data=keyword_count[:40],
            label="Keyword Count", color="b")

kw.set_xticklabels(labels=keyword_count['Keyword'][:40] , rotation=90)

plt.show()


# In[7]:


q2 ="""
SELECT
    DISTINCT name,
    ABO
FROM
    Movies_DB.top_movies top
INNER JOIN
    Movies_DB.MBO bo
ON
    top.title = bo.movies
JOIN
    Movies_DB.direct_creds dc
ON
    top.movie_id = dc.movie_id
JOIN
    Movies_DB.Dlist dl
ON
    dc.name = dl.Director
INNER JOIN
    Movies_DB.keywords kw
ON
    top.movie_id = kw.movie_id
WHERE
    kw.keyword = 'based on novel or book'
ORDER BY
    ABO DESC
LIMIT 10
;
"""


# In[8]:


cursor.execute(q2)
data2 = cursor.fetchall()


# In[9]:


top_dir = pd.DataFrame(data2)
top_dir.rename(columns={0:'Name', 1:'Average Box Office'}, inplace=True)
top_dir.info()


# In[10]:


plt.figure(figsize=(20,10))

kw = sns.barplot(x="Name", y="Average Box Office", data=top_dir,
            label="Top 10 Grossing Directors", color="r")

kw.set_xticklabels(labels=top_dir['Name'], rotation=90)

plt.show()

