#!/usr/bin/env python
# coding: utf-8

# In[1]:


from bs4 import BeautifulSoup
import requests
import pandas as pd
get_ipython().system('pip install mysql-connector-python')
import mysql.connector 
from mysql.connector import errorcode


# In[2]:


def movie(web_content):
    movie = web_content.select('b a')
    movie = [i.get_text() for i in movie] 
    movie = movie[:100]
    movie = [x.replace("â\x80\x99", "'") for x in movie]
    return movie


# In[3]:


def dbo(web_content):
    dbo = web_content.find_all('td', align='right')
    dbo = [td.text for td in dbo]
    dbos =[]
    for number in list(range(0,len(dbo))):
        if number%3 == 0:
            dbos.append(dbo[number])
    dbos = [x[1:] for x in dbos]
    dbos = [x.replace(',', '') for x in dbos]
    dbos = [int(x) for x in dbos]
    return dbos


# In[4]:


movies = []
dbos = []

for i in list(range(0,10)):
    if i == 0: 
        url = 'https://www.the-numbers.com/box-office-records/domestic/all-movies/cumulative/all-time'
    else: 
        url = 'https://www.the-numbers.com/box-office-records/domestic/all-movies/cumulative/all-time/{}01'.format(i)
    html_page = requests.get(url)
    web_content = BeautifulSoup(html_page.content, 'html.parser')
    movies += movie(web_content)
    dbos += dbo(web_content)
    


# In[5]:


df = pd.DataFrame([movies, dbos]).transpose()
df.columns = ['Movie', 'Domestic Box Office']
df


# In[11]:


cnx = mysql.connector.connect(
    host = 'database-1.cupf7l8r9ow5.us-east-2.rds.amazonaws.com',
    user = 'newuser',
    passwd = 'Movie-Project!123',
    database = 'Movies_DB'
)
print(cnx)

cursor = cnx.cursor()


# In[12]:


DB_NAME = 'Movies_DB'

TABLES = {}
TABLES['MBO'] = ("CREATE TABLE MBO("
                       " id INTEGER PRIMARY KEY AUTO_INCREMENT,"
                       " movies TEXT,"
                       " dbo varchar(30)"
                       ")ENGINE = InnoDB")

for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description) #inserts the information of the table to the database 
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

cursor.close()
cnx.close()


# In[13]:


cnx = mysql.connector.connect(
    host = 'database-1.cupf7l8r9ow5.us-east-2.rds.amazonaws.com',
    user = 'newuser',
    passwd = 'Movie-Project!123',
    database = 'Movies_DB'
)
print(cnx)

cursor = cnx.cursor()


# In[14]:


def insert_movies(df):
    for idx, row in df.iterrows():
        cursor.execute("""
                       INSERT INTO Movies_DB.MBO(movies, dbo)
                       VALUES ("{}", "{}")
                       """.format(df.iloc[idx]['Movie'], df.iloc[idx]['Domestic Box Office']))
    cnx.commit()
insert_movies(df)


# In[6]:


# url = requests.get('https://www.the-numbers.com/box-office-records/domestic/all-movies/cumulative/all-time')
# url.content
# web_content = BeautifulSoup(url.content, 'html.parser')
# movie = web_content.select('b a')
# movie = [i.get_text() for i in movie] 
# movie = movie[:100]
# movie = [x.replace("â\x80\x99", "'") for x in movie]
# movie


# In[7]:


# dbo = web_content.find_all('td', align='right')
# dbo = [td.text for td in dbo]
# dbos =[]
# for number in list(range(0,len(dbo))):
#     if number%3 == 0:
#         dbos.append(dbo[number])
# dbos = [x[1:] for x in dbos]
# dbos = [x.replace(',', '') for x in dbos]
# dbos = [int(x) for x in dbos]
# dbos


# In[ ]:




