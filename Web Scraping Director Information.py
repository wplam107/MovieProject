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


def retrieve_directors(web_content):
    directors = web_content.select('b a')
    directors = [i.get_text() for i in directors]  # get_text() retrieves information inside the tags
    directors = directors[:100]
    return directors


# In[3]:


def domestic_BO(web_content):
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


def movie_count(web_content):
    mc = web_content.find_all('td', align='right')
    mc = [td.text for td in mc]
    mcs = []
    for number in list(range(0,len(mc))):
        if number%3 == 1:
            mcs.append(mc[number])
    mcs = [int(x) for x in mcs]
    return mcs


# In[5]:


def average_BO(web_content):
    abo = web_content.find_all('td', align='right')
    abo = [td.text for td in abo]
    abos =[]
    for number in list(range(0,len(abo))):
        if number%3 == 2:
            abos.append(abo[number])
    abos = [x[1:] for x in abos]
    abos = [x.replace(',', '') for x in abos]
    abos = [int(x) for x in abos]
    return abos 


# In[ ]:


director_name = []
domestic_bo = []
movie_counts = []
average_bo = []


for i in list(range(0,10)):
    if i == 0: 
        url = 'https://www.the-numbers.com/box-office-star-records/domestic/lifetime-specific-technical-role/director'
    else: 
        url = 'https://www.the-numbers.com/box-office-star-records/domestic/lifetime-specific-technical-role/director/{}01'.format(i)
    html_page = requests.get(url)
    web_content = BeautifulSoup(html_page.content, 'html.parser')
    director_name += retrieve_directors(web_content)
    domestic_bo += domestic_BO(web_content)
    movie_counts += movie_count(web_content)
    average_bo += average_BO(web_content)


# In[ ]:


df = pd.DataFrame([director_name, domestic_bo, movie_counts, average_bo]).transpose()
df.columns = ['Director', 'Domestic Box Office', 'No. Movies', 'Average Box Office']
print(df.shape)
df
for idx, name in enumerate(df['Director']):
    df['Director'][idx] = df['Director'][idx].replace('"', '')
df.head(20)


# In[ ]:


cnx = mysql.connector.connect(
    host = 'database-1.cupf7l8r9ow5.us-east-2.rds.amazonaws.com',
    user = 'newuser',
    passwd = 'Movie-Project!123',
    database = 'Movies_DB'
)
print(cnx)

cursor = cnx.cursor()


# In[ ]:


DB_NAME = 'Movies_DB'

TABLES = {}
TABLES['Dlist'] = ("CREATE TABLE Dlist("
                       " id INTEGER PRIMARY KEY AUTO_INCREMENT,"
                       " Director TEXT,"
                       " DBO varchar(30),"
                       " No_Movies INT,"
                       " ABO INT"
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


# In[ ]:


cnx = mysql.connector.connect(
    host = 'database-1.cupf7l8r9ow5.us-east-2.rds.amazonaws.com',
    user = 'newuser',
    passwd = 'Movie-Project!123',
    database = 'Movies_DB'
)
print(cnx)

cursor = cnx.cursor()


# In[10]:


def insert_directors(df):
    for idx, row in df.iterrows():
        cursor.execute("""
                       INSERT INTO Movies_DB.Dlist(Director, DBO, No_Movies, ABO)
                       VALUES ("{}", "{}", "{}", "{}")
                       """.format(df.iloc[idx]['Director'], df.iloc[idx]['Domestic Box Office'], df.iloc[idx]['No. Movies'], df.iloc[idx]['Average Box Office']))
    cnx.commit()
insert_directors(df)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




