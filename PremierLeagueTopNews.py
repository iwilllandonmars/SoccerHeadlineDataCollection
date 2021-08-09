import re
from selenium import webdriver as wd
from selenium.webdriver.chrome.options import Options
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import *
url = "https://www.newsnow.co.uk/h/Sport/Football/Premier+League"
chroptions = Options()
chroptions.add_argument("--headless")
driver = wd.Chrome(options=chroptions)
driver.get(url)

srctxt = driver.page_source
endmarker = 'Latest News <span class="rs-topic-heading__link__icon">'
pattForTitles = '<div class="hl__inner"><a class="hll" .*>(.*?)</a>'
pattForPubs = 'data-pub=".*?">(.*?)<'
pattForTimes = 'data-time="\d{1,}">(.*?)</span>'
txt = srctxt[:srctxt.find(endmarker)]

headlines = re.findall(pattForTitles, txt)
publishers = re.findall(pattForPubs, txt)
times = re.findall(pattForTimes,txt)

df = pd.DataFrame({'headline':headlines,'publisher': publishers, 'time_of_publishing':times})
pd.set_option('display.max_colwidth',1000)
engine= create_engine('postgresql://postgres:CRXjmt27!29@localhost/SoccerData')
df.to_sql('tls', engine,if_exists='append')

unique_query = '''
         Select Distinct headline, publisher,
         time_of_publishing FROM tls
        '''
unique_table = pd.read_sql_query(unique_query, engine)
unique_table.to_sql('top_headlines', engine, if_exists='replace')

publ_query = '''SELECT top_headlines.publisher, Count(top_headlines.publisher)
as publication_counts FROM top_headlines
GROUP BY top_headlines.publisher ORDER BY publication_counts DESC'''
publ_table = pd.read_sql_query(publ_query,engine)
publ_table.to_sql('publications',engine, if_exists='replace')

import requests
from bs4 import BeautifulSoup

URL = 'https://www.newsnow.co.uk/h/Sport/Football/Premier+League/Arsenal'
page = requests.get(URL)
soup = BeautifulSoup(page.content, 'html.parser')
results = soup.find(id='nn_container')
resultss = results.find_all(class_='hl')
arsedf = pd.DataFrame(columns=['headline', 'publisher', 'time_of_publishing'])
pd.set_option('display.max_colwidth',1000)

for i in range(20): #first 150 relevant to topic "Arsenal" but just want TOP 20 HEADLINES (INDEX BEGINS AT 0,
    #ENDS AT 9)
    headline = resultss[i].find(class_='hll')
    publisher = resultss[i].find(class_='src-part')
    time = resultss[i].find(class_='time')
    if None in (headline, publisher, time):
        continue
    arsedf.loc[i] = [headline.text, publisher.text, time.text]
arsedf.to_csv("arsenalwebscapereplicausingpython.csv")
arsedf.to_sql("arstls",engine,if_exists='append')
ars_unique_query = '''
         Select Distinct headline, publisher,
         time_of_publishing FROM arstls
        '''
ars_unique_table = pd.read_sql_query(ars_unique_query, engine)
ars_unique_table.to_sql('arsenal_headlines', engine, if_exists='replace')

ars_publ_query = '''SELECT arsenal_headlines.publisher, Count(arsenal_headlines.publisher)
as publication_counts FROM arsenal_headlines
GROUP BY arsenal_headlines.publisher ORDER BY publication_counts DESC'''
publ_table = pd.read_sql_query(ars_publ_query,engine)
publ_table.to_sql('arsenal_publications',engine, if_exists='replace')
