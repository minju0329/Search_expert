
from Consumer import consumer

site = ['ntis', 'SCIENCEON', 'Scopus']

for i in range(len(site)):
    consumer(name=site[i]).start()         #thread 생성

