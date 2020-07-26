
from test import Consumer

site = ['NTIS', 'SCIENCEON']

for i in range(len(site)):
    Consumer(site[i]).start()    # consumer 객체생성

