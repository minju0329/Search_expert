
from Thread_consumer import Consumer

site = ['NTIS']

for i in range(len(site)):
    Consumer(site[i]).start()    # consumer 객체생성



