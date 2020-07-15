from threading import Thread
import threading

class consumer(threading.Thread):
    def run(self):
        for i in range(100):
            print(threading.currentThread().getName())

