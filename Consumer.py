import  threading

class Consummer(threading.Thread):

    def run(self):
        return print(self)


