from Thread_Consumer import Consumer
from Thread_Analysis_rev2 import analyzerProject

def main():
    sites = ['DBPIA']
    #sites = ['NTIS']
    for site in sites:
         #analyzerProject(7845, site).start()
         Consumer(site).start()    # consumer 객체생성

if __name__ == "__main__":
    main()
