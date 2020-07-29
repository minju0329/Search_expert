
from test import Consumer


def main():
    site = ['NTIS', 'SCIENCEON']

    for i in range(len(site)):
        Consumer(site[i]).start()    # consumer 객체생성

if __name__ == "__main__":
    main()