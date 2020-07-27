import threading
import time
import logging
import re
from pymongo import MongoClient
from kafka import KafkaConsumer
from json import loads

client = MongoClient('localhost:27017')
db = client.test_db
NTIS_Author = db.collect
SCIENCEON_Author = db.collect

def search(Name, Inst, site):
    if site=='NTIS':
        Author = NTIS_Author
    else:
        Author = SCIENCEON_Author

    search_Id = Author.find_one({"name": Name, "inst": Inst})
    if search_Id:
        ID = search_Id["_id"]
    else:
        return 0
    return ID

def check(ID,site):                                      #
    if site=='NTIS':
        Author = NTIS_Author
    else:
        Author = SCIENCEON_Author

    search_mngID = Author.find_one({"_id":ID})
    if search_mngID:
        return search_mngID["_id"]
    return 0

def check_inst(name, ID, Inst,site):                     #  책임연구자, 참여연구자 두 경우 모두 존재할 때, 책임연구자 소속으로 부여
    if site=='NTIS':
        Author = NTIS_Author
    else:
        Author = SCIENCEON_Author

    mng_rsc = Author.find_one({"_id":ID,"name":name})
    if mng_rsc:
        if mng_rsc["inst"]!=Inst:
            'site'+'Author'.update({"name":name, "_id":ID},{'$set':{"inst":Inst}})
    return 0
# ============================================================================================= NTIS 이름, ID, 소속 DB저장
def go_ntis(site, Consumer):
    for msg in Consumer:
        A_id = []
        mng_Name = re.sub('["]', "", msg["mng"]).strip()
        mng_ID = re.sub('[\"]', '', msg["mngId"]).replace('ntis:B551186-B551186>HMO.', '')
        Inst = re.sub('"', "", msg["ldAgency"])

        # 책임연구자 db저장
        if mng_ID == 'null':
            if search(mng_Name, Inst, site) == 0:
                mng_ID = 's'+str(NTIS_Author.count())

            else:
                mng_ID =search(mng_Name, Inst, site)

        check_inst(mng_Name, mng_ID, Inst, site)          # 책임 연구자, 참여 연구자 모두 해당 될 경우 책임연구자 소속으로 소속 변경
        result = {'_id': mng_ID, 'name': mng_Name, 'inst': Inst}

        if check(mng_ID, site) == 0:                      # Author에 저장되어 있는지 확인. 없으면 0, 있으면 해당 id
            NTIS_Author.insert_one(result)               # 책임 연구자 db넣기
            A_id.append(mng_ID)
        else:
            A_id.append(check(mng_ID,site))

        # 참여연구자 db저장
        rsc = re.sub('"', "", msg["rsc"]).split(";")
        rscID = re.sub('"', "", msg["rscId"]).split(";")

        if rsc[0]!='null' and rscID[0]!='null':
            for name, ID in zip(rsc, rscID):
                rsc_name = re.sub('".',"",name).strip()
                rsc_Id = re.sub('HMO.',"",ID).strip()

                if rsc_Id != "없음":
                    _id = check(rsc_Id, site)
                    result = {'_id': rsc_Id, 'name': rsc_name, 'inst': Inst}
                    if _id == 0:  # 중복이 없으면
                        NTIS_Author.insert(result)
                        A_id.append(rsc_Id)
                    else:  # 중복이면
                        A_id.append(_id)  # 배열 출력
                elif rsc_Id == "없음":
                    _id = search(rsc_name, Inst, site)  # 이름과 소속으로 못찾으면
                    #print(rsc_name,rsc_Id,_id)
                    if _id == 0:
                        ID = 's' + str(NTIS_Author.count())  # ID부여
                        result = {'_id': ID, 'name': rsc_name, 'inst': Inst}
                        NTIS_Author.insert(result)
                        A_id.append(ID)
                    else:
                        A_id.append(_id)
        print(site, A_id)

def go_scienceon(site, Consumer):
    for msg in Consumer:
        a_id = []
        name = msg["author"].split(";")
        author_inst = msg["author_inst"].strip().split(";")
        issuing = msg["issue_inst"]

        if not author_inst:  # 저자의 소속이 없을때
            for i in name:
                tempId = check(i, issuing, site)
                if tempId == 0:  # 저자와발행기관중복비교
                    _id = SCIENCEON_Author.count() + 1  # 중복이 없으면 아이디 부여
                    result = {"_id": _id, "name": i.strip(), "inst": issuing}
                    #logging.warning("if")
                    a_id.append(_id)
                    #logging.warning(result)
                    SCIENCEON_Author.insert_one(result)
                else:
                    a_id.append(tempId)
        else:
            for Name,inst in zip(name, author_inst):  # 이름, 소속 둘다 있으면
                Inst = inst.strip(Name)  # 소속에서 이름제거
                tempInst = Inst[1:len(Inst) - 1]
                if 'affiliationid' in tempInst:
                    tempInst = tempInst.split('<affiliationid')[0]
                tempId = search(Name.strip(), tempInst, site)

                if tempId == 0:  # 중복비교
                    #logging.warning("2_if")
                    _id = SCIENCEON_Author.count() + 1
                    result = {"_id": _id, "name": Name.strip(), "inst": tempInst}
                    SCIENCEON_Author.insert(result)
                    a_id.append(_id)
                    #logging.warning(result)
                else:
                    a_id.append(tempId)
        print(site, a_id)

class Consumer(threading.Thread):
    def __init__(self, site):
        threading.Thread.__init__(self)
        bootstrap_servers = ["203.255.92.48:9092"]
        self.consumer = KafkaConsumer(
            self.site,
            bootstrap_servers=bootstrap_servers,
            group_id='ntis',
            enable_auto_commit=True,
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda x: loads(x.decode('utf-8')))
        logging.warning("Consumer on")

    def run(self):
        if self.site == 'NTIS':
            go_ntis(self.site, self.consumer)

        elif self.site == 'SCIENCEON':
            go_scienceon(self.site, self.consumer)


