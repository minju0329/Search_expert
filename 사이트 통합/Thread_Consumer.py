import threading
import logging
import re
from pymongo import MongoClient
import time
import datetime
import json
from kafka import KafkaConsumer
from json import loads
from pymongo.errors import BulkWriteError
#from Thread_Analysis import analyzerProject



class Consumer(threading.Thread):


    client =  MongoClient('localhost:27017')
    db = None

    def __init__(self, site):
        threading.Thread.__init__(self)
        self.site = site
        self.collections = ['AuthorRelation', 'QueryKeyword', 'AuthorPapers', 'Author', 'Rawdata']
        self.dbs = {}
        db = self.client[self.site]

        db2 = self.client['PUBLIC']
        self.dbs['public_QueryKeyword'] = db2.QueryKeyword
        for col in self.collections :
            self.dbs[col] = db[col]

        bootstrap_servers = ["203.255.92.48:9092"]
        self.consumer = KafkaConsumer(
            self.site,
            bootstrap_servers=bootstrap_servers,
            group_id=self.site,
            enable_auto_commit=True,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x))
        #logging.warning("Consumer on")


    # def  progress_rate(self, keyId):
    #
    #     doc = QueryKeyword.find({"_id": keyId})
    #     for i in doc:
    #         pre_progress, pre_state =i["progress"], i["state"]
    #         return pre_progress, pre_state
    #     return 0, 0

    def processing_rate(self, data, keyId, Author_info_Dic):         # 진행률, 진행 상태 함수
        # db_progress, db_state = progress_rate(keyId, site)  # db에 저장되어있는 진행, 상태 받아오기

        raw_progress = data["progress"] * 100  # raw data 진행률 받아오기

        if (self.pre_progress + 5 <= raw_progress ) and (raw_progress != 100):
            print(str(raw_progress))
            self.dbs['QueryKeyword'].update({"_id": keyId}, {'$set': {"progress": str(raw_progress)}})
            self.pre_progress = raw_progress
        elif (raw_progress == 100):
            print(str(raw_progress))
            dt = datetime.datetime.now()
            Author_info_list = []
            count = self.dbs['Rawdata'].count({"keyId": keyId})
            idcount = self.dbs['AuthorPapers'].count()+1
            self.dbs['QueryKeyword'].update({"_id": keyId}, {'$set': {"progress": 0, "state": 1, "data": count, "crawl_time": dt.strftime("%Y-%m-%d %H:%M:%S")}})
            for i in Author_info_Dic.keys():
                Author_info_list.append({'A_ID':i,'keyId':keyId, 'papers':Author_info_Dic[i],'_id':idcount})
                idcount +=1
            try:
                # print(Author_info_list)
                if Author_info_list:
                    self.dbs['AuthorPapers'].insert_many(Author_info_list)
            except BulkWriteError as bwe:
                print(bwe.details)
                raise
            self.pre_progress = 0
            if self.site == 'NTIS':
                analyzerProject(keyId, self.site).start()
            else :
                analyzerScienceon(keyId, self.site).start()

    def getA_Id(self, ID):                     # ID중복확인
        search_Id = self.dbs['Author'].find_one({"_id":ID})
        return search_Id['_id'] if search_Id is not None else 0

    # ID를 모를 때, 이름과 소속으로 ID찾기
    def searchA_ID(self, name, inst):
        search_Id = self.dbs['Author'].find_one({"name": name, "inst": inst})
        return  search_Id['_id'] if search_Id is not None else 0

    def checkAndUpdateInst(self, name, mng_ID, inst):                     #  책임연구자, 참여연구자 두 경우 모두 존재할 때, 책임연구자 소속으로 부여
        mng_rsc = self.dbs['Author'].find_one({"_id":mng_ID,"name":name})
        if mng_rsc is not None and mng_rsc["inst"] != inst:
            self.dbs['Author'].update({"name":name, "_id":mng_ID},{'$set':{"inst":inst}})
        return 0

    def getAIds(self, data):
        A_id = []
        if self.site == 'NTIS' :
            mng_Name = re.sub('["]', "", data["mng"]).strip()
            if data['perfAgent']['@code']=='03' and mng_Name != "null" :
                self.dbs['Rawdata'].insert_one(data)
                #mng_ID = re.sub('["]', '', data["mngId"]).replace('ntis:B551186-B551186>HMO.', '')
                Inst = re.sub('"', "", data["ldAgency"])
                # 책임연구자 db저장
                if data["mngId"] == 'null' or data["mngId"] == None:
                    tempAid = self.searchA_ID(mng_Name, Inst)

                    if tempAid == 0:
                        mng_ID = 's'+ str(self.cnt)
                        self.cnt +=1
                    else:
                        mng_ID = tempAid
                        self.checkAndUpdateInst(mng_Name, mng_ID, Inst)          # 책임 연구자, 참여 연구자 모두 해당 될 경우 책임연구자 소속으로 소속 변경
                elif 'HMO.' in data["mngId"]:
                    mng_ID = re.sub('"', '', data["mngId"]).split('.')[1]
                else:
                    mng_ID =  re.sub('"', '', data["mngId"])

                if self.getA_Id(mng_ID) == 0:                      # Author에 저장되어 있는지 확인. 없으면 0, 있으면 해당 id
                    result = {'_id': mng_ID, 'name': mng_Name, 'inst': Inst}
                    self.dbs['Author'].insert_one(result)               # 책임 연구자 db넣기

                A_id.append(mng_ID)

                # 참여연구자 db저장
                if data["rsc"] != None and data["rscId"] != None:
                    rscs   = re.sub('"', "", data["rsc"]).split(";")
                    rscIds = re.sub('"', "", data["rscId"]).split(";")
                    for name, ID in zip(rscs, rscIds):
                        r_rsc_name = re.sub('".', "", name).strip()
                        rsc_name = re.sub(r'<span class=\\search_word\\>(.+)</span>','',r_rsc_name)
                        rsc_Id   = re.sub('HMO.', "", ID).strip()
                        _id = -1
                        if rsc_Id != "없음" and rsc_Id != 'null':

                            _id = self.getA_Id(rsc_Id)

                            if _id == 0:  # 중복이 없으면
                                result = {'_id': rsc_Id, 'name': rsc_name, 'inst': Inst}
                                self.dbs['Author'].insert(result)
                                _id = rsc_Id

                            # A_id.append(_id)  # 배열 출력

                        elif rsc_Id == "없음" or rsc_Id == 'null':
                            _id = self.searchA_ID(rsc_name, Inst)  # 이름과 소속으로 못찾으면
                            if _id == 0:
                                _id = 's' + str(self.cnt)  # ID부여
                                self.cnt +=1
                                result = {'_id': _id, 'name': rsc_name, 'inst': Inst}
                                # print(result)
                                self.dbs['Author'].insert(result)

                        A_id.append(_id)

        else :   # SCIENCEON
            self.dbs['Rawdata'].insert_one(data)
            names = data["author"].split(";")
            author_inst = data["author_inst"].strip().split(";")
            issuing = data["issue_inst"]

            if not author_inst:  # 저자의 소속이 없을때
                for name in names:
                    tempId = self.searchA_ID(name, issuing)
                    if tempId == 0:  # 저자와발행기관중복비교
                        tempId = self.cnt  # 중복이 없으면 아이디 부여
                        self.cnt +=1
                        result = {"_id": tempId, "name": name.strip(), "inst": issuing}
                        self.dbs['Author'].insert_one(result)
                    A_id.append(tempId)
                    # else:
                    #     a_id.append(tempId)
            else:
                for Name,inst in zip(names, author_inst):  # 이름, 소속 둘다 있으면
                    Inst = inst.strip(Name)  # 소속에서 이름제거
                    tempInst = Inst[1:len(Inst) - 1]
                    if 'affiliationid' in tempInst:
                        tempInst = tempInst.split('<affiliationid')[0]
                    tempId = self.searchA_ID(Name.strip(), tempInst)

                    if tempId == 0:  # 중복비교
                        tempId = self.cnt  # 중복이 없으면 아이디 부여
                        self.cnt +=1
                        result = {"_id": tempId, "name": Name.strip(), "inst": tempInst}
                        self.dbs['Author'].insert_one(result)
                    A_id.append(tempId)

        return A_id




    def run(self):
        logging.warning(self.site+"_Consumer on")
        self.cnt = self.dbs['Author'].count() + 1
        # consumeMessage()
        Author_info_Dic = {}
        # Author_info_list = []
        self.pre_progress = 0

        while True :
            time.sleep(1)
            logging.warning(self.site+" trying to consume messages")

            for msg in self.consumer:             # scienceon Author collection
                if self.site == 'SCIENCEON':
                    data = json.loads(msg.value)
                else:
                    data = msg.value

            # for msg in self.consumer:             # scienceon Author collection
            #     data = msg.value

                key_id = data['keyId']
                r = self.dbs['Rawdata'].find_one({'$and': [{'id': data['id']}, {'keyId': key_id}]})

                if r is not None:
                    continue
                else:
                    a_id = self.getAIds(data)
                    if len(a_id) != 0:
                        object_id = data['_id']
                        for aid in a_id:
                            if aid in Author_info_Dic.keys():
                                Author_info_Dic[aid].append(object_id)
                            else:
                                Author_info_Dic[aid] = [object_id]
                    self.processing_rate(data, key_id, Author_info_Dic)

        logging.warning(self.site+"_Consumer END")
        KafkaConsumer.close()
