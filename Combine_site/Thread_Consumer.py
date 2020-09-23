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
from Thread_Analysis_rev2 import analyzerProject
from Thread_Analysis_rev2 import analyzerPaper

class Consumer(threading.Thread):
    client =  MongoClient('localhost:27017')
    db = None

    def __init__(self, site):
        threading.Thread.__init__(self)
        self.site = site
        self.collections = ['AuthorRelation', 'QueryKeyword', 'AuthorPapers', 'Author', 'Rawdata']
        self.dbs = {}
        self.Author_info_Dic = {}
        db = self.client[self.site]

        db2 = self.client['PUBLIC']
        self.dbs['public_QueryKeyword'] = db2.QueryKeyword
        self.dbs['DBPIA_CRAWLER'] = db2.DBPIA_CRAWLER
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

    def processing_rate(self, data, keyId):         # 진행률, 진행 상태 함수
        # db_progress, db_state = progress_rate(keyId, site)  # db에 저장되어있는 진행, 상태 받아오기

        raw_progress = data["progress"] * 100  # raw data 진행률 받아오기
        if keyId not in self.pre_progress:
            self.pre_progress[keyId] = 0
        if (self.pre_progress[keyId] + 1 <= raw_progress ) and (raw_progress != 100):
            print(keyId, str(raw_progress))
            self.dbs['QueryKeyword'].update({"_id": keyId}, {'$set': {"progress": str(raw_progress)}})
            self.pre_progress[keyId] = raw_progress
        elif (raw_progress == 100):
            print(keyId,str(raw_progress))
            dt = datetime.datetime.now()
            Author_info_list = []
            count = self.dbs['Rawdata'].count({"keyId": keyId})
            idcount = self.dbs['AuthorPapers'].count()+1
            # print(sorted(self.Author_info_Dic.keys(), key = str.lower))
            # print(len(self.Author_info_Dic.keys()))
            for i in self.Author_info_Dic.keys():
                Author_info_list.append({'A_ID':i,'keyId':keyId, 'papers':self.Author_info_Dic[i],'_id':idcount})
                idcount +=1
            try:
                # print(Author_info_list)
                # print(len(Author_info_list))
                if Author_info_list:
                    self.dbs['AuthorPapers'].insert_many(Author_info_list)
                    self.Author_info_Dic = {}
            except BulkWriteError as bwe:
                print(bwe.details)
                raise

            self.dbs['QueryKeyword'].update({"_id": keyId}, {'$set': {"progress": 0, "state": 1, "data": count, "crawl_time": dt.strftime("%Y-%m-%d %H:%M:%S")}})

            del self.pre_progress[keyId]
            if self.site == 'NTIS':
                analyzerProject(keyId, self.site).start()
            else :
                # print("분석")
                analyzerPaper(keyId, self.site).start()
 #               if self.site == 'DBPIA':
#                    self.dbs['DBPIA_CRAWLER'].find_one_and_update({"_id": 4865}, {"$inc": {"total": len(Author_info_list)}})

    def getAndInsert(self, ID, name, Inst):                     # ID중복확인
        search_Id = self.dbs['Author'].find_one({"_id":ID})
        if search_Id is None :
            self.dbs['Author'].insert_one({'_id': ID, 'name': name, 'inst': Inst})
        # return search_Id['_id'] if search_Id is not None else 0


        # self.getAndInsert(mng_ID, mng_Name, Inst)                      # Author에 저장되어 있는지 확인. 없으면 0, 있으면 해당
            # result = {'_id': mng_ID, 'name': mng_Name, 'inst': Inst}

    # ID를 모를 때, 이름과 소속으로 ID찾기
    def searchA_ID(self, name, issuing, tempAids, idx):
        if len(tempAids) == 0:       # scienceon
            search_Id = self.dbs['Author'].find_one({"name": name, "inst": issuing})
        else:       # dbpia
            search_Id = self.dbs['Author'].find_one({"_id" : tempAids[idx]})
        return  search_Id['_id'] if search_Id is not None else 0

    def checkAndUpdateInst(self, name, mng_ID, inst):                     #  책임연구자, 참여연구자 두 경우 모두 존재할 때, 책임연구자 소속으로 부여
        mng_rsc = self.dbs['Author'].find_one({"_id":mng_ID,"name":name})
        if mng_rsc is not None and mng_rsc["inst"] != inst:
            self.dbs['Author'].update({"name":name, "_id":mng_ID},{'$set':{"inst":inst}})
        return 0

    def insertID(self, name, inst, tempAids, idx):
        # temp = inst
        tempId = ""
        # temp = issuing
        insertData = {}

        if len(tempAids) == 0 :
            tempId = "s"+str(self.cnt)  # 중복이 없으면 아이디 부여
            self.cnt +=1
        else :
            tempId = tempAids[idx]
            if self.site == 'DBPIA':
                insertData['hasInst'] = False            # self.dbs['Author'].insert_one({"_id": tempId, "name": name.strip(), "inst": temp, "hasInst": False})
                self.dbs['DBPIA_CRAWLER'].find_one_and_update({"_id": 4865}, {"$inc": {"total": 1}})

        insertData['_id'] = tempId
        insertData['name'] = name
        insertData['inst'] = inst

        self.dbs['Author'].insert_one(insertData)
        return tempId

    def search_info(self, id):
        temp = self.dbs['Author'].find_one({"_id": id})
        return temp
        # return (temp["name"], temp["inst"]) if temp is not None else (0, 0)

    def getAIds(self, data):
        A_id = []
        tempAids = []
        # print("getAIDS")
        if self.site == 'NTIS' :
            # print("NTIS")
            mng_Name = data["mng"]
            if data['perfAgent'] is not None and data['perfAgent']['@code'] == '03':
                Inst = data["ldAgency"].replace(' <span class=\search_word\>','').replace('</span>','')
                # 책임연구자 db저장
                if data["mngId"] == 'null' or data["mngId"] == None:    # mngId가 없는경우
                    tempAid = self.searchA_ID(mng_Name, Inst, tempAids, -1)

                    if tempAid == 0:
                        mng_ID = 's'+ str(self.cnt)
                        self.cnt +=1
                    else:   # mngId가 있는경우
                        mng_ID = tempAid
                        self.checkAndUpdateInst(mng_Name, mng_ID, Inst)        # 책임 연구자, 참여 연구자 모두 해당 될 경우 책임연구자 소속으로 소속 변경
                else :
                    idx = data["mngId"].find('.')
                    mng_ID = re.sub('"', '', data["mngId"])[idx+1:]
                    self.checkAndUpdateInst(mng_Name, mng_ID, Inst)

                self.getAndInsert(mng_ID, mng_Name, Inst)                      # Author에 저장되어 있는지 확인. 없으면 0, 있으면 해당

                data['mng']   = mng_Name
                data['mngId']= mng_ID
                A_id.append(mng_ID)

                # 참여연구자 db저장
                # print('mng ENd')
                if data["rsc"] != None and data["rscId"] != None:
                    # print('rsc Check')
                    rscs   = re.sub('"', "", data["rsc"]).split(";")
                    rscIds = re.sub('"', "", data["rscId"]).split(";")
                    rsc_names = []
                    rsc_ids = []
                    for idx in range(len(rscIds)):
                        if rscIds[idx] != "없음" and rscIds[idx] != "null":   # id가 존재하는 경우
                            rscIds[idx] = re.sub('"','',rscIds[idx])[rscIds[idx].find('.')+1:]
                            temp = self.search_info(rscIds[idx])
                            if temp is not None :
                            # if rsc_name != 0 and rsc_inst != 0:     # id만 가지고 저자 검색
                                self.getAndInsert(rscIds[idx], temp['name'], temp['inst'])
                                rsc_names.append(temp['name'])
                                rsc_ids.append(rscIds[idx])
                                A_id.append(rscIds[idx])
                            else: # 이름 체크
                                if idx<len(rscs):
                                    rsc_name = rscs[idx].replace('<span class=\search_word\>','').replace('</span>','')
                                    if rsc_name != "없음" and rsc_name != "null" and "..." not in rsc_name:
                                        self.getAndInsert(rscIds[idx], rsc_name, Inst)
                                        rsc_names.append(rsc_name)
                                        rsc_ids.append(rscIds[idx])
                                        A_id.append(rscIds[idx])
                    # print("rsc_names")
                    data["rsc"] = ';'.join(rsc_names)
                    # print("rscId")
                    data["rscId"] = ';'.join(rsc_ids)
                # print(A_id)
                self.dbs['Rawdata'].insert_one(data)
                    #=====================수정중======================

        else :   # SCIENCEON, DBPIA
            ids = []
            names = data["author"].split(";")
            author_inst = data["author_inst"]
            issuing = data["issue_inst"]
            tempAids = []
            processedNames = ""
            processedInsts = ""


            if 'author_id' in data :
                ids = data["author_id"].split(";")
                tempAids = ids
                tempAids = ids[0:len(names)]

            idx = 0
            if not author_inst: # 저자의 소속이 없을때(SCIENCEON), DBPIA
                for name in names:
                    processedNames += name+";"
                    # processedInsts.append()
                    tempId     = self.searchA_ID(name, "", tempAids, idx)
                    if tempId == 0:  # 중복 ID가 없으면
                        tempId = self.insertID(name, "", tempAids, idx)
                    idx += 1
                    A_id.append(tempId)
            else:
                if 'KISTI' in data["author_inst"]:
                    length = len(author_inst.split(';'))
                    author_inst = [author_inst.split(";")[0].replace('&lt','') for i in range(length)]
                else:
                    author_inst = author_inst.split(";")
                # idx = 0
                for Name,inst in zip(names, author_inst):  # 이름, 소속 둘다 있으면
                    # if len(tempAids) <= idx :
                    inst = inst.strip(Name)  # 소속에서 이름제거
                    Name = Name.strip()
                    # tempInst = Inst[1:len(Inst) - 1]

                    if 'affiliationId' in inst:
                        # idx = Inst.find('&lt')
                        # Inst = Inst[:idx]
                        Inst = inst.replace('affiliationId type=','').replace('&gt','')
                    processedNames += Name+";"
                    processedInsts += Inst+";"
                    # processedNames.append(Name)
                    # processedInsts.append(Inst)

                    # print(Name)
                    # print(inst)
                    tempId = self.searchA_ID(Name, Inst, tempAids, idx)
                    # print(tempId)

                    if tempId == 0:  # 중복비교idx
                        tempId = self.insertID(Name, Inst, tempAids, idx)
#                        tempId = self.cnt  # 중복이 없으면 아이디 부여
#                        self.cnt +=1
#                        result = {"_id": tempId, "name": Name.strip(), "inst": tempInst}
#                        self.dbs['Author'].insert_one(result)
#                        if self.site == "DBPIA":
#                            result = {"_id": tempId, "name": Name.strip(), "inst": tempInst}
#                            self.dbs['Author'].insert_one(result)
                    idx += 1
                    A_id.append(tempId)
            if len(processedNames) != 0 :
                data['author'] = processedNames
            if len(processedInsts) != 0 :
                data['author_inst'] = processedInsts

            data['author_id'] = ';'.join(A_id)
            self.dbs['Rawdata'].insert_one(data)
        return A_id


    def del_data(self):
        for msg in self.consumer:             # scienceon Author collection
            if self.site == 'SCIENCEON':
                data = json.loads(msg.value)
            else:
                data = msg.value

    def processingMsg(self, msg) :
        key_id = msg['keyId']
        a_id = self.getAIds(msg)
        if len(a_id) != 0:
            object_id = msg['_id']
            for aid in a_id:
                if aid in self.Author_info_Dic.keys():
                    self.Author_info_Dic[aid].append(object_id)
                else:
                    self.Author_info_Dic[aid] = [object_id]
        self.processing_rate(msg, key_id)

    def run(self):
        logging.warning(self.site+"_Consumer on")
        self.cnt = self.dbs['Author'].count() + 1

        self.pre_progress = {}
        tempRaw = {}
        # while True :
        time.sleep(1)
        logging.warning(self.site+" trying to consume messages")
        print(len(self.Author_info_Dic.keys()))
        # for msg in self.consumer:
        #     print(msg.value)

        for msg in self.consumer:
            try:
                data = msg.value
                # print(data)
                if not data:
                    # print('not data')
                    pass
                else:
                    key_id = data['keyId']
                    # print('fail checked')
                    if 'fail' in data :
                        dt = datetime.datetime.now()
                        print("producer die")
                        self.dbs['QueryKeyword'].update({"_id": key_id}, {'$set': {"progress": 100, "state": -1, "data": 0, "crawl_time": dt.strftime("%Y-%m-%d %H:%M:%S")}})
                        self.Author_info_Dic = {}
                        if key_id in self.pre_progress:
                            del self.pre_progress[key_id]
                    else :
                        # print('progress checked')
                        if(data['progress'] == 1 and 'id' not in data):
                            self.processing_rate(data, key_id)
                        else:
                            r = self.dbs['Rawdata'].find_one({'$and': [{'id': data['id']}, {'keyId': key_id}]})
                            # print('is None prev')
                            if r is not None:
                                # print('None checked')
                                if data['progress'] == 1.0 :
                                    if len(tempRaw)  == 0:
                                        self.processing_rate(data, key_id)
                                    else :
                                        tempRaw['progress'] = 1
                                        self.processingMsg(tempRaw)
                                    tempRaw = {}
                                continue
                            else:
                                if len(tempRaw) > 0:
                                   self.processingMsg(tempRaw)
                                tempRaw = data
                                if data['progress'] == 1 :
                                    tempRaw['progress'] = 1
                                    self.processingMsg(tempRaw)
                                    tempRaw = {}
            except Exception as e:
                # print(data)
                print(e)
                print("consumer die")
                dt = datetime.datetime.now()
                self.dbs['QueryKeyword'].update({"_id": key_id}, {'$set': {"progress": 100, "state": -2, "data": 0, "crawl_time": dt.strftime("%Y-%m-%d %H:%M:%S")}})
                self.Author_info_Dic = {}
                tempRaw = {}
                if key_id in self.pre_progress:
                    del self.pre_progress[key_id]

#            if tempRaw is not None:
#                tempRaw['progress'] = 1
#                self.processingMsg(tempRaw)
        logging.warning(self.site+"_Consumer END")
        self.consumer.close()
