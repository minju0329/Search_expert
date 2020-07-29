from kafka import KafkaConsumer
from json import loads
from time import sleep
from pymongo import MongoClient
from bson.objectid import ObjectId
from sklearn.feature_extraction.text import TfidfVectorizer
from numpy import dot
from numpy.linalg import norm
import numpy as np
import sys, os, time, atexit, logging, random, math, json, re, signal

import datetime


logging.basicConfig(filename='./crawler2.log',level=logging.WARNING)


"""Generic linux daemon base class for python 3.x."""


def consumer():

    bootstrap_servers = ["203.255.92.48:9092"]
    consumer = KafkaConsumer(
        "NTIS",
        bootstrap_servers=bootstrap_servers,
        group_id = 'ntis',
        enable_auto_commit=True,
        auto_offset_reset='earliest',
        consumer_timeout_ms = 5000,
        value_deserializer=lambda x: loads(x.decode('utf-8')))
    logging.warning("Consumer on")
    while True :
        time.sleep(1)
        logging.warning("trying to consume messages")
        for msg in consumer:
            # print(msg.value)
            data = msg.value

            key_id = data['keyId']
            db_progress, db_state = serchpro(key_id)	#db에 저장되어있는 진행, 상태 받아오기
            raw_progress = data["progress"]*100		#raw data 진행률 받아오기

            if(db_progress!=raw_progress)and(raw_progress!=100):
                print(str(raw_progress))
                QueryKeyword.update({"_id":key_id},{'$set':{"progress":str(raw_progress)}})
            elif (raw_progress==100):
                print(str(raw_progress))
                dt = datetime.datetime.now()
                count = Rawdata.count({"keyId" : key_id})
                QueryKeyword.update({"_id":key_id},{'$set':{ "progress":0, "state":1, "data" : count, "crawl_time" : dt.strftime("%Y-%m-%d %H:%M:%S")}})
                calculateExpertFactors(key_id)
            else:
                logging.warning('s3 '+str(raw_progress))

            if data.get('id', None) is None:
                continue

            r = Rawdata.find_one({'$and':[{'id':data['id']},{'keyId':data['keyId']}]})
            if r is not None:
                continue


            else:
                mng_Name =re.sub('["]',"", data["mng"]).strip()
                if '03' in data['perfAgent'].split(',')[0] and mng_Name != "null" :
                    Rawdata.insert_one(data)
                    # logging.warning(f"value={data}")
                    A_id = []

                    Raw_HumanID = data["mngId"].replace('ntis:B551186-B551186>HMO.','')		#책임연구자 ID
                    mng_ID = re.sub('[\"]','', Raw_HumanID)
                    R_Inst = data["ldAgency"]
                    Inst = re.sub('[\"]',"",R_Inst)

                    if len(mng_ID) <=4:
                        _id = search(mng_Name, Inst)
                        if _id is not 0:
                            mng_ID = _id
                        else:
                            mng_ID='s'+str(Author.count())

                    result = {'_id':mng_ID, 'name':mng_Name, 'inst':Inst}
                    _id=check(mng_ID)
                    val=check_inst(mng_Name, mng_ID, Inst)
                    R_rsc = re.sub('[ ]',"", data["rsc"])
                    R_rscID = data["rscId"]
                    if _id == 0 and val == 0:	# 책임연구자 중복이 없으면
                        Author.insert(result)	#책임 연구자 db넣기
                        A_id.append(mng_ID)
                    else:
                        A_id.append(_id)

                    if(R_rsc is not 'null')and(len(R_rscID)is not 4):	#참여연구자가 있고 참여연구원 ID가 있을때
                        rsc = R_rsc.split(";")
                        rscID = R_rscID.split(";")
                        for name, ID in zip(rsc, rscID):
                            rsc_name = re.sub('[".]',"",name)
                            rsc_Id = re.sub('[HMO."]',"", ID)

                            if len(rsc_Id) >= 8:	#없음 아니면
                                _id=check(rsc_Id)
                                result = {'_id':rsc_Id, 'name':rsc_name, 'inst':Inst}
                                if _id == 0:	# 중복이 없으면
                                    Author.insert(result)
                                    A_id.append(rsc_Id)
                                else:		#중복이면
                                    A_id.append(_id)	#배열 출력

                            else:			#참여 연구자 ID가 없음일때(없음일때)
                                _id = search(rsc_name, Inst)	#이름과 소속으로 못찾으면
                                if _id == 0:
                                    ID='s'+str(Author.count())	#ID부여
                                    result = {'_id': ID, 'name':rsc_name, 'inst':Inst}
                                    Author.insert(result)
                                    A_id.append(ID)
                                else:
                                    A_id.append(_id)
                    else:
                        pass
                    print(A_id)

                    object_id = data['_id']

                    for i in range(len(A_id)):
                        AuthorPapers.update({'A_ID':A_id[i], 'keyId':key_id}, {'$push' :{'papers':{'$each':[object_id]}}},True,False)
#                    if len(A_id) > 1:
#                        for i in range(0,len(A_id)-1):
#                            for j in range(i+1,len(A_id)):
#                                AuthorRelation.update_one({'sourceAId':A_id[i], 'targetAId':A_id[j], 'keyId':key_id},{'$inc':{'count':1}}, True, False)
#                                AuthorRelation.update_one({'sourceAId':A_id[j], 'targetAId':A_id[i], 'keyId':key_id},{'$inc':{'count':1}}, True, False)
    logging.warning("Consumer END")
    KafkaConsumer.close()
# -- daemon control class ----------------------------------------------- #


client = MongoClient('localhost:27017')
db = client.NTIS
QueryKeyword = db.QueryKeyword
Author = db.Author
Rawdata = db.Rawdata
AuthorPapers = db.AuthorPapers
ExpertFactor = db.ExpertFactor
AuthorRelation = db.AuthorRelation
public_QueryKeyword = client.PUBLIC.QueryKeyword


"""
Explain:
To Calculate recentness Factor
"""

def recentness(prdEnd):
    rct_list = []
    for i in range(len(prdEnd)):
        meanYear = sum(prdEnd[i])/len(prdEnd[i])
        if meanYear >= 2015:
            rct = 1
        elif 2005 < meanYear <= 2014:
            rct = round((1-(2015-meanYear)*0.1),2)
        else:
            rct = 0
        rct_list.append(rct)
    return rct_list
"""
Explain:
To Calculate durability Factor
"""

def career(prdEnd):
    crr_list = []
    for i in range(len(prdEnd)):
        _max = max(prdEnd[i])
        _min = min(prdEnd[i])
        crr = _max-_min+1
        crr_list.append(crr)
    return crr_list

def durability(prdEnd):
    maxLen = []
    for i in range(len(prdEnd)):
        prdEnd[i].sort(reverse=True)

        packet = []
        tmp = []

        v = prdEnd[i].pop()
        tmp.append(v)
        # print(v)
        while(len(prdEnd[i])>0):
            vv = prdEnd[i].pop()
            # print(vv)
            if v+1 == vv:
                tmp.append(vv)
                v = vv
            else:
                packet.append(tmp)
                tmp = []
                tmp.append(vv)
                v = vv
        packet.append(tmp)
        # print(packet)
        maxLen.append(packet)
    # print(maxLen[0])
    # print(len(maxLen))

    xx_list = []
    for i in range(len(maxLen)):
        x = []
        # print(maxLen[i])
        for j in range(len(maxLen[i])):
            # print(maxLen[i][j])
            # print(len(maxLen[i][j]))
            x.append(len(maxLen[i][j]))
            # print(x)
        xx_list.append(max(x))
    # print(xx)
    return xx_list


"""
Explain:
make a prdEnd(period EndTime) to 2nd array according to papers[]
"""
"""
Explain:
To calculate qty(quantity), cont(contribution) Factor
"""
def qty(papers):
    qt = []
    for i in range(0,len(papers)):
        cnt = 0;
        cnt = math.log(len(papers[i]))
        qt.append(cnt)
    max_qt = max(qt)
    min_qt = min(qt)
    norm =  max_qt-min_qt
    quantity = []
    for i in range(0, len(qt)):
        quantity.append((qt[i]-min_qt)/norm)
    return quantity

def quality(papers):
    fund = []
    for i in range(len(papers)):
        fund_list = []
        for j in range(len(papers[i])):
            doc = Rawdata.find_one({"_id":papers[i][j]})
            fund_list.append(int(doc['totalFund'][1:-1]))
        fund.append(sum(fund_list))
    max_fund = max(fund)
    min_fund = min(fund)
    norm = max_fund-min_fund
    quality = []
    for i in range(0,len(fund)):
        quality.append((fund[i]-min_fund)/norm)
    return quality


def cos_sim(A, B):
	return dot(A, B)/(norm(A)*norm(B))

def removeSkeywords(str):
	rtv = []
	for i in range(len(str)):
		temp = re.sub('[-=+,#/\?:^$.@*\"※~&%ㆍ!』\\‘|\(\)\[\]\<\>`\'…》]', '', str[i])
		if temp != ('') :
			rtv.append(temp)
	return rtv

def acc(query, keywords):

	# sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding = 'utf-8')
	# sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding = 'utf-8')


	#doc 넣어주는거 공백 검사
	# rtv = []
	maxCosArr = []
	for i in range(len(keywords)):
		tfidf_vectorizer = TfidfVectorizer()
		keyword_str = keywords[i]
		keyword_result = removeSkeywords(keyword_str)
		tfidf_vectorizer.fit(keyword_result)

		# print(keyword_result)

		qryArr = tfidf_vectorizer.transform(query).toarray()
		# print(qryArr)
		docArr = tfidf_vectorizer.transform(keyword_result).toarray()
		# print(docArr[0])

		cosSim = []
		for i in range(len(docArr)):
			if qryArr.sum() == 0 :
				cosSim.append(0)
			else :
				cosSim.append(cos_sim(qryArr, docArr[i])[0]) #get Max cos_sim
		# print(cosSim)
		maxCos = max(cosSim)
		maxCosArr.append(maxCos)
	# print(sum(maxCosArr))
	return sum(maxCosArr) / len(maxCosArr)
	# return dot(qryArr, docArr)/(norm(qryArr)*norm(docArr))

def cont(papers, A_ID):
    point = []
    for i in range(len(papers)):
        pt = 0
        for j in range(len(papers[i])):
            doc  = Rawdata.find_one({"_id":(papers[i][j])})
            if A_ID[i] is doc['mngId'].replace('ntis:B551186-B551186>HMO.',''):
                pt = pt+10
            else:
                pt = pt+1
        point.append(pt)
    max_cont = max(point)
    min_cont = min(point)
    norm = max_cont-min_cont
    contribution = []
    for i in range(0,len(point)):
        contribution.append((point[i]-min_cont)/norm)
    return contribution


def calculateExpertFactors(key_id):
    A_ID = []
    papers = []
    qry = []
    for doc in AuthorPapers.find({"keyId": key_id}):
        A_ID.append(doc['A_ID'])
        papers.append(doc['papers'])
    for doc in public_QueryKeyword.find({"_id": key_id}):
        qry.append(doc['query_keyword'])

    a = re.sub('"',"", qry[0])
    b = a.split()
    qry_result=''
    for i in b:
        if i[0]=='!':
            pass
        else:
            qry_result += i
    #print(papers)
    print("get Accuracy")
    if(len(A_ID) != 0):
        prdEnd = []
        accuracy = []
        for i in range(len(papers)):
            _prdEnd = []
            keyword = []
            for j in range(len(papers[i])):
                _keyword = []
                for doc in Rawdata.find({"keyId":key_id, "_id":ObjectId(papers[i][j])}):
                    if(doc['prdEnd'][1:5] == 'ull') :
                        _prdEnd.append(2002)
                    else :
                        _prdEnd.append(int(float(doc['prdEnd'][1:5])))
                    _keyword.append(doc['koTitle'])
                    _keyword.append(doc['enTitle'])
                    _keyword.append(doc['koKeyword'])
                    _keyword.append(doc['enKeyword'])
                keyword.append(_keyword)
            accuracy.append(acc([qry_result], keyword))
            prdEnd.append(_prdEnd)
        print("recnet")
        rctt = recentness(prdEnd)
        crrt = career(prdEnd)
        durat = durability(prdEnd)

        print("contribution")
        contrib = cont(papers, A_ID)

        print("quality")
        qual = quality(papers)

        print("qty")
        qt = qty(papers)

        expf = []
        for i in range(len(A_ID)):
            exp = {}
            exp['A_ID'] = A_ID[i]
            exp['Key_ID'] = key_id
            exp['Productivity'] = qt[i]
            exp['Contrib'] = contrib[i]
            # exp['Contrib'] = cont(papers[i])
            exp['Durability'] = durat[i]/crrt[i]
            exp['Recentness'] = rctt[i]
            exp['Coop'] = 0
            exp['Quality'] = qual[i]
            exp['Acc'] =  accuracy[i]
            # random.random()
            expf.append(exp)

        x = ExpertFactor.insert_many(expf)
    dt = datetime.datetime.now()
    count = len(A_ID)
    QueryKeyword.update({"_id":key_id},{'$set':{"progress":100, "state":2, "experts" : count, "a_time" : dt.strftime("%Y-%m-%d %H:%M:%S")}})
    logging.warning("Calculated end : ")
    logging.warning(key_id)
    # print(x.inserted_ids)

def check_inst(name, ID, Inst):
    doc = Author.find_one({"_id":ID,"name":name})
    if doc is not None:
        if doc["inst"]!=Inst:
            result=Author.update({"name":name, "_id":ID},{'$set':{"inst":Inst}})
    return 0



def check(ID):
    doc = Author.find_one({"_id":ID})
    if doc is not None:
        return doc["_id"]
    return 0

def search(Name, Inst):
    doc = Author.find_one({"name":Name, "inst":Inst})
    if doc is not None:
        return doc["_id"]
    return 0

def serchpro(_id):
    doc = QueryKeyword.find({"_id": _id})
    for i in doc:
        pre_progress=i["progress"]

        pre_state=i["state"]
        return pre_progress, pre_state
    return 0, 0

# calculateExpertFactors(1)
# with daemon.DaemonContext():
consumer()
