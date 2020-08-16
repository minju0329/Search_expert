import threading
from kafka import KafkaConsumer
from json import loads
from time import sleep
from pymongo import MongoClient
import re
import json
import sys
import random
import math
import time
from bson.objectid import ObjectId
import logging
import datetime
import sys
import io
from sklearn.feature_extraction.text import TfidfVectorizer
from numpy import dot
from numpy.linalg import norm
import numpy as np
import re
import threading
from kafka import KafkaConsumer
from json import loads
from time import sleep
from pymongo import MongoClient
import re
import json
import sys
import random
import math
import time
from bson.objectid import ObjectId
import logging
import datetime
# import daemon
# from signal import SIGTERM

logging.basicConfig(filename='./crawler.log',level=logging.WARNING)

bootstrap_servers = ["203.255.92.48:9092"]
consumer = KafkaConsumer(
	"SCIENCEON",
	bootstrap_servers=bootstrap_servers,
	group_id = 'sr',
	enable_auto_commit=True,
	auto_offset_reset='earliest',
	consumer_timeout_ms = 5000,
	value_deserializer=lambda x: loads(x.decode('utf-8')))

logging.warning("1")
client = MongoClient('localhost:27017')
db = client.SCIENCEON
db2 = client.PUBLIC

logging.warning("2")

KCI = db2.KCI
SCI = db2.SCI
public_QueryKeyword = db2.QueryKeyword
AuthorPapers = db.AuthorPapers
QueryKeyword = db.QueryKeyword
Rawdata = db.Rawdata
Author = db.Author
AuthorRelation = db.AuthorRelation
ExpertFactor = db.ExpertFactor





# data = {"id": "NART69853904",
#         "title": "명품도시 세종, 현재와 미래!",
#         "english_title": "",
#         "author": "백기영 ; 박종광 ; 황희연 ; 제해성 ; 강병주 ; 김성종 ; 박상범",
#         "journal": "도시정보 = Urban information service",
#         "issue_inst": "대한국토·도시계획학회",
#         "issue_year": "2011",
#         "issue_lang": "kor",
#         "start_page": "3",
#         "end_page": "19",
#         "paper_keyword": "",
#         "abstract": "",
#         "english_abstract": "",
#         "author_inst": "",
#         "keyId": 33646}
#
# key_id = data["keyId"]


#////////////가중치 코드


# def norm(x):
#     return np.sqrt((x.T*x).A)

# def dot(A,B):
#     return (sum(a*b for a,b in zip(A,B)))
#
# def cos_sim(a,b):
#     return dot(a,b) / ( (dot(a,a) **.5) * (dot(b,b) ** .5) )

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

	sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding = 'utf-8')
	sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding = 'utf-8')


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




def recentness(issue_year):
    rct_list = []
    for i in range(len(issue_year)):
        meanYear = sum(issue_year[i])/len(issue_year[i])
        if meanYear >= 2015:
            rct = 1
        elif 2005 < meanYear :
            rct = round((1-(2015-meanYear)*0.1),1)
        else:
            rct = 0
        rct_list.append(rct)
    return rct_list


def career(issue_year):
    crr_list = []
    for i in range(len(issue_year)):
        _max = max(issue_year[i])
        _min = min(issue_year[i])
        crr = _max-_min+1
        crr_list.append(crr)
    return crr_list

def durability(issue_year):
    maxLen = []
    for i in range(len(issue_year)):
        issue_year[i].sort(reverse=True)

        packet = []
        tmp = []

        v = issue_year[i].pop()
        tmp.append(v)
        # logging.warning(v)
        while(len(issue_year[i])>0):
            vv = issue_year[i].pop()
            # logging.warning(vv)
            if v+1 == vv:
                tmp.append(vv)
                v = vv
            else:
                packet.append(tmp)
                tmp = []
                tmp.append(vv)
                v = vv
        packet.append(tmp)
        # logging.warning(packet)
        maxLen.append(packet)
    # logging.warning(maxLen[0])
    # logging.warning(len(maxLen))

    xx_list = []
    for i in range(len(maxLen)):
        x = []
        # logging.warning(maxLen[i])
        for j in range(len(maxLen[i])):
            # logging.warning(maxLen[i][j])
            # logging.warning(len(maxLen[i][j]))
            x.append(len(maxLen[i][j]))
            # logging.warning(x)
        xx_list.append(max(x))
    # logging.warning(xx)
    return xx_list


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
	IF = []
	for i in range(len(papers)):
		IF_list = []
		for j in range(len(papers[i])):
			doc = Rawdata.find_one({"_id":papers[i][j]})
			if doc['issue_lang'] == 'kor':
				temp =  KCI.find_one({'name':doc['issue_inst']})
				if temp is None:
					IF_list.append(1)
				else:
					IF_list.append(temp['IF']+1)
			else:
				temp =  SCI.find_one({'name':doc['issue_inst']})
				if temp is None:
					IF_list.append(3)
				else:
					IF_list.append((temp['IF']+1)*3)
		IF.append(sum(IF_list))
	max_IF = max(IF)
	min_IF = min(IF)
	norm = max_IF-min_IF
	quality = []
	for i in range(0,len(IF)):
		quality.append((IF[i]-min_IF)/norm)
	return quality

def cont(papers, name, inst):
	point = []
	for i in range(len(papers)):
		pt = 0
		for j in range(len(papers[i])):
			doc  = Rawdata.find_one({"_id":(papers[i][j])})
			if doc['author_inst'] is not "":
				indiv = doc['author_inst'].split(';')
				for k in range(0,len(indiv)):
					if indiv[k].find(name[i]) >-1 and indiv[k].find(inst[i]) > -1:
						if k == 0  or  k == (len(indiv)-1) :
							pt = pt+1
							break
						else :
							pt = pt + 1/k
							break
			else:
				indiv = doc['author'].split(';')
				for k in range(0,len(indiv)):
					if indiv[k].find(name[i]) >-1 and doc['issue_inst'].find(inst[i]) > -1:
						if k  == 0 or k == (len(indiv)-1) :
							pt = pt+1
							break
						else :
							pt = pt + 1/k
							break
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
		print(qry)
	a = re.sub('"',"", qry[0])
	b = a.split()
	qry_result=''
	for i in b:
		if i[0]=='!':
			pass
		else:
			qry_result += i
	print(qry_result)

	accuracy = []
	issue_year = []
	ll = len(papers)

	for i in range(ll):
		_issue_year = []
		keyword = [] # i author
		for j in range(len(papers[i])):
			_keyword = []
			for doc in Rawdata.find({"keyId":key_id, "_id":ObjectId(papers[i][j])}):
				_keyword.append(doc['title'])
				_keyword.append(doc['english_title'])
				_keyword.append(doc['paper_keyword'])
				_keyword.append(doc['abstract'])
				_keyword.append(doc['english_abstract'])
				_issue_year.append(int(doc['issue_year']))
			keyword.append(_keyword)

		accuracy.append(acc([qry_result], keyword))
		issue_year.append(_issue_year)

	# print(accuracy)

	name = []
	inst = []
	for i in range(0, len(A_ID)):
		n = Author.find_one({"_id":A_ID[i]})
		name.append(n['name'])
		inst.append(n['inst'])

	rctt = recentness(issue_year)
	crrt = career(issue_year)
	durat = durability(issue_year)
	contrib = cont(papers, name, inst)
	quly = quality(papers)
	quat = qty(papers)
	# accuracy = acc(qry[0], keyword)

	expf = []
	for i in range(len(A_ID)):
		exp = {}
		exp['A_ID'] = A_ID[i]
		exp['Key_ID'] = key_id
		exp['Productivity'] = quat[i]
		exp['Contrib'] = contrib[i]
		exp['Durability'] = durat[i]/crrt[i]
		exp['Recentness'] = rctt[i]
		exp['Coop'] = 0
		exp['Quality'] = quly[i]
		exp['Acc'] = accuracy[i]
		expf.append(exp)

	logging.warning("Calculated end : " + str(key_id))
	x = ExpertFactor.insert_many(expf)
	dt = datetime.datetime.now()
	count = len(A_ID)
	QueryKeyword.update({"_id":key_id},{'$set':{"progress":100, "state":2, "experts" : count, "a_time" : dt.strftime("%Y-%m-%d %H:%M:%S")}})
	logging.warning(x.inserted_ids)
	logging.warning("ExpertFactor 성공")

#logging.warning(calculateExpertFactors(key_id))
#//////////////////////////////



def check(name, inst):
    doc = Author.find_one({"name":name,"inst":inst})
    if doc is not None:
        return doc["_id"]
    return 0

def serch(_id):
    doc = QueryKeyword.find({"_id": _id})
    for i in doc:
        global pre_progress
        pre_progress=i["progress"]
        global pre_state
        pre_state=i["state"]
    return pre_progress, pre_state


def consume():

	while True :
		time.sleep(1)
		logging.warning('consume')
		for msg in consumer:
			cnt = 1
			data = json.loads(msg.value)
			max = len(data)
			logging.warning(max)
			logging.warning(cnt)
			for paper in data:
				key_id = paper["keyId"]
				raw_progress = (cnt/max)*100
				if(raw_progress!=100):
					logging.warning('s1 ' + str(raw_progress))
					QueryKeyword.update({"_id":key_id},{'$set':{"progress":str(raw_progress)}})
				elif (raw_progress==100):
					logging.warning('s2 '+ str(raw_progress))
					dt = datetime.datetime.now()
					count = Rawdata.count({"keyId" : key_id})
					QueryKeyword.update({"_id":key_id},{'$set':{ "progress":0, "state":1, "data" : count, "crawl_time" : dt.strftime("%Y-%m-%d %H:%M:%S")}})
                    # QueryKeyword.update({"_id":key_id},{'$set':{"progress":0, "state":1}})
					calculateExpertFactors(key_id)
				else:
					logging.warning('s3 '+str(raw_progress))
				cnt += 1


                #duplicate
				r = Rawdata.find_one({'$and':[{'id':paper['id']},{'keyId':key_id}]})
				if r is not None:
					continue
				else:
					Rawdata.insert_one(paper)
                    #logging.warning(f"value={paper}")
					data  = Rawdata.find({})
					a_id = []
					#logging.warning(paper)
					name = paper["author"]
					name_info = paper["author_inst"]
					issuing = paper["issue_inst"]
					name_split = name.split(";")
					name_info_split = name_info.split(";")
					#	logging.warning(name)

					#		if len(name_split)>1:                   #저자가 여러명일때
					if not name_info:               #저자의 소속이 없을때
						for i in name_split:
							tempId = check(i.strip(), issuing)
							if tempId==0:        #저자와발행기관중복비교
								_id=Author.count()+1                          #중복이 없으면 아이디 부여
								result={"_id":_id,"name":i.strip(),"inst":issuing}
								logging.warning("if")
								a_id.append(_id)
								logging.warning(result)
								Author.insert_one(result)
							else:
								a_id.append(tempId)
					else:
						for i,j in zip(name_split, name_info_split):            #이름, 소속 둘다 있으면
							info = j.strip()                        #소속 빈칸제거
							remove_info = info.strip(i)          #소속에서 이름제거
							range = len(remove_info)-1
							tempInst = remove_info[1:range]
							if 'affiliationid' in tempInst :
								tempInst = tempInst.split('<affiliationid')[0]
							tempId = check(i.strip(), tempInst)

							if tempId == 0:   #중복비교
								logging.warning("2_if")
								_id=Author.count()+1
								result={"_id":_id,"name":i.strip(),"inst":tempInst}
								Author.insert(result)
								a_id.append(_id)
								logging.warning(result)
							else :
								a_id.append(tempId)
                    #
                    # logging.warning(A_id)
					object_id = paper['_id']
					for i in a_id:
						AuthorPapers.update({'A_ID':i, 'keyId':key_id}, {'$push' :{'papers':{'$each':[object_id]}}},True,False)

					aidLen = len(a_id)
					if aidLen > 1:
						for i, val in enumerate(a_id):
							if i+1 != aidLen :
								for j, valj in enumerate(a_id):
									if i != j :
										AuthorRelation.update_one({'sourceAId':val, 'targetAId':valj, 'keyId':key_id},{'$inc':{'count':1}}, True, False)
										AuthorRelation.update_one({'sourceAId':valj, 'targetAId':val, 'keyId':key_id},{'$inc':{'count':1}}, True, False)

consume()
# calculateExpertFactors(9989)
