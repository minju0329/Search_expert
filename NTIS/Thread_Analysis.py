from sklearn.feature_extraction.text import TfidfVectorizer
from bson.objectid import ObjectId
from pymongo import MongoClient
from threading import Thread
from random import randint
from numpy.linalg import norm
from time import sleep
from numpy import dot
import numpy as np
import re, math, time, threading, logging, datetime, sys, io

class Analysis(threading.Thread):

    client =  MongoClient('localhost:27017')
    db = None

    def __init__(self, keyId, site):
        threading.Thread.__init__(self)
        self.keyId = keyId
        self.site = site
        global AuthorRelation, QueryKeyword, AuthorPapers, ExpertFactor, Author, Rawdata, db2, public_QueryKeyword, KCI, SCI

        db = self.client[self.site]
        db2 = self.client.PUBLIC
        public_QueryKeyword = db2.QueryKeyword
        AuthorRelation = db.AuthorRelation
        QueryKeyword = db.QueryKeyword
        AuthorPapers = db.AuthorPapers
        ExpertFactor = db.ExpertFactor
        Author = db.Author
        Rawdata = db.Rawdata
        KCI = db2.KCI
        SCI = db2.SCI

    def recentness(self, stdYear):
        rct_list = []
        for i in range(len(stdYear)):
            meanYear = sum(stdYear[i])/len(stdYear[i])
            if meanYear >= 2015:
                rct = 1
            elif 2005 < meanYear <= 2014:
                rct = round((1-(2015-meanYear)*0.1),2)
            else:
                rct = 0
            rct_list.append(rct)
        return rct_list

    def career(self, stdYear):
        crr_list = []
        for i in range(len(stdYear)):
            _max = max(stdYear[i])
            _min = min(stdYear[i])
            crr = _max-_min+1
            crr_list.append(crr)
        return crr_list

    def durability(self, stdYear):
        maxLen = []
        for i in range(len(stdYear)):
            stdYear[i].sort(reverse=True)
            packet = []
            tmp = []
            v = stdYear[i].pop()
            tmp.append(v)
            while(len(stdYear[i])>0):
                vv = stdYear[i].pop()
                if v+1 == vv:
                    tmp.append(vv)
                    v = vv
                else:
                    packet.append(tmp)
                    tmp = []
                    tmp.append(vv)
                    v = vv
            packet.append(tmp)
            maxLen.append(packet)

        xx_list = []
        for i in range(len(maxLen)):
            x = []
            for j in range(len(maxLen[i])):
                x.append(len(maxLen[i][j]))
            xx_list.append(max(x))
        return xx_list

    def qty(self, papers):
        qt = []
        for i in range(0,len(papers)):
            cnt = 0
            cnt = math.log(len(papers[i]))
            qt.append(cnt)
        max_qt = max(qt)
        min_qt = min(qt)
        norm =  max_qt-min_qt
        quantity = []
        for i in range(0, len(qt)):
            quantity.append((qt[i]-min_qt)/norm)
        return quantity

    def quality(self):
        pass    # pass

    def cos_sim(self, A, B):
        return dot(A, B)/(norm(A)*norm(B))

    def removeSkeywords(self, str):
        rtv = []
        for i in range(len(str)):
            temp = re.sub('[-=+,#/\?:^$.@*\"※~&%ㆍ!』\\‘|\(\)\[\]\<\>`\'…》]', '', str[i])
            if temp != ('') :
                rtv.append(temp)
        return rtv

    def acc(self, query, keywords):
        maxCosArr = []
        print(keywords)
        for i in range(len(keywords)):
            tfidf_vectorizer = TfidfVectorizer()
            #print(keywords)
            print(query)
            keyword_str = keywords[i]
            #print('keyword_str: ',keyword_str)
            #print(keyword_str)
            #print("acc")
            keyword_result = self.removeSkeywords(keyword_str)
            #print('keyword_result: ',keyword_result)
            tfidf_vectorizer.fit(keyword_result)

            qryArr = tfidf_vectorizer.transform(query).toarray()
            docArr = tfidf_vectorizer.transform(keyword_result).toarray()

            cosSim = []
            for i in range(len(docArr)):
                if qryArr.sum() == 0 :
                    cosSim.append(0)
                else :
                    cosSim.append(Analysis.cos_sim(qryArr, docArr[i])[0]) #get Max cos_sim

            maxCos = max(cosSim)
            maxCosArr.append(maxCos)
        return sum(maxCosArr) / len(maxCosArr)

    def cont(self):
        pass        # pass

    def storeExpertFactors(self, A_ID, rctt, crrt, durat, contrib, qual, qt, accuracy):
        expf = []
        for i in range(len(A_ID)):
            exp = {}
            exp['A_ID'] = A_ID[i]
            exp['keyId'] = self.keyId
            exp['Productivity'] = qt[i]
            exp['Contrib'] = contrib[i]
            exp['Durability'] = durat[i]/crrt[i]
            exp['Recentness'] = rctt[i]
            exp['Coop'] = 0
            exp['Quality'] = qual[i]
            exp['Acc'] =  accuracy[i]
            expf.append(exp)
        x = ExpertFactor.insert_many(expf)

        dt = datetime.datetime.now()
        count = len(A_ID)
        QueryKeyword.update({"_id" : self.keyId},{'$set':{"progress":100, "state":2, "experts" : count, "a_time" : dt.strftime("%Y-%m-%d %H:%M:%S")}})
        logging.warning("Calculated end : ")
        logging.warning(self.keyId)

    def getQrykeyword(self):
        qry = []
        for doc in public_QueryKeyword.find({"_id":self.keyId}):
            qry.append(doc['query_keyword'])

        a = re.sub('"',"", qry[0])
        b = a.split()
        qry_result=[]
        for i in b:
            if i[0]=='!':
                pass
            else:
                qry_result.append(i)
        return qry_result

    def getBackdata(self, i, dataPerPage):
        A_ID = []
        papers = []

        # A_ID, papers 생성
        for doc in AuthorPapers.find({"keyId":self.keyId}).skip((i*dataPerPage)+1).limit(dataPerPage): #31323
            A_ID.append(doc['A_ID'])
            papers.append(doc['papers'])
        return A_ID, papers

    def get_Accuracy(self):
        pass


    def run(self):
        All_count = AuthorPapers.count({"keyId":self.keyId})
        dataPerPage = 100
        qry = self.getQrykeyword()

        for i in range (0, (All_count//dataPerPage)+1):
            print('='*100)
            # self.getBackdata(i , dataPerPage)
            (A_ID, papers) = self.getBackdata(i, dataPerPage)
            (pYears, keywords) = self.get_Accuracy(papers)
            # rctt = self.recentness(stdYear)
            # crrt = self.career(stdYear)
            # durat = self.durability(stdYear)
            # contrib = analyzerProject.cont(papers, A_ID)
            # qual = analyzerProject.quality(papers)
            # qt = self.qty(papers)
            # self.storeExpertFactors(A_ID, rctt, crrt, durat, contrib, qual, qt, accuracy)

#====================================================================== PROJECT
class analyzerProject(Analysis):
    def __init__(self, keyId, site):
        super().__init__(keyId, site)

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

    def cont(papers, A_ID):
        point = []
        for i in range(len(papers)):
            pt = 0
            for j in range(len(papers[i])):
                doc  = Rawdata.find_one({"_id":(papers[i][j])})
                if doc['mngId'] != None:
                    if A_ID[i] is doc['mngId'].replace('ntis:B551186-B551186>HMO.',''):
                        pt = pt+10
                    else:
                        pt = pt+1
            point.append(pt)
        max_cont = max(point)
        min_cont = min(point)
        norm = (max_cont-min_cont)+1            # ZeroDivisionError 발생
        contribution = []
        for i in range(0,len(point)):
            contribution.append((point[i]-min_cont)/norm)
        return contribution

    def get_Accuracy(self, papers):
        print("get Accuracy")

        if (len(papers) != 0):
            pYears = []
            keywords = []

            for i in range(len(papers)):
                _pYear = []
                _keywords = []
                for j in range(len(papers[i])):         # j --> 전문가가 쓴 논문 id list
                    __keyword = []
                    for doc in Rawdata.find({"keyId": self.keyId, "_id": ObjectId(papers[i][j])}):
                        if doc['prdEnd'] != 'null':
                            _pYear.append(int(float(doc['prdEnd'][0:5])))
                        elif (doc['prdEnd'] == 'null') and (doc['prdStart']!= 'null'):
                            _pYear.append(int(float(doc['prdStart'][0:5])))
                        else:
                            _pYear.append(int(2000))
                        __keyword.append(doc['koTitle'])
                        __keyword.append(doc['enTitle'])
                        __keyword.append(doc['koKeyword'])
                        __keyword.append(doc['enKeyword'])
                    _keywords.append(__keyword)
            keywords.append(_keywords)
            pYears.append(_pYear)
            #accuracy.append(self.acc(qry_result, keyword[0]))
        return pYears, keywords

    #def calculateExpertFactors(self, keyId):

        A_ID, papers, stdYear, accuracy = getBackdata()

        print("recent")
        rctt = self.recentness()
        crrt = Analysis.career(stdYear)
        durat = Analysis.durability(stdYear)

        print("contribution")
        #contrib = analyzerNtis.cont(papers, A_ID)
        contrib = self.cont(papers, A_ID)

        print("quality")
        #qual = analyzerNtis.quality(papers)
        qual = self.quality(papers)

        print("qty")
        qt = Analysis.qty(papers)

        expf = []
        for i in range(len(A_ID)):
            exp = {}
            exp['A_ID'] = A_ID[i]
            exp['keyId'] = keyId
            exp['Productivity'] = qt[i]
            exp['Contrib'] = contrib[i]
            exp['Durability'] = durat[i]/crrt[i]
            exp['Recentness'] = rctt[i]
            exp['Coop'] = 0
            exp['Quality'] = qual[i]
            exp['Acc'] =  accuracy[i]
            expf.append(exp)
        x = ExpertFactor.insert_many(expf)

        dt = datetime.datetime.now()
        count = len(A_ID)
        QueryKeyword.update({"_id" : keyId},{'$set':{"progress":100, "state":2, "experts" : count, "a_time" : dt.strftime("%Y-%m-%d %H:%M:%S")}})
        logging.warning("Calculated end : ")
        logging.warning(keyId)
        print(x.inserted_ids)

    def run(self):
        super().run()
#====================================================================== PAPER
class analyzerScienceon(Analysis):
    def __init__(self, papers, name, inst, keyId):
        self.papers = papers
        self.name = name
        self.inst = inst
        self.keyId = keyId

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
        norm = (max_IF-min_IF)+1
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

    def calculateExpertFactors(keyId):
        A_ID = []
        papers = []
        qry = []
        for doc in AuthorPapers.find({"keyId" : keyId}):
            A_ID.append(doc['A_ID'])
            papers.append(doc['papers'])

        for doc in public_QueryKeyword.find({"_id": keyId}):
            qry.append(doc['query_keyword'])
            #print(qry)
        a = re.sub('"',"", qry[0])
        b = a.split()
        qry_result=''
        for i in b:
            if i[0]=='!':
                pass
            else:
                qry_result += i
        print("get Accuracy")
        accuracy = []
        stdYear = []
        for i in range(len(papers)):
            _stdYear = []
            keyword = [] # i author
            #print(0)
            for j in range(len(papers[i])):
                _keyword = []
                #print(1)
                for doc in Rawdata.find({"keyId" : keyId, "_id":ObjectId(papers[i][j])}):
                    #print(2)
                    #continue
                    _keyword.append(doc['title'])
                    _keyword.append(doc['english_title'])
                    _keyword.append(doc['paper_keyword'])
                    _keyword.append(doc['abstract'])
                    _keyword.append(doc['english_abstract'])
                    _stdYear.append(int(doc['issue_year']))
                #print(_keyword)
                keyword.append(_keyword)
            accuracy.append(Analysis.acc([qry_result], keyword))
            stdYear.append(_stdYear)
        print("recent")
        name = []
        inst = []
        for i in range(0, len(A_ID)):
            n = Author.find_one({"_id":A_ID[i]})
            name.append(n['name'])
            inst.append(n['inst'])

        rctt = Analysis.recentness(stdYear)
        crrt = Analysis.career(stdYear)
        durat = Analysis.durability(stdYear)
        print("contribution")
        contrib = analyzerScienceon.cont(papers, name, inst)
        print("quality")
        quly = analyzerScienceon.quality(papers)
        print("aty")
        quat = Analysis.qty(papers)
        # accuracy = acc(qry[0], keyword)

        expf = []
        for i in range(len(A_ID)):
            exp = {}
            exp['A_ID'] = A_ID[i]
            exp['keyId'] = keyId
            exp['Productivity'] = quat[i]
            exp['Contrib'] = contrib[i]
            exp['Durability'] = durat[i]/crrt[i]
            exp['Recentness'] = rctt[i]
            exp['Coop'] = 0
            exp['Quality'] = quly[i]
            exp['Acc'] = accuracy[i]
            expf.append(exp)

        #logging.warning("Calculated end : " + str(keyId))
        x = ExpertFactor.insert_many(expf)
        dt = datetime.datetime.now()
        count = len(A_ID)
        QueryKeyword.update({"_id":keyId},{'$set':{"progress":100, "state":2, "experts" : count, "a_time" : dt.strftime("%Y-%m-%d %H:%M:%S")}})
        logging.warning(x.inserted_ids)
        logging.warning("ExpertFactor 성공")

    # def run(self):
    #     self.calculateExpertFactors(keyId)
    #     Analysis.recentness(stdYear)
    #     Analysis.career(stdYear)
    #     Analysis.durability(stdYear)
    #     Analysis.qty(papers)
    #     Analysis.cos_sim(A, B)
    #     Analysis.removeSkeywords(str)
    #     Analysis.acc(query, keywords)
