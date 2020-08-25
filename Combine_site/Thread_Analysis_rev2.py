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
import nltk
nltk.download('punkt')
from nltk.tokenize import sent_tokenize


class Analysis(threading.Thread):

    client =  MongoClient('localhost:27017')
    db = None

    def __init__(self, keyId, site):
        threading.Thread.__init__(self)
        self.keyId = keyId
        self.site = site
        self.defaultScore = 0.02
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

    def recentness(self, pYears):
        rct_list = []
        for i in range(len(pYears)):
            meanYear = sum(pYears[i])/len(pYears[i])
            if meanYear >= 2015:
                rct = 1
            elif 2005 < meanYear <= 2014:
                rct = round((1-(2015-meanYear)*0.1),2)
            else:
                rct = 0
            rct_list.append(rct)
        return rct_list

    def career(self, pYears):
        crr_list = []
        for i in range(len(pYears)):
            _max = max(pYears[i])
            _min = min(pYears[i])
            crr = _max-_min+1
            crr_list.append(crr)
        return crr_list

    def durability(self, pYears):
        maxLen = []
        for i in range(len(pYears)):
            pYears[i].sort(reverse=True)
            packet = []
            tmp = []
            v = pYears[i].pop()
            tmp.append(v)
            while(len(pYears[i])>0):
                vv = pYears[i].pop()
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

    def minmaxNorm(self, arr):
        rtv = []
        max_val = max(arr)
        min_val = min(arr)
        norm =  max_val-min_val
        for i in range(0, len(arr)):
            if norm is 0:
                rtv.append(1)
            elif arr[i] == min_val:
                rtv.append(self.defaultScore)

            else :
                rtv.append((arr[i]-min_val)/norm)

        return rtv

    def qty(self, papers):
        qt = []
        for i in range(0,len(papers)):
            cnt = 0
            cnt = math.log(len(papers[i]))
            qt.append(cnt)
#        quantity = []
#        max_qt = max(qt)
#        min_qt = min(qt)
#        norm =  max_qt-min_qt+1
#        for i in range(0, len(qt)):
#            quantity.append((qt[i]-min_qt)/norm)
        return self.minmaxNorm(qt)



    def cos_sim(self, A, B):
        return dot(A, B)/(norm(A)*norm(B))

    def removeSkeywords(self, strArr):
        #print(strArr)
        rtv = []
        for i in range(len(strArr)):
            temp = re.sub('[-=.#/?:$}()]','', str(strArr[i]))

#            temp = re.findall(r'[A-Za-z가-힇 ]+', temp)[0]
            if temp != ('')  and temp != 'None':
                rtv.append(temp)
        return rtv

    def acc(self, query, keywords, A_ID):
        rtv = []
        for i in range(len(A_ID)):
            temp = self.calAcc(query, keywords[i])
            if temp == 0.0 :
                rtv.append(self.defaultScore)
            else :
                rtv.append(temp)
        return rtv

    def calAcc(self, query, keywords):
        maxCosArr = []
  #      print(keywords)
        query_result = self.removeSkeywords(query)
 #       print('query_result: ',query_result)
        for i in range(len(keywords)):
            tfidf_vectorizer = TfidfVectorizer()
            #print(keywords)
 #           print(query)
            keyword_str = keywords[i]
            #print('keyworid_str: ',keyword_str)
            #print(keyword_str)
            #print("acc")
            keyword_result = self.removeSkeywords(keyword_str)
#            print('keyword_result: ',keyword_result)
            if len(keyword_result) != 0 :
                tfidf_vectorizer.fit(keyword_result)

                qryArr = tfidf_vectorizer.transform(query_result).toarray()
                docArr = tfidf_vectorizer.transform(keyword_result).toarray()
#                print(qryArr)
                cosSim = []
                for i in range(len(docArr)):
                    if qryArr.sum() == 0 :
                        cosSim.append(0)
                    else :
                        cosSim.append(self.cos_sim(qryArr, docArr[i])[0]) #get Max cos_sim

                maxCos = max(cosSim)
                maxCosArr.append(maxCos)
            else:
                maxCosArr.append(0)

        return sum(maxCosArr) / len(maxCosArr)



    def storeExpertFactors(self, A_ID, rctt, crrt, durat, contrib, qual, qt, accuracy, coop):
        expf = []
        for i in range(len(A_ID)):
            exp = {}
            exp['A_ID'] = A_ID[i]
            exp['keyId'] = self.keyId
            exp['Productivity'] = qt[i]

            exp['Contrib'] = contrib[i]
            exp['Durability'] = durat[i]/crrt[i]
            exp['Recentness'] = rctt[i]
            if coop is None :
                exp['Coop'] = 0
            else :
                exp['Coop'] = coop[i]
            exp['Quality'] = qual[i]
            exp['Acc'] =  accuracy[i]

            expf.append(exp)
        x = ExpertFactor.insert_many(expf)



    def getBackdata(self, i, dataPerPage):
        A_ID = []
        papers = []

        for doc in AuthorPapers.find({"keyId":self.keyId}).skip(i*dataPerPage).limit(dataPerPage): #31323
            A_ID.append(doc['A_ID'])
            papers.append(doc['papers'])

        return  A_ID, papers


    def quality(self, _qtyBackdata):
        pass

    def cont(self, _contBackdata):
        pass

    def coop(self, _coopBackdata):
        pass

    def getRawBackdata(self, papers, A_ID):
        pass

    def run(self):
        All_count = AuthorPapers.count({"keyId":self.keyId})
        dataPerPage = 50
        print(All_count)
        progress = 0
        qry_result = ""
        qry = []
        for doc in public_QueryKeyword.find({"_id":self.keyId}):
            qry.append(doc['query_keyword'])
        a = re.sub('"',"", qry[0])
        b = a.split()
        qry_result = ''
        for i in b:
            if i[0] == '!':
                pass
            else:
                qry_result += i
        for i in range ((All_count//dataPerPage)+1):
            #print('='*50)
            A_ID, papers = self.getBackdata(i, dataPerPage)
            (pYears, keywords, _qtyBackdata, _contBackdata, _coopBackdata) = self.getRawBackdata(papers, A_ID)

            rctt = self.recentness(pYears)
            crrt = self.career(pYears)
            durat = self.durability(pYears)
            qt = self.qty(papers)
            print(keywords)
            accuracy = self.acc([qry_result], keywords, A_ID)

            coop = self.coop(_coopBackdata)
            contrib = self.cont(_contBackdata)
            qual = self.quality(_qtyBackdata)

            exft = self.storeExpertFactors(A_ID, rctt, crrt, durat, contrib, qual, qt, accuracy, coop)
            #print(exft)
            progress = i/(All_count//dataPerPage)

            QueryKeyword.update({"_id":self.keyId},{'$set':{"progress":progress}})

        dt = datetime.datetime.now()

        QueryKeyword.update({"_id":self.keyId},{'$set':{"progress":100, "state":2, "experts" : All_count, "a_time" : dt.strftime("%Y-%m-%d %H:%M:%S")}})

        logging.warning("ExpertFactor 성공")


class analyzerProject(Analysis):
    def __init__(self, keyId, site):
        super().__init__(keyId, site)

    def quality(self, totalFunds):

        return self.minmaxNorm(totalFunds)

    def cont(self, _contBackdata):
        #print(_contBackdata)
        mngIds = _contBackdata['mngIds']
        A_ID   = _contBackdata['A_ID']
        point  = []
        for i in range(len(mngIds)):
            pt = 0
            for j in range(len(mngIds[i])):
                if mngIds[i][j] != None:
                    if A_ID[i] is mngIds[i][j].replace('ntis:B551186-B551186>HMO.',''):
                        pt = pt+10
                    else:
                        pt = pt+1
            point.append(pt)

        return self.minmaxNorm(point)

    def getRawBackdata(self, papers, A_ID):
        print("get Accuracy")

        pYears = []
        keywords = []
        totalFunds = []
        mngIds = []
        qry = []


        if (len(papers) != 0):
            for i in range(len(papers)):
                _pYear = []
                _keywords = []
                fund_list = []
                _mngIds = []

                __keyword = []
                for doc in Rawdata.find({"keyId": self.keyId, "_id": {"$in" : papers[i]}}):
                    fund_list.append(int(doc['totalFund'][1:-1]))
                    _mngIds.append(doc['mngId'])

                    if doc['prdEnd'] != 'null':
                        _pYear.append(int(float(doc['prdEnd'][1:5])))
                    elif (doc['prdEnd'] == 'null') and (doc['prdStart'] != 'null'):
                        _pYear.append(int(float(doc['prdStart'][1:5])))
                    else:
                        _pYear.append(int(2000))
                    __keyword.append(doc['koTitle'])
                    __keyword.append(doc['enTitle'])
                    __keyword.append(doc['koKeyword'])
                    __keyword.append(doc['enKeyword'])

                _keywords.append(__keyword)

                totalFunds.append(sum(fund_list))
                mngIds.append(_mngIds)
                keywords.append(_keywords)
                pYears.append(_pYear)


        return pYears, keywords, totalFunds, {'mngIds' : mngIds, 'A_ID' : A_ID}, None


    def run(self):
        super().run()

class analyzerPaper(Analysis):
    def __init__(self, keyId, site):
        self.oemList = ["Hyundai", "Kia","Toyota","Honda","Nissan","General Motors", "Chevrolet","Ford motor", "Volkswagen", "Audi", "BMW", "Bayerische Motoren Werke", "Mercedes-Benz", "daimler", "Volvo", "Renault", "Jaguar", "Acura", "Mazda", "Subaru", "Suzuki", "Isuzu","Daihatsu","Peugeot","Mclaren", "Bugatti", "Rolls Royce", "Bentley", "Aston Martin", "Land Rover", "Lotus","Lexus",   "Infiniti", "Datson", "Mitsubishi", "Mitsuoka","Great Wall","Cadillac", "Tesla", "Jeep", "Dodge", "Chrysler","Porsche", "Opel", "Borgward", "Gumfut", "FIAT", "Ferrari", "Lamborghini", "Maserati","Peugeot"]

        super().__init__(keyId, site)

    def coop(self, _coopBackdata):
        score = []
        for i in range(len(_coopBackdata)):
            # scoreList = []
            point = 0
            for insts in _coopBackdata[i]:
                if insts != None :
                    for oem in self.oemList :
                        if oem in insts:
                            point = point + 1
                            break

            score.append(point)

        return self.minmaxNorm(score)

    def quality(self, _qtyBackdata):
        issueInsts = _qtyBackdata['issueInsts']
        issueLangs = _qtyBackdata['issueLangs']

        IF = []
        for i in range(len(issueInsts)):
            IF_list = []
            for j in range(len(issueInsts[i])):
                if issueLangs[i][j] == 'kor':
                    temp =  KCI.find_one(issueInsts[i][j])
                    if temp is None:
                        IF_list.append(1)
                    else:
                        IF_list.append(temp['IF']+1)
                else:
                    temp =  SCI.find_one(issueInsts[i][j])
                    if temp is None:
                        IF_list.append(3)
                    else:
                        IF_list.append((temp['IF']+1)*3)
            IF.append(sum(IF_list))

        return self.minmaxNorm(IF)

    def cont(self, _contBackdata):

        authors = _contBackdata['authors']
        authorInsts = _contBackdata['authorInsts']
        names = _contBackdata['names']
        insts = _contBackdata['insts']
        issueInsts = _contBackdata['issueInsts']

        point = []
        for i in range(len(authors)):
            pt = 0
            for j in range(len(authors[i])):
                if authorInsts[i][j] is not "" and authorInsts[i][j] != None:
                    indiv = authorInsts[i][j].split(';')
                    for k in range(len(indiv)):
                        if indiv[k].find(names[i]) >-1 and indiv[k].find(insts[i]) > -1:
                            if k == 0  or  k == (len(indiv)-1) :
                                pt = pt+1
                                break
                            else :
                                pt = (pt + 1)/k
                                break
                else:
                    indiv = []
                    if self.site == 'SCIENCEON':
                        indiv = authors[i][j].split(';')
                    else :
                        if type(authors[i][j]) == dict :
                            indiv.append(authors[i][j]['author_name'])
                        else:
                            for author in authors[i][j] :
                              #  print('aa', author)

                                indiv.append(author['author_name'])
                    for k in range(len(indiv)):
                        if (self.site == 'DBPIA' and indiv[k].find(names[i]) >-1 ) or ( indiv[k].find(names[i]) >-1  and issueInsts[i][j].find(insts[i]) > -1):
                            if k  == 0 or k == (len(indiv)-1) :
                                pt = pt+1
                                break
                            else :
                                pt = (pt + 1)/k
                                break
            point.append(pt)

        return self.minmaxNorm(point)

    def getRawBackdata(self, papers, A_ID):

        pYears = []
        keywords = []
        authorInsts = []
        authors = []
        issueInsts = []
        names = []
        insts = []
        issueLangs = []
        for i in range(len(papers)):
            _pYear = []
            _keywords = []
            _keyword =   []
            _authorInsts =[]
            _authors =    []
            _issueInsts = []
            _issueLangs = []
            for doc in Rawdata.find({"keyId": self.keyId, "_id": {"$in" : papers[i]}}):
                _keyword.append(doc['title'])
                _keyword.append(doc['english_title'])
                _keyword.append(doc['paper_keyword'])
                _keyword.append(doc['abstract'])
                _keyword.append(doc['english_abstract'])
                _pYear.append(int(doc['issue_year'][0:4]))
                _authorInsts.append(doc['author_inst'])
                _authors.append(doc['author'])
                _issueInsts.append(doc['issue_inst'])
                _issueLangs.append(doc['issue_lang'])

            authorInsts.append(_authorInsts)
            authors.append(_authors)
            issueInsts.append(_issueInsts)
            _keywords.append(_keyword)
            pYears.append(_pYear)
            issueLangs.append(_issueLangs)
            keywords.append(_keywords)


        for n in Author.find({"_id": {"$in" : A_ID}}):
            names.append(n['name'])
            insts.append(n['inst'])

        return pYears, keywords, {'issueInsts' : issueInsts, 'issueLangs' : issueLangs}, {'authors' : authors, 'authorInsts' : authorInsts, 'names' : names , 'insts' : insts, 'issueInsts' : issueInsts }, authorInsts #for coop

    def run(self):
        super().run()
