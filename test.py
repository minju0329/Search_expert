import threading
import time
import logging
import re

Consumer_NTISs=[{"ldAgency":"충북대학교", "mng":"유재수!", "mngId":"ntis:B551186-B551186>HMO.12345!", "rsc":"최도진;오영호;편도웅;방민주;한건희;김현","rscID":"2010037001;2020037111;2020037222;2016037110;2015039092;2015039001"}]
Consumer_SCIENCEONs = [{"author":"복경수;전종우", "author_inst":"복경수(원광대학교);전종우(충북대학교)","issue_inst":"충북대학교"}]

def search(Name, Inst):
    search_Id = Author.find_one({"name": Name, "inst": Inst})
    if search_Id:
        return search_Id["_id"]
    return 0

def check(ID):                                      #
    search_mngID = Author.find_one({"_id":ID})
    if search_mngID:
        return search_mngID["_id"]
    return 0

def check_inst(name, ID, Inst):                     #  책임연구자, 참여연구자 두 경우 모두 존재할 때, 책임연구자 소속으로 부여
    mng_rsc = Author.find_one({"_id":ID,"name":name})
    if mng_rsc:
        if mng_rsc["inst"]!=Inst:
            result=Author.update({"name":name, "_id":ID},{'$set':{"inst":Inst}})
    return 0

def go_ntis():
    for msg in Consumer_NTISs:
        A_id = []
        mng_Name = re.sub('!', "", msg["mng"]).strip()
        mng_ID = re.sub('!', '', msg["mngId"]).replace('ntis:B551186-B551186>HMO.', '')
        Inst = re.sub('"', "", msg["ldAgency"])

        if mng_ID == '없음':
            if search(mng_Name, Inst) == 0:
                mng_ID = 's'+str(Author.count())
            else:
                mng_ID =search(mng_Name, Inst)
        check_inst(mng_Name, mng_ID, Inst)          # 책임 연구자, 참여 연구자 모두 해당 될 경우 책임연구자 소속으로 소속 변경
        result = {'_id': mng_ID, 'name': mng_Name, 'inst': Inst}

        if check(mng_ID) == 0:                      # Author에 저장되어 있는지 확인. 없으면 0, 있으면 해당 id
            Author.insert(result)  # 책임 연구자 db넣기
            A_id.append(mng_ID)
        else:
            A_id.append(check(mng_ID))
# ================================================================================================= 책임연구자 db저장
        R_rsc = re.sub('[ ]', "", msg["rsc"])
        R_rscID = msg["rscId"]

def go_scienceon():
    for msg in Consumer_SCIENCEONs:
        name = msg["author"].split(";")
        Inst = msg["author_inst"].split(";")
        issuing = msg["issue_inst"]



class Consumer(threading.Thread):
    def __init__(self, site):
        threading.Thread.__init__(self)
        self.site = site

    def run(self):
        while True:
            if self.site == 'NTIS':
                go_ntis()
            elif self.site == 'SCIENCEON':
                go_scienceon()


