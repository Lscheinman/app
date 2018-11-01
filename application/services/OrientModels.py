# -*- coding: utf-8 -*-
import pyorient
import pandas as pd
import time, os, json, requests, random, jsonify, decimal

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from openpyxl import load_workbook
from openpyxl import Workbook
from threading import Thread
from passlib.hash import bcrypt
debugging = True


class OrientModel():

    def __init__(self, HDB):

        self.user = "root"
        self.pswd = "admin"
        self.Verbose = False
        self.AUTHS = ['A1', 'A2', 'A3', 'A4', 'A5', 'A6', 'B1', 'B2', 'B3', 'B4', 'B5', 'C1', 'C2', 'C3']
        self.client = pyorient.OrientDB("localhost", 2424)
        self.session_id = self.client.connect(self.user, self.pswd)        
        if HDB == 'None' or HDB == None:
            self.HDB  = None
        else:
            self.HDB = HDB
    
    def start(self):
        
        self.client = pyorient.OrientDB("localhost", 2424)
        self.session_id = self.client.connect(self.user, self.pswd)
        self.Verbose = False
        self.entities = ['Person', 'Object', 'Location', 'Event']
        self.reltypes = ['AccountCreated', 'ActionToSupport', 'AnalysisToSupport', 'BornOn', 'BornIn', 'Called', 'CalledBy', 'Canceled', 'CanceledBy',
                             'ChargedWith', 'CollectionToSupport', 'CommittedCrime', 'CreatedAt', 'CreatedBy', 'CreatedOn', 'DocumentIn', 'DocumentMentioning', 'DocumentedBy',
                             'Family', 'Follows', 'Found', 'FromFile', 'HasAttribute', 'HasRole', 'HasStatus','HasParent', 'HasSibling', 'HasChild', 'IncludesTag', 'IncludesTerm', 'Involves', 'Knows',
                             'Likes', 'LivesAt', 'LocatedAt', 'MarriedTo', 'ModifiedBy', 'ModifiedOn', 'OfType', 'On', 'OccurredAt', 'Owns', 'PartOf', 'PhotoOf',
                             'ProcessedIntel', 'Published', 'PublishedIntel', 'PublishedTask', 'Related', 'ReportedAt', 'RegisteredOn', 'ReferenceLink',
                             'RecordedBy', 'ResultedIn', 'Searched', 'SubjectOf', 'SubjectofContact', 'Supporting', 'Tagged', 'TaskedTo', 'TAReference', 'TextAnalytics',
                             'TweetLocation', 'Tweeted', 'TA_REFERENCE_SAME_SENTENCE', 'UpdatedBy', 'Witnessed', 'CloseColleagueWith', 'ColleagueWith', 'BasedAt',
                             'EmployedBy', 'CloseFriendsWith', 'WorksAt', 'OppositeCause', 'WorksFor', 'WorksWith']
        self.setDemoDataPath()
        self.AUTHS = ['A1', 'A2', 'A3', 'A4', 'A5', 'A6', 'B1', 'B2', 'B3', 'B4', 'B5', 'C1', 'C2', 'C3']
    
        # If the POLER schema doesn't exist create it
        try:
            self.openDB('POLER')
        except:
            self.initialize_reset()        

    def setDemoDataPath(self):
        if '\\' in os.getcwd():
            if debugging == False:
                self.BaseBook   = '%s\\application\\services\\data\\BaseBook.xlsx' % (os.getcwd())
                self.SocialPath = '%s\\application\\services\\data\\Social.csv' % (os.getcwd())
                self.Config     = '%s\\application\\services\\config\\' % (os.getcwd())
            else:
                self.BaseBook   = '%s\\data\\BaseBook.xlsx' % (os.getcwd()) # debugging line
                self.SocialPath = '%s\\data\\Social.csv' % (os.getcwd()) # debugging line
                self.Config     = '%s\\config\\' % (os.getcwd())
        else:
            if debugging == False:
                self.BaseBook   = '%s/application/services/data/BaseBook.xlsx' % (os.getcwd())
                self.SocialPath = '%s/application/services/data/Social.csv' % (os.getcwd())
                self.Config     = '%s/application/services/config/' % (os.getcwd())
            else:
                self.BaseBook   = '%s/data/BaseBook.xlsx' % (os.getcwd()) # debugging line
                self.SocialPath = '%s/data/Social.csv' % (os.getcwd())
                self.Config     = '%s/config/' % (os.getcwd())

    def shutdown(self):
        self.client.shutdown(self.user, self.pswd)

    def goLive(self):
        self.openDB('POLER')

    def openDB(self, DB):
        self.client.db_open(DB, self.user, self.pswd)

    def setToken(self):
        self.sessionToken = self.client.get_session_token()
        self.client.set_session_token(self.sessionToken)

    def createPOLER(self):

        try:
            self.openDB('POLER')
            self.client.command('select * from Object')
        except:
            try:
                self.client.db_create('POLER', pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_PLOCAL)
            except:
                pass
            for e in self.entities:
                self.client.command("create class %s extends V" % e)
            for r in self.reltypes:
                self.client.command("create class %s extends E" % r)

    def closeDB(self):
        self.client.db_close()

    def check_date(self, E_DATE):

        if str(type(E_DATE)) == "<class 'datetime.datetime'>":
            E_DATE = E_DATE.strftime('%Y-%m-%d %H:%M:%S')
            return E_DATE
        elif str(type(E_DATE)) == "<class 'datetime.date'>":
            E_DATE = E_DATE.strftime('%Y-%m-%d %H:%M:%S') 
            return E_DATE
        elif str(type(E_DATE)) == "<class 'pandas._libs.tslib.Timestamp'>":
            E_DATE = E_DATE.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')
            return E_DATE
        elif str(type(E_DATE)) == "<class 'pandas._libs.tslib.NaTType'>":
            E_DATE = '2000-01-01 00:00'
            return E_DATE
        
        datePatterns = ['%Y-%m-%d', '%d-%m-%Y', '%m-%d-%Y', '%Y-%d-%m', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d', '%Y/%d/%m', '%d.%m.%Y']
        for p in datePatterns:
            try:
                TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                checkedE_DATE = datetime.strftime((datetime.strptime(E_DATE[:10], p)), datePatterns[0])
                if self.Verbose == True:
                    print("[%s_ODB-check_date]: received pattern %s with %s and returned %s." % (TS, p, E_DATE, checkedE_DATE))
                return checkedE_DATE
            except:
                checkedE_DATE = datetime.strptime('2000-01-01', '%Y-%m-%d')
                if self.Verbose == True:
                    print("[%s_ODB-check_date]: received unknown pattern %s and returned %s." % (TS, E_DATE, checkedE_DATE))
                return checkedE_DATE


    def get_user(self, username):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-findUser]: process started." % (TS))

        sql = '''select GUID, LOGSOURCE, O_CLASS2, CATEGORY from Object where O_CLASS1 = '%s' ''' % username
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-findUser]: %s." % (TS, sql))
        
        print(sql)
        r = self.client.command(sql)
        if len(r) == 0:
            user = None
        else:
            user = {}
            r = r[0].oRecordData
            user['GUID'] = r['GUID']
            user['UserAuth'] = r['LOGSOURCE']
            user['UserPassword'] = r['O_CLASS2']
            user['UserType'] = r['CATEGORY']
            user['UserName'] = username
        
        print(user)
        return user


    def delete_user(self, GUID):
        # Change the password/CLASS2 into CLASS3 and set the CLASS2 to a closed code which keeps the user for records but closes access to the app
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        CLOSED_CODE = TS + "-" + str(random.randint(1000000,9999999))
        sql = '''update Object set O_CLASS3 = O_CLASS2 where TYPE = 'User' and GUID = %d
        ''' % (int(GUID))
        self.client.command(sql)
        sql = '''update Object set O_CLASS2 = 'Closed-%s' where TYPE = 'User' and GUID = %d
        ''' % (CLOSED_CODE, int(GUID))
        self.client.command(sql)
        sql = '''select O_DESC from Object where TYPE = 'User' and GUID = %d
            ''' % (int(GUID))
        r = self.client.command(sql)[0].oRecordData['O_DESC']
        DELETE_DESC = "%s\nDeleted: %s" % (r, CLOSED_CODE)
        sql = '''update Object set O_DESC = '%s' where O_TYPE = 'User' and GUID = %d
            ''' % (DELETE_DESC, int(GUID))
        self.client.command(sql)

        return CLOSED_CODE

    def get_user_profile(self, GUID):

        User = {}
        sql = '''select CATEGORY, O_CLASS1, O_DESC, LOGSOURCE, GUID, ORIGIN from Object where TYPE = 'User' and GUID = %d ''' % int(GUID)
        r = self.client.command(sql)[0].oRecordData
        User['UserType'] = r['CATEGORY']
        User['UserName'] = r['O_CLASS1']
        User['GUID'] = r['GUID']
        User['DESC'] = str(r['O_DESC'])
        User['EMAIL'] = r['LOGSOURCE']
        User['UserAuth']  = r['ORIGIN']
        User['DESC'] = 'Name: %s\nRole: %s\nAuthorization: %s\nEmail: %s\n%s' % (r['O_CLASS1'], r['CATEGORY'], r['ORIGIN'], r['LOGSOURCE'], User['DESC'])

        User['UserActivities'] = []
        User['UserTasks']      = []
        sql = '''
        match {class: Object, as: u, where: (TYPE = 'User' and GUID = %d)}.both() {class: Event, as: e, where: (TYPE = 'UserAction')}
        return e.CATEGORY, e.E_DESC, e.DATE, e.DTG, e.GUID, e.CLASS1, e.ORIGIN, e.XCOORD, e.YCOORD
        ''' % int(GUID)
        r = self.client.command(sql)
        for e in r:
            e = e.oRecordData
            data = {}
            data['CATEGORY'] = e['e_CATEGORY']
            data['DESC']   = str(e['e_E_DESC'])
            data['DATE']   = e['e_DATE']
            data['DTG']    = e['e_DTG']
            data['GUID']   = e['e_GUID']
            data['CLASS1'] = e['e_CLASS1']
            data['ORIGIN'] = e['e_ORIGIN']
            data['XCOORD'] = e['e_XCOORD']
            data['YCOORD'] = e['e_YCOORD']
            if data['CATEGORY'] == 'Task':
                if data not in User['UserTasks']:
                    User['UserTasks'].append(data)
            else:
                if data not in User['UserActivities']:
                    User['UserActivities'].append(data)

        User['UserActivities'] = sorted(User['UserActivities'], key=lambda i: i['DTG'], reverse=True)
        User['UserTasks'] = sorted(User['UserTasks'], key=lambda i: i['DTG'], reverse=True)

        return User

    def getResponse(self, url, auth):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-getResponse]: getting url %s." % (TS, url))
        if 'aa.com.tr' in url:
            iObj = [{'showNavigation' : 'false',
                     'startURL'       : url,
                     'searchLanguage' : 'all',
                     'searchDepth'    : '',
                     'searchTerms'    : '',


            }]
            c = oc.Crawler(iObj)
            c.startChrome()
            t = c.getSelectionShareable()
            return t

        if auth == None:
            try:
                response = requests.get(url)
            except:
                s = requests.Session()
                s.trust_env = False
                response = s.get(url)
        else:
            try:
                response = requests.get(url, auth=auth)
            except:
                s = requests.Session()
                s.trust_env = False
                response = s.get(url, auth=auth)

        return response

    def get_task(self, GUID):

        if GUID != 'None':
            sql = '''select GUID, TYPE, CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, ORIGIN from Object where GUID = '%d'  ''' % GUID
        else:
            sql = '''select GUID, TYPE, CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, ORIGIN from Object  where TYPE = 'RFI' or TYPE = 'RFS'  '''

        r = self.client.command(sql)[0].oRecordData
        task = {'GUID'   : r['GUID'],
                'NAME'   : r['GUID'],
                'DESC'   : str(r['O_DESC']),
                'CLASS1' : r['O_CLASS1'],
                'DATE'   : r['O_CLASS2'],
                'DTG'    : (r['O_CLASS3']),
                'STATUS' : r['ORIGIN'],
                'ORIGIN' : r['ORIGIN'],
                'TYPE'   : r['TYPE'],
                'CATEGORY'   : r['CATEGORY']
                }
        if GUID == 'None':
            GUID = int(task['GUID'])
        
        if task['STATUS'] == 'Open':
            task['INFOSTATUS'] = 'Error'
        elif task['STATUS'] == 'Closed':
            task['INFOSTATUS'] = 'Success'
        else:
            task['INFOSTATUS'] = 'Warning'        
        
        if task['TYPE'] == 'RFS':
            t = task['ORIGIN']
            FROM = t[:t.find('-')]
            TO = t[t.find('-')+1:]
            task['FROM'] = self.client.command("select O_CLASS1 from Object where GUID = %d " % int(FROM))[0].oRecordData['O_CLASS1']
            task['TO']   = self.client.command("select O_CLASS1 from Object where GUID = %d " % int(TO))[0].oRecordData['O_CLASS1']

        return task
    
    def search_person(self, FNAME, LNAME):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')     
        sql = ''' select GUID from Person where FNAME = '%s' and LNAME = '%s' ''' % (FNAME, LNAME)
        print("[%s_APP-ODB-search_person]: Searching for person with %s" % (TS, sql))   
        r = self.client.command(sql)
        if len(r) > 0:
            return r[0].oRecordData['GUID']
        else:
            return None

    def get_entity(self, GUID):

        result = {'VAL' : False, 'GUID' : GUID}
        Profile = {}

        if str(GUID)[0] == '1':
            TYPE = 'Person'
        if str(GUID)[0] == '2':
            TYPE = 'Object'
        if str(GUID)[0] == '3':
            TYPE = 'Location'
        if str(GUID)[0] == '4':
            TYPE = 'Event'

        sql = ''' select *, OUT().GUID, IN().GUID from %s where GUID = %s ''' % (TYPE, GUID)
        r = self.client.command(sql)[0].oRecordData
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-get_entity]: Getting %s entity profile for GUID %s:\nSQL:\t%s\nRESULT:\t%s" % (TS, TYPE, GUID, sql, r))
        if TYPE == 'Person':

            result['NAME']     = r['FNAME'] + ' ' + r['LNAME']
            result['DESC']     = "ID: %s\nGender: %s\n%s was born on %s in %s." % (GUID, r['GEN'], result['NAME'], r['DOB'], r['POB'])
            result['POLER']    = 'Person'
            result['TYPE']     = r['GEN']
            result['GEN']      = r['GEN']
            result['FNAME']    = r['FNAME']
            result['LNAME']    = r['LNAME']
            result['DOB']      = str(r['DOB'])
            result['POB']      = r['ORIGIN']
            result['VAL']      = True
            result['ProfileType'] = 'Person'

        elif TYPE == 'Object':

            result['NAME']     = r['TYPE'] + ' ' + r['CATEGORY']
            # Special for Complex Description types which are actually dictionaries
            if r['TYPE'] == 'CONOP' or r['TYPE'] == 'DRAFT_CONOP':
                stringNAR= str(r['O_DESC']).replace("'", "\"")
                CONOP_NAR = json.loads(stringNAR)
                cleanNAR = ''
                for k in CONOP_NAR.keys():
                    cleanNAR = '%s%s: %s\n' % (cleanNAR, k, CONOP_NAR[k])
                result['DESC'] = cleanNAR
            else:
                result['DESC']     = "ID: %s\nObject with description: %s. Classifications %s , %s, %s." % (GUID, r['O_DESC'], r['O_CLASS1'], r['O_CLASS2'], r['O_CLASS3'])
            result['POLER']    = 'Object'
            result['TYPE']     = r['TYPE']
            result['CATEGORY'] = r['CATEGORY']
            result['CLASS1']   = r['O_CLASS1']
            result['CLASS2']   = r['O_CLASS2']
            result['DATE']     = str(r['O_CLASS3'] )
            result['ORIGIN']   = r['ORIGIN']
            result['VAL']      = True
            result['ProfileType'] = 'Object'
            result['Description'] = r['O_DESC']

        elif TYPE == 'Location':

            result['NAME']     = r['TYPE'] + ' ' + r['L_DESC']
            result['DESC']     = "%s. Location at %s, %s with data %s and %s." % (r['L_DESC'], r['XCOORD'], r['YCOORD'], r['ZCOORD'], r['CLASS1'])
            result['POLER']    = 'Location'
            result['TYPE']     = r['TYPE']
            result['CATEGORY'] = r['L_DESC']
            result['CLASS1']   = r['XCOORD']
            result['CLASS2']   = r['YCOORD']
            result['DATE']     = result['CLASS1']
            result['ORIGIN']   = r['ORIGIN']
            result['VAL']      = True
            result['ProfileType'] = 'Location'

        elif TYPE == 'Event':

            result['NAME']     = r['TYPE'] + ' ' + r['CATEGORY']
            result['DESC']     = "Event on %s. %s" % (r['DATE'], r['E_DESC'])
            result['POLER']    = 'Event'
            result['TYPE']     = r['TYPE']
            result['CATEGORY'] = r['CATEGORY']
            result['CLASS1']   = str(r['TIME'])
            result['CLASS2']   = r['DTG']
            result['DATE']     = str(r['DATE'])
            result['ORIGIN']   = r['ORIGIN']
            result['VAL']  = True
            result['ProfileType'] = 'Event'
            
        else:
            return None

        result['Relations'], result['pRelCount'], result['oRelCount'], result['lRelCount'], result['eRelCount'] = self.get_entity_relations(GUID, TYPE)

        if isinstance(result['NAME'], str) == False:
            result['NAME'] == str(result['NAME'])

        return result


    def get_entity_relations(self, GUID, TYPE):

        relations = []
        pRelCount = 0
        oRelCount = 0
        lRelCount = 0
        eRelCount = 0

        sql = ''' match {class: %s, as: u, where: (GUID = %d)}.both() {class: V, as: e } return $elements''' % (TYPE, GUID)
        run = self.client.command(sql)
        GUIDs = []
        for r in run:
            r = r.oRecordData
            if r['GUID'] != str(GUID):
                for key in r.keys():
                    if key[0:3] == 'in_' or key[0:4] == 'out_':
                        if r['GUID'] not in GUIDs:
                            if int(r['GUID'][0]) == 1:
                                t = 'Person'
                                pRelCount+=1
                            if int(r['GUID'][0]) == 2:
                                t = 'Object'
                                oRelCount+=1
                            if int(r['GUID'][0]) == 3:
                                t = 'Location'
                                lRelCount+=1
                            if int(r['GUID'][0]) == 4:
                                t = 'Event'
                                eRelCount+=1
                            if key[0:3] == 'in_':
                                result = {'TYPE': t, 'GUID': r['GUID'], 'REL': key[3:]}
                            if key[0:4] == 'out_':
                                result = {'TYPE': t, 'GUID': r['GUID'], 'REL': key[4:]}
                            relations.append(result)
                            GUIDs.append(r['GUID'])
                            
        data = {'GUID' : GUID,
                'relations' : relations,
                'pRelCount' : pRelCount,
                'oRelCount' : oRelCount,
                'lRelCount' : lRelCount,
                'eRelCount' : eRelCount,
                }

        return data


    def get_users(self):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-getUsers]: process started." % (TS))

        sql = ''' select O_CLASS1, GUID, CATEGORY from Object where TYPE = 'User' '''
        r = self.client.command(sql)
        results = []
        for e in r:
            e = e.oRecordData
            data = {}
            data['NAME']  = e['O_CLASS1']
            data['GUID']  = e['GUID']
            data['ROLE']  = e['CATEGORY']
            results.append(data)

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-getUsers]: process completed with %d users." % (TS, len(results)))

        return results

    def EntityResolve(self, entity):
        '''
        '''

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_ODB-EntityResolve]: process started with %s." % (TS, entity))

        entity['LOOKUP'] = bytes(entity['LOOKUP'], 'utf-8').decode('utf-8', 'ignore')
        entity['LOOKUP'] = entity['LOOKUP'].replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
        if entity['TYPE'] == 'Person':
            lead = '1'
            sql = '''select * from %s where FNAME = '%s' AND LNAME = '%s' AND 'GEN' = '%s' ''' % (entity['TYPE'], entity['FNAME'], entity['LNAME'], entity['GEN'])
        elif entity['TYPE'] == 'Object':
            lead = '2'
            sql = '''select * from %s where TYPE = '%s' AND CATEGORY = '%s' AND ORIGINREF containstext '%s' ''' % (entity['TYPE'], entity['oTYPE'], entity['CATEGORY'], entity['LOOKUP'])
        elif entity['TYPE'] == 'Location':
            lead = '3'
            sql = '''select * from %s where ORIGINREF containstext '%s' ''' % (entity['TYPE'], entity['LOOKUP'])
        elif entity['TYPE'] == 'Event':
            lead = '4'
            sql = '''select * from %s where ORIGINREF containstext '%s' ''' % (entity['TYPE'], entity['LOOKUP'])


        if self.Verbose == True:
            print("[%s_ODB-EntityResolve]: %s SQL\n %s" % (TS, entity['TYPE'], sql))
        try:
            r = self.client.command(sql)
        except:
            r = []
            

        if len(r) == 0:
            # TODO encode other information in the GUID such as expunge date, classification, and data source
            GUID = str(lead + str(time.time()).replace(".", ""))
            lenDiff = 19 - len(GUID)
            for i in range(lenDiff):
                GUID = GUID + '0'
            GUID = int(GUID)
            exists = 0
        else:
            '''
            First check if it has the same pattern
            Second check if the pattern is exact
            refs = ORIGINREF.split('-')
            for r in refs: if check == r, exists
            '''
            GUID = int(r[0].oRecordData['GUID'])
            exists = 1

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-EntityResolve]: Exists [%d] GUID-%s from sql %s." % (TS, exists, GUID, sql))

        return GUID, exists

    def insertUser(self, username, password, email, tel, location, utype):

        User = 'User'
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_ODB-insertUser]: started." % (TS))

        O_DESC = 'Username %s of type %s created on %s can be reached at %s' % (username, utype, TS, email)
        O_ORIGINREF = username + email
        O_GUID = self.insertObject(User, utype, O_DESC, username, password, tel, email, location)
        PGUID = self.insertPerson('U', User, username, TS, location, O_GUID, 'A1', O_DESC)
        if self.HDB:
            self.HDB.insertODBUser(O_GUID, PGUID, username, bcrypt.encrypt(password), email, tel, location, O_ORIGINREF, utype)
        self.insertRelation(PGUID, 'Person', 'AccountCreation', O_GUID, 'Object')
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-insertUser]: Created Associated HANA GUID: Person %s." % (TS, O_GUID))

        return O_GUID

    def name_extract(self, name):
        
        # Change the name into a list for cleaning
        nlist = name.split(' ')
        name = ''
        for n in nlist:
            n = n.capitalize()
            name = name + n + ' '
        name = name.strip()
    
        # Check if this is a full name or not and separate the given names and surname
        if name.count(' ') == 1:
            #1st space
            sp = name.find(' ') 
            LNAME = name[sp+1:len(name)]  
            
        elif name.count(' ') == 2:
            #2nd space
            sp = name[name.find(' ')+1:].find(' ') + name.find(' ') + 1
            LNAME = name[sp+1:len(name)]  
            
        elif name.count(' ') == 3:
            #3rd space
            sp = name[name[name.find(' ')+1:].find(' ') + name.find(' ') + 2:].find(' ') + name[name.find(' ')+1:].find(' ') + name.find(' ') + 2
            LNAME = name[sp+1:len(name)]          
           
        else:
            sp = len(name)
            LNAME = 'None found'
            
        FNAME = name[:sp]
            
        return FNAME.strip().replace("'", ""), LNAME.strip().replace('"', "")    
    
    def get_first_degree_of_entity_type(self, sourceGUID, sourceTYPE, targetTYPE, targetVAR, targetVAL):
        # TODO
        # match person GUID to targetTYPE vertex where targetVAR = targetVAL
        # Test with Person GUID to Object where CATEGORY = 'UPRN'
        
        return GUID
    
    def get_family(self, name):
        # TODO match (person)-[family]--(personb) where person name = name
        
        return GUID
    
    def check_family(self, lastname):
        #TODO
        # Check if there is already a link to family (return person details who have family rel)
        # Use the last name to find all other people with the same last name
        # For each similarlast name paerson is there already a link? 
        # If no link check if the same "LivesAt" relation exists to the same location
        # If same LivesAt then make a relation of family
        
        return GUID
    
    
    def insertPerson(self, GEN, FNAME, LNAME, DOB, POB, ORIGIN, LOGSOURCE, DESC):
        '''
        Attempt to identify the person by basic entities that are included in the data source
        '''
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_ODB-insertPerson]: process started." % (TS))
        
        # Cleaning Gender
        try:
            GEN = str(GEN).upper().strip()[0]
        except:
            GEN = 'U'
        GENS = ['F', 'M', 'U']
        if GEN == None or GEN not in GENS:
            GEN = 'U'  
        
        # Cleaning Name
        if type(FNAME) != str:
            FNAME = 'Unk'           
        if len(str(LNAME)) == 0 or type(LNAME) != str:
            FNAME, LNAME = self.name_extract(FNAME) 
        
        FNAME = FNAME.replace("'", "").replace('"', "").strip()
        LNAME = LNAME.replace("'", "").replace('"', "").strip()
        
        # Clean Date of Birth 
        if str(type(DOB)) == "<class 'datetime.datetime'>":
            DOB = DOB.strftime('%Y-%m-%d %H:%M:%S')        
            
        DOB = str(self.check_date(DOB))[:10]        
             
        if LOGSOURCE not in self.AUTHS:
            LOGSOURCE = 0          

        if len(str(DESC)) == 0 or DESC == None:
            DESC = 'Record created on %s' % TS

        if type(POB) == str:
            POB = POB.replace("'", "").replace('"', '')
            
        ORIGINREF = ("%s%s%s%s%s" % (FNAME, LNAME, GEN, DOB, POB)).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace("-", "").replace('\r', '')[:2000]
        GUID, exists = self.EntityResolve({'TYPE' : 'Person', 'LOOKUP' : '%s' % ORIGINREF, 'FNAME' : FNAME, 'LNAME' : LNAME, 'GEN' : GEN})
        if exists == 0:
            sql = '''create vertex Person set GUID = '%s', GEN = '%s', FNAME = '%s', LNAME = '%s', DOB = '%s', POB = '%s', ORIGIN = '%s', ORIGINREF = '%s', LOGSOURCE = '%s' ''' % (GUID, GEN, FNAME, LNAME, DOB, POB, ORIGIN, ORIGINREF, LOGSOURCE)
            self.client.command(sql)
            if self.HDB != None:
                try:
                    self.HDB.insertODBPerson(GUID, GEN, FNAME, LNAME, DOB, POB, ORIGIN, ORIGINREF, LOGSOURCE, DESC)
                except:
                    pass

        return GUID

    def insertObject(self, TYPE, CATEGORY, O_DESC, CLASS1, CLASS2, CLASS3, ORIGIN, LOGSOURCE):
        '''
            An Object is any virtual or physical item that can be described and associated with a person, location or event.
            The Type could be HairColor, Religion, SocialMediaAccount, CommunicationDevice, Weapon...
            Coorespondng category Brown, Atheist, Twitter, MobilePhone, Hand-Gun
            Cooresponding desc N/A, Doesn't believe in God, Username: Tweeter1 established on 5.05.05 associated with..., Phone Number: ...., SN/ 444 registered to on ...
            Cooresponding Class1 N/A, N/A, Tweeter1, 555-5555, Glock9
            Cooresponding Class2 N/A, N/A, ID-394949, SN-393910, SN-444
        '''
        CLASS1 = (str(CLASS1).replace("'", ""))
        CLASS2 = (str(CLASS2).replace("'", ""))
        CLASS3 = (str(CLASS3).replace("'", ""))

        if type(CATEGORY) != str:
            CATEGORY = 'Unk'
        if type(O_DESC) != str:
            O_DESC = 'Unk'

        if LOGSOURCE not in self.AUTHS:
            LOGSOURCE = 0    
        LOGSOURCE = LOGSOURCE.replace("'", "")

        if CATEGORY != None:
            CATEGORY = CATEGORY.replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '').replace('\r', '')
        else:
            CATEGORY = 'Unknown'
        ORIGINREF = ('%s%s%s%s%s%s' % (TYPE, CATEGORY, O_DESC, CLASS1, CLASS2, CLASS3)).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '').replace('\r', '')
        ORIGINREF = (ORIGINREF[:2000]).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '').replace('\r', '')         
        if O_DESC != None:
                O_DESC = ('%s' % O_DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace('\r', ''))[:5000]
        else:
            O_DESC = 'No description'
        GUID, exists = self.EntityResolve({'TYPE' : 'Object', 'LOOKUP' : '%s' % ORIGINREF, 'oTYPE' : TYPE, 'CATEGORY' : CATEGORY})
        if exists == 0:
            sql = ''' create vertex Object set GUID = '%s', TYPE = '%s', CATEGORY = '%s', O_DESC = '%s', O_CLASS1 = '%s', O_CLASS2 = '%s', O_CLASS3 = '%s', ORIGIN = '%s', ORIGINREF = '%s', LOGSOURCE = '%s' '''    % (GUID, TYPE, CATEGORY, O_DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
            sql = sql.replace("\r", "")
            self.client.command(sql)

            if self.HDB != None:
                try:
                    self.HDB.insertODBObject(GUID, TYPE, CATEGORY, O_DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                except:
                    pass
        return GUID

    def insertEvent(self, E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_LOGSOURCE):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_ODB-insertEvent]: process started." % (TS))

        if E_LOGSOURCE not in self.AUTHS:
            E_LOGSOURCE = 0

        E_DATE = self.check_date(E_DATE)
        
        if ':' in E_DATE:
            if 'pm' in E_DATE:
                E_TIME = E_DATE[:E_DATE.find('p')][:4]
                hh = int(E_TIME[:E_TIME.find(':')])
                mm = int(E_TIME[E_TIME.find(':')+1:])
                if hh > 12:
                    hh = hh + 12
                E_TIME = '%d:%d' % (hh, mm)

            elif 'am' in E_DATE:
                E_TIME = E_DATE[:E_DATE.find('a')][:4]
                hh = int(E_TIME[:E_TIME.find(':')])
                mm = int(E_TIME[E_TIME.find(':')+1:])
                E_TIME = '%d:%d' % (hh, mm)

        E_DATE = self.check_date(E_DATE)
        if ':' not in str(E_TIME):
            E_TIME = '12:00'
        E_DESC = bytes(E_DESC, 'utf-8').decode('utf-8', 'ignore')
        E_DESC = '%s' % E_DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace('\r', '')
        E_DESC = E_DESC[:5000]
        if isinstance(E_DTG, int) == False:
            E_DTG = str(E_DATE).replace("-", "").replace(":", "").replace(" ", "")
        if E_ORIGIN == None:
            E_ORIGIN = E_LOGSOURCE
        else:
            E_ORIGIN = '%s' % str(E_ORIGIN).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
        if E_CLASS1 != None and isinstance(E_CLASS1, str) == True:
            E_CLASS1 = (E_CLASS1.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', ''))[:200]

        E_ORIGINREF = ('%s%s%s%s%s%s' % (E_TYPE, E_CATEGORY, E_DESC, E_DTG, E_CLASS1, E_ORIGIN)).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '').replace('\r', '')
        E_ORIGINREF = (E_ORIGINREF[:2000]).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '').replace('\r', '')
        E_ORIGIN = E_ORIGIN[:200]

        if isinstance(E_XCOORD, int) == False:
            if isinstance(E_XCOORD, float) == False:
                E_XCOORD = 0
        if isinstance(E_YCOORD, int) == False:
            if isinstance(E_YCOORD, float) == False:
                E_YCOORD = 0

        E_GUID, exists = self.EntityResolve({'TYPE' : 'Event', 'LOOKUP' : '%s' % E_ORIGINREF})
        if exists == 0:

            sql = '''create vertex Event set GUID = '%s', TYPE = '%s', CATEGORY = '%s', E_DESC = '%s', LANG = '%s', CLASS1 = '%s', TIME = '%s', DATE = '%s',
            DTG = %s, XCOORD = %s, YCOORD = %s, ORIGIN = '%s', ORIGINREF = '%s', LOGSOURCE = '%s' ''' % (
                E_GUID, E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)
            sql = sql.replace("\r", "")
            self.client.command(sql)

            if self.HDB != None:
                try:
                    self.HDB.insertODBEvent(E_GUID, E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)
                except:
                    pass
        return E_GUID

    def insertLocation(self, L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1, L_ORIGIN, L_LOGSOURCE):
        
        if L_LOGSOURCE not in self.AUTHS:
            L_LOGSOURCE = 0        
        
        DESClist = L_DESC.split(' ')
        L_DESC = ''
        for d in DESClist:
            L_DESC = L_DESC + d + ' '
        L_DESC = L_DESC.strip()
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        L_LOGSOURCE = str(L_LOGSOURCE)
        L_DESC = str(L_DESC).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')

        if isinstance(L_ORIGIN, str):
            L_ORIGIN = L_ORIGIN.replace("'", '')
        try:
            L_XCOORD = float(L_XCOORD)
        except:
            L_XCOORD = 0.000
        try:
            L_YCOORD = float(L_YCOORD)
        except:
            L_YCOORD = 0.000
        if len(str(L_LOGSOURCE)) > 199:
            L_LOGSOURCE = L_LOGSOURCE[:200]

        L_ORIGINREF = ('%s%s%s%s%s%s' % (L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1))
        L_ORIGINREF = (L_ORIGINREF).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '')
        L_CLASS1 = 0

        L_GUID, exists = self.EntityResolve({'TYPE' : 'Location', 'LOOKUP' : '%s' % L_ORIGINREF })
        if exists == 0:
            sql = ''' create vertex Location set GUID = '%s', TYPE = '%s', L_DESC = '%s', XCOORD = %s, YCOORD = %s, ZCOORD = '%s', CLASS1 = '%s', ORIGIN = '%s', ORIGINREF = '%s', LOGSOURCE = '%s'
            ''' % (L_GUID, L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1, L_ORIGIN, L_ORIGINREF, L_LOGSOURCE)
            sql = sql.replace("\r", "")
            self.client.command(sql)

            if self.HDB != None:
                try:
                    self.HDB.insertODBLocation(L_GUID, L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1, L_ORIGIN, L_ORIGINREF, L_LOGSOURCE)
                except:
                    pass
        return L_GUID
    
    
    def EditEntity(self, eTYPE, ATTR, newVal, GUID):
        
        sql = ''' update %s set %s = '%s' where GUID = %d ''' % (eTYPE, ATTR, newVal, GUID)
        
        try:
            self.client.command(sql)
            return 200
        except Exception as e:
            return e
    
    def GUIDcheck(self, GUID):      
        
        if str(GUID)[0] == '1':
            return 'Person'
        elif str(GUID)[0] == '2':
            return 'Object'
        elif str(GUID)[0] == '3':
            return 'Location'
        elif str(GUID)[0] == '4':
            return 'Event'  
        else:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-GUIDcheck]: %s is not a valid GUID." % (TS, GUID))
            return None
    
    def insertRelation(self, SOURCEGUID, SOURCETYPE, TYPE, TARGETGUID, TARGETTYPE):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        
        SOURCETYPE = self.GUIDcheck(SOURCEGUID)
        TARGETTYPE = self.GUIDcheck(TARGETGUID)
        if SOURCETYPE == None or TARGETTYPE == None:
            return None
        
        sql = ''' match {class: V, as: u, where: (GUID = %s)}.both('%s') {class: V, as: e, where: (GUID = %s) } return $elements ''' % (TARGETGUID, TYPE, SOURCEGUID)
        check = self.client.command(sql)
        if len(check) == 0:
            sql = ''' create edge %s from (select from %s where GUID = %s) to (select from %s where GUID = %s) set TIMESTAMP = '%s' ''' % (TYPE, SOURCETYPE, SOURCEGUID, TARGETTYPE, TARGETGUID, TS)
            self.client.command(sql)

            if self.HDB != None:
                try:
                    self.HDB.insertRelation(SOURCEGUID, SOURCETYPE, TYPE, TARGETGUID, TARGETTYPE)
                except:
                    pass
   
    def merge_ORGREF_BlockChain(self, GUID1, GUID2, GUID1type, GUID2type):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_ODB-merge_ORGREF_BlockChain]: process started with %s %s %s %s" % (TS, GUID1, GUID2, GUID1type, GUID2type))
        # Get the ORIGINREF of the disolving entity and append to the end of the absorbing entity
        sql = ''' select ORIGINREF FROM %s WHERE GUID = %s ''' % (GUID2type, GUID2)
        bORIGINREFval = self.client.command(sql)[0].oRecordData['ORIGINREF']
        sql = ''' select ORIGINREF FROM %s WHERE GUID = %s ''' % (GUID1type, GUID1)
        aORIGINREFval = self.client.command(sql)[0].oRecordData['ORIGINREF']
        aORIGINREFval = "%s-%s" % (aORIGINREFval, bORIGINREFval)
        sql = ''' update %s set ORIGINREF = '%s' WHERE GUID = %s ''' % (GUID1type, aORIGINREFval, GUID1)
        self.client.command(sql)
        return "[%s_ODB-merge_ORGREF_BlockChain] %s" % (TS, aORIGINREFval)

    def check_entity_type(self, GUID):

        if str(GUID)[0] == '1':
            GUIDtype  = 'Person'

        elif str(GUID)[0] == '2':
            GUIDtype  = 'Object'

        elif str(GUID)[0] == '3':
            GUIDtype  = 'Location'

        elif str(GUID)[0] == '4':
            GUIDtype  = 'Event'

        return GUIDtype


    def merge_entities(self, TYPE, GUID1, GUID2):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_ODB-merge_entities]: process started." % (TS))

        GUID1type  = self.check_entity_type(GUID1)
        GUID2type  = self.check_entity_type(GUID2)
        self.merge_ORGREF_BlockChain(GUID1, GUID2, GUID1type, GUID2type)

        sql = '''
        match {class: %s, as: u, where: (GUID = %d)}.both() {class: V, as: e } return $elements
        ''' % (GUID2type, int(GUID2))
        r = self.client.command(sql)
        for e in r:
            e = e.oRecordData
            if e['GUID'] != GUID1:
                for k in e.keys():
                    if 'out_' in k:
                        TYPE = k.replace('out_', '')
                        SOURCEGUID = e['GUID']
                        SOURCETYPE = self.check_entity_type(SOURCEGUID)
                        TARGETGUID = GUID1
                        TARGETTYPE = GUID1type
                        self.insertRelation(SOURCEGUID, SOURCETYPE, TYPE, TARGETGUID, TARGETTYPE)
                    if 'in_' in k:
                        TYPE = k.replace('in_', '')
                        SOURCEGUID = GUID1
                        SOURCETYPE = GUID1type
                        TARGETGUID = e['GUID']
                        TARGETTYPE = self.check_entity_type(TARGETGUID)
                        self.insertRelation(SOURCEGUID, SOURCETYPE, TYPE, TARGETGUID, TARGETTYPE)
                        
        self.delete_entity(GUID2)

    def delete_entity(self, GUID):
        GUIDtype = self.check_entity_type(GUID)
        sql = '''delete vertex from %s where GUID = %d ''' % (GUIDtype, int(GUID))
    
    def update_user(self, iObj):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        message = {'SRC' : '%s_SCP_update_user' % TS, 'TXT' : '', 'TYPE' : True, 'TRACE' : []}
        DESC = ''

        if iObj['TYPE'][0] == 'USER_DETAIL':
            if iObj['EMAIL'][0] != '':
                sql = '''
                update Object set ORIGINREF = '%s'
                where TYPE = 'User' and GUID = %d ''' % (iObj['EMAIL'][0], int(iObj['GUID']))
                DESC = '%s Updated email to %s.\n' % (TS, iObj['EMAIL'][0])
                self.cursor.execute(sql)
            if iObj['TEL'][0] != '':
                sql = '''
                update Object set "O_CLASS3" = '%s'
                where O_TYPE = 'User' and O_GUID = %d ''' % (iObj['TEL'][0], int(iObj['GUID']))
                DESC = '%s%s Updated telephone to %s.\n' % (DESC, TS, iObj['TEL'][0])
                self.cursor.execute(sql)
            if iObj['PASSWORD'][0] != '':
                sql = '''
                update Object set "O_CLASS2" = '%s'
                where O_TYPE = 'User' and O_GUID = %d ''' % (bcrypt.encrypt(iObj['PASSWORD'][0]), int(iObj['GUID']))
                DESC = '%s%s Updated password.\n' % (DESC, TS)
                self.cursor.execute(sql)
            if iObj['ROLE'][0] != '':
                sql = '''
                update Object set "O_CATEGORY" = '%s'
                where O_TYPE = 'User' and O_GUID = %d ''' % ((iObj['ROLE'][0]), int(iObj['GUID']))
                DESC = '%s%s Updated role to %s.\n' % (DESC, TS, iObj['ROLE'][0])
                self.cursor.execute(sql)
            if iObj['AUTH'][0] != '':
                sql = '''
                update Object set "O_ORIGIN" = '%s'
                where O_TYPE = 'User' and O_GUID = %d ''' % ((iObj['AUTH'][0]), int(iObj['GUID']))
                DESC = '%s%s Updated role to %s.\n' % (DESC, TS, iObj['AUTH'][0])
                self.cursor.execute(sql)
            sql = '''
            select O_DESC from Object where GUID = %d
            ''' % (int(iObj['GUID']))
            oDESC = self.cursor.execute(sql).fetchone()
            DESC = '%s\n%s' % (oDESC[0], DESC)
            sql = '''
            update Object set O_DESC = '%s'
            where O_TYPE = 'User' and O_GUID = %d ''' % (DESC, int(iObj['GUID']))
            self.cursor.execute(sql)

        return message

    def initialize_users(self):
        password = 'welcome123'
        self.insertUser('SYSTEM', bcrypt.encrypt(password), 'A1A2A3B1C1', '000-000', 'A1A2A3B1C1A4A5A6', 'Admin')

        password = 'test123'
        self.insertUser('Tim S', bcrypt.encrypt(password), 'A1A2A3B1', '555-5555', 'A1A2A3B1', 'Social')

        password = 'test123'
        self.insertUser('Patty', bcrypt.encrypt(password), 'A1A2A3B1C1', '555-5555', 'A1A2A3B1C1', 'Field')

        password = 'test123'
        self.insertUser('Farah', bcrypt.encrypt(password), 'A1A2A3B1C1', '555-5555', 'A1A2A3B1C1', 'Manager')

        password = 'test123'
        self.insertUser('Hakim', bcrypt.encrypt(password), 'A1A2A3B1B2', '555-5555', 'A1A2A3B1B2', 'Health')

        password = 'test123'
        self.insertUser('Hans', bcrypt.encrypt(password), 'A1A2A3B1C1', '555-5555', 'A1A2A3B1C1', 'Analyst')

        password = 'test123'
        self.insertUser('Fahim', bcrypt.encrypt(password), 'A1A2A3B1C1', '555-5555', 'A1A2A3B1C1', 'Arabic')

        password = 'cantloginbecauserolewontseeanything'
        self.insertUser('Open Task', bcrypt.encrypt(password), 'OpenTasks@email.com', '555-5555', 'Open Task', 'Open to any role')

    def initialize_reset(self):

        TS1 = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        try:
            for s in ['Person', 'Object', 'Location', 'Event']:
                self.client.command( 'DELETE from %s UNSAFE' % s)

            self.client.db_drop('POLER', pyorient.STORAGE_TYPE_MEMORY)
        except:
            pass
        
        try:
            self.HDB.initialize_reset()
            self.HDB.initialize_POLER()
            self.HDB.initialize_CONDIS_Customization()
        except:
            self.HDB = None

        self.createPOLER()
        self.initialize_users()
        self.openDB('POLER')
        self.client.tx_commit()
        TS2 = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        return 'Reset started at %s and completed at %s' % (TS1, TS2)

    def get_search(self, uaa, terms):
        sql = ''' select GUID, TYPE, CATEGORY, E_DESC, LOGSOURCE, DTG from Event where E_DESC containstext '%s' ''' % terms
        print(sql)
        r = self.client.command(sql)  
        results = []
        for e in r:
            e = e.oRecordData
            d = {}
            d['GUID'] = e['GUID']
            d['NAME'] = e['TYPE']
            d['DESC'] = 'Event'
            d['AUTH'] = e['LOGSOURCE']
            if d['AUTH'] in uaa or str(d['AUTH']) == '0':
                results.append(d)
            
        sql = ''' select GUID, TYPE, CATEGORY, O_DESC, LOGSOURCE from Object where O_DESC containstext '%s' ''' % terms
        print(sql)
        r = self.client.command(sql)  
        for e in r:
            e = e.oRecordData
            d = {}
            d['GUID'] = e['GUID']
            d['NAME'] = e['TYPE'] + ' ' + e['CATEGORY']
            d['DESC'] = 'Object'
            d['AUTH'] = e['LOGSOURCE']
            if d['AUTH'] in uaa or str(d['AUTH']) == '0':
                results.append(d)      
                
        sql = ''' select GUID, FNAME, LNAME, POB, DOB, LOGSOURCE from Person where FNAME containstext '%s' ''' % terms
        print(sql)
        r = self.client.command(sql)  
        for e in r:
            e = e.oRecordData
            d = {}
            d['GUID'] = e['GUID']
            d['NAME'] = e['FNAME'] + ' ' + e['LNAME']
            d['DESC'] = 'Person'
            d['AUTH'] = e['LOGSOURCE']
            if d['AUTH'] in uaa or str(d['AUTH']) == '0':
                results.append(d)   
                
        sql = ''' select GUID, FNAME, LNAME, POB, DOB, LOGSOURCE from Person where LNAME containstext '%s' ''' % terms
        print(sql)
        r = self.client.command(sql)  
        for e in r:
            e = e.oRecordData
            d = {}
            d['GUID'] = e['GUID']
            d['NAME'] = e['FNAME'] + ' ' + e['LNAME']
            d['DESC'] = 'Person'
            d['AUTH'] = e['LOGSOURCE']
            if d['AUTH'] in uaa or str(d['AUTH']) == '0':
                results.append(d)  
                
        sql = ''' select GUID, TYPE, L_DESC, LOGSOURCE from Location where L_DESC containstext '%s' ''' % terms
        print(sql)
        r = self.client.command(sql)  
        for e in r:
            e = e.oRecordData
            d = {}
            d['GUID'] = e['GUID']
            d['NAME'] = e['L_DESC']
            d['DESC'] = 'Location'
            d['AUTH'] = e['LOGSOURCE']
            if d['AUTH'] in uaa or str(d['AUTH']) == '0':
                results.append(d)            
        
        return results  
    
    def search_location_byName(self, terms):
        
        sql = ''' select GUID, TYPE, L_DESC, LOGSOURCE from Location where L_DESC containstext '%s' ''' % terms
        r = self.client.command(sql)  
        results = []
        for e in r:
            e = e.oRecordData
            d = {}
            d['GUID'] = e['GUID']
            d['NAME'] = e['L_DESC']
            d['DESC'] = 'Location'
            d['AUTH'] = e['LOGSOURCE']
            if d['AUTH'] in uaa or str(d['AUTH']) == '0':
                results.append(d) 
        
        return results
        
'''           
O = OrientModel(None)
O.start()
O.initialize_reset()
'''