#!/usr/bin/env python3
# -*- coding: utf-8 -*
'''
System Microservices:
Responsible for making calls to the database to produce information and views to the UX through Process services
Requires a DB from a set of pre-configured databases with the standard calls that can plug into these services such
as insert person or get search criteria
'''
import os, json, time, math, random, logging
import pandas as pd
from datetime import datetime
from flask import flash, request
from passlib.hash import bcrypt
import uuid
from threading import Thread

from application.services import OrientModels as om
from application.services import HANAModels as HM

class Worker:
    
    def __init__(self, TargetFunction, args):
        
        self.currentState = None
        self.CountMax = random.randint(10,20)
        self.ODB = om.OrientModel(None)
        self.ODB.start()   
        self.DB = self.ODB
        
        if TargetFunction == 'countdown':
            self.TD = Thread(target=self.countdown)
        if TargetFunction == 'TED_Cohort':
            self.TD = Thread(target=self.ETL_TED_Cohort, args=(args,))
        if TargetFunction == 'Academy':
            self.TD = Thread(target=self.ETL_Academy, args=(args,))   
        if TargetFunction == 'TF_Families':
            self.TD = Thread(target=self.ETL_TF_Families, args=(args,))   
        if TargetFunction == 'Education_Reference':
            self.TD = Thread(target=self.ETL_EDU_Reference, args=(args,))         
    
    def start(self):
        self.TD.start()
        self.currentState = 0
          
    def getCurrentState(self):
        return self.currentState
    
    def countdown(self):
        
        i = 0
        while i < self.CountMax:
            i+=1
            self.currentState = i
            time.sleep(5)
        self.currentState = 'Complete' 
    
    def ETL_Academy(self, df):
        ORIGIN = 'Academy'
        LOGSOURCE = 'A2'
        DESC = ""           
        dfSize = df.size
        df = df.fillna("")
        for index, row in df.iterrows():
            # Person Details
            FNAME = row['Forename']
            LNAME = row['Surname']
            GEN   = row['Gender']
            DOB   = row['DOB'] 
            POB   = 'UNK'
            NAME = '%s %s' % (FNAME, LNAME)
            
            ClaimRefNo = row['ClaimRefNo']
            ClaimType = row['ClaimType']
            Income_Code = row['Income_Code']

            NI_Number = row['NI_Number']
            Academy_Address = row['Academy_Address']
            UPRN = row['UPRN']  
            
            if len(str(FNAME)) > 1:
                pGUID = self.DB.insertPerson(GEN, FNAME, LNAME, DOB, POB, ORIGIN, LOGSOURCE, DESC)
            
                if len(str(NI_Number)) > 1:
                    nGUID = self.DB.insertObject('Reference Code', 'NIN', '%s National Insurance Number assigned to %s' % (NI_Number, NAME), NI_Number, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(nGUID, 'Object', 'Involves', pGUID, 'Person')
                
                if len(str(UPRN)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'UPRN', '%s Unique Property Registration Number.' % UPRN, UPRN, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')
                
                if len(str(Income_Code)) > 1:
                    iGUID = self.DB.insertObject('Reference Code', 'Income', 'Income code %s assigned to %s' % (Income_Code, NAME), Income_Code, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(iGUID, 'Object', 'Involves', pGUID, 'Person')
                                    
                if len(str(ClaimRefNo)) > 1:
                    cGUID = self.DB.insertObject('Reference Code', 'Claim', '%s claim %s involving %s' % (ClaimType, ClaimRefNo, NAME), ClaimRefNo, ClaimType, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(cGUID, 'Object', 'Involves', pGUID, 'Person')  
                
                if len(str(Academy_Address)) > 1:
                    lGUID = self.DB.insertLocation('Academy', Academy_Address, 0, 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(lGUID, 'Location', 'Involves', pGUID, 'Person') 
                    
        self.currentState = 'complete'
        return {'message' : 'Extracted %d records from %s' % (index, ORIGIN)}          
    

    def ETL_EDU_Qualification_History(self, df):
        
        ORIGIN = 'Education_Qualification'
        LOGSOURCE = 'A2'
        DESC = ""           
        dfSize = df.size 
        df = df.fillna("")
        for index, row in df.iterrows():
            CAPID  = row['ID']
            TERM1  = row['QUALIFY_141_143']
            TERM2  = row['QUALIFY_142_151']
            TERM3  = row['QUALIFY_143_152']
            TERM4  = row['QUALIFY_151_153']
            TERM5  = row['QUALIFY_152_161']
            TERM6  = row['QUALIFY_153_162']
            TERM7  = row['QUALIFY_161_163']
            TERM8  = row['QUALIFY_162_171']
            TERM9  = row['QUALIFY_163_172']
            TERM10 = row['QUALIFY_18_07'] 
            
            if len(str(CAPID)) > 1:
                # Get the person related 
                uGUID = self.DB.insertObject('Reference Code', 'CapitaID', '%s ID.' % CAPID, CAPID, 0, 0, ORIGIN, LOGSOURCE)
    
                if TERM1 == 1:
                    e1GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 1 2014', 'en', 0, '12:00', '2014-04-30', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e2GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 2 2014', 'en', 0, '12:00', '2014-08-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e3GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 3 2014', 'en', 0, '12:00', '2014-12-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(e1GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e2GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e3GUID, 'Event', 'Involves', uGUID, 'Object')
                    
                if TERM2 == 1:
                    e1GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 2 2014', 'en', 0, '12:00', '2014-08-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e2GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 3 2014', 'en', 0, '12:00', '2014-12-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e3GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 1 2015', 'en', 0, '12:00', '2015-04-30', 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(e1GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e2GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e3GUID, 'Event', 'Involves', uGUID, 'Object')  
                
                if TERM3 ==  1:
                    e1GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 1 2014', 'en', 0, '12:00', '2014-12-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e2GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 2 2014', 'en', 0, '12:00', '2015-04-30', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e3GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 3 2014', 'en', 0, '12:00', '2015-08-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(e1GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e2GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e3GUID, 'Event', 'Involves', uGUID, 'Object')
                    
                if TERM4 == 1:
                    e1GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 1 2014', 'en', 0, '12:00', '2015-04-30', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e2GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 2 2014', 'en', 0, '12:00', '2015-08-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e3GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 3 2014', 'en', 0, '12:00', '2015-12-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(e1GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e2GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e3GUID, 'Event', 'Involves', uGUID, 'Object') 
                    
                if TERM5 == 1:
                    e1GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 2 2014', 'en', 0, '12:00', '2015-08-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e2GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 3 2014', 'en', 0, '12:00', '2015-12-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e3GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 1 2015', 'en', 0, '12:00', '2016-04-30', 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(e1GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e2GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e3GUID, 'Event', 'Involves', uGUID, 'Object')                  
                
                    
                if TERM6 == 1:
                    e1GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 1 2014', 'en', 0, '12:00', '2015-12-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e2GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 2 2014', 'en', 0, '12:00', '2016-04-30', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e3GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 3 2014', 'en', 0, '12:00', '2016-08-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(e1GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e2GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e3GUID, 'Event', 'Involves', uGUID, 'Object')                
                    
                if TERM7 == 1:
                    e1GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 1 2014', 'en', 0, '12:00', '2016-04-30', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e2GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 2 2014', 'en', 0, '12:00', '2016-08-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e3GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 3 2014', 'en', 0, '12:00', '2016-12-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(e1GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e2GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e3GUID, 'Event', 'Involves', uGUID, 'Object') 
                
                if TERM8 == 1:
                    e1GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 2 2014', 'en', 0, '12:00', '2016-08-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e2GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 3 2014', 'en', 0, '12:00', '2016-12-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e3GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 1 2015', 'en', 0, '12:00', '2017-04-30', 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(e1GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e2GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e3GUID, 'Event', 'Involves', uGUID, 'Object')                  
                
                    
                if TERM9 == 1:
                    e1GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 1 2014', 'en', 0, '12:00', '2016-12-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e2GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 2 2014', 'en', 0, '12:00', '2017-04-30', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e3GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 3 2014', 'en', 0, '12:00', '2017-08-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(e1GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e2GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e3GUID, 'Event', 'Involves', uGUID, 'Object')   
                if TERM10 == 1:
                    e1GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 1 2014', 'en', 0, '12:00', '2018-04-30', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e2GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 2 2014', 'en', 0, '12:00', '2018-08-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    e3GUID = self.DB.insertEvent('Absence', 'School', 'Absence limit reached for Term 3 2014', 'en', 0, '12:00', '2018-12-31', 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(e1GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e2GUID, 'Event', 'Involves', uGUID, 'Object')
                    self.DB.insertRelation(e3GUID, 'Event', 'Involves', uGUID, 'Object')  
                    
        self.currentState = 'complete'
        return {'message' : 'Extracted %d records from %s' % (index, ORIGIN)}                      
    
    def ETL_EDU_Period(self, df):
       
        ORIGIN = 'Education_Period'
        LOGSOURCE = 'A2'
        DESC = ""           
        dfSize = df.size 
        df = df.fillna("")
        for index, row in df.iterrows():
            FIRSTNAME = row['FIRSTNAME']
            LASTNAME  = row['LASTNAME']
            DOB       = row['DOB']
            GENDER    = row['GENDER']
            FSM       = row['FREE SCHOOL MEALS']
            NOR       = row['NOR_TYPE']
            EXCLUSION = row['EXCLUSION']
            PERATTEND = row['PERCENT ATTENDANCE']   
            PRU       = row['PRU']
            PRU_REG   = row['PRU_REG']
            HOUSENO   = row['HOUSE NUMBER']
            HOUSE     = row['HOUSE']
            APARTMENT = row['APARTMENT']
            ADDRESS   = "%s %s %s %s" % (row['ADDRESS0'], row['ADDRESS1'], row['ADDRESS2'], row['TOWN'])
            POSTCODE  = row['POSTCODE']
            BASENAME  = row['BASE_NAME']
            ID        = row['ID']   
            
            if len(str(FIRSTNAME)) > 1:
                pGUID = self.DB.insertPerson(GENDER, FIRSTNAME, LASTNAME, DOB, '', ORIGIN, LOGSOURCE, DESC) 
                
                if len(str(row['ADDRESS0'])) > 1:
                    lGUID = self.DB.insertLocation('Residence', ADDRESS, 0, 0, 0, HOUSENO, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'LivesAt', lGUID, 'Location')      
                    
                if len(str(BASENAME)) > 1:
                    oGUID = self.DB.insertObject('Organisation', 'School', BASENAME, 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'LocatedAt', oGUID, 'Object')   
                      
                if str(EXCLUSION) != '':
                    oGUID = self.DB.insertObject('Exclusion', 'Code', EXCLUSION, 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'Involves', oGUID, 'Object')   
                    
                if len(str(PERATTEND)) > 1:
                    oGUID = self.DB.insertObject('Attendance', 'Percentage', PERATTEND, 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'Involves', oGUID, 'Object')                   
                
                if len(str(NOR)) > 1:
                    oGUID = self.DB.insertObject('Attendance', 'Non-Enroll', NOR, 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'Involves', oGUID, 'Object')                     
                
                if (str(FSM)) == 'T':
                    oGUID = self.DB.insertObject('Plan', 'Free School Meal', 'Education based plan', 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'RegisteredOn', oGUID, 'Object')     
                    
                if len(str(ID)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'ID', '%s ID.' % ID, ID, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')                  
                       
        self.currentState = 'complete'
        return {'message' : 'Extracted %d records from %s' % (index, ORIGIN)}          

    def ETL_Civica(self, df):
        
        ORIGIN = 'Civica'
        LOGSOURCE = 'A2'
        DESC = ""           
        dfSize = df.size        
        df = df.fillna("")
        for index, row in df.iterrows():
            UPRN = row['UPRN']
            REF  = row['Civica_ReferenceNo']
            DATE = row['ASB_Date']
            DESC = row['ASB_Description']
            ADDR = row['Civica_Address']
            ADSC = row['ASB_Decision']
            TENU = row['Civica_Tenure']
            OCCU = row['ASB_Occupier Name']                   
            
            if len(str(UPRN)) > 1:
                uGUID = self.DB.insertObject('Reference Code', 'UPRN', '%s Unique Pupil Registration Number' % UPRN, UPRN, 0, 0, ORIGIN, LOGSOURCE)                    
                
                if len(str(REF)) > 1:
                    rGUID = self.DB.insertObject('Reference Code', 'Civica', '%s reference on %s' % (REF, DATE), REF, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', rGUID, 'Object')
                
                if len(str(ADDR)) > 1:
                    aGUID = self.DB.insertLocation('Civic Location', "Tenure:%s Occupier:%s" % (ADDR, OCCU), 0, 0, 0, TENU, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'OccurredAt', aGUID, 'Location')
                    
                if len(str(DATE)) > 1:
                    eGUID = self.DB.insertEvent('ASB', ADSC, '%s with decision %s by %s about %s' % (DESC, ADSC, TENU, OCCU), 'en', 0, '12:00', DATE, 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(eGUID, 'Event', 'Involves', uGUID, 'Object') 
                            
        self.currentState = 'complete'
        return {'message' : 'Extracted %d records from %s' % (index, ORIGIN)}          

    
    
    
    def ETL_EDU_Reference(self, df):
    
        ORIGIN = 'EDU_Reference'
        LOGSOURCE = 'A2'
        DESC = ""           
        t = df.shape[0]
        df = df.fillna("") 
        p = 0     
        for index, row in df.iterrows():
            
            FNAME     = row['FIRSTNAME']
            LNAME     = row['LASTNAME']
            GEN       = row['GENDER']
            DOB       = row['DOB']  
            HOUSENO   = row['HOUSE_NO']
            HOUSE     = row['HOUSE_NAME']
            APARTMENT = row['APARTMENT']
            ADDRESS1  = row['ADDRESS1']
            ADDRESS2  = row['ADDRESS2']
            ADDRESS3  = row['ADDRESS3']
            TOWN      = row['TOWN']
            POST      = row['POSTCODE']
            UPRN      = row['UPRN']
            ID        = row['ID']
            ADDRESS   = "%s %s %s %s %s %s" % (HOUSENO, HOUSE, APARTMENT, ADDRESS1, ADDRESS2, ADDRESS3)
            
            if index > p:
                TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                try:
                    print("[%s] ETL_EDU_Reference %d of %d complete" % (TS, index, t))
                    p+=50 
                except:
                    pass            

            if len(str(FNAME)) > 1:
                pGUID = self.DB.insertPerson(GEN, FNAME, LNAME, DOB, '', ORIGIN, LOGSOURCE, DESC)  
            
                try:
                
                    if len(str(UPRN)) > 1:
                            uGUID = self.DB.insertObject('Reference Code', 'UPRN', '%s Unique Property Registration Number.' % UPRN, UPRN, 0, 0, ORIGIN, LOGSOURCE)
                            self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')  
                            
                    if len(str(ID)) > 1:
                        uGUID = self.DB.insertObject('Reference Code', 'ID', '%s ID.' % ID, ID, 0, 0, ORIGIN, LOGSOURCE)
                        self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')                 
                        
                    if len(str(ADDRESS)) > 4:
                        lGUID = self.DB.insertLocation('Residence', ADDRESS, 0, 0, 0, HOUSENO, ORIGIN, LOGSOURCE)
                        self.DB.insertRelation(pGUID, 'Person', 'LivesAt', lGUID, 'Location')  
                
                except Exception as e:
                    print("[%s] ETL_EDU_Reference error with %s: %s" % (TS, index, e))                
                
        
        self.currentState = 'complete'
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        message = {'message' : '[%s] Extracted %d records from %s' % (TS, index, ORIGIN)}
        print(message)
        return message 
    
    def ETL_EHA(self, df):
        
        ORIGIN = 'EHA'
        LOGSOURCE = 'A2'
        DESC = "" 
        E_LANG = 'en'
        E_CLASS1 = 0
        dfSize = df.size 
        df = df.fillna("")
        for index, row in df.iterrows():
            FNAME        = row['Forename']
            LNAME        = row['Surname']
            GEN          = row['GENDER']
            DOB          = row['DOB']  
            Locality     = row['Locality']  
            Setting      = row['Setting']
            OpenDate     = row['Open Date']
            CloseDate    = row['Close Date']
            Status       = row['Status']
            Source       = row['Source']
            Reason       = row['Reason']
            Outcome      = row['Outcome']
            Practitioner = row['Practitioner']
            Referrer     = row['Referrer']
            HOUSENO      = row['HOUSE NUMBER']
            HOUSE        = row['HOUSE']
            APARTMENT    = row['APARTMENT']
            ADDRESS1     = row['ADDRESS0']
            ADDRESS2     = row['ADDRESS1']
            ADDRESS3     = row['ADDRESS2']
            TOWN         = row['TOWN']
            POST         = row['POSTCODE']
            UPRN         = row['UPRN']
            ID           = row['ID']
            ADDRESS      = "%s %s %s %s %s %s %s" % (HOUSENO, HOUSE, APARTMENT, ADDRESS1, ADDRESS2, ADDRESS3, POST)            
            
            if len(str(FNAME)) > 1:
                pGUID = self.DB.insertPerson(GEN, FNAME, LNAME, DOB, '', ORIGIN, LOGSOURCE, DESC)  
                
                if len(str(ID)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'ID', '%s ID.' % ID, ID, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')             
                       
                if len(str(ADDRESS)) > 4:
                    lGUID = self.DB.insertLocation('Residence', ADDRESS, 0, 0, 0, POST, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'LivesAt', lGUID, 'Location')  
                    
                if len(str(Locality)) > 1:
                    lGUID = self.DB.insertLocation('Locality', Locality, 0, 0, 0, POST, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'LivesAt', lGUID, 'Location')              
                    
                if len(str(UPRN)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'UPRN', '%s Unique Property Registration Number.' % UPRN, UPRN, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')   
                
                if len(str(OpenDate)) > 1:
                    E_DESC = '%s %s reported by %s for %s with the following outcome: %s\n Referred by:%s' % (FNAME, LNAME, Source, Reason, Outcome, Referrer)
                    openGUID = self.DB.insertEvent('EHA', Reason, E_DESC, E_LANG, E_CLASS1, '12:00', OpenDate, '', 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(openGUID, 'Event', 'Involves', pGUID, 'Person')  
                else:
                    openGUID = None
            
                if len(str(CloseDate)) > 1:
                    E_DESC = '%s %s reported by %s for %s with the following outcome: %s\n Referred by:%s' % (FNAME, LNAME, Source, Reason, Outcome, Referrer)
                    closeGUID = self.DB.insertEvent('EHA', Reason, E_DESC, E_LANG, E_CLASS1, '12:00', CloseDate, '', 0, 0, ORIGIN, LOGSOURCE)                  
                    self.DB.insertRelation(openGUID, 'Event', 'Involves', pGUID, 'Person')
                else:
                    closeGUID = None                
                    
                if len(str(Outcome)) > 1:
                    OutcomeGUID = self.DB.insertObject('EHA', 'Outcome', Outcome, 0, 0, 0, ORIGIN, LOGSOURCE)
                    if openGUID != None:
                        self.DB.insertRelation(openGUID, 'Event', 'Involves', OutcomeGUID, 'Object') 
                    if closeGUID != None:
                        self.DB.insertRelation(closeGUID, 'Event', 'Involves', OutcomeGUID, 'Object')                     
                
                if len(str(Reason)) > 1:
                    ReasonGUID = self.DB.insertObject('EHA', 'Reason', Reason, 0, 0, 0, ORIGIN, LOGSOURCE) 
                    if openGUID != None:
                        self.DB.insertRelation(openGUID, 'Event', 'Involves', ReasonGUID, 'Object') 
                    if closeGUID != None:
                        self.DB.insertRelation(closeGUID, 'Event', 'Involves', ReasonGUID, 'Object') 
                        
                if len(str(Source)) > 1:
                    SourceGUID = self.DB.insertObject('EHA', 'Source', Source, 0, 0, 0, ORIGIN, LOGSOURCE)    
                    if openGUID != None:
                        self.DB.insertRelation(openGUID, 'Event', 'ReportedBy', SourceGUID, 'Object') 
                    if closeGUID != None:
                        self.DB.insertRelation(closeGUID, 'Event', 'ReportedBy', SourceGUID, 'Object')   
                        
                if len(str(Status)) > 1:
                    StatusGUID = self.DB.insertObject('EHA', 'Status', Status, 0, 0, 0, ORIGIN, LOGSOURCE)    
                    if openGUID != None:
                        self.DB.insertRelation(openGUID, 'Event', 'HasStatus', StatusGUID, 'Object') 
                    if closeGUID != None:
                        self.DB.insertRelation(closeGUID, 'Event', 'HasStatus', StatusGUID, 'Object')                   
                    
                if len(str(Practitioner)) > 1:
                    pFNAME = Practitioner.split(",")[1]
                    pLNAME = Practitioner.split(",")[0]
                    prGUID = self.DB.insertPerson('U', pFNAME, pLNAME, '0', '0', ORIGIN, LOGSOURCE, DESC)
                    oGUID = self.DB.insertObject('Role', 'Practicioner', 'This is a role of a Practicioner', 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(prGUID, 'Person', 'HasRole', oGUID, 'Object') 
                    if openGUID != None:
                        self.DB.insertRelation(prGUID, 'Person', 'Supporting', openGUID, 'Event')  
                    if closeGUID != None:
                        self.DB.insertRelation(prGUID, 'Person', 'Supporting', closeGUID, 'Event')                     
                    
                if len(str(Referrer)) > 1:
                    pFNAME = Referrer.split(",")[1]
                    pLNAME = Referrer.split(",")[0]
                    rfGUID = self.DB.insertPerson('U', pFNAME, pLNAME, '0', '0', ORIGIN, LOGSOURCE, DESC)  
                    oGUID = self.DB.insertObject('Role', 'Referrer', 'This is a role of a Referrer', 0, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(rfGUID, 'Person', 'HasRole', oGUID, 'Object') 
                    if openGUID != None:
                        self.DB.insertRelation(rfGUID, 'Person', 'Supporting', openGUID, 'Event')  
                    if closeGUID != None:
                        self.DB.insertRelation(rfGUID, 'Person', 'Supporting', closeGUID, 'Event')         
        
        self.currentState = 'complete'
        return {'message' : 'Extracted %d records from %s' % (index, ORIGIN)}  
    
    def ETL_TED_DV(self, df):
        
        ORIGIN = 'TED_DV'
        LOGSOURCE = 'A2'
        DESC = "" 
        E_LANG = 'en'
        E_CLASS1 = 0
        dfSize = df.size 
        df = df.fillna("") 
        for index, row in df.iterrows():

            ID           = row['TED_CHILD_ID']
            CAPID        = row['CAPITA_ID']
            FNAME        = row['Forename']
            LNAME        = row['Surname']
            GEN          = row['Gender']
            ETHNIC       = row['Ethnicity']            
            DISABILITY   = row['Disability']
            DOB          = row['D O B'] 
            ADDRESS      = row['Address']
            UPRN         = row['UPRN']
            POST         = row['Postcode']
            Status       = row['Status']
            StatusStart  = row['Status Start Date']
            
            if len(str(FNAME)) > 1:
                pGUID = self.DB.insertPerson(GEN, FNAME, LNAME, DOB, '', ORIGIN, LOGSOURCE, DESC)
                
                if len(str(UPRN)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'UPRN', '%s Unique Property Registration Number.' % UPRN, UPRN, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')  
                    
                if len(str(ETHNIC)) > 1:
                    ethnicGUID = self.DB.insertObject('HumanAttribute', 'Ethnicity', '%s ethnicity.' % ETHNIC, ETHNIC, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'HasAttribute', ethnicGUID, 'Object')  
                
                if len(str(DISABILITY)) > 1:
                    disGUID = self.DB.insertObject('HumanAttribute', 'Disability', '%s disability type.' % DISABILITY, DISABILITY, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'HasAttribute', disGUID, 'Object')                  
                    
                if len(str(ADDRESS)) > 4:
                    lGUID = self.DB.insertLocation('Residence', ADDRESS, 0, 0, 0, POST, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'LivesAt', lGUID, 'Location')  
                    
                if len(str(ID)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'TED ID', '%s TED ID.' % ID, ID, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person') 
                    
                if len(str(CAPID)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'CapitaID', '%s ID.' % CAPID, CAPID, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')                 
                 
                if len(str(StatusStart)) > 1:
                    E_DESC = '%s %s put on %s status program on %s' % (FNAME, LNAME, StatusStart, Status)
                    StatusStartGUID = self.DB.insertEvent('StatusStart', 'TED', E_DESC, E_LANG, E_CLASS1, '12:00', StatusStart, '', 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(StatusStartGUID, 'Event', 'Involves', pGUID, 'Person') 
                else:
                    StatusStartGUID = None

                if len(str(Status)) > 1:
                    StatusGUID = self.DB.insertObject('TED_DV', 'Status', Status, 0, 0, 0, ORIGIN, LOGSOURCE)    
                    if StatusStartGUID != None:
                        self.DB.insertRelation(StatusStartGUID, 'Event', 'HasStatus', StatusGUID, 'Object') 
              
        self.currentState = 'complete'
        return {'message' : 'Extracted %d records from %s' % (index, ORIGIN)}  
    
    def ETL_TF_Families(self, df):
        
        ORIGIN = 'TF_Families'
        LOGSOURCE = 'A2'
        DESC = "" 
        E_LANG = 'en'
        E_CLASS1 = 0
        t = df.shape[0]
        df = df.fillna("") 
        p = 0        
        for index, row in df.iterrows():
            FamID        = row['TF_Family_ID']
            PerID        = row['TF_Person_ID']   
            FNAME        = "%s %s" % (row['FirstName'], row['MiddleName'])
            LNAME        = row['Surname']
            GEN          = row['Gender']   
            Relationship = row['Relationship']
            DOB          = row['DOB']  
            SW           = row['CaseWorker']
            CAPID        = row['Capita_ID']
            TED_ID       = row['TED_ID']
            UPRN         = row['UPRN']
            
            if index > p:
                TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                try:
                    print("[%s] ETL_TF_Families %d of %d complete" % (TS, index, t))
                    p+=50 
                except:
                    pass
            
            if len(str(FNAME)) > 1:
                pGUID = self.DB.insertPerson(GEN, FNAME, LNAME, DOB, '', ORIGIN, LOGSOURCE, DESC)  
                
                if len(str(TED_ID)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'TED ID', '%s TED ID.' % TED_ID, TED_ID, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person') 
            
                if len(str(PerID)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'TF ID', '%s Think Family Person ID.' % PerID, PerID, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person') 
                
                if len(str(FamID)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'TF Family ID', '%s Think Family Unit ID.' % FamID, FamID, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'PartOf', uGUID, 'Object')   
                    
                if len(str(Relationship)) > 1:
                    uGUID = self.DB.insertObject('Family', 'Role', '%s in the family.' % Relationship, Relationship, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'HasRole', uGUID, 'Object')                  
                    
                if len(str(CAPID)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'CapitaID', '%s ID.' % CAPID, CAPID, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')   
                    
                if len(str(UPRN)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'UPRN', '%s Unique Property Registration Number.' % UPRN, UPRN, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person') 
                    
                if len(str(SW)) > 1:
                    FNAME = str(SW).split()[0]
                    if len(str(SW).split()) > 1:
                        LNAME = str(SW).split()[1]
                    else:
                        LNAME = ""
                    swGUID = self.DB.insertPerson('U', FNAME, LNAME, '', '', ORIGIN, LOGSOURCE, DESC) 
                    self.DB.insertRelation(swGUID, 'Person', 'Supporting', pGUID, 'Person')
                
        self.currentState = 'complete'
        return {'message' : 'Extracted %d records from %s' % (index, ORIGIN)}  
    
    def ETL_TED_Cohort(self, df):
        
        ORIGIN = 'TED_Cohort'
        LOGSOURCE = 'A2'
        DESC = "" 
        E_LANG = 'en'
        E_CLASS1 = 0
        dfSize = df.size 
        df = df.fillna("") 
        for index, row in df.iterrows():
            FNAME        = row['TED_FORENAME']
            LNAME        = row['TED_SURNAME']
            GEN          = row['TED_GENDER']
            ETHNIC       = row['TED_ETHNICITY']
            ID           = row['TED_CHILD_ID']
            DISABILITY   = row['TED_DISABILITY']
            DOB          = row['TED_DOB']  
            ADDRESS      = row['TED_ADDRESS']
            UPRN         = row['TED_UPRN']
            Status       = row['TED_STATUS']
            LACStart     = row['TED_LAC_START']
            LACEnd       = row['TED_LAC_END']
            CPPStart     = row['TED_CPP_START']
            CPPEnd       = row['TED_CPP_END']
            CINStart     = row['TED_CIN_START']
            CINEnd       = row['TED_CIN_END']  
            SW           = row['TED_SW']
            TEAM         = row['TED_TEAM']
            
            if len(str(FNAME)) > 1:
                pGUID = self.DB.insertPerson(GEN, FNAME, LNAME, DOB, '', ORIGIN, LOGSOURCE, DESC)
                
                if len(str(UPRN)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'UPRN', '%s Unique Property Registration Number.' % UPRN, UPRN, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')  
                    
                if len(str(ETHNIC)) > 1:
                    ethnicGUID = self.DB.insertObject('HumanAttribute', 'Ethnicity', '%s ethnicity.' % ETHNIC, ETHNIC, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'HasAttribute', ethnicGUID, 'Object')  
                
                if len(str(DISABILITY)) > 1:
                    disGUID = self.DB.insertObject('HumanAttribute', 'Disability', '%s disability type.' % DISABILITY, DISABILITY, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'HasAttribute', disGUID, 'Object')                  
                    
                if len(str(ADDRESS)) > 4:
                    lGUID = self.DB.insertLocation('Residence', ADDRESS, 0, 0, 0, '', ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'LivesAt', lGUID, 'Location')  
                    
                if len(str(ID)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'ID', '%s ID.' % ID, ID, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person') 
                
                if len(str(Status)) > 1:
                    StatusGUID = self.DB.insertObject('TED_DV', 'Status', Status, 0, 0, 0, ORIGIN, LOGSOURCE)    
                    self.DB.insertRelation(pGUID, 'Person', 'HasStatus', StatusGUID, 'Object') 
                 
                
                # Looked after Children (LAC highest threat)
                if len(str(LACStart)) > 1:
                    E_DESC = '%s %s put on Looked After Children (LAC) program on %s' % (FNAME, LNAME, LACStart)
                    LACStartGUID = self.DB.insertEvent('ProgramStart', 'LAC', E_DESC, E_LANG, E_CLASS1, '12:00', LACStart, '', 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(LACStartGUID, 'Event', 'Involves', pGUID, 'Person')  
                else:
                    LACStartGUID = None     
                if len(str(LACEnd)) > 1:
                    E_DESC = '%s %s taken off Looked After Children (LAC) program on %s' % (FNAME, LNAME, LACEnd)
                    LACEndGUID = self.DB.insertEvent('ProgramStart', 'LAC', E_DESC, E_LANG, E_CLASS1, '12:00', LACEnd, '', 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(LACEndGUID, 'Event', 'Involves', pGUID, 'Person')  
                else:
                    LACEndGUID = None
                # Child Protection ()
                if len(str(CPPStart)) > 1:
                    E_DESC = '%s %s put on Child Protection (CPP) program on %s' % (FNAME, LNAME, CPPStart)
                    CPPStartGUID = self.DB.insertEvent('ProgramStart', 'CPP', E_DESC, E_LANG, E_CLASS1, '12:00', CPPStart, '', 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(CPPStartGUID, 'Event', 'Involves', pGUID, 'Person')  
                else:
                    CPPStartGUID = None     
                if len(str(CPPEnd)) > 1:
                    E_DESC = '%s %s taken off Child Protection (CPP) program on %s' % (FNAME, LNAME, CPPEnd)
                    CPPEndGUID = self.DB.insertEvent('ProgramStart', 'CPP', E_DESC, E_LANG, E_CLASS1, '12:00', CPPEnd, '', 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(CPPEndGUID, 'Event', 'Involves', pGUID, 'Person')  
                else:
                    CPPEndGUID = None  
                # Child in Need ()             
                if len(str(CINStart)) > 1:
                    E_DESC = '%s %s put on Child in Need (CIN) program on %s' % (FNAME, LNAME, CINStart)
                    CINStartGUID = self.DB.insertEvent('ProgramStart', 'CIN', E_DESC, E_LANG, E_CLASS1, '12:00', CINStart, '', 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(CINStartGUID, 'Event', 'Involves', pGUID, 'Person')  
                else:
                    CINStartGUID = None     
                if len(str(CINEnd)) > 1:
                    E_DESC = '%s %s taken off Child in Need (CIN)  program on %s' % (FNAME, LNAME, CINEnd)
                    CINEndGUID = self.DB.insertEvent('ProgramStart', 'CPP', E_DESC, E_LANG, E_CLASS1, '12:00', CINEnd, '', 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(CINEndGUID, 'Event', 'Involves', pGUID, 'Person')  
                else:
                    CINEndGUID = None  
                
                if len(str(SW)) > 1:
                    FNAME = str(SW).split()[0]
                    if len(str(SW).split()) > 1:
                        LNAME = str(SW).split()[1]
                    else:
                        LNAME = ""
                    swGUID = self.DB.insertPerson('U', FNAME, LNAME, '', '', ORIGIN, LOGSOURCE, DESC)
                    if LACStartGUID != None:
                        self.DB.insertRelation(LACStartGUID, 'Event', 'Involves', swGUID, 'Person')  
                    if LACEndGUID != None:
                        self.DB.insertRelation(LACEndGUID, 'Event', 'Involves', swGUID, 'Person') 
                    if CPPStartGUID != None:
                        self.DB.insertRelation(CPPStartGUID, 'Event', 'Involves', swGUID, 'Person')  
                    if CPPEndGUID != None:
                        self.DB.insertRelation(CPPEndGUID, 'Event', 'Involves', swGUID, 'Person') 
                    if CINStartGUID != None:
                        self.DB.insertRelation(CINStartGUID, 'Event', 'Involves', swGUID, 'Person')  
                    if CINEndGUID != None:
                        self.DB.insertRelation(CINEndGUID, 'Event', 'Involves', swGUID, 'Person') 
                else:
                    swGUID = None
                    
                if len(str(TEAM)) > 1:
                    TEAMGUID = self.DB.insertObject('Organisation', 'Team', '%s Social Work team.' % TEAM, 0, 0, 0, ORIGIN, LOGSOURCE)
                    if LACStartGUID != None:
                        self.DB.insertRelation(LACStartGUID, 'Event', 'Involves', TEAMGUID, 'Object')  
                    if LACEndGUID != None:
                        self.DB.insertRelation(LACEndGUID, 'Event', 'Involves', TEAMGUID, 'Object') 
                    if CPPStartGUID != None:
                        self.DB.insertRelation(CPPStartGUID, 'Event', 'Involves', TEAMGUID, 'Object')  
                    if CPPEndGUID != None:
                        self.DB.insertRelation(CPPEndGUID, 'Event', 'Involves', TEAMGUID, 'Object') 
                    if CINStartGUID != None:
                        self.DB.insertRelation(CINStartGUID, 'Event', 'Involves', TEAMGUID, 'Object')  
                    if CINEndGUID != None:
                        self.DB.insertRelation(CINEndGUID, 'Event', 'Involves', TEAMGUID, 'Object')  
                    if swGUID != None:
                        self.DB.insertRelation(swGUID, 'Person', 'HasRole', TEAMGUID, 'Object') 
                
        self.currentState = 'complete'
        return {'message' : 'Extracted %d records from %s' % (index, ORIGIN)}  
    
    def ETL_YOT(self, df):
        
        ORIGIN = 'YOT'
        LOGSOURCE = 'A2'
        DESC = "" 
        E_LANG = 'en'
        E_CLASS1 = 0
        dfSize = df.size 
        df = df.fillna("") 
        for index, row in df.iterrows():
            FNAME        = row['Client Name']
            LNAME        = ""
            YOTID        = row['YOT_ID']
            REFNO        = row['Ref No']
            Client1      = row['Client No_1'] 
            Client2      = row['Client No_2'] 
            DOB          = row['DOB']
            HOUSENO      = row['House Number']
            POST         = row['Postcode']
            ADDRESS      = row['Address']
            UPRN         = row['UPRN']
            OutcomeDate  = row['Outcome Date']
            Outcome      = row['Outcome Type Desc']
            Duration     = row['Duration (Hrs)']
            
            if len(str(FNAME)) > 1:
                pGUID = self.DB.insertPerson('U', FNAME, LNAME, DOB, '', ORIGIN, LOGSOURCE, DESC)
                
                if len(str(ADDRESS)) > 4:
                    lGUID = self.DB.insertLocation('Residence', "%s %s" % (HOUSENO, ADDRESS), 0, 0, 0, POST, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'LivesAt', lGUID, 'Location')  
                    
                if len(str(UPRN)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'UPRN', '%s Unique Property Registration Number.' % UPRN, UPRN, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')                  
               
                if len(str(YOTID)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'YOTID', '%s Youth Offending Team.' % YOTID, YOTID, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')                    
            
                if len(str(REFNO)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'REFNO', '%s Youth Offending Reference.' % REFNO, REFNO, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')  
                    
                if len(str(Client1)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'Client1', '%s Youth Offending Client 1 Reference.' % Client1, Client1, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')   
                    
                if len(str(Client2)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'Client2', '%s Youth Offending Client 2 Reference.' % Client2, Client2, 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')  
                    
                if len(str(OutcomeDate)) > 1:
                    E_DESC = '%s had %s on %s lasting for %s' % (FNAME, Outcome, OutcomeDate, Duration)
                    OutcomeDateGUID = self.DB.insertEvent('OutcomeDate', 'YOT', E_DESC, E_LANG, E_CLASS1, '12:00', OutcomeDate, '', 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(OutcomeDateGUID, 'Event', 'Involves', pGUID, 'Person')   
                else:
                    OutcomeDateGUID = None
                    
                if len(str(Outcome)) > 1:
                    E_DESC = '%s had %s on %s' % (FNAME, Outcome, OutcomeDate)
                    uGUID = self.DB.insertObject('Outcome', Outcome, '%s Youth Offending outcome.' % Outcome, Outcome, 0, 0, ORIGIN, LOGSOURCE) 
                    if OutcomeDateGUID != None:
                        self.DB.insertRelation(OutcomeDateGUID, 'Event', 'Involves', uGUID, 'Object')
                    else:
                        self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')         
        
        self.currentState = 'complete'
        return {'message' : 'Extracted %d records from %s' % (index, ORIGIN)}      
    
    def ETL_ADMS(self, df):
        
        ORIGIN = 'ADMS'
        LOGSOURCE = 'A2'
        DESC = "" 
        E_LANG = 'en'
        E_CLASS1 = 0
        dfSize = df.size 
        df = df.fillna("") 
        FEMALES = ['MISS', 'MS', 'MRS']
        for index, row in df.iterrows():
            UPRN         = row['UPRN'] 
            ADDRESS      = str(row['ADDRESS1_LA']).capitalize()
            TOWN         = str(row['ADDRESS2_LA']).capitalize()
            POST         = str(row['POSTCODE_LA'])
            FNAME        = str(row['FIRST_FORENAME']).capitalize()
            LNAME        = str(row['SURNAME']).capitalize()
            DOB          = str(row['DATE_OF_BIRTH'])
            JSA          = row['START_DATE_JSA']
            ESA          = row['START_DATE_ESA']
            IS           = row['START_DATE_IS']
            IB           = row['START_DATE_IB']
            SDA          = row['START_DATE_SDA']
            CA           = row['START_DATE_CA']
            GEN           = row['TITLE']
            
            if str(GEN) in FEMALES:
                GEN = 'F'
            else:
                GEN = 'M'
            if len(str(FNAME)) > 1:
                pGUID = self.DB.insertPerson(GEN, FNAME, LNAME, DOB, '', ORIGIN, LOGSOURCE, DESC)   
                
                ADDRESS = '%s %s %s' % (ADDRESS, TOWN, POST)
                if len(str(ADDRESS)) > 5:
                    lGUID = self.DB.insertLocation('Residence', ADDRESS, 0, 0, 0, POST, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'LivesAt', lGUID, 'Location')                  
                
                # Job Seekers Allowance
                if len(str(JSA)) > 1:
                    E_DESC = '%s %s started JSA status on %s' % (FNAME, LNAME, JSA)
                    eGUID = self.DB.insertEvent('ProgramStart', 'JSA', E_DESC, E_LANG, E_CLASS1, '12:00', JSA, '', 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(eGUID, 'Event', 'Involves', pGUID, 'Person')  
                if len(str(ESA)) > 1:
                    E_DESC = '%s %s started ESA status on %s' % (FNAME, LNAME, ESA)
                    eGUID = self.DB.insertEvent('ProgramStart', 'ESA', E_DESC, E_LANG, E_CLASS1, '12:00', ESA, '', 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(eGUID, 'Event', 'Involves', pGUID, 'Person') 
                if len(str(IS)) > 1:
                    E_DESC = '%s %s started IS status on %s' % (FNAME, LNAME, IS)
                    eGUID = self.DB.insertEvent('ProgramStart', 'IS', E_DESC, E_LANG, E_CLASS1, '12:00', IS, '', 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(eGUID, 'Event', 'Involves', pGUID, 'Person')                 
                if len(str(IB)) > 1:
                    E_DESC = '%s %s started IB status on %s' % (FNAME, LNAME, IB)
                    eGUID = self.DB.insertEvent('ProgramStart', 'IB', E_DESC, E_LANG, E_CLASS1, '12:00', IB, '', 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(eGUID, 'Event', 'Involves', pGUID, 'Person')  
                if len(str(SDA)) > 1:
                    E_DESC = '%s %s started SDA status on %s' % (FNAME, LNAME, SDA)
                    eGUID = self.DB.insertEvent('ProgramStart', 'SDA', E_DESC, E_LANG, E_CLASS1, '12:00', SDA, '', 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(eGUID, 'Event', 'Involves', pGUID, 'Person')
                if len(str(CA)) > 1:
                    E_DESC = '%s %s started CA status on %s' % (FNAME, LNAME, CA)
                    eGUID = self.DB.insertEvent('ProgramStart', 'CA', E_DESC, E_LANG, E_CLASS1, '12:00', CA, '', 0, 0, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(eGUID, 'Event', 'Involves', pGUID, 'Person')   
        
        
        self.currentState = 'complete'
        return {'message' : 'Extracted %d records from %s' % (index, ORIGIN)}      
    
    def ETL_NEET(self, df):
       
        ORIGIN = 'NEET'
        LOGSOURCE = 'A2'
        DESC = "" 
        E_LANG = 'en'
        E_CLASS1 = 0
        dfSize = df.size 
        df = df.fillna("") 
        for index, row in df.iterrows():
            FNAME        = row['Firstname']
            LNAME        = row['Surname'] 
            DOB          = row['DOB']
            GEN          = row['Sex']
            SUP          = row['Support Level']
            Status       = row['Status']
            UPRN         = row['UPRN']
            POST         = row['Postcode']
            Ward         = row['Ward']
            
            if len(str(FNAME)) > 1:
                pGUID = self.DB.insertPerson(GEN, FNAME, LNAME, DOB, '', ORIGIN, LOGSOURCE, DESC) 
                
                if len(str(Status)) > 1:
                    StatusGUID = self.DB.insertObject('NEET', 'Status', Status, 0, 0, 0, ORIGIN, LOGSOURCE) 
                    self.DB.insertRelation(pGUID, 'Person', 'HasStatus', StatusGUID, 'Object') 
                
                if len(str(SUP)) > 0:
                    SupportGUID = self.DB.insertObject('NEET', 'Support Level', SUP, 0, 0, 0, ORIGIN, LOGSOURCE) 
                    self.DB.insertRelation(pGUID, 'Person', 'HasStatus', SupportGUID, 'Object')             
                
                if len(str(POST)) > 1:
                    lGUID = self.DB.insertLocation('Residence', '%s %s' % (POST, Ward), 0, 0, 0, POST, ORIGIN, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'LivesAt', lGUID, 'Location')                 
                    
                
        self.currentState = 'complete'
        return {'message' : 'Extracted %d records from %s' % (index, ORIGIN)}          


class System:

    def __init__(self, DB):
        self.is_authenticated = False
        self.is_active = False
        self.is_anonymous = True
        self.GUID = None
        self.fpaths = []
        self.set_file_paths()
        if DB == 'ODB+' or DB == 'HDB':
            self.HDB = HM.HANAModel()
        else:
            self.HDB = None
        self.ODB = om.OrientModel(self.HDB)
        self.ODB.start()
        self.set_DB(DB)
        
    
    def start_worker(self, TargetFunction, args):
        
        self.currentState = None
        if TargetFunction == 'ETL':
            self.TD = Thread(target=self.ETL_Civica, args=(args,))
    
    def get_CurrentState():
        return self.currentState
    
    def set_DB(self, DB):
        if DB == 'ODB':
            self.DB = self.ODB
  
    def get_user(self, username):     
        '''Response['data'] format:
            ['data']['GUID']         = r['GUID']
            ['data']['UserAuth']     = r['LOGSOURCE']
            ['data']['UserType']     = r['CATEGORY']
            ['data']['UserName']     = username
        '''
        response = {'status' : 200,
                    'received' : username,
                    'service' : 'system.verify_user',
                    'timestamp' : datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}
        response['data'] = self.DB.get_user(username)
        self.TYPE = response['data']['UserType']
        if response['data'] == None or response['data'] == 'None':
            response['status'] = 201
            response['message'] = 'User %s not found' % username
        else:
            response['message'] = 'User %s found' % username
        return response        
    
    def verify_user(self, username):
        '''Response['data'] format:
            ['data']['GUID']         = r['GUID']
            ['data']['UserAuth']     = r['LOGSOURCE']
            ['data']['UserPassword'] = r['O_CLASS2']
            ['data']['UserType']     = r['CATEGORY']
            ['data']['UserName']     = username
        '''
        response = {'status' : 200,
                    'received' : username,
                    'service' : 'system.verify_user',
                    'timestamp' : datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}
        
        response['data'] = self.DB.get_user(username)
        
        
        if response['data'] == None:
            response['status'] = 201
            response['message'] = 'User %s not found' % username
        else:
            response['message'] = 'User %s found' % username
        return response
    
    def login(self, user, password):
        '''Response['data'] format:
            ['data']['GUID']         = r['GUID']
            ['data']['UserAuth']     = r['LOGSOURCE']
            ['data']['UserPassword'] = r['O_CLASS2']
            ['data']['UserType']     = r['CATEGORY']
            ['data']['UserName']     = username
        '''        
        response = {'status' : 200,
                    'received' : (user, password),
                    'service' : 'system.login',
                    'timestamp' : datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}

        if not bcrypt.verify(password, user['UserPassword']):
            response['status'] = 201
            response['data'] = {'GUID' : user['GUID']}
            response['message'] = 'The password entered does not match what is on record for user with ID %s' % user['GUID']
        else:
            user['UserPassword'] = 'XXXX'
            response['data']     = user
            response['message']  = 'Welcome %s' % response['data']['UserName']
        
        return response
               
    def get_user_profile(self, user):
        '''Response['data'] format:
            ['data']['GUID']         = r['GUID']
            ['data']['UserAuth']     = r['LOGSOURCE']
            ['data']['UserType']     = r['CATEGORY']
            ['data']['UserName']     = username
        '''   
        response = {'status' : 200,
                    'received' : (user),
                    'service' : 'system.get_user_profile',
                    'timestamp' : datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}
       
        try:
            response['data'] = self.DB.get_user_profile(user['GUID'])
            response['data']['UserName'] = user['UserName'] 
            response['data']['UserType'] = user['UserType']
            response['data']['UserAuth'] = user['UserAuth'] 
            response['message'] = "User %s profile loaded" % user['UserName'] 
        except Exception as e :
            response['message'] = "Error getting the user profile: %s" % str(e)
            response['data'] = user
            response['status'] = 500
        
        return response
    
    def reset_system(self):
        '''Response['data'] format:
           None returned. Only message of completion 
        '''   
        response = {'status' : 200,
                    'received' : 'Post call',
                    'service' : 'system.reset_system',
                    'timestamp' : datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}
        
        response['message'] = self.DB.initialize_reset()   
            
        return response
    
    def insertPerson(self, GEN, FNAME, LNAME, DOB, POB, ORIGIN, SOURCE, DESC):
        
        response = {'status' : 200,
                    'received' : 'GEN:%s, FNAME:%s, LNAME:%s, DOB:%s, POB:%s, ORIGIN:%s, SOURCE:%s, DESC:%s' % (GEN, FNAME, LNAME, DOB, POB, ORIGIN, SOURCE, DESC),
                    'service' : 'system.insertPerson',
                    'timestamp' : datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}
        
        GUID = self.DB.insertPerson(GEN, FNAME, LNAME, DOB, POB, ORIGIN, SOURCE, DESC)
        response['message'] = 'Entered new Person with GUID: %s' % GUID
        
        return response
        
    def set_file_paths(self):
        cwd = os.getcwd()
        if 'C:\\' in cwd:
            
            self.lakeURL = 'C:\\Users\\d063195\\Desktop\\Lake\\'
            self.TFURL = 'C:\\Users\\d063195\\Desktop\\Lake\\Think_Family\\'
            self.sep     = '\\'
        else:
            self.dataURL = '%s/data/sets' % cwd
            self.lakeURL = '%s/data/lake' % cwd
            self.TFURL = '%s/data/lake/TF/' % cwd
            self.sep = '/'   
        
        return {'message' : 'TF Paths set to %s' % self.TFURL}  
        
    def get_files(self):
        '''
        get files from a target URL and return a list of the paths and size of file
        data[{'path' : url, 'size' : int }]
        '''
        for f in os.listdir(self.TFURL):
            if os.path.isfile(self.TFURL + f) == True:
                self.fpaths.append({'path' : self.TFURL + f, 'size' : os.path.getsize(self.TFURL + f)})
        
        return {'message' : '%d files found' % len(self.fpaths), 'data' : self.fpaths}    
    
    def open_file(self, fpath):
        
        if fpath[-3:] == 'csv':
            return pd.read_csv(fpath)
        elif fpath[-3:] == 'lsx':
            return pd.read_excel(fpath)    
    
    
    def ETL_Civica(self, paths):
        
        for f in paths:
            if 'PostProcessed_TBL_Civica_ASB' in f['path']:  
                df = self.open_file(f['path'])
                print("DF size: %d" % df.size)
                t = df.size
                p = 0
                for index, row in df.iterrows():
                    
                    if index > p:
                        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                        print("[%s] ETL Civica %d of %d complete" % (TS, index, t))
                        p+=50                        
                    
                    UPRN = row['UPRN']
                    REF = row['Civica_ReferenceNo']
                    DATE = row['ASB_Date']
                    DESC = row['ASB_Description']
                    ADDR = row['Civica_Address']
                    ADSC = row['ASB_Decision']
                    TENU = row['Civica_Tenure']
                    OCCU = row['ASB_Occupier Name'] 
                    ORIGIN = 'CIVICA'
                    LOGSOURCE = 'A2'
                    
                    # get_first_degree_of_entity_type(uGUID, Object, Person, any, any)
                                   
                    if len(str(UPRN)) > 1:
                        uGUID = self.DB.insertObject('Reference Code', 'UPRN', '%s Unique Pupil Registration Number' % UPRN, UPRN, 0, 0, ORIGIN, LOGSOURCE)                    
                        
                        if len(str(REF)) > 1:
                            rGUID = self.DB.insertObject('Reference Code', 'Civica', '%s reference on %s' % (REF, DATE), REF, 0, 0, ORIGIN, LOGSOURCE)
                            self.DB.insertRelation(uGUID, 'Object', 'Involves', rGUID, 'Object')
                        
                        if len(str(ADDR)) > 1:
                            aGUID = self.DB.insertLocation('Civic Location', ADDR, 0, 0, 0, 0, ORIGIN, ORIGIN, LOGSOURCE)
                            self.DB.insertRelation(uGUID, 'Object', 'OccurredAt', aGUID, 'Location')
                            
                        if len(str(DATE)) > 1:
                            eGUID = self.DB.insertEvent('ASB', ADSC, '%s with decision %s by %s about %s' % (DESC, ADSC, TENU, OCCU), 'en', 0, '12:00', DATE, 0, 0, 0, ORIGIN, ORIGIN, LOGSOURCE)
                            self.DB.insertRelation(eGUID, 'Event', 'Involves', uGUID, 'Object')  
                
        return {'message' : 'CIVICA found with %d records' % (index)}  
    
    
    def get_name(self):
        return self.username
    
    def ETL_TED_DV(self, paths):
        
        ORIGIN = 'TED_DV'
        LOGSOURCE = 'A2'
        DESC = ""
        for f in paths:
            if 'TBL_TED_DV' in f['path']:
                df = self.open_file(f['path'])
                print("DF size: %d" % df.size)
                t = df.size
                p = 0
                for index, row in df.iterrows():
                    
                    if index > p:
                        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                        print("[%s] ETL TED_DV %d of %d complete" % (TS, index, t))
                        p+=50                
                    
                    P_FNAME = str(row['Forename']) + ' ' + str(row['Surname'])
                    P_GEN = str(row['Gender']).upper()
                    P_DOB = row['D O B']
                    
                    #Codes that establish statuses of the Child
                    TEDID = row['TED_CHILD_ID']
                    TED_ETHNICITY = row['Ethnicity']
                    TED_DISABILITY = row['Disability']
                    TED_UPRN = row['UPRN']
                    TED_STATUS = row['Status']
                    TED_CAPID = row['CAPITA_ID']
                    
                    TED_ADDRESS = str(row['Address']) + ' ' + str(row['Postcode'])
                    START_DATE = row['Status Start Date']
                    
                    # TODO get first degree of person to UPRN 

                    if len(P_FNAME) > 1:
                        pGUID = self.DB.insertPerson(P_GEN, P_FNAME, '', P_DOB, '', ORIGIN, LOGSOURCE, DESC)
                        
                        if len(str(TEDID)) > 1:
                            nGUID = self.DB.insertObject('Reference Code', 'TED', '%s TED ID assigned to %s' % (TEDID, P_FNAME), TEDID, 0, 0, ORIGIN, LOGSOURCE)
                            self.DB.insertRelation(nGUID, 'Object', 'Involves', pGUID, 'Person')
                        
                        if len(str(TED_ETHNICITY)) > 1:
                            iGUID = self.DB.insertObject('Reference Code', 'Ethnicity', 'Ethnicity %s assigned to %s' % (TED_ETHNICITY, P_FNAME), TED_ETHNICITY, 0, 0, ORIGIN, LOGSOURCE)
                            self.DB.insertRelation(iGUID, 'Object', 'Involves', pGUID, 'Person')
 
                        if len(str(TED_DISABILITY)) > 1:
                            iGUID = self.DB.insertObject('Reference Code', 'Disability', 'Disability %s assigned to %s' % (TED_DISABILITY, P_FNAME), TED_DISABILITY, 0, 0, ORIGIN, LOGSOURCE)
                            self.DB.insertRelation(iGUID, 'Object', 'Involves', pGUID, 'Person')
 
                        if len(str(TED_UPRN)) > 1:
                            uGUID = self.DB.insertObject('Reference Code', 'UPRN', '%s Unique Pupil Registration Number' % TED_UPRN, TED_UPRN, 0, 0, ORIGIN, LOGSOURCE)
                            self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')

                        if len(str(TED_STATUS)) > 1:
                            sGUID = self.DB.insertObject('Reference Code', 'Status', 'Status %s assigned to %s' % (TED_STATUS, P_FNAME), TED_STATUS, 0, 0, ORIGIN, LOGSOURCE)
                            self.DB.insertRelation(sGUID, 'Object', 'Involves', pGUID, 'Person')

                        if len(str(TED_ADDRESS)) > 1:
                            lGUID = self.DB.insertLocation('Home Address', TED_ADDRESS, 0, 0, 0, 0, ORIGIN, ORIGIN, LOGSOURCE)
                            self.DB.insertRelation(pGUID, 'Person', 'LivesAt', lGUID, 'Location')

                        if len(str(TED_CAPID)) > 1:
                            swGUID = self.DB.insertPerson('U', TED_CAPID, '', '', '', ORIGIN, LOGSOURCE, '')
                            self.DB.insertRelation(swGUID, 'Person', 'Supported', pGUID, 'Person')
                            
                        if len(str(START_DATE)) > 1:
                            dateGUID = self.DB.insertEvent('Status', 'Start', '%s for %s started %s' % (TED_STATUS, P_FNAME, START_DATE), 'en', 0, 0, START_DATE, 0, 0, 0, 0, 0, 0)
                            self.DB.insertRelation(sGUID, 'Object', 'On', dateGUID, 'Event')   
                            
                return {'message' : 'TED_DV found with %d records' % (index)}
 