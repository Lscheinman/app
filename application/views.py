#!/usr/bin/env python3
# -*- coding: utf-8 -*
'''
Process Microservices:
Responsible for integrating UX services with the System level services within the application models
Imports the 


'''
import os, time, csv, requests, base64, json
from flask import Flask, request, session, redirect, url_for, render_template, flash, send_file, jsonify
from werkzeug.utils import secure_filename
from application import flask, app
from application.system import System, Worker
from datetime import datetime
from threading import Thread
import numpy as np
import logging

ODB = 'ODB'
HDB = 'HDB'
@app.route("/login", methods=["POST"])
def login():
    '''USES: verify_user, login
    
        Response['data'] format:
        ['data']['GUID']         = r['GUID']
        ['data']['UserAuth']     = r['LOGSOURCE']
        ['data']['UserPassword'] = XXXX
        ['data']['UserType']     = r['CATEGORY']
        ['data']['UserName']     = username
    '''    
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_Process-login]: FlaskG,%s Session.UserName, %s" % (TS, flask.g, session.get("username")))
    if request.method == "POST":
        username = request.form["UserName"]
        password = request.form["UserPassword"]
        
        # Start the system level microservices which are linked to the preconfigured DB
        S = System(ODB)
        # Verify the User for Logging In
        response = S.verify_user(username)
        if response['status'] == 200:
            # There is an existing user so log in with the DB user and entered password and set the session variables
            session['username'] = username
            session['UserAuth'] = response['data']['UserAuth']
            session['UserType'] = response['data']['UserType']
            response = S.login(response['data'], password)
            flask.g.user = username
            session['alerts'] = 5
            session['newmsg'] = 1

    else:
        response = {'status': 405, 'message': 'Method not allowed'}
    
    print("[%s_Process-login]: Complete. %s" % (TS, response['message']))
    return jsonify(response)

@app.route("/reset_system", methods=["POST"])
def reset_system():
    S = System(ODB)
    try:
        response = S.get_user(session['username'])
    except:
        S.reset_system()
        response = {'status' : 201, 'message' : 'Rest process started'}
    if response['status'] == 200:
            if response['data']['UserType'] == 'Admin':
                response = S.reset_system()
            else:
                response['message'] = 'Not authorized to conduct system reset.'

    return jsonify(response)

@app.route("/initialize_load", methods=["POST"])
def initialize_load():
    S = System(ODB)
    
    running_message = "START"
    response = S.set_file_paths()
    running_message = "%s\n%s" % (running_message, response['message'])
    response = S.get_files()
    running_message = "%s\n%s" % (running_message, response['message'])
    print(running_message)
    # Step One
    for f in response['data']:
        if 'TF_Families' in f['path']:  
            df = S.open_file(f['path']) 
            W1 = Worker('TF_Families', df)
            #W1.start()
    # Step Two
    for f in response['data']:
        if 'Education_Reference' in f['path']:  
            df = S.open_file(f['path']) 
            W2 = Worker('Education_Reference', df)
            W2.start()    
    response['message'] = running_message + '\nStarted'
    return jsonify(response)
            
    
@app.route("/load_user_data", methods=["GET"])
def load_user_data():
    '''USES: get_user, get_user_profile
    
        Response['data'] format:
        ['data']['GUID']         = r['GUID']
        ['data']['UserAuth']     = r['LOGSOURCE']
        ['data']['UserType']     = r['CATEGORY']
        ['data']['UserName']     = username
        
    '''        
    S = System(ODB)
    # Get the user to start up the data collection
    response = S.get_user(session['username'])
    if response['status'] == 200:
        response = S.get_user_profile(response['data'])

    
    return jsonify(response)

@app.route("/extract_file", methods=["POST"])
def extract_file():
    
    iObj = request.form.to_dict(flat=False)
    S = System(ODB)
    running_message = "START"
    response = S.set_file_paths()
    running_message = "%s\n%s" % (running_message, response['message'])
    response = S.get_files()
    running_message = "%s\n%s" % (running_message, response['message'])
    print(running_message)
    
    if iObj['Type'][0] == 'Police':
        response = S.ETL_Civica(response['data'])
        running_message = "%s\n%s" % (running_message, response['message'])
        
    if iObj['Type'][0] == 'TED_DV':
        for f in response['data']:
            if 'TBL_TED_DV' in f['path']:  
                df = S.open_file(f['path'])   
                frames = np.array_split(df, 2)
                worker1 = Worker('TED_DV', frames[0])
                worker1.start()
                worker2 = Worker('TED_DV', frames[1])
                worker2.start()
                
    if iObj['Type'][0] == 'TED_Cohort':
        for f in response['data']:
            if 'TBL_TED_DV' in f['path']:  
                df = S.open_file(f['path'])   
                frames = np.array_split(df, 2)
                worker1 = Worker('TED_Cohort', frames[0])
                worker1.start()
                worker2 = Worker('TED_Cohort', frames[1])
                worker2.start()     
                
    if iObj['Type'][0] == 'Academy':
        for f in response['data']:
            if 'Academy' in f['path']:  
                df = S.open_file(f['path'])   
                frames = np.array_split(df, 2)
                worker1 = Worker('Academy', frames[0])
                worker1.start()
                worker2 = Worker('Academy', frames[1])
                worker2.start()  
                
    response['message'] = "Process started"
    running_message = "%s\n%s" % (running_message, response['message'])    
        
    print(iObj)
    return jsonify(response)



@app.route("/new_programme_entry", methods=["POST"])
def new_programme_entry():
    '''USES: get_user, get_user_profile
    
        Response['data'] format:
        ['data']['GUID']         = r['GUID']
        ['data']['UserAuth']     = r['LOGSOURCE']
        ['data']['UserType']     = r['CATEGORY']
        ['data']['UserName']     = username
        
    '''  
    S = System(ODB)
    iObj = request.form.to_dict(flat=False)
    SOURCE = ORIGIN = 'APP'
    F = {}
    F['Forename'] =         iObj['TF-Forename'][0]
    F['Middlename'] =       iObj['TF-Middlename'][0]
    F['Surname'] =          iObj['TF-Surname'][0]
    F['DOB'] =              iObj['TF-DOB'][0]
    F['Gender'] =           iObj['TF-Gender'][0]
    F['Ethnicity'] =        iObj['TF-Ethnicity'][0]
    F['Parent'] =           iObj['TF-Parent'][0]
    F['Grandparent'] =      iObj['TF-Grandparent'][0]
    F['Dependent'] =        iObj['TF-Dependent'][0]
    F['OtherAdult'] =       iObj['TF-OtherAdult'][0]
    F['OtherChild'] =       iObj['TF-OtherChild'][0]
    F['Address'] =          iObj['TF-Address'][0]
    F['PostCode'] =         iObj['TF-PostCode'][0]
    F['HousingTenure'] =    iObj['TF-HousingTenure'][0]
    F['StartDate'] =        iObj['TF-StartDate'][0]
    F['EndDate'] =          iObj['TF-EndDate'][0]
    F['AllocatedWorker'] =  iObj['TF-AllocatedWorker'][0]    
    F['TFEASupport'] =      iObj['TF-TFEASupport'][0]
    F['TFEWOSupport'] =     iObj['TF-TFEWOSupport'][0]
    F['IssueCrimeASB'] =    iObj['TF-IssueCrimeASB'][0]
    F['IssueEducation'] =   iObj['TF-IssueEducation'][0]    
    F['IssueFinancial'] =   iObj['TF-IssueFinancial'][0]
    F['ChildrenNeedHelp'] = iObj['TF-ChildrenNeedHelp'][0]
    F['DomesticAbuse'] =    iObj['TF-DomesticAbuse'][0]
    F['HealthIssue'] =      iObj['TF-HealthIssue'][0] 
    
    DESC = "Person entered as part of TF programme new entry"
    
    # Get the user to start up the data collection
    response = S.get_user(session['username'])
    if response['status'] == 200:
        if F['Forename'] != "":
            response = S.insertPerson(F['Gender'], F['Forename'] + ' ' + F['Middlename'], F['Surname'], F['DOB'], "Unk", ORIGIN, SOURCE, DESC)

    
    return jsonify(response)

@app.route("/logout", methods=["POST"])
def logout():
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    try:
        message = '%s logged out as of %s' % (session['username'], TS)
        session.pop("username")
        session.pop("UserType")
        session.pop("UserAuth")        
    except:
        message = 'Logged out as of %s' % TS

    return jsonify({'message' : message})


@app.route("/")
def index():

    return render_template("index.html")

