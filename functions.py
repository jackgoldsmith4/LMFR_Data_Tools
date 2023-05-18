from zeep import Client
from io import StringIO
import pandas as pd
import numpy as np
import datetime
import requests
import json
import math
import time
import sys

### AUTH FUNCTIONS

# login function that returns a Salesforce API session
def loginToSalesforce(username, password, securityToken):
    wsdl = './basic_wsdl.xml'
    
    SOAPclient = Client(wsdl)
    data = {'username': username, 'password': password+securityToken}
    response = SOAPclient.service.login(**data)
    sessionId = response.sessionId

    # create session for Bulk 2.0 API calls
    session = requests.Session()
    session.headers.update({'Authorization': 'Bearer '+ sessionId})
    
    return session

# login function for Salesforce sandbox, returns a dev session for testing
# DEVELOPMENT MODE -- FOR TESTING ONLY
# need to pull client ID and client secret from a sandbox in Salesforce and plug them into this function below
def loginToSalesforceSANDBOX(username, password, securityToken):
    # API variables for development mode
    clientId = ''
    clientSecret = ''
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type':'password',
        'client_id': clientId,
        'client_secret': clientSecret,
        'username': username,
        'password': password+securityToken
    }
    response = requests.post('https://test.salesforce.com/services/oauth2/token', data=data, headers=headers)
    sessionId = response.json()['access_token']

    # create session for Bulk 2.0 API calls
    session = requests.Session()
    session.headers.update({'Authorization': 'Bearer '+ sessionId})
    
    return session

### GENERAL HELPERS

# helper function to cleanup whitespace between words in a DF column
def cleanupNameWhitespace(df, colName):
    for index, row in df.iterrows():
        df.at[index, colName] = ' '.join(str(df.at[index, colName]).split())
    return df

### SALESFORCE BULK 2.0 API FUNCTIONS: QUERY AND INGEST

# function to query Salesforce and return a Pandas Dataframe
def getDataframeFromSalesforce(query, session, uri):  
    session.headers.update({'Content-Type': 'application/json;charset=utf-8'})
    
    # create a job to query all Account records
    data = json.dumps({
      "operation": "query",
      "query": query,
    })
    response = session.post(uri+'query', data=data)

    if (response.status_code == 200):
        print('Query job created.')
    else:
        print('Query job creation failed:\n' + str(response.json()))
        print('status code: ' + str(response.status_code))
        sys.exit()

    # pull out job ID to use for future requests
    jobId = response.json().get('id')

    # wait for job to complete before getting results
    print('Waiting for query job to complete...')
    jobComplete = False

    while not jobComplete:
        response = session.get(uri+'query/'+jobId)
        jsonRes = response.json()
        if str(jsonRes['state']) == 'JobComplete':
            jobComplete = True
        time.sleep(0.5)

    # get job results
    response = session.get(uri+'query/'+jobId+'/results')
    s = str(response.content,'utf-8')
    data = StringIO(s)

    df = pd.read_csv(data)
    print('Done.\n')
    return df

# function to create and execute a Salesforce bulk upload or delete job
def executeSalesforceIngestJob(operation, importData, objectType, session, uri):    
    # create data import job
    data = json.dumps({
       "operation":operation,
       "object":objectType,
       "contentType":"CSV",
       "lineEnding":"LF"
    })
    response = session.post(uri+'ingest/', data=data)

    if response.status_code == 200:
        if operation == 'insert':
            print('Upload job created.')
        elif operation == 'delete':
            print('Delete job created.')
        elif operation == 'update':
            print('Update job created.')
    else:
        if operation == 'insert':
            print('Upload job creation failed.')
        elif operation == 'delete':
            print('Delete job creation failed.')
        elif operation == 'update':
            print('Update job creation failed.')
        print(response.json())
        sys.exit()

    jobId = response.json().get('id')

    # add data to job
    session.headers.update({'Content-Type': 'text/csv;charset=UTF-8'})
    response = session.put(uri+'ingest/'+jobId+'/batches', data=importData.encode('utf-8'))

    if response.status_code == 201:
        print('Data added to job.')
    else:
        print('Data add failed.')
        print(response.json())
        sys.exit()

    # close the job => Salesforce begins processing the job
    session.headers.update({'Content-Type': 'application/json;charset=utf-8'})
    data = json.dumps({ 'state': 'UploadComplete' })
    response = session.patch(uri+'ingest/'+jobId, data=data)

    # wait for job to complete before getting results
    print('Waiting for job to complete...')
    jobComplete = False

    while not jobComplete:
        response = session.get(uri+'ingest/'+jobId)
        jsonRes = response.json()
        if str(jsonRes['state']) == 'JobComplete':
            jobComplete = True
        elif str(jsonRes['state']) == 'Failed':
            print('Job Failed. Please check Bulk Data Load Jobs in Salesforce Setup')
            print(jsonRes['errorMessage'])
            sys.exit()
        time.sleep(0.25)
    
    if operation == 'insert':
        print('Upload complete!\n')
    elif operation == 'delete':
        print('Deletion complete.\n')
    
    # get job results to display to user
    print('Job results:')
    response = session.get(uri+'ingest/'+jobId)
    jsonRes = response.json()
    print('Records processed: ' + str(jsonRes['numberRecordsProcessed']))
    print('Records failed: ' + str(jsonRes['numberRecordsFailed']) + '\n')
    if jsonRes['numberRecordsFailed'] > 0:
        response = session.get(uri+'ingest/'+jobId+'/failedResults')
        print('---ERROR MESSAGE---')
        print(response.text)
        print('-------------------')
        print('Please check Salesforce for further explanation: Setup > Bulk Data Load Jobs\n')
    
### WRAPPER FUNCTIONS

# generic function to upload Account (both donor and nonprofit) data to Salesforce
def uploadAccounts(salesforceAccountsDF, adminAccountsDF, accountType, session, uri):
    # TODO add Total_Weight__c and Total_Rescues__c once new fields and hierarchy are in Salesforce
    adminAccountsDF.columns = ['Parent Name', 'Name', 'ShippingStreet', 'ShippingCity', 'ShippingState', 'ShippingPostalCode', 'County__c']

    # clean Accounts data
    salesforceAccountsDF = salesforceAccountsDF[salesforceAccountsDF['RecordTypeId'] == accountType]
    salesforceAccountsDF = salesforceAccountsDF[['Id', 'Name']]
    salesforceAccountsDF = salesforceAccountsDF.reset_index().drop(axis='columns', columns=['index'])
    
    # cleanup whitespace from names and parent names
    salesforceAccountsDF = cleanupNameWhitespace(salesforceAccountsDF, 'Name')
    adminAccountsDF = cleanupNameWhitespace(adminAccountsDF, 'Parent Name')
    adminAccountsDF = cleanupNameWhitespace(adminAccountsDF, 'Name')

    # find all accounts in the admin tool not in salesforce
    accountsNotInSalesforceDF = pd.merge(adminAccountsDF, salesforceAccountsDF, on='Name', how='left')
    accountsNotInSalesforceDF = accountsNotInSalesforceDF[accountsNotInSalesforceDF['Id'].isnull()]
    accountsNotInSalesforceDF = accountsNotInSalesforceDF.reset_index().drop(axis='columns', columns=['index', 'Id'])

    # add columns for ParentId and RecordTypeId
    accountsNotInSalesforceDF['ParentId'] = None
    accountsNotInSalesforceDF['RecordTypeId'] = accountType

    # create empty dataframes for first and second upload jobs
    uploadDFRows = []
    uploadDF2Rows = []
    
    # iterate through accounts to determine parent-child relationships
    for index, row in accountsNotInSalesforceDF.iterrows():
        childName = row['Name']
        parentName = row['Parent Name']
        if (childName == parentName):
            uploadDFRows.append(row.values)
        elif (parentName in salesforceAccountsDF['Name'].values):
            parentId = salesforceAccountsDF[salesforceAccountsDF['Name'] == parentName]['Id'].item()
            row['ParentId'] = parentId
            uploadDFRows.append(row.values)
        else:
            # create generic record for the new parent account
            parentRow = [parentName, parentName, None, None, None, None, None, None, accountType]
            uploadDFRows.append(parentRow)
            # add child account to the second job
            uploadDF2Rows.append(row.values)

    # prepare dataframe for first upload
    uploadDF = pd.DataFrame(uploadDFRows, columns=accountsNotInSalesforceDF.columns)
    uploadDF.drop_duplicates(inplace=True)
    uploadDF = uploadDF.reset_index().drop(axis='columns', columns=['Parent Name', 'index'])
        
    # fix zip code formatting
    if not uploadDF['ShippingPostalCode'].dtype == 'object':
        uploadDF['ShippingPostalCode'] = uploadDF['ShippingPostalCode'].astype('Int64')
    
    # upload first job to Salesforce
    executeSalesforceIngestJob('insert', uploadDF.to_csv(index=False), 'Account', session, uri)

    # pull down new list of Accounts
    salesforceAccountsDF = getDataframeFromSalesforce('SELECT Id, Name, RecordTypeId FROM Account', session, uri)

    # clean new Accounts data
    salesforceAccountsDF = salesforceAccountsDF[salesforceAccountsDF['RecordTypeId'] == accountType]
    salesforceAccountsDF = salesforceAccountsDF[['Id', 'Name']]
    salesforceAccountsDF = salesforceAccountsDF.reset_index().drop(axis='columns', columns=['index'])

    # attach ID of parent to each record
    uploadDF2 = pd.DataFrame(uploadDF2Rows, columns=accountsNotInSalesforceDF.columns)
    for index, row in uploadDF2.iterrows():
        parentName = row['Parent Name']
        parentId = salesforceAccountsDF[salesforceAccountsDF['Name'] == parentName]['Id'].item()
        uploadDF2.at[index, 'ParentId'] = parentId
        
    # fix zip code formatting
    if not uploadDF2['ShippingPostalCode'].dtype == 'object':
        uploadDF2['ShippingPostalCode'] = uploadDF2['ShippingPostalCode'].astype('Int64')

    # drop parent name column and upload the new child accounts to salesforce
    uploadDF2.drop(axis='columns', columns=['Parent Name'], inplace=True)    
    executeSalesforceIngestJob('insert', uploadDF2.to_csv(index=False), 'Account', session, uri)
    
# generic function to upload Food Rescue data to Salesforce
def uploadFoodRescues(rescuesDF, session, uri):
    # load in Accounts from Salesforce
    salesforceAccountsDF = getDataframeFromSalesforce('SELECT Id, Name, RecordTypeId FROM Account', session, uri)

    # load in Contacts from Salesforce
    salesforceContactsDF = getDataframeFromSalesforce('SELECT Id, Name, AccountId FROM Contact', session, uri)

    # cleanup rescuesDF
    rescuesDF.drop(axis='columns', columns=['donor_name', 'recipient_name'], inplace=True)
    rescuesDF = rescuesDF[(rescuesDF['rescue_state'] == 'Canceled') | (rescuesDF['rescue_state'] == 'Complete')]
    rescuesDF = rescuesDF.reset_index().drop(axis='columns', columns='index')
    
    # change new field values for rescue state (Complete, Canceled) to be compatible with Salesforce fields (completed, canceled)
    rescuesDF['rescue_state'] = rescuesDF['rescue_state'].str.replace('Complete', 'completed')
    rescuesDF['rescue_state'] = rescuesDF['rescue_state'].str.replace('Canceled', 'canceled')

    # get list of Food Donors
    salesforceDonorsDF = salesforceAccountsDF[salesforceAccountsDF['RecordTypeId'] == '0123t000000YYv2AAG']
    salesforceDonorsDF = salesforceDonorsDF[['Id', 'Name']]
    salesforceDonorsDF = salesforceDonorsDF.reset_index().drop(axis='columns', columns=['index'])
    salesforceDonorsDF.columns = ['Food_Donor_Account_Name__c', 'donor_location_name']

    # get list of Nonprofit Partners
    salesforcePartnersDF = salesforceAccountsDF[salesforceAccountsDF['RecordTypeId'] == '0123t000000YYv3AAG']
    salesforcePartnersDF = salesforcePartnersDF[['Id', 'Name']]
    salesforcePartnersDF = salesforcePartnersDF.reset_index().drop(axis='columns', columns=['index'])
    salesforcePartnersDF.columns = ['Agency_Name__c', 'recipient_location_name']

    # get list of Volunteers
    salesforceVolunteersDF = salesforceContactsDF[salesforceContactsDF['AccountId'] == '0013t00001teMBwAAM']
    salesforceVolunteersDF = salesforceVolunteersDF[['Id', 'Name']]
    salesforceVolunteersDF = salesforceVolunteersDF.reset_index().drop(axis='columns', columns=['index'])
    salesforceVolunteersDF.columns = ['Volunteer_Name__c', 'volunteer']

    # cleanup whitespace in name fields before performing vlookups
    salesforceDonorsDF = cleanupNameWhitespace(salesforceDonorsDF, 'donor_location_name')
    salesforcePartnersDF = cleanupNameWhitespace(salesforcePartnersDF, 'recipient_location_name')
    salesforceVolunteersDF = cleanupNameWhitespace(salesforceVolunteersDF, 'volunteer')
    rescuesDF = cleanupNameWhitespace(rescuesDF, 'donor_location_name')
    rescuesDF = cleanupNameWhitespace(rescuesDF, 'recipient_location_name')
    rescuesDF['volunteer'] = rescuesDF['volunteer'].astype(str)
    rescuesDF = cleanupNameWhitespace(rescuesDF, 'volunteer')

    # Dataframe merges (vlookups) to add links to rescuesDF
    mergedDF = pd.merge(rescuesDF, salesforceDonorsDF, on='donor_location_name', how='left')
    mergedDF = pd.merge(mergedDF, salesforcePartnersDF, on='recipient_location_name', how='left')
    mergedDF = pd.merge(mergedDF, salesforceVolunteersDF, on='volunteer', how='left')

    # fix pickup_start column
    mergedDF['pickup_start'] = pd.to_datetime(mergedDF['pickup_start'], infer_datetime_format=True)
    mergedDF['pickup_start'] = mergedDF['pickup_start'].dt.date

    # fix columns to prepare for upload
    mergedDF.drop(axis='columns', columns=['Unnamed: 0', 'donor_location_name', 'recipient_location_name', 'volunteer', 'estimated_quantity', 'reported_quantity', ' unit_weight '], inplace=True)
    mergedDF.columns=['Rescue_Detail_URL__c', 'Rescue_Id__c', 'Day_of_Pickup__c', 'Food_Type__c', 'Description__c', 'Type__c', 'State__c', 'County__c', 'Weight__c', 'Food_Donor_Account_Name__c', 'Agency_Name__c', 'Volunteer_Name__c']

    # upload rescues to Salesforce
    executeSalesforceIngestJob('insert', mergedDF.to_csv(index=False), 'Food_Rescue__c', session, uri)
    
# wrapper function to upload Food Donors to Salesforce => purpose is to hide code from the IPYNB
def uploadFoodDonors(accountsDF, session, uri):
    # load in donor data from admin tool
    donorsDF = pd.read_csv('lastmile_donors.csv')

    # filter out unnecessary data columns
    # NOTE: for donors, shipping street is currently just line1, line2 is NA for all records
    # TODO: add total_weight and total_rescues once new fields and hierarchy are in Salesforce
    donorsDF = donorsDF[['Name', 'location_name', 'line1', 'city', 'state', 'zip', 'county']]

    # upload Food Donors (type ID: '0123t000000YYv2AAG') to Salesforce
    uploadAccounts(accountsDF, donorsDF, '0123t000000YYv2AAG', session, uri)

# wrapper function to upload Nonprofit Partners => purpose is to hide code from the IPYNB
def uploadNonprofitPartners(accountsDF, session, uri):
    # load in partner data from admin tool
    partnersDF = pd.read_csv('lastmile_partners.csv')

    # filter out unnecessary data columns
    # TODO: add back Weight and rescues columns once new fields and hierarchy in Salesforce
    partnersDF = partnersDF[['Name', 'location_name', 'line1', 'city', 'state', 'zip']]
    
    # add dummy column for county because admin tool nonprofits data doesn't store county
    partnersDF['county'] = ''

    # upload Nonprofit Partners (type ID: '0123t000000YYv3AAG') to Salesforce
    uploadAccounts(accountsDF, partnersDF, '0123t000000YYv3AAG', session, uri)
    
# wrapper function to upload Volunteers => purpose is to hide code from the IPYNB
def uploadVolunteers(contactsDF, session, uri):
    # load volunteer data from admin tool
    volunteersDF = pd.read_csv('lastmile_volunteers.csv')

    # filter all Contacts to just get Food Rescue Heroes (id: '0013t00001teMBwAAM')
    salesforceVolunteersDF = contactsDF[contactsDF['npsp__Primary_Affiliation__c'] == '0013t00001teMBwAAM']
    salesforceVolunteersDF = salesforceVolunteersDF[['Id', 'Name']] # TODO keep volunteer ID

    # exclude volunteers who aren't Active from the upload
    volunteersDF = volunteersDF[volunteersDF['user_state'] == 'Active']
    
    # clean up columns
    volunteersDF = volunteersDF[['user_id', 'first_name', 'last_name', 'email', 'phone', 'address', 'city', 'state', 'zip', 'county']]
    volunteersDF.columns = ['Volunteer_Id__c', 'FirstName', 'LastName', 'Email', 'Phone', 'MailingStreet', 'MailingCity', 'MailingState', 'MailingPostalCode', 'County__c']

    # exclude volunteers with #admin in last name from the upload
    volunteersDF = volunteersDF[~volunteersDF["LastName"].str.contains("#admin", na=False)]
    
    # create one temp Name column for merging with Salesforce
    volunteersDF['Name'] = volunteersDF['FirstName'] + ' ' + volunteersDF['LastName']
    
    # cleanup whitespace in the Name fields to increase matches
    volunteersDF = cleanupNameWhitespace(volunteersDF, 'Name')
    salesforceVolunteersDF = cleanupNameWhitespace(salesforceVolunteersDF, 'Name')

    # find all volunteers in the admin tool not in Salesforce
    volunteersNotInSalesforceDF = pd.merge(volunteersDF, salesforceVolunteersDF, on='Name', how='left') # TODO merge on volunteer ID
    volunteersNotInSalesforceDF = volunteersNotInSalesforceDF[volunteersNotInSalesforceDF['Id'].isnull()]
    volunteersNotInSalesforceDF = volunteersNotInSalesforceDF.reset_index().drop(axis='columns', columns=['index', 'Id'])

    ###
    # TODO: before uploading volunteers, create parent accounts for each new volunteer
    # - then query accounts, find based on primary contact ID (or some other way to find the correct account ID
    # - finally, for each contact, do a merge to add the AccountId field to the DF to be uploaded
    ###
    
    # add a column to register these Contacts as Food Rescue Heroes via the Primary Affiliation field
    volunteersNotInSalesforceDF['npsp__Primary_Affiliation__c'] = '0013t00001teMBwAAM'
    
    # format phone numbers, clean up columns
    volunteersNotInSalesforceDF['Phone'] = volunteersNotInSalesforceDF['Phone'].astype('Int64')
    volunteersNotInSalesforceDF = volunteersNotInSalesforceDF[['FirstName', 'LastName', 'Email', 'Phone', 'MailingStreet', 'MailingCity', 'MailingState', 'MailingPostalCode', 'County__c', 'npsp__Primary_Affiliation__c']]

    # upload Volunteers to Salesforce
    executeSalesforceIngestJob('insert', volunteersNotInSalesforceDF.to_csv(index=False), 'Contact', session, uri)

# wrapper function that finds all new Food Rescues and uploads them to Salesforce
def uploadNewFoodRescues(session, uri):
    # read in all rescues from admin tool
    rescuesDF = pd.read_csv('lastmile_rescues.csv')

    # read in all rescues currently in Salesforce
    salesforceRescuesDF = getDataframeFromSalesforce('SELECT Id, Rescue_Id__c, Food_Type__c, Weight__c FROM Food_Rescue__c', session, uri).drop_duplicates()
    salesforceRescuesDF.columns = ['Id', 'rescue_id', 'food_type', ' total_weight ']

    # clarify types for total_weight column in both DFs
    salesforceRescuesDF[' total_weight '] = salesforceRescuesDF[' total_weight '].astype(np.int64)
    # clean up admin rescues due to new format
    rescuesDF = rescuesDF[rescuesDF[' total_weight '] != 'Please assign weight']
    rescuesDF[' total_weight '] = rescuesDF[' total_weight '].astype(np.int64)

    # find list of rescues not yet in Salesforce
    mergedDF = pd.merge(rescuesDF, salesforceRescuesDF, on=['rescue_id', 'food_type', ' total_weight '], how='left')
    mergedDF = mergedDF[mergedDF['Id'].isnull()]
    mergedDF = mergedDF.reset_index().drop(axis='columns', columns=['index', 'Id'])

    # upload these new rescues to Salesforce
    uploadFoodRescues(mergedDF, session, uri)

# master function to upload new data to Salesforce (Accounts, Contacts, Rescues)
def uploadDataToSalesforce(accountsDF, contactsDF, session, uri):
    # first make sure all new Donors, Nonprofits, and Volunteers are uploaded to Salesforce
    print('-----------------------------')
    print('Checking for new Food Donors:')
    print('-----------------------------')
    uploadFoodDonors(accountsDF, session, uri)
    print('------------------------------------')
    print('Checking for new Nonprofit Partners:')
    print('------------------------------------')
    uploadNonprofitPartners(accountsDF, session, uri)
    print('----------------------------')
    print('Checking for new Volunteers:')
    print('----------------------------')
    uploadVolunteers(contactsDF, session, uri)
    
    # upload new rescue data
    print('-------------------------------')
    print('Uploading all new Food Rescues:')
    print('-------------------------------')
    uploadNewFoodRescues(session, uri)
    print('\nDone!')

### WRAPPER FUNCTIONS FOR HELPER TOOLS

# generic function to find duplicate records
def findDuplicateRecords(df, colName):
    duplicatesDF = None
    try:
        duplicatesDF = pd.concat(g for _, g in df.groupby(colName) if len(g) > 1)
    except ValueError:
        duplicatesDF = 'No duplicates were found!'
        
    return duplicatesDF

# wrapper function that returns duplicate Food Donor Accounts in Salesforce
def findDuplicateFoodDonors(accountsDF, session, uri):
    # filter all Accounts to just get Food Donors (id: '0123t000000YYv2AAG')
    foodDonorsDF = accountsDF[accountsDF['RecordTypeId'] == '0123t000000YYv2AAG']

    return findDuplicateRecords(foodDonorsDF, 'Name')

# wrapper function that returns duplicate Nonprofit Partner Accounts in Salesforce
def findDuplicateNonprofitPartners(accountsDF, session, uri):
    # filter all Accounts to just get Nonprofit Partners (id: '0123t000000YYv3AAG')
    nonprofitPartnersDF = accountsDF[accountsDF['RecordTypeId'] == '0123t000000YYv3AAG']

    return findDuplicateRecords(nonprofitPartnersDF, 'Name')

# wrapper function that returns duplicate Volunteer Contacts in Salesforce
def findDuplicateVolunteers(contactsDF, session, uri):
    # filter all Contacts to just get Food Rescue Heroes (id: '0013t00001teMBwAAM')
    volunteersDF = contactsDF[contactsDF['AccountId'] == '0013t00001teMBwAAM']

    return findDuplicateRecords(volunteersDF, 'Name')

# function to find old rescues that haven't been marked as completed or canceled
def findIncompleteRescues():
    # filter out completed and canceled rescues
    rescuesDF = pd.read_csv('lastmile_rescues.csv')
    rescuesDF = rescuesDF[(rescuesDF['Rescue State'] != 'completed') & (rescuesDF['Rescue State'] != 'canceled')]
    rescuesDF = rescuesDF.reset_index().drop(axis='columns', columns=['index'])

    # convert date strings to date objects for comparison
    for index, _ in rescuesDF.iterrows():
        rescuesDF.at[index, 'Day of Pickup Start'] = datetime.datetime.strptime(rescuesDF.at[index, 'Day of Pickup Start'], '%Y-%m-%d').date()

    # return rescues before today that haven't been completed or canceled
    today = datetime.date.today()
    rescuesDF = rescuesDF[rescuesDF['Day of Pickup Start'] < today]
    return rescuesDF[['Rescue ID', 'Day of Pickup Start', 'Rescue State', 'Rescue Detail URL']].drop_duplicates().reset_index().drop(axis='columns', columns=['index'])

# function to update Salesforce rescues with comments from an excel file
def updateSFRescuesWithComments(session, uri):
    # get rescues from Salesforce
    salesforceRescuesDF = getDataframeFromSalesforce('SELECT Id, Rescue_Id__c, Comments__c FROM Food_Rescue__c', session, uri)
    salesforceRescuesDF.columns = ['Id', 'Rescue ID', 'Comments']

    # create rescues DF from comments CSV file
    commentsDF = pd.read_csv('lastmile_rescue_comments.csv')
    commentsDF = commentsDF[['Rescue ID', 'Comments']]

    # filter out rescues that already have associated comments
    salesforceRescuesDF = salesforceRescuesDF.loc[salesforceRescuesDF.Comments.isnull()]
    # drop the comments column (which is all NaN after above filter)
    salesforceRescuesDF = salesforceRescuesDF[['Id', 'Rescue ID']]

    # filter out records that have no comments to upload
    commentsDF = commentsDF.loc[commentsDF.Comments.notnull()]

    # merge two dataframes on Rescue IDs
    mergedCommentsDF = pd.merge(salesforceRescuesDF, commentsDF, on='Rescue ID', how='left')

    # filter so only rows that picked up comments to upload remain
    mergedCommentsDF = mergedCommentsDF[mergedCommentsDF['Comments'].notnull()]

    # drop Rescue ID column, rename Comments column, and update Salesforce with new Comments
    mergedCommentsDF.drop(axis='columns', columns=['Rescue ID'], inplace=True)
    mergedCommentsDF.columns = ['Id', 'Comments__c']
    executeSalesforceIngestJob('update', mergedCommentsDF.to_csv(index=False), 'Food_Rescue__c', session, uri)
    
# function to find all food rescue discrepancies between Salesforce and the admin tool
def findRescueDiscrepancies(session, uri, choose):
    salesforceRescuesDF = getDataframeFromSalesforce('SELECT State__c, Food_Type__c, Day_of_Pickup__c, Rescue_Detail_URL__c, Rescue_Id__c FROM Food_Rescue__c', session, uri)
    salesforceRescuesDF['Day_of_Pickup__c'] = pd.to_datetime(salesforceRescuesDF['Day_of_Pickup__c'])
    
    # only completed rescues
    salesforceRescuesDF = salesforceRescuesDF[salesforceRescuesDF['State__c'] == 'completed']

    # sort by Rescue ID
    salesforceRescuesDF = salesforceRescuesDF.sort_values(by='Rescue_Id__c')
    
    df = pd.read_csv('lastmile_rescues.csv')
    df['Day of Pickup Start'] = pd.to_datetime(df['Day of Pickup Start'])

    # only completed rescues
    df = df[df['Rescue State'] == 'completed']

    # sort by Rescue ID
    df = df.sort_values(by='Rescue ID')
    
    adminRescueID = df['Rescue ID']
    salesforceRescueID = salesforceRescuesDF['Rescue_Id__c']
    
    if (choose == 1):
        # print all rescue IDs in Salesforce but not in admin
        res = salesforceRescueID[~salesforceRescueID.isin(adminRescueID)]
        print('All rescue IDs that are marked completed in Salesforce but not in the admin tool:')
    elif (choose == 2):
        # print all rescue IDs in the admin tool but not in Salesforce
        res = adminRescueID[~adminRescueID.isin(salesforceRescueID)]
        print('All rescue IDs that are marked completed in the admin tool but not in Salesforce:')
    
    print('Record Count:')
    print(res.count())
    return res
