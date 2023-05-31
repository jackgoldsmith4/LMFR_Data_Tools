### OLD FUNCTIONS - WERE FOR HELPER TOOLS IN THE SAME FOLDER

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
