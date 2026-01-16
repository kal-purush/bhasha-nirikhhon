import json
import boto3
import os

def add_user_to_group(userpoolid, username, groupname):
    client = boto3.client('cognito-idp')
    response = client.admin_add_user_to_group(
    UserPoolId=userpoolid,
    Username=username,
    GroupName=groupname
    )
    print(response)
    
def remove_user_from_all_groups(userpoolid, username):
    client = boto3.client('cognito-idp')
    response = client.list_groups(
    UserPoolId=userpoolid,
    Limit=60
    )
    response_j = json.dumps(response,default=str)
    response_j1 = json.loads(response_j)
    for groups in response_j1['Groups']:
        groupname = groups['GroupName']
        if not groupname.startswith('eu-west-1'):
            response = client.admin_remove_user_from_group(
            UserPoolId=userpoolid,
            Username=username,
            GroupName=groupname
            )

def get_username_from_preferred_username(userpoolid, in_preferred_username):
    client = boto3.client('cognito-idp')
    filter = "preferred_username = \"" + in_preferred_username + "\""
    response = client.list_users(UserPoolId=userpoolid,AttributesToGet=[], Filter=filter)
    response_j = json.dumps(response,default=str)
    response_j1 = json.loads(response_j)
    return response_j1['Users'][0]['Username']

def get_username_from_email_and_providername(userpoolid, in_email, in_provider_name):
    out_username='None'
    client = boto3.client('cognito-idp')
    filter = "email = \"" + in_email + "\""
    response = client.list_users(UserPoolId=userpoolid,AttributesToGet=['identities'], Filter=filter)
    response_j = json.dumps(response,default=str)
    response_j1 = json.loads(response_j)
    for users in response_j1['Users']:
        json_u = json.loads(users['Attributes'][0]['Value'].replace('[','').replace(']',''))
        if json_u['providerName'] == in_provider_name:
            out_username = users['Username']
            break
    return out_username
    
    
def get_userpoolid(project, user_pool_suffix):
    client = boto3.client('cognito-idp')
    response = client.list_user_pools(
        NextToken="PaginationKeyType",
        MaxResults=20
    )
    filtered_list_project = filter(lambda x: project in x['Name'], response['UserPools']) 
    filtered_list_suffix = filter(lambda x: user_pool_suffix in x['Name'],filtered_list_project)
    for list in filtered_list_suffix:
        userpoolid=(list['Id'])
        break
    return(userpoolid)

def get_provider_name_from_cognito_event(cognitoevent):
    json_u = json.loads(cognitoevent['request']['userAttributes']['identities'].replace('[','').replace(']',''))
    return json_u['providerName']

def lambda_handler(event, context):
    
    s3 = boto3.client('s3')
    conf_rvs_bucket = os.getenv("CONFIGURATION_SOURCE_BUCKET")
    conf_file = os.getenv("CONFIGURATION_FILE")
    s3_object = s3.get_object(Bucket=conf_rvs_bucket, Key=conf_file)
    data = s3_object['Body'].read()
    contents = data.decode('utf-8')
    configuration_content = json.loads(contents)

    if 'userName' in event and 'userPoolId' in event:
        #Scenario 1 - When the lambda gets triggered by Cognito Post Confirmation trigger
        #Please refer to the test even EventFromCognitoTrigger for the sample event recieved
        #In this event userpoolid and username are passed from Cognito to Lambda
        
        username = event['userName']
        userpoolid = event['userPoolId']
        preferred_username = event['request']['userAttributes']['preferred_username']
        email = event['request']['userAttributes']['email']
        providername = get_provider_name_from_cognito_event(event)
        print('EventFromCognitoTrigger')
        print("email = " + email)
        print("Cognito Username  = " +username)
        print("Cognito UserPoolId = " +userpoolid)
        print("Cognito ProviderName = " +providername)
        
    elif 'providerName' in event or 'email' in event:
        #Scenario 2 - When the lambda gets called in a loop by another script from the CICD pipeline 
        #This scenario expects input like preferred_username
        #In this event userpoolid and username are calulated from CognitoIDP
        userpoolid = get_userpoolid('cas-etl-winston','file-uploader-user-pool')
        #username = get_username(userpoolid,preferred_username)
        username = get_username_from_email_and_providername(userpoolid,event['email'],event['providerName'])
        email = event['email']
        providername = event['providerName']
        print('EventFromRolesRights')
        print("email = " + email)
        print("Cognito Username  = " +username)
        print("Cognito UserPoolId = " +userpoolid)
        print("Cognito ProviderName = " +providername)

    #Write the code to read the configuration file which has the mapping of preferred_username
    #and the groups that it should be username should be assined to.
    
    if username == 'None':
        print("Username for email "+ event['email'] + " not found")
    else:
        #Frist remove the user from all the groups
        remove_user_from_all_groups(userpoolid, username)
        for user in configuration_content['users']:
            email_from_config = user['email']
            providername_from_config = user['providerName']
            if email_from_config == email and providername_from_config == providername:
                for group in user['groups']:
                    add_user_to_group(userpoolid, username, group)
            break
        
                
    return(event)