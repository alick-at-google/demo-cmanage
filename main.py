
'''
This Cloud function is responsible for:
- Finding content (dashboards & looks) that hasn't been accessed in 90 days
- Removing content to trash
- Notifying content owner of removal
- Deletes content permanently from trash for content not accessed in past month
'''
import looker_sdk
from looker_sdk import models
import os
import pandas as pd
import json
import csv
import numpy as np
import pytz
from datetime import datetime, timedelta, date, timezone
import os
import base64
import google.auth
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request
from google.oauth2 import service_account
import requests
import smtplib
#######################GSHEETS AUTH#########################
SERVICE_ACCOUNT_FILE = 'gsheets-api.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# SPREADSHEET_ID = '10rD-oV5FpojvPAqni0J1QHmA3ICnfxueG0DvOPyVRYw'
SPREADSHEET_ID = "17wh68_SaT5bkw3gRFp7PeRyWPzalyq9qaNNPp1_urXY"
###########Initilize Services#############
#looker sdk
sdk = looker_sdk.init31() 
credentials, project = google.auth.default()
#gsheets
service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()
#smtp
server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
server.ehlo()
server.login(os.environ.get('GMAIL_USER'), os.environ.get('GMAIL_PASS'))

def find_linked_dash(look_ids):
    '''
    Helper function to find dashboard(s) our linked look is on
    '''
    ######query parameters#######
    model = 'system__activity'
    view = 'dashboard'
    fields = ['dashboard.id', 'user.id', 'user.first_name', 'user.last_name', 'look.id']
    filters = {'look.id': ''}


    #######update dashboard filter####
    look_id_list = look_ids.values
    str_of_look_id = ",".join(look_id_list)
    filters['look.id'] = str_of_look_id

    ########construct query#############
    updated_query = (models.WriteQuery(
                            model=model,
                            view=view,
                            fields=fields,
                            filters=filters,
                            limit=-1))

    linked_dash_query = (sdk.run_inline_query(body = updated_query,
                               result_format = "csv")
                              .splitlines())

    linked_dash_df = pd.DataFrame(list(csv.reader(linked_dash_query)))
    headers = ['dashboard_id', 'user_id', 'first_name', 'last_name', 'id']
    linked_dash_df = linked_dash_df.iloc[1:]
    linked_dash_df.columns = headers
    return linked_dash_df

def get_users(content_type, content_df):
    '''
    Helper function to find the owner of the content
    '''
    #construct query parameters with filters
    model = 'system__activity'
    content_id = content_df['id']
    
    #gets dashboard owner
    if content_type == 'dashboard':
        view = 'dashboard'
        dash_fields = (['dashboard.id', 'dashboard.title',
                        'user.id', 'user.first_name', 
                        'user.last_name', 'user.email'])
        filters = {'dashboard.id': ''}
        unused_dash_id_filter_val = ",".join(content_id.values)
        filters['dashboard.id'] = unused_dash_id_filter_val

        #write query
        unused_dash_users = (models.WriteQuery(
                                    model=model,
                                    view=view,
                                    fields=dash_fields,
                                    pivots=[],
                                    filters=filters,
                                    limit=-1))
        #run query
        dashboard_users_df = (pd.DataFrame(list(csv.reader(
                              sdk.run_inline_query(body = unused_dash_users, 
                              result_format='csv').
                              splitlines()))))
        dashboard_users_df = dashboard_users_df.iloc[1:]
        dashboard_users_df.columns = ['id', 'content_name','user_id', 'first_name', 'last_name', 'email']
        dashboard_users_df['content_type'] = 'dashboard'
        dashboard_users_df = dashboard_users_df.merge(content_df, on='id').sort_values('last_accessed_date', ascending = False)
        return dashboard_users_df
    
    #gets look owner
    elif content_type == 'look':
        view = 'look'
        look_fields = (['look.id', 'look.title',
                       'user.id', 'user.first_name',
                       'user.last_name', 'user.email'])
        filters = {'look.id': ''}
        unused_look_id_filter_val = ",".join(content_id.values)
        filters['look.id'] = unused_look_id_filter_val
        #write query
        unused_look_users = (models.WriteQuery(
                                    model=model,
                                    view=view,
                                    fields=look_fields,
                                    pivots=[],
                                    filters=filters,
                                    limit=-1))
        #run query
        look_users_df = (pd.DataFrame(list(csv.reader(
                              sdk.run_inline_query(body = unused_look_users, 
                                                   result_format='csv')
                                                   .splitlines()))))
        look_users_df = look_users_df.iloc[1:]
        look_users_df.columns = ['id', 'content_name','user_id', 'first_name', 'last_name', 'email']
        look_users_df['content_type'] = 'look'
        look_users_df = (look_users_df.merge(content_df, on='id')
                                      .sort_values('last_accessed_date', ascending = False))

        return look_users_df
    else:
        raise ValueError

def construct_tables(df):
    '''
    Generates our unused content (looks & dashboards)
    '''
    unused_df = (pd.DataFrame(list(csv.reader(df.splitlines())))
                                    .replace("", np.nan, regex = True)     
                                    .iloc[1:])
    columns = ['last_accessed_date', 'dashboard_id', 'dashboard_title', 'look_id', 'look_title', 'look_used_on_dash']
    unused_df.columns = columns
    #dashboards that haven't been ran in the past 90 days
    unused_dashboards = (unused_df[unused_df['dashboard_id'].notna()]
                        [['last_accessed_date', 'dashboard_id']]
                        .rename(columns={'dashboard_id': 'id'}))


    #looks not ran in 90 days & not linked to dashboard
    unused_looks = (unused_df[(unused_df['look_id'].notna()) &
                   (unused_df['look_used_on_dash'] == 'No')]
                   [['last_accessed_date', 'look_id']]
                   .rename(columns={'look_id': 'id'}))

    #looks not ran in 90 days & linked to dashboard
    unused_looks_linked = (unused_df[(unused_df['look_id'].notna()) &
                          (unused_df['look_used_on_dash'] == 'Yes')]
                          [['last_accessed_date', 'look_id']]
                          .rename(columns={'look_id':'id'}))


    #filter for linked looks whose dashboards also hasn't been ran in past 90 days
    linked_dashboards = find_linked_dash(unused_looks_linked['id'])
    linked_looks_to_delete = ((linked_dashboards)
                             .merge(unused_dashboards
                             .rename(columns={'dashboard_id': 'id'}), on='id', how='inner')
                             [['last_accessed_date', 'id']])

    #looks not ran in 90 days (both linked and unlinked)    
    unused_looks = pd.concat([unused_looks, linked_looks_to_delete])

    #get the owners associated with the content
    unused_dashboards_df = get_users('dashboard',unused_dashboards)
    unused_looks_df = get_users('look',unused_looks)
    unused_content_df = pd.concat([unused_dashboards_df, unused_looks_df])
    return unused_content_df

def date_threshold(days):
    '''
    Helper function to return a date "X days ago"
    '''
    utc = pytz.utc
    #returns a date that X days ago
    return utc.localize(datetime.now() - timedelta(days=days))


def move_to_trash(df):
    '''
    Moves unused content to trash
    '''
    for row in df.iterrows():
        content_id = row[1]['id']
        content_type = row[1]['content_type']
        if(content_type == 'look'):
            sdk.update_look(content_id, body=models.WriteLookWithQuery(deleted=True))
            print('look {} has successfully been moved to trash'.format(content_id))
        elif (content_type == "dashboard"):
            sdk.update_dashboard(str(content_id), body=models.WriteDashboard(deleted=True))
            print('dashboard {} has successfully been moved to trash'.format(content_id))
        else:
            pass
        

def alert_user(df):
    '''
    Function to generate email and alerts content owner of deletion
    '''    
    #filter out duplicate user records
    df = df.drop_duplicates('email')
    for row in df.iterrows():
        #construct email
        first_name = row[1]['first_name']
        last_name = row[1]['last_name']
        email = row[1]['email']
        content_type = row[1]['content_type']
        content_id = row[1]['id']
        content_name = row[1]['content_name']
        last_accessed_date = row[1]['last_accessed_date']
        instance_name = 'DemoExpo'
        emailMsg = ('<h2">Hey {first_name} {last_name}, <br><br>  <b>Your content has been moved to trash due to inactivity (not accessed in 90 or more days) on one of our dev instances. Please reference <a href="https://docs.google.com/spreadsheets/d/17wh68_SaT5bkw3gRFp7PeRyWPzalyq9qaNNPp1_urXY/edit?usp=sharing">this Google Sheets</a> to find content. If you still need the content, please:<br><br> <ol><li>Check the checkbox in the <b>restored<b> column (last column)</li><li>Click the <b>Util</b> action bar menu by the Help button (making sure you leave your cell selection on the restored column and on the row which contains the content you want to restore)</li><li>Click Restore</li></ol><br><br>Otherwise, it will be permanetely removed from the instance in 1 week. If you have additional questions, feel free to reach out! <br><br> alick@ </h2>'
                    .format(first_name=first_name, last_name=last_name, content_name=content_name, content_type=content_type, content_id=content_id, last_accessed_date=last_accessed_date))    
        #set up STMP info
        msg = MIMEMultipart('alternative')
        msg['From'] = "alick@google.com"
        msg['To'] = email
        msg['Subject'] = '[{instance_name}]: {content_type} {content_id} has been moved to trash'.format(instance_name=instance_name, content_type=content_type, content_id=content_id)
        msg.attach(MIMEText(emailMsg, 'html'))
        try:
            #send email
            server.sendmail(msg['From'], msg['To'], msg.as_string()) 
        except:
            print("Couldn't establish connection or authenticate")
    server.quit()
    return "Successfully Alerted User(s)", 200

def update_gsheet(df):
    '''
    Function to log content that's been moved to trash
    '''
    df['deleted_at'] = datetime.now().__str__()
    df['instance'] = 'demoexpo'
    # df['url'] = df['instance'] + ".looker.com/" + df['content_type'] + "s/" + df['id'] 
    df['url'] = df['instance'] + ".looker.com/browse/trash"
    data = df.values.tolist()
    dat = {'values': data}
    request = (sheet.values().append(spreadsheetId=SPREADSHEET_ID, range="unused_content!A1", 
                                    valueInputOption="USER_ENTERED", body={'values': data}).execute())
    print(data, request)
    return "Successfully Updated Google Sheets", 200

def trash_content():
    '''
    Handles searching through trash and deleting content + logs permanent deletion
    '''    
    #days since deleted (default: 30 days ago)
    deleted_threshold = date_threshold(30)

    trash_dict = {column: [] for column in ['deleted_at','id','user_id', 'content_type']}

    for deleted_look in sdk.search_looks(deleted=True):
        if deleted_look.deleted_at < deleted_threshold:
            trash_dict['deleted_at'].append(deleted_look.deleted_at)
            trash_dict['id'].append(deleted_look.id)
            trash_dict['user_id'].append(deleted_look.user_id)
            trash_dict['content_type'] = 'look'

    for deleted_dash in sdk.search_dashboards(deleted=True):
        if deleted_dash.deleted_at < deleted_threshold:
            trash_dict['deleted_at'].append(deleted_dash.deleted_at)
            trash_dict['id'].append(deleted_dash.id)
            trash_dict['user_id'].append(deleted_dash.user_id)
            trash_dict['content_type'] = 'dashboard'
        else:
            pass

    trash_df = pd.DataFrame(trash_dict).sort_values('deleted_at', ascending = False)
    return trash_df

def main(request):
    '''
    Functions to run script
    - generates table from csv input
    - moves unused content to trash
    - alerts users if their content has been moved to trash
    - logs deletion in GSheets
    '''
    dat_json = request.get_json()
    df = construct_tables(dat_json['message'])
    move_to_trash(df)
    alert_user(df)
    update_gsheet(df)
    return "Successfully removed unused content to trash", 200


# sdk.look("973")

####################Removed Functions####################
# def send_alert(row):
#     '''
#     Helper function to generation of email and alerts content owner of deletion
#     '''    
#     first_name = row['first_name']
#     last_name = row['last_name']
#     content_type = row['content_type']
#     content_id = row['id']
#     content_name = row['content_name']
#     last_accessed_date = row['last_accessed_date']
#     instance_name = 'DemoExpo'
#     emailMsg = ('<h2">Hey {first_name} {last_name}, <br><br>  <b>{content_type}_id {content_id}</b> [{content_name}] was last accessed on <b>{last_accessed_date}</b> and has been moved to trash due to inactivity (not accessed in 90 or more days). If you still need the content, please recover it from the trash and run a query against it. Otherwise, it will be permanetely removed from the instance in 1 week. <br><br> If you have additional questions, feel free to reach out! <br><br> @alick </h2>'
#                 .format(first_name=first_name, last_name=last_name, content_name=content_name, content_type=content_type, content_id=content_id, last_accessed_date=last_accessed_date))
#     mimeMessage = MIMEMultipart()
#     mimeMessage['to'] = row['email']
#     mimeMessage['subject'] = '{instance_name}: {content_type} {content_id} has been moved to trash'.format(instance_name=instance_name, content_type=content_type, content_id=content_id)
#     mimeMessage.attach(MIMEText(emailMsg, 'html'))
#     raw_string = base64.urlsafe_b64encode(mimeMessage.as_bytes()).decode()
#     try:
#         message = service.users().messages().send(userId='me', body={'raw': raw_string}).execute() # pylint: disable=maybe-no-member
#         print(message)
#     except:
#         raise ValueError
        
# def soft_delete_and_alert(df):
#     '''
#     Function to remove content to trash and alert user 
#     '''
#     for row in df.iterrows():
#         content_id = row[1]['id']
#         content_type = row[1]['content_type']
#         if(content_type == 'look'):
#             sdk.update_look(content_id, body=models.WriteLookWithQuery(deleted=True))
#             send_alert(row[1])
#         elif (content_type == "dashboard"):
#             sdk.update_dashboard(str(content_id), body=models.WriteDashboard(deleted=True))
#             send_alert(row[1])
#         else:
#             print("Unexpected error")
#         print('{} {} has successfully been deleted on {}'.format(content_type, content_id, datetime.now()))   

# def test():
#     '''
#     Function to simluate script:
#     - generates a CSV response from Looker
#     - generates unused content table
#     - moves content to trash
#     - alerts user
#     '''
#     ######################CALL FUNCTION######################
#     deletion_ids = ['847', '848']
#     for i in deletion_ids:
#         q = sdk.look(i).query
#         test_input = (models.WriteQuery(model=q.model,
#                                         view=q.view,
#                                         fields=q.fields,
#                                         pivots=[],
#                                         filters=q.filters,
#                                         limit=-1))

#         test_input = sdk.run_inline_query(body=test_input, result_format='csv')
#         print(test_input)
#         main(test_input)