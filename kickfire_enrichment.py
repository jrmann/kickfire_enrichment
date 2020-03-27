import requests
import json
import time
import datetime
import credentials
import pandas as pd
from simple_salesforce import Salesforce
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime
from sqlalchemy.orm import sessionmaker
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

sf = Salesforce(username=credentials.login['username'],password=credentials.login['password'],
                security_token=credentials.login['security_token'])

results = []
main = sf.query_all('''SELECT Id, Name, Email_Domain__c                            
             FROM Account 
             WHERE (Kickfire_Num_Employees__c='' OR Kickfire_Revenue__c='')                                  
             AND Email_Domain__c != ''
             ORDER BY Name ASC''')
results.append(main['records'])

records = [dict(Name=i['Name'], Id=i['Id'], EmailDomain=i['Email_Domain__c'],\
                CreatedDate=datetime.datetime.today().replace(microsecond=0)) for i in results[0]]
df = pd.DataFrame(records)

engine = create_engine('sqlite:///accounts.db', echo=False)
Base = declarative_base()


class Account(Base):
    __tablename__ = 'accounts'

    id = Column(String, primary_key=True)
    name = Column(String)
    email_domain = Column(String)
    created_date = Column(DateTime)
    employees = Column(String)
    revenue = Column(String)
    
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

#Create the initial databse so we don't re-run ~5k accounts through the API
# for item in records:
#     listing = Account(
#     id=item['Id'],
#     name=item['Name'],
#     email_domain=item['EmailDomain'],
#     created_date=item['CreatedDate']
#     employees=employees,
#     revenue=revenue
#     )
#     session.add(listing)
#     session.commit()

def notify_error(error):
    username = credentials.email_login['username']
    password = credentials.email_login['password']
    fromaddr = "pythonworker1870@gmail.com"
    toaddr = "juhno.mann@ekata.com"
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "Kickfire API Failed"
    body = f'The script has failed to run \n\nError: {error}'
    msg.attach(MIMEText(body, 'plain'))
    text = msg.as_string()
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(username, password)
        server.sendmail(fromaddr, toaddr, text)
        server.close()
        print('Failure Email sent!')
    except:
        print('Something went wrong with failure notification')
        
def notify_success(name, id, domain):
    username = credentials.email_login['username']
    password = credentials.email_login['password']
    fromaddr = "pythonworker1870@gmail.com"
    toaddr = "juhno.mann@ekata.com"
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = f'Kickfire API Success - {name}'
    body = f'The Kickfire API has been run for {name} on domain {domain}'
    msg.attach(MIMEText(body, 'plain'))
    text = msg.as_string()
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(username, password)
        server.sendmail(fromaddr, toaddr, text)
        server.close()
        print('Success Email sent!')
    except:
        print('Something went wrong with success notification')

api_key = credentials.kickfire['api_key']
limiter = 0
try:
    for item in records:
        account = session.query(Account).filter_by(id=item['Id']).first()
        if account is None and limiter <= 20:
            domain = item['EmailDomain']
            print(item['Name'])
            kickfire = requests.get(f'https://api.kickfire.com/v3/company:\
            (employees,revenue)?website={domain}&key={api_key}').json()
            if kickfire['status'] == 'success':
                print('Good Domain')
                employees = kickfire['data'][0]['employees']
                revenue = kickfire['data'][0]['revenue']
                sf.Account.update(item['Id'],{'Kickfire_Num_Employees__c':f'{employees}',\
                                              'Kickfire_Revenue__c':f'{revenue}'})
                
                listing = Account(
                id=item['Id'],
                name=item['Name'],
                email_domain=item['EmailDomain'],
                created_date=item['CreatedDate'],
                employees=employees,
                revenue=revenue
                )
                session.add(listing)
                session.commit()
                notify_success(item['Name'], item['Id'], item['EmailDomain'])
                limiter += 1
                time.sleep(1)

            elif kickfire['status'] == 'not found':
                print('Bad Domain')
                listing = Account(
                id=item['Id'],
                name=item['Name'],
                email_domain=item['EmailDomain'],
                created_date=item['CreatedDate']
                )
                session.add(listing)
                session.commit()
                
                limiter += 1
                print(limiter)
                
                continue

            else:
                break

except Exception as e:
    notify_error(e)        