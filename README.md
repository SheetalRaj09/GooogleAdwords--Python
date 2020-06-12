# GooogleAdwords--Python
To retrieve Google Adwords data on a campaign scale by connecting to GA API via Python and storing in a table generated. 

import snowflake.connector


import pandas as pd
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import glob
import re
import datetime
from datetime import datetime
from sqlalchemy import create_engine
from oauth2client.service_account import ServiceAccountCredentials
import sys
sys.path.append('C:/Users/User/Python')
import SF_CRED
user=SF_CRED.USER
pwd=SF_CRED.PASSWORD
db=SF_CRED.DB
schema=SF_CRED.SCHEMA
warehouse=SF_CRED.WH2
role=SF_CRED.ROLE1

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'C:/Users/User/Python/abc.json'
VIEW_ID = '####'
page_size = "200000"
DIMENSIONS = ["ga:date","ga:adwordsCampaignID","ga:campaign"]
METRICS = ["ga:adcost","ga:impressions","ga:adClicks"]

def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
    KEY_FILE_LOCATION, SCOPES)
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    return analytics

def get_report(analytics):
    return analytics.reports().batchGet(
        body={
            'reportRequests': [
            {
                  'viewId': VIEW_ID,
                  'pageSize': page_size,
                  'dateRanges': [{'startDate': '2019-01-01', 'endDate': 'yesterday'}],
                  'metrics': [{'expression':i} for i in METRICS],
                  'dimensions': [{'name':j} for j in DIMENSIONS]                  
            }]
              }).execute()

def convert_to_dataframe(response):
	for report in response.get('reports', []):
		columnHeader = report.get('columnHeader', {})
		dimensionHeaders = columnHeader.get('dimensions', [])
		metricHeaders = [i.get('name',{}) for i in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])]
		finalRows = []
		for row in report.get('data', {}).get('rows', []):
			dimensions = row.get('dimensions', [])
			metrics = row.get('metrics', [])[0].get('values', {})
			rowObject = {}
		
			for header, dimension in zip(dimensionHeaders, dimensions):
				rowObject[header] = dimension
			
			for metricHeader, metric in zip(metricHeaders, metrics):
				rowObject[metricHeader] = metric
			finalRows.append(rowObject)
			rowObject['ga:date'] = datetime.strptime(rowObject['ga:date'], '%Y%m%d').strftime('%Y-%m-%d')
	dataFrameFormat = pd.DataFrame(finalRows)
	return dataFrameFormat

def main():
    analytics = initialize_analyticsreporting()
    response = get_report(analytics)
    df = convert_to_dataframe(response)
    df = df[['ga:date','ga:adwordsCampaignID','ga:campaign','ga:adcost','ga:impressions','ga:adClicks']] 
    df.columns = ['DATE','CAMPAIGN_ID','CAMPAIGN','COST','IMPRESSIONS','CLICKS'] 
    df = df.reset_index(drop = True)
    engine =create_engine("######")
    connection = engine.connect()
    try:
        for k in range(0,len(df),10000):
            temp_df = df[k:k+9999]
            temp_df.to_sql(name='adwords_data',con=connection, if_exists='append', index=False)

		  
            status = 'success'
    except Exception as sqlError:
        status = sqlError
        print(status)
    print(df)    
if __name__ == '__main__':
    main()
