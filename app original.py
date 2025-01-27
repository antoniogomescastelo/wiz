import re
import sys
import csv
import time
import json
import codecs
import logging
import requests
import glob
import os
import shutil

import pandas as pd

from datetime import datetime

from contextlib import closing

import streamlit as st

import altair as alt

from requests.auth import HTTPBasicAuth

from services import ImportService

from harvester import HarvesterService

import threading

from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx

from snowflake.snowpark import Session

#from streamlit_lottie import st_lottie

# import http.client


#graphql conf
MAX_RETRIES_FOR_QUERY = 5
MAX_RETRIES_FOR_DOWNLOAD_REPORT = 5
RETRY_TIME_FOR_QUERY = 2
RETRY_TIME_FOR_DOWNLOAD_REPORT = 60
CHECK_INTERVAL_FOR_DOWNLOAD_REPORT = 20


#authentication     
AUTH0_URLS = ['https://auth.wiz.io/oauth/token', 'https://auth0.gov.wiz.io/oauth/token']
COGNITO_URLS = ['https://auth.app.wiz.io/oauth/token', 'https://auth.gov.wiz.io/oauth/token']


#get resources query
GET_RESOURCES_QUERY = (
    """
      query CloudResourceSearch(
          $filterBy: CloudResourceFilters
          $first: Int
          $after: String
        ) {
          cloudResources(
            filterBy: $filterBy
            first: $first
            after: $after
          ) {
            nodes {
              ...CloudResourceFragment
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
        fragment CloudResourceFragment on CloudResource {
          id
          name
          type
          subscriptionId
          subscriptionExternalId
          graphEntity{
            id
            providerUniqueId
            name
            type
            projects {
              id
            }
            properties
            firstSeen
            lastSeen
          }
        }
    """
)


#get report query
GET_REPORT_QUERY = (
    """
    query ReportsTable($filterBy: ReportFilters, $first: Int, $after: String) {
      reports(first: $first, after: $after, filterBy: $filterBy) {
        nodes {
          id
          name
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """
)


#create report mutation
CREATE_REPORT_MUTATION = (
    """
    mutation CreateReport($input: CreateReportInput!) {
      createReport(input: $input) {
        report {
          id
        }
      }
    }
    """
)


#rerun report mutation
RERUN_REPORT_MUTATION = (
    """
    mutation RerunReport($reportId: ID!) {
        rerunReport(input: { id: $reportId }) {
            report {
                id
            }
        }
    }
    """
)


#report download query
DOWNLOAD_REPORT_QUERY = (
    """
    query ReportDownloadUrl($reportId: ID!) {
        report(id: $reportId) {
            lastRun {
                url
                status
            }
        }
    }
    """
)

# change
def x(l, k, v):
    l[k] = v


#set logging
def set_logging():
    logging.getLogger().setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stderr)

    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

    handler.setFormatter(formatter)

    logging.getLogger().handlers = [handler]


#get config
def get_config():
    logging.getLogger().debug("get config")

    with open('config.json', "r") as f:
        config = json.load(f)

    return config


#get auth params
def generate_authentication_params(config):
    if config['wizio_token_url'] in AUTH0_URLS:
        return {
            'grant_type': 'client_credentials',
            'audience': 'beyond-api',
            'client_id': config['wizio_client_id'],
            'client_secret': config['wizio_client_secret']
        }
    
    elif config['wizio_token_url'] in COGNITO_URLS:
        return {
            'grant_type': 'client_credentials',
            'audience': 'wiz-api',
            'client_id': config['wizio_client_id'],
            'client_secret': config['wizio_client_secret']
        }
    
    else:
        raise Exception('Error: wrong token url')


#get token
def get_token(config):
    response = requests.post(
        config['wizio_token_url'],
        headers = {'Content-Type': 'application/x-www-form-urlencoded'},
        data = generate_authentication_params(config)
    )

    if response.status_code != requests.codes.ok:
        raise Exception(f'Error: {response.text}') 

    if not response.json().get('access_token'):
        raise Exception(f'Error: {response.json().get("message")}')

    config['wizio_token'] = response.json().get('access_token')

    return config


#send request
def send_request(config, query, variables):    
    if config['wizio_token']:
        return requests.post(
            config['wizio_api_endpoint_url'],
            headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + config['wizio_token']},
            json = {'query': query, 'variables': variables}
        )
    
    raise Exception('Error: wizio_token not found')


#query
def query(config, query, variables):
    retries = 0

    response = send_request(config, query, variables)
    
    if response.status_code == requests.codes.unauthorized or response.status_code == requests.codes.forbidden:
        raise Exception(f'Error: {response.text}') 
    
    elif response.status_code == requests.codes.not_found:
        raise Exception(f'Error: {response.text}') 
    
    while response.status_code != requests.codes.ok:
        if retries >= MAX_RETRIES_FOR_QUERY:
            raise Exception(f'Error: {response.text}') 
        
        time.sleep(RETRY_TIME_FOR_QUERY)

        response = send_request(config, query, variables)

        retries += 1
    
    if not response.json().get('data'):
        raise Exception(f'Error: {response.json().get("errors")}')
   
    logging.getLogger().debug(f"Info: {response.json().get('data')}")
    
    return response.json().get('data')


#get resources
def get_resources(config, project_id):
    logging.getLogger().debug("get resources")

    variables = {
        "first": 500,
        "filterBy": {
            "projectId": [
                project_id
            ],
        }
    }
    
    nodes = []

    while True:
        response = query(config, GET_RESOURCES_QUERY, variables)

        try:
            nodes = nodes + response['cloudResources']['nodes']

            variables['after'] = response['cloudResources']['pageInfo']['endCursor']

            if response['cloudResources']['pageInfo']['hasNextPage'] == False:
                break

        except Exception as error:
            break
            
    return nodes


#create report
def create_report(config, project_id, report_prefix, report_type):
    variables = {
        "input": {
            "name": re.sub(' |\.|:|-','', f'{report_prefix}_{datetime.now()}'),
            "type": report_type,
            "projectId": project_id
        }
    }
    
    response = query(config, CREATE_REPORT_MUTATION, variables)
    
    report_id = response['createReport']['report']['id']

    return report_id


#rerun report
def rerun_report(config, report_id):
    variables = {
        'reportId': report_id
    }

    response = query(config, RERUN_REPORT_MUTATION, variables)

    report_id = response['rerunReport']['report']['id']

    return report_id


#get report url and status
def get_report_url_and_status(config, report_id):
    num_of_retries = 0

    while num_of_retries < MAX_RETRIES_FOR_DOWNLOAD_REPORT:
        time.sleep(CHECK_INTERVAL_FOR_DOWNLOAD_REPORT)

        response = query(config, DOWNLOAD_REPORT_QUERY, {'reportId': report_id})

        status = response['report']['lastRun']['status']

        if status == 'COMPLETED':
            return response['report']['lastRun']['url']
        
        elif status == 'FAILED' or status == 'EXPIRED':
            rerun_report(report_id)

            time.sleep(RETRY_TIME_FOR_DOWNLOAD_REPORT)

            num_of_retries += 1

    raise Exception('Error: get report fail')


#get report content
def get_report_content(download_url):
    report_data = []

    with closing(requests.get(download_url, stream=True)) as r:
        reader = csv.reader(codecs.iterdecode(r.iter_lines(), 'utf-8'))

        for row in reader:     
            report_data.append(row)
    
    if report_data:
        logging.debug(f'Info: {report_data}')

        return report_data
    
    else:
        raise Exception('Error: download failed')


#get report content to dataframe
def get_report_content_to_dataframe(download_url):
    return pd.read_csv(download_url)    


#get report
def get_report(config, project_id):    
    logging.getLogger().debug("get report")
    
    report_data = {}
    
    report_type= "DATA_SCAN"

    report_id = create_report(config, project_id, report_type, report_type)

    report_url = get_report_url_and_status(config, report_id)
    
    #report_data[report_type] = get_report_content(report_url)
    report_data[report_type] = get_report_content_to_dataframe(report_url)

    return report_data


#get external id
def get_external_id(x):
    try:
        return x['properties']['externalId']
    
    except Exception as error:
        try:
            return x['externalId']
        
        except Exception as error:
            return None


#get number of findings
def get_number_of_findings(x):
    try:
        return len(json.loads(x))
    
    except Exception as error:
        return None
    

#get data findings
@st.cache_resource
def get_data_findings(config):
    logging.getLogger().debug("get data findings")

    config= get_token(config)

    session = st.connection("snowflake").session()
    #session = Session.builder.config("connection_name", "wizio").create()

    # resources_df = pd.DataFrame(get_resources(config, config['wizio_project_id']) )

    # session.write_pandas(resources_df, "RESOURCES", auto_create_table=True, overwrite=True)

    # resources_df = session.table("RESOURCES").to_pandas()

    # resources_df['externalId'] = resources_df['graphEntity'].apply(get_external_id)

    # session.write_pandas(resources_df, "RESOURCES_READY", auto_create_table=True, overwrite=True)

    # resources_ready_df = session.table("RESOURCES_READY").to_pandas()

    # reports = get_report(config, config['wizio_project_id']) 

    # data_scan_df = reports['DATA_SCAN']

    # session.write_pandas(data_scan_df, "DATA_SCAN", auto_create_table=True, overwrite=True)

    # data_scan_df = session.table("DATA_SCAN").to_pandas()

    # data_scan_resources_df = data_scan_df.set_index('Resource External ID').join(resources_df.set_index('externalId'))

    # data_scan_resources_df.reset_index(inplace=True)

    # session.write_pandas(data_scan_resources_df, "DATA_SCAN_RESOURCES", auto_create_table=True, overwrite=True)

    # data_scan_resources_df = session.table("DATA_SCAN_RESOURCES").to_pandas()

    # data_scan_resources_ready_df = pd.json_normalize(data_scan_resources_df['graphEntity'])

    # data_scan_resources_ready_df[['Finding ID', 'Category', 'Classifier', 'Unique Matches', 'Total Matches', 'Severity', 'Finding Examples']] = data_scan_resources_df[['ID', 'Category', 'Classifier', 'Unique Matches', 'Total Matches', 'Severity', 'Finding Examples']]

    # data_scan_resources_ready_df = data_scan_resources_ready_df.rename(lambda x: x.replace('properties.','_'), axis='columns')

    # data_scan_resources_ready_df['Examples Count'] = data_scan_resources_ready_df['Finding Examples'].apply(get_number_of_findings)

    # data_scan_resources_ready_df['_creationYYMM']=data_scan_resources_ready_df['_creationDate'].str[0:7]

    # data_scan_resources_ready_df.to_csv('datascanresourcesready.csv', index=False)

    # data_scan_resources_ready_df = pd.read_csv('datascanresourcesready.csv')  

    # session.write_pandas(data_scan_resources_ready_df, "DATA_SCAN_RESOURCES_READY", auto_create_table=True, overwrite=True)

    data_scan_resources_ready_df = session.table("DATA_SCAN_RESOURCES_READY").to_pandas()

    # data_scan_resources_exploded_df = data_scan_resources_ready_df.query("type in ('BUCKET', 'DATABASE', 'DB_SERVER')")

    # data_scan_resources_exploded_df['Finding Examples'] = data_scan_resources_exploded_df['Finding Examples'].apply(lambda x: eval(x) if x is not None else None)

    # columns=['id', 'name', 'type', '_cloudPlatform', '_subscriptionExternalId', '_region', '_creationDate', '_externalId', 'Finding ID', 'Category', 'Classifier', 'Finding Examples']

    # data_scan_resources_exploded_df = data_scan_resources_exploded_df[columns].explode('Finding Examples', ignore_index=True)

    # exploded_df = pd.json_normalize(data_scan_resources_exploded_df['Finding Examples'])

    # columns=['id', 'name', 'type', '_cloudPlatform', '_subscriptionExternalId', '_region', '_creationDate', '_externalId', 'Finding ID', 'Category', 'Classifier']
     
    # data_scan_resources_exploded_df = pd.concat([data_scan_resources_exploded_df[columns], exploded_df[['key', 'path']]], axis=1)

    # session.write_pandas(data_scan_resources_exploded_df, "DATA_SCAN_RESOURCES_EXPLODED", auto_create_table=True, overwrite=True)    

    data_scan_resources_exploded_df = session.table("DATA_SCAN_RESOURCES_EXPLODED").to_pandas()

    return data_scan_resources_ready_df, data_scan_resources_exploded_df


#show dashboard
def show_dashboard(config):
    logging.getLogger().debug("show dashboard")

    try:
        style = """
            <style>
                .stVegaLiteChart {
                    background-color: #EEEEEE;
                }
                .stMarkdown {
                    text-align: justify;
                }
            </style>
        """

        data_scan_resources_ready_df, data_scan_resources_exploded_df = get_data_findings(config)

        resources_per_cloud_platform = data_scan_resources_ready_df[['_cloudPlatform','id']].drop_duplicates().groupby(by=['_cloudPlatform']).count().reset_index().rename(columns={"id": "count"})

        resources_per_environment = data_scan_resources_ready_df[['__environments','id']].drop_duplicates().groupby(by=['__environments']).count().reset_index().rename(columns={"id": "count"})

        resources_per_status = data_scan_resources_ready_df[['_status','id']].drop_duplicates().groupby(by=['_status']).count().reset_index().rename(columns={"id": "count"})

        resources_per_region = data_scan_resources_ready_df[['_region','id']].drop_duplicates().groupby(by=['_region']).count().reset_index().rename(columns={"id": "count"})

        resources_per_type = data_scan_resources_ready_df[['type','id']].drop_duplicates().groupby(by=['type']).count().reset_index().rename(columns={"id": "count"})

        resources_per_creation_date = data_scan_resources_ready_df[['_creationYYMM','id']].drop_duplicates().groupby(by=['_creationYYMM']).count().reset_index().rename(columns={"id": "count"})

        resources_per_category = data_scan_resources_ready_df[['Category','id']].drop_duplicates().groupby(by=['Category']).count().reset_index().rename(columns={"id": "count"})

        resources_per_severity = data_scan_resources_ready_df[['Severity','id']].drop_duplicates().groupby(by=['Severity']).count().reset_index().rename(columns={"id": "count"})

        findings_per_region = data_scan_resources_ready_df[['_region','Finding ID']].groupby(by=['_region']).count().reset_index().rename(columns={"Finding ID": "count"})

        findings_per_type = data_scan_resources_ready_df[['type','Finding ID']].groupby(by=['type']).count().reset_index().rename(columns={"Finding ID": "count"})

        findings_per_classifier = data_scan_resources_ready_df[['Classifier','Finding ID']].groupby(by=['Classifier']).count().reset_index().rename(columns={"Finding ID": "count"})

        findings_per_type_and_severity = data_scan_resources_ready_df[['type', 'Severity', 'Finding ID']].groupby(by=['type','Severity']).count().reset_index().rename(columns={"Finding ID": "count"})

        findings_per_type_and_classifier = data_scan_resources_ready_df[['type', 'Classifier', 'Finding ID']].groupby(by=['type','Classifier']).count().reset_index().rename(columns={"Finding ID": "count"})

        total_matches_per_region = data_scan_resources_ready_df[['_region','Total Matches']].groupby(by=['_region']).sum().reset_index().rename(columns={"Total Matches": "count"})

        total_matches_per_type = data_scan_resources_ready_df[['type','Total Matches']].groupby(by=['type']).sum().reset_index().rename(columns={"Total Matches": "count"})

        total_matches_per_classifier = data_scan_resources_ready_df[['Classifier','Total Matches']].groupby(by=['Classifier']).sum().reset_index().rename(columns={"Total Matches": "count"})

        total_matches_per_type_and_severity = data_scan_resources_ready_df[['type', 'Severity', 'Total Matches']].groupby(by=['type','Severity']).sum().reset_index().rename(columns={"Total Matches": "count"})

        total_matches_per_type_and_classifier = data_scan_resources_ready_df[['type', 'Classifier', 'Total Matches']].groupby(by=['type','Classifier']).sum().reset_index().rename(columns={"Total Matches": "count"})

        st.write(time.strftime("%Y-%m-%d %H:%M:%S")) 

        st.markdown(style, unsafe_allow_html=True)

        st.subheader("General Dashboard")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("AWS", resources_per_cloud_platform.iloc[0]['count'], delta=str(resources_per_creation_date.iloc[-1]['count']), delta_color="normal", help=None, label_visibility="visible", border=False)

        with col2:
            st.metric("Production", resources_per_environment.iloc[0]['count'], delta=None, delta_color="normal", help=None, label_visibility="visible", border=False)

        with col3:
            st.metric("Active", resources_per_status.iloc[0]['count'], delta=None, delta_color="normal", help=None, label_visibility="visible", border=False)

        with col4:
            st.metric("Inactive", resources_per_status.iloc[1]['count'], delta=None, delta_color="normal", help=None, label_visibility="visible", border=False)

        st.write("#")

        #group 1
        st.subheader("Resources Summary")

        #group 1.1
        c = (alt.Chart(resources_per_creation_date)
              .encode(alt.X('_creationYYMM:O', axis=alt.Axis(labels=True, labelAngle=0)).timeUnit("yearmonth").title('Resource creation'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="lightgreyteal", reverse=False), alt.Text('count'), tooltip=["_creationYYMM:T", "count"])
              .properties(title='Number of resources per date')
         )

        st.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True)

        st.write("#")

        #group 1.2
        col1, col2, col3 = st.columns([1,1,1])
        
        with col1:
            st.markdown(
            """
            ## Calling us-east-1

            The analysis provides a breakdown of the resources identified across different regions and their types. As illustrated in the graphs on the right, more than 70% of the resources with data findings are located in the us-east-1 region, nearly 50% are categorized as buckets, while 15% are classified as databases.
            """
            )

        with col2:
            c = (
                alt.Chart(resources_per_region)
                .encode(alt.X('_region', axis=alt.Axis(labels=True, labelAngle=0)).title('Resource region'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="lightgreyteal", reverse=False), alt.Text('count'), tooltip=["_region", "count"])
                .properties(title='Number of resources per region')
            )

            st.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True)

        with col3:
            c = (alt.Chart(resources_per_type)
                .encode(alt.X('type', axis=alt.Axis(labels=True, labelAngle=0)).title('Resource type'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="lightgreyteal", reverse=False), alt.Text('count'), tooltip=["type", "count"])
                .properties(title='Number of resources per type')
            )

            st.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True)

        st.write("#")

        #group 1.3
        col1, col2, col3 = st.columns([1,1,1])

        with col1:
            c = (alt.Chart(resources_per_severity)
                .encode(alt.X('Severity', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding severity'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="lightgreyteal", reverse=False), alt.Text('count'), tooltip=["Severity", "count"])
                .properties(title='Number of resources per severity')
            )
                    
            st.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True) 

        with col2:
            c = (alt.Chart(resources_per_category)
                .encode(alt.X('Category', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding classifier'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="lightgreyteal", reverse=False), alt.Text('count'), tooltip=["Category", "count"])
                .properties(title='Number of resources per classifier')
            )
                    
            st.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True) 

        with col3:
            st.markdown(
            """
            ## Should we worry about it

            The analysis offers a comprehensive overview of the identified resources, highlighting their severity and classifications. As shown in the graphs on the left, 44 out of the 92 (48%) resources exhibit significant findings, categorized as high and critical data with Personally Identifiable, Financial, and Digital Identity information being in the top 5 categories.
            """
            )

        st.write("#")

        #group 2
        st.subheader("Data Findinds Summary")

        #group 2.1
        col1, col2, col3 = st.columns([1,1,1])

        with col1:
            st.markdown(
            """
            ## Houston, we have a problem

            The analysis offers a detailed overview of the unique findings discovered across various regions and their classifications. As demonstrated in the graphs on the right, nearly 75% of the resources containing data findings are situated in the us-east-1 region. Additionally, over 70% of these resources are categorized as buckets, while around 20% are identified as databases. This reinforces our earlier observations that buckets and databases are the most critical components.
            """            
            )

        with col2:
            c = (alt.Chart(findings_per_region)
                .encode(alt.X('_region', axis=alt.Axis(labels=True, labelAngle=0)).title('Resource region'), alt.Y('count', axis=alt.Axis(labels=False)).title(''), alt.Color('count', legend=None).scale(scheme="lightorange", reverse=False), alt.Text('count'), tooltip=["_region", "count"])
                .properties(title='Number of findings per region')
            )
            
            st.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True)

        with col3:
            c = (alt.Chart(findings_per_type)
                .encode(alt.X('type', axis=alt.Axis(labels=True, labelAngle=0)).title('Resource type'), alt.Y('count', axis=alt.Axis(labels=False)).title(''), alt.Color('count', legend=None).scale(scheme="lightorange", reverse=False), alt.Text('count'), tooltip=["type", "count"])
                .properties(title='Number of findings per type')
            )
            
            st.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True)

        st.write("#")

        #group 2.2
        col1, col2 = st.columns([2,1])

        with col1:
            c = (alt.Chart(findings_per_classifier)
                .encode(alt.X('Classifier', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding classifier'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="lightorange", reverse=False), alt.Text('count'), tooltip=["Classifier", "count"])
                .properties(title='Number of findings per classifier')
            )
                    
            st.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True) 

        with col2:
            st.markdown(
            """
            ## Who you gonna call, today 

            The analysis provides a thorough overview of the identified resources and their classifications. The graph on the left illustrates that key data points, including names, emails, phone numbers, addresses, gender, and transaction details, are prominently featured.
            """
            )

        st.write("#")

        #group 2.3
        col1, col2 = st.columns([1,2])
 
        with col1:
            st.markdown(
            """
            ## The most bang for the buck
            """            
            )

        with col2:
            c = (alt.Chart(findings_per_type_and_classifier)
                .encode(alt.X('Classifier', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding classifier'), alt.Y('type', axis=alt.Axis(labels=False, labelAngle=0)).title('Resource type'), alt.Color('count', legend=None).scale(scheme="orangered", reverse=False), alt.Text('count'), tooltip=["Classifier","type","count"])
                .properties(title='Number of findings per resource type and classifier')
            )

            st.altair_chart(c.mark_rect(), use_container_width=True) 

        #group 2.4
        col1, col2 = st.columns([1,2])

        with col1:
            c = (alt.Chart(findings_per_type_and_severity)
                .encode(alt.X('Severity', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding severity'), alt.Y('type', axis=alt.Axis(labels=False, labelAngle=0)).title('Resource type'), alt.Color('count', legend=None).scale(scheme="orangered", reverse=False), alt.Text('count'), tooltip=["Severity","type","count"])
                .properties(title='Number of findings per resource type and severity')
            )

            col1.altair_chart((c.mark_rect() + c.mark_text(baseline="middle", fontWeight="bold").encode(color=alt.value("white"))), use_container_width=True) 

        with col2:
            st.markdown(
            """
            When spending time or money, it is essential to insist on getting the most bang for the buck.
            """
            )

        st.write("#")

        #group 3
        st.subheader("Total Matches Summary")

        #group 3.1
        col1, col2, col3 = st.columns([1,1,1])

        with col1:
            st.markdown(
            """
            ## In all its magnitude

            The analysis provides a comprehensive overview of the total matches identified across different regions and their classifications. As illustrated in the graphs on the right, more than 75% of the resources containing data findings are located in the us-east-1 region. Furthermore, nearly 97% of these resources are classified as buckets, while merely 2% are recognized as databases. If you're looking to begin your work, start with your buckets..
            """            
            )

        with col2:
            c = (alt.Chart(total_matches_per_region)
                .encode(alt.X('_region', axis=alt.Axis(labels=True, labelAngle=0)).title('Resource region'), alt.Y('count', axis=alt.Axis(labels=False)).title(''), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["_region", "count"])
                .properties(title='Number of total matches per region')
            )
            
            st.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True)

        with col3:
            c = (alt.Chart(total_matches_per_type)
                .encode(alt.X('type', axis=alt.Axis(labels=True, labelAngle=0)).title('Resource type'), alt.Y('count', axis=alt.Axis(labels=False)).title(''), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["type", "count"])
                .properties(title='Number of total matches per type')
            )
            
            st.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True)

        st.write("#")

        #group 3.2
        col1, col2 = st.columns([2,1])

        with col1:
            c = (alt.Chart(total_matches_per_classifier)
                .encode(alt.X('Classifier', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding classifier'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["Classifier", "count"])
                .properties(title='Number of total matches per classifier')
            )
                    
            st.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True) 

        with col2:
            st.markdown(
            """
            ## Kill 'Em All
            .. and get rid of mushrooms in your yard. 
            """
            )

        st.write("#")

        #group 3.3
        col1, col2 = st.columns([1,2])
 
        with col1:
            st.markdown(
            """
            ## Allow me
            """
            )

        with col2:
            c = (alt.Chart(total_matches_per_type_and_classifier)
                .encode(alt.X('Classifier', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding classifier'), alt.Y('type', axis=alt.Axis(labels=False, labelAngle=0)).title('Resource type'), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["Classifier","type","count"])
                .properties(title='Number of total matches per resource type and classifier')
            )

            st.altair_chart(c.mark_rect(), use_container_width=True) 

        #group 3.4
        col1, col2 = st.columns([1,2])

        with col1:
            st.write("")

            c = (alt.Chart(total_matches_per_type_and_severity)
               .encode(alt.X('Severity', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding severity'), alt.Y('type', axis=alt.Axis(labels=False, labelAngle=0)).title('Resource type'), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["Severity","type","count"])
               .properties(title='Number of total matches per resource type and severity')
            )

            st.altair_chart((c.mark_rect() + c.mark_text(baseline="middle", fontWeight="bold").encode(color=alt.value("white"))), use_container_width=True) 

        with col2:
            st.markdown(
            """
            Prioritize addressing the critical findings first, followed by the high findings. 
            
            Go ahead, select the community where you want to register your storage on and push the Start button below.
            """            
            )

        st.write("#")

        #group 5
        st.markdown(
            """
            ###### Playground
            """
        )
        
        columns=['name', 'type', 'Classifier','Unique Matches','Total Matches','Severity']
        
        st.dataframe(data_scan_resources_ready_df[columns].pivot_table(values=["Unique Matches","Total Matches"], index=["name","type","Severity"], columns="Classifier", aggfunc="sum"))

        st.write("")

        #group 6
        with st.expander("Finding Examples"):
            st.dataframe(data_scan_resources_exploded_df,hide_index=True,column_config={"id":"Resource Id","name":"Resource Name","type":"Resource Type","_subscriptionExternalId":"Resource Account","Category": "Finding Category","Classifier": "Finding Classifier","key": "Key","path":"Path"})
            
        st.write("#")


        #do all findings
        do_all_findings(config, data_scan_resources_ready_df, data_scan_resources_exploded_df)

        st.markdown("[Results](https://print.collibra.com/profile/9693d5ce-9fb4-4e97-b46e-7218526eda14/activities)")
        
        st.stop()


    except Exception as error:
        raise Exception('Error: %s', error)


#get collibra
def get_collibra(config):
    logging.getLogger().debug("get collibra")

    collibra = {}

    collibra["host"] = f"https://{config['collibra_host']}"

    collibra["username"] = config['collibra_username']

    collibra["password"] = config['collibra_password']

    collibra["endpoint"] = f"{collibra['host']}{config['collibra_api_endpoint']}"

    collibra["session"] = requests.Session()

    collibra.get("session").auth = HTTPBasicAuth(collibra.get("username"), collibra.get("password"))

    return collibra


#show dialog
@st.dialog("Choose")
def show_dialog(communities):
    resources_community = st.selectbox(
        label="Select the community where you want to find your resources on ",
        options=sorted([f"{k}" for k, v in communities.items()]),
        index=None
    )

    if not resources_community:
        st.warning("Please specify.") & st.stop()

    do_finding_examples = st.checkbox("Check to register your finding examples")

    if st.button("Submit"):
        st.session_state.submitted = True
        st.session_state.resources_community = resources_community
        st.session_state.do_finding_examples = do_finding_examples

        st.rerun()


#show progress
def show_progress(runId):
    progress = 0

    st.caption('Updated')

    bar = st.progress(progress)
    
    while progress < 100:
        files = list(filter(lambda x: 'beam' not in x.lower(), glob.glob(f'./runs/{runId}.json.step.*')))
        
        progress = len(files)*7 +1

        bar.progress(progress, text=f'{len(files)} of 15')

        time.sleep(1)

    bar.progress(100)

    time.sleep(1)


def do_finding(importService, config, entries, x):
    logging.getLogger().debug("do finding")

    # data category
    if x['Category'] not in entries[0]:
        entries[0][x['Category']] = {
            "entry": importService.get_asset("Privacy and Risk community", "Data categories", "Data Category", x['Category'], x['Category'])
        }

    # data concept
    if x['Classifier'] not in entries[1]:
        entries[1][x['Classifier']] = {
            "entry": importService.get_asset("Data Architects community", "Business Data Models", "Data Concept", x['Classifier'], x['Classifier']),
            "relations": [],
            "attributes": []
        }

    if x['Category'] not in entries[1][x['Classifier']]['relations']:
        entries[1][x['Classifier']]['relations'].append(x['Category'])
        importService.add_relations(entries[1][x['Classifier']]['entry'], "c0e00000-0000-0000-0000-000000007316", "SOURCE", "Data categories", "Privacy and Risk community", x['Category'])

    if x['Severity'] not in entries[1][x['Classifier']]['attributes']:
        entries[1][x['Classifier']]['attributes'].append(x['Severity'])
        importService.add_attributes(entries[1][x['Classifier']]['entry'], 'Severity', x['Severity'], 'string')

    # domain
    if x['_subscriptionExternalId'] not in entries[2]:
        entries[2][x['_subscriptionExternalId']] = {
            "entry": importService.get_domain(config['community_to_query'], "Technology Asset Domain", x['_subscriptionExternalId']),
        }

    # system
    if x['_subscriptionExternalId'] not in entries[3]:
        entries[3][x['_subscriptionExternalId']] = {
            "entry": importService.get_asset(config['community_to_query'], x['_subscriptionExternalId'], "System", x['_subscriptionExternalId'], x['_subscriptionExternalId']),
            "attributes": []

        }

    if x['_cloudPlatform'] not in entries[3][x['_subscriptionExternalId']]['attributes']:
        entries[3][x['_subscriptionExternalId']]['attributes'].append(x['_cloudPlatform'])
        importService.add_attributes(entries[3][x['_subscriptionExternalId']]['entry'], 'Platform', x['_cloudPlatform'], 'string')

    if x['_subscriptionExternalId'] not in entries[3][x['_subscriptionExternalId']]['attributes']:
        entries[3][x['_subscriptionExternalId']]['attributes'].append(x['_subscriptionExternalId'])
        importService.add_attributes(entries[3][x['_subscriptionExternalId']]['entry'], 'Account Name', x['_subscriptionExternalId'], 'string')


    # if buckets
    if x['type'] == 'BUCKET':        
        # file system
        if x['_externalId'] not in entries[4]:
            entries[4][x['_externalId']] = {
                "entry": importService.get_asset(config['community_to_query'], x['_subscriptionExternalId'], "S3 File System", x['_externalId'], x['_externalId']),
                "relations": [],
                "attributes": []
            }

        if x['_subscriptionExternalId'] not in entries[4][x['_externalId']]['relations']:
            entries[4][x['_externalId']]['relations'].append(x['_subscriptionExternalId'])
            importService.add_relations(entries[4][x['_externalId']]['entry'], "00000000-0000-0000-0000-000000007054", "SOURCE", x['_subscriptionExternalId'], config['community_to_query'], x['_subscriptionExternalId'])

        if x['_region'] not in entries[4][x['_externalId']]['attributes']:
            entries[4][x['_externalId']]['attributes'].append(x['_region'])
            importService.add_attributes(entries[4][x['_externalId']]['entry'], 'Region', x['_region'], 'string')

        if x['_creationDate'] not in entries[4][x['_externalId']]['attributes']:
            entries[4][x['_externalId']]['attributes'].append(x['_creationDate'])
            importService.add_attributes(entries[4][x['_externalId']]['entry'], 'Created At', x['_creationDate'], 'string')

        # storage container
        if x['_externalId'] not in entries[5]:
            entries[5][x['_externalId']] = {
                "entry": importService.get_asset(config['community_to_query'], x['_subscriptionExternalId'], "S3 Bucket", f"s3://{x['_externalId']}", f"s3://{x['_externalId']}"),
                "relations": [],
                "attributes": []
            }

        if x['_externalId'] not in entries[5][x['_externalId']]['relations']:
            entries[5][x['_externalId']]['relations'].append(x['_externalId'])
            importService.add_relations(entries[5][x['_externalId']]['entry'], "00000000-0000-0000-0001-002600000000", "SOURCE", x['_subscriptionExternalId'], config['community_to_query'], x['_externalId'])

        if x['Category'] not in entries[5][x['_externalId']]['relations']:
            entries[5][x['_externalId']]['relations'].append(x['Category'])
            importService.add_relations(entries[5][x['_externalId']]['entry'], "01930192-86fb-77b0-8baf-30a80dccb864", "TARGET", "Data categories", "Privacy and Risk community", x['Category'])

        if x['Classifier'] not in entries[5][x['_externalId']]['relations']:
            entries[5][x['_externalId']]['relations'].append(x['Classifier'])
            importService.add_relations(entries[5][x['_externalId']]['entry'], "01930192-f332-70fc-8572-9f7283c4cfd4", "TARGET",  "Business Data Models", "Data Architects community", x['Classifier'])

        if x['_region'] not in entries[5][x['_externalId']]['attributes']:
            entries[5][x['_externalId']]['attributes'].append(x['_region'])
            importService.add_attributes(entries[5][x['_externalId']]['entry'], 'Region', x['_region'], 'string')

        if x['_creationDate'] not in entries[5][x['_externalId']]['attributes']:
            entries[5][x['_externalId']]['attributes'].append(x['_creationDate'])
            importService.add_attributes(entries[5][x['_externalId']]['entry'], 'Created At', x['_creationDate'], 'string')

        # directory
        if x['_externalId'] not in entries[6]:
            entries[6][x['_externalId']] = {
                "entry": importService.get_asset(config['community_to_query'], x['_subscriptionExternalId'], "Directory", f"s3://{x['_externalId']}/", "/"),
                "relations": [],
                "attributes": []
            }

        if x['_externalId'] not in entries[6][x['_externalId']]['relations']:
            entries[6][x['_externalId']]['relations'].append(x['_externalId'])
            importService.add_relations(entries[6][x['_externalId']]['entry'], "00000000-0000-0000-0001-002600000001", "SOURCE", x['_subscriptionExternalId'], config['community_to_query'], f"s3://{x['_externalId']}")

        if x['Category'] not in entries[6][x['_externalId']]['relations']:
            entries[6][x['_externalId']]['relations'].append(x['Category'])
            importService.add_relations(entries[6][x['_externalId']]['entry'], "01930192-86fb-77b0-8baf-30a80dccb864", "TARGET", "Data categories", "Privacy and Risk community", x['Category'])

        if x['Classifier'] not in entries[6][x['_externalId']]['relations']:
            entries[6][x['_externalId']]['relations'].append(x['Classifier'])
            importService.add_relations(entries[6][x['_externalId']]['entry'], "01930192-f332-70fc-8572-9f7283c4cfd4", "TARGET",  "Business Data Models", "Data Architects community", x['Classifier'])

        if x['_region'] not in entries[6][x['_externalId']]['attributes']:
            entries[6][x['_externalId']]['attributes'].append(x['_region'])
            importService.add_attributes(entries[6][x['_externalId']]['entry'], 'Region', x['_region'], 'string')

        if x['_creationDate'] not in entries[6][x['_externalId']]['attributes']:
            entries[6][x['_externalId']]['attributes'].append(x['_creationDate'])
            importService.add_attributes(entries[6][x['_externalId']]['entry'], 'Created At', x['_creationDate'], 'string')

        # measure
        entries[8][f"{x['_externalId']}:{x['Classifier']}:Unique Matches"] = {
            "entry": importService.get_asset("Governance council", "New Data Findings Metrics", "Measure", f"{x['_externalId']}:{x['Classifier']}:Unique Matches", f"{x['Classifier']} Unique Matches")
        }

        importService.add_attributes(entries[8][f"{x['_externalId']}:{x['Classifier']}:Unique Matches"]['entry'], 'Count', x['Unique Matches'], 'string')

        importService.add_relations(entries[8][f"{x['_externalId']}:{x['Classifier']}:Unique Matches"]['entry'], "01930b23-1a84-7d44-b817-275206442bf6", "TARGET",  "Business Data Models", "Data Architects community",  x['Classifier'])
        
        importService.add_relations(entries[8][f"{x['_externalId']}:{x['Classifier']}:Unique Matches"]['entry'], "01930b24-2617-722b-9502-8c30d4b3818c", "SOURCE",  x['_subscriptionExternalId'], config['community_to_query'], f"s3://{x['_externalId']}/")

        entries[8][f"{x['_externalId']}:{x['Classifier']}:Total Matches"] = {
            "entry": importService.get_asset("Governance council", "New Data Findings Metrics", "Measure", f"{x['_externalId']}:{x['Classifier']}:Total Matches", f"{x['Classifier']} Total Matches")
        }

        importService.add_attributes(entries[8][f"{x['_externalId']}:{x['Classifier']}:Total Matches"]['entry'], 'Count', x['Total Matches'], 'string')

        importService.add_relations(entries[8][f"{x['_externalId']}:{x['Classifier']}:Total Matches"]['entry'], "01930b23-1a84-7d44-b817-275206442bf6", "TARGET",  "Business Data Models", "Data Architects community",  x['Classifier'])
        
        importService.add_relations(entries[8][f"{x['_externalId']}:{x['Classifier']}:Total Matches"]['entry'], "01930b24-2617-722b-9502-8c30d4b3818c", "SOURCE",  x['_subscriptionExternalId'], config['community_to_query'], f"s3://{x['_externalId']}/")

        # dimension
        if x['Classifier'] not in entries[9]:
            entries[9][x['Classifier']] = {
                "entry": importService.get_asset("Governance council", "Data Findings Dimensions", "Data Findings Dimension", x['Classifier'], x['Classifier'])
            }

        # metric    
        entries[10][f"s3://{x['_externalId']}/:{x['Classifier']}:Unique Matches:Rule"] = {
            "entry": importService.get_asset("Governance council", "Data Findings Rules", "Data Findings Rule",f"s3://{x['_externalId']}/:{x['Classifier']}:Unique Matches", f"{x['Classifier']} Unique Matches")
        }

        importService.add_relations(entries[10][f"s3://{x['_externalId']}/:{x['Classifier']}:Unique Matches:Rule"]['entry'], "00000000-0000-0000-0000-000000007018", "SOURCE",  x['_subscriptionExternalId'], config['community_to_query'], f"s3://{x['_externalId']}/")
        

        entries[10][f"s3://{x['_externalId']}/:{x['Classifier']}:Total Matches:Rule"] = {
            "entry": importService.get_asset("Governance council", "Data Findings Rules", "Data Findings Rule", f"s3://{x['_externalId']}/:{x['Classifier']}:Total Matches", f"{x['Classifier']} Total Matches")
        }

        importService.add_relations(entries[10][f"s3://{x['_externalId']}/:{x['Classifier']}:Total Matches:Rule"]['entry'], "00000000-0000-0000-0000-000000007018", "SOURCE",  x['_subscriptionExternalId'], config['community_to_query'], f"s3://{x['_externalId']}/")

        entries[10][f"s3://{x['_externalId']}/:{x['Classifier']}:Unique Matches:Metric"] = {
            "entry": importService.get_asset("Governance council", "Data Findings Metrics", "Data Findings Metric", f"s3://{x['_externalId']}/:{x['Classifier']}:Unique Matches", f"{x['Classifier']} Unique Matches")
        }

        importService.add_attributes(entries[10][f"s3://{x['_externalId']}/:{x['Classifier']}:Unique Matches:Metric"]['entry'], 'Passing Fraction', x['Unique Matches'], 'string')

        importService.add_relations(entries[10][f"s3://{x['_externalId']}/:{x['Classifier']}:Unique Matches:Metric"]['entry'], "01931f87-3dca-7b65-a03c-dce0146ade76", "TARGET",  "Data Findings Dimensions", "Governance council", x['Classifier'])
        
        importService.add_relations(entries[10][f"s3://{x['_externalId']}/:{x['Classifier']}:Unique Matches:Metric"]['entry'], "01931feb-4b9a-7b6b-a456-e1a2759ceca4", "SOURCE",  "Data Findings Rules", "Governance council", f"s3://{x['_externalId']}/:{x['Classifier']}:Unique Matches")
        
        entries[10][f"s3://{x['_externalId']}/:{x['Classifier']}:Total Matches:Metric"] = {
            "entry": importService.get_asset("Governance council", "Data Findings Metrics", "Data Findings Metric", f"s3://{x['_externalId']}/:{x['Classifier']}:Total Matches", f"{x['Classifier']} Total Matches")
        }

        importService.add_attributes(entries[10][f"s3://{x['_externalId']}/:{x['Classifier']}:Total Matches:Metric"]['entry'], 'Passing Fraction', x['Total Matches'], 'string')

        importService.add_relations(entries[10][f"s3://{x['_externalId']}/:{x['Classifier']}:Total Matches:Metric"]['entry'], "01931f87-3dca-7b65-a03c-dce0146ade76", "TARGET",  "Data Findings Dimensions", "Governance council", x['Classifier'])

        importService.add_relations(entries[10][f"s3://{x['_externalId']}/:{x['Classifier']}:Total Matches:Metric"]['entry'], "01931feb-4b9a-7b6b-a456-e1a2759ceca4", "SOURCE",  "Data Findings Rules", "Governance council", f"s3://{x['_externalId']}/:{x['Classifier']}:Total Matches")


    # if database
    if x['type'] in ('DATABASE', 'DB_SERVER'):
        if x['_externalId'] not in entries[7]:
            entries[7][x['_externalId']] = {
                "entry": importService.get_asset(config['community_to_query'], x['_subscriptionExternalId'], "System", x['name'], x['name']), #Database
                "relations": [],
                "attributes": []
            }

        if x['_subscriptionExternalId'] not in entries[7][x['_externalId']]['relations']:
            entries[7][x['_externalId']]['relations'].append(x['_subscriptionExternalId'])
            importService.add_relations(entries[7][x['_externalId']]['entry'], "00000000-0000-0000-0000-000000007054", "SOURCE",  x['_subscriptionExternalId'], config['community_to_query'], x['_subscriptionExternalId'])

        if x['Category'] not in entries[7][x['_externalId']]['relations']:
            entries[7][x['_externalId']]['relations'].append(x['Category'])
            importService.add_relations(entries[7][x['_externalId']]['entry'], "019465e7-438a-7115-8158-68545ff8d12d", "TARGET", "Data categories", "Privacy and Risk community", x['Category']) #01944282-004e-73ea-a9d6-5a418e9738a7

        if x['Classifier'] not in entries[7][x['_externalId']]['relations']:
            entries[7][x['_externalId']]['relations'].append(x['Classifier'])
            importService.add_relations(entries[7][x['_externalId']]['entry'], "019465e8-5d94-76a6-a34b-68a3f8d7c74c", "TARGET",  "Business Data Models", "Data Architects community", x['Classifier']) #01944282-9d1a-7185-97a6-3b2aef01c556

        if x['_region'] not in entries[7][x['_externalId']]['attributes']:
            entries[7][x['_externalId']]['attributes'].append(x['_region'])
            importService.add_attributes(entries[7][x['_externalId']]['entry'], 'Region', x['_region'], 'string')

        if x['_creationDate'] not in entries[7][x['_externalId']]['attributes']:
            entries[7][x['_externalId']]['attributes'].append(x['_creationDate'])
            importService.add_attributes(entries[7][x['_externalId']]['entry'], 'Created At', x['_creationDate'], 'string')

        if x['_externalId'] not in entries[7][x['_externalId']]['attributes']:
            entries[7][x['_externalId']]['attributes'].append(x['_externalId'])
            importService.add_attributes(entries[7][x['_externalId']]['entry'], 'Principal Identifier', x['_externalId'], 'string')

        # measure
        entries[8][f"{x['name']}:{x['Classifier']}:Unique Matches"] = {
            "entry": importService.get_asset("Governance council", "New Data Findings Metrics", "Measure", f"{x['name']}:{x['Classifier']}:Unique Matches", f"{x['Classifier']} Unique Matches")
        }

        importService.add_attributes(entries[8][f"{x['name']}:{x['Classifier']}:Unique Matches"]['entry'], 'Count', x['Unique Matches'], 'string')

        importService.add_relations(entries[8][f"{x['name']}:{x['Classifier']}:Unique Matches"]['entry'], "01930b23-1a84-7d44-b817-275206442bf6", "TARGET",  "Business Data Models", "Data Architects community",  x['Classifier'])
        
        importService.add_relations(entries[8][f"{x['name']}:{x['Classifier']}:Unique Matches"]['entry'], "019465e9-0c5a-7293-863b-adad740124cc", "SOURCE",  x['_subscriptionExternalId'], config['community_to_query'], x['name']) #01944259-fa74-7122-902a-f019e671cc3a

        entries[8][f"{x['name']}:{x['Classifier']}:Total Matches"] = {
            "entry": importService.get_asset("Governance council", "New Data Findings Metrics", "Measure", f"{x['name']}:{x['Classifier']}:Total Matches", f"{x['Classifier']} Total Matches")
        }

        importService.add_attributes(entries[8][f"{x['name']}:{x['Classifier']}:Total Matches"]['entry'], 'Count', x['Total Matches'], 'string')

        importService.add_relations(entries[8][f"{x['name']}:{x['Classifier']}:Total Matches"]['entry'], "01930b23-1a84-7d44-b817-275206442bf6", "TARGET",  "Business Data Models", "Data Architects community",  x['Classifier'])
        
        importService.add_relations(entries[8][f"{x['name']}:{x['Classifier']}:Total Matches"]['entry'], "019465e9-0c5a-7293-863b-adad740124cc", "SOURCE",  x['_subscriptionExternalId'], config['community_to_query'], x['name']) #01944259-fa74-7122-902a-f019e671cc3a

        # dimension
        if x['Classifier'] not in entries[9]:
            entries[9][x['Classifier']] = {
                "entry": importService.get_asset("Governance council", "Data Findings Dimensions", "Data Findings Dimension", x['Classifier'], x['Classifier'])
            }

        # metric    
        entries[10][f"{x['name']}:{x['Classifier']}:Unique Matches:Rule"] = {
            "entry": importService.get_asset("Governance council", "Data Findings Rules", "Data Findings Rule",f"{x['name']}:{x['Classifier']}:Unique Matches", f"{x['Classifier']} Unique Matches")
        }

        importService.add_relations(entries[10][f"{x['name']}:{x['Classifier']}:Unique Matches:Rule"]['entry'], "00000000-0000-0000-0000-000000007018", "SOURCE",  x['_subscriptionExternalId'], config['community_to_query'], f"{x['name']}")
        

        entries[10][f"{x['name']}:{x['Classifier']}:Total Matches:Rule"] = {
            "entry": importService.get_asset("Governance council", "Data Findings Rules", "Data Findings Rule", f"{x['name']}:{x['Classifier']}:Total Matches", f"{x['Classifier']} Total Matches")
        }

        importService.add_relations(entries[10][f"{x['name']}:{x['Classifier']}:Total Matches:Rule"]['entry'], "00000000-0000-0000-0000-000000007018", "SOURCE",  x['_subscriptionExternalId'], config['community_to_query'], f"{x['name']}")

        entries[10][f"{x['name']}:{x['Classifier']}:Unique Matches:Metric"] = {
            "entry": importService.get_asset("Governance council", "Data Findings Metrics", "Data Findings Metric", f"{x['name']}:{x['Classifier']}:Unique Matches", f"{x['Classifier']} Unique Matches")
        }

        importService.add_attributes(entries[10][f"{x['name']}:{x['Classifier']}:Unique Matches:Metric"]['entry'], 'Passing Fraction', x['Unique Matches'], 'string')

        importService.add_relations(entries[10][f"{x['name']}:{x['Classifier']}:Unique Matches:Metric"]['entry'], "01931f87-3dca-7b65-a03c-dce0146ade76", "TARGET",  "Data Findings Dimensions", "Governance council", x['Classifier'])
        
        importService.add_relations(entries[10][f"{x['name']}:{x['Classifier']}:Unique Matches:Metric"]['entry'], "01931feb-4b9a-7b6b-a456-e1a2759ceca4", "SOURCE",  "Data Findings Rules", "Governance council", f"{x['name']}:{x['Classifier']}:Unique Matches")
        
        entries[10][f"{x['name']}:{x['Classifier']}:Total Matches:Metric"] = {
            "entry": importService.get_asset("Governance council", "Data Findings Metrics", "Data Findings Metric", f"{x['name']}:{x['Classifier']}:Total Matches", f"{x['Classifier']} Total Matches")
        }

        importService.add_attributes(entries[10][f"{x['name']}:{x['Classifier']}:Total Matches:Metric"]['entry'], 'Passing Fraction', x['Total Matches'], 'string')

        importService.add_relations(entries[10][f"{x['name']}:{x['Classifier']}:Total Matches:Metric"]['entry'], "01931f87-3dca-7b65-a03c-dce0146ade76", "TARGET",  "Data Findings Dimensions", "Governance council", x['Classifier'])

        importService.add_relations(entries[10][f"{x['name']}:{x['Classifier']}:Total Matches:Metric"]['entry'], "01931feb-4b9a-7b6b-a456-e1a2759ceca4", "SOURCE",  "Data Findings Rules", "Governance council", f"{x['name']}:{x['Classifier']}:Total Matches")


def do_finding_example(importService, config, entries, x):
    # if bucket
    if x['type'] == 'BUCKET':        
        file = f"s3://{x['name']}/{x['path']}"

        entries[11][file] = {
            "entry": importService.get_asset(config['community_to_query'], x['_subscriptionExternalId'], "File", file, x['path']),
            "relations": []
        }

        importService.add_relations(entries[11][file]['entry'], "00000000-0000-0000-0000-000000007060", "SOURCE", x['_subscriptionExternalId'], config['community_to_query'], f"s3://{x['name']}/")

        importService.add_relations(entries[11][file]['entry'], "01943678-0ab4-7015-ba1f-0f9a168a6ade", "TARGET", "Data categories", "Privacy and Risk community", x['Category'])

        importService.add_relations(entries[11][file]['entry'], "01943678-ebf1-7cd5-bc9c-c78b2d115f3c", "TARGET",  "Business Data Models", "Data Architects community", x['Classifier'])


    # if database
    if x['type'] in ('DATABASE', 'DB_SERVER'):
        parts = x['path'].split('.')[::-1] 

        parts = (parts + [parts[-1], parts[-1]])[:3]

        #database
        database = parts.pop()
        if f"{x['name']}>{database}" not in entries[12]:
            entries[12][f"{x['name']}>{database}"] = {
                "entry": importService.get_asset(config['community_to_query'], x['_subscriptionExternalId'], "Database", f"{x['name']}>{database}", database),
                "relations": []
            }

        if x['name'] not in entries[12][f"{x['name']}>{database}"]['relations']:
            entries[12][f"{x['name']}>{database}"]['relations'].append(x['name'])
            importService.add_relations(entries[12][f"{x['name']}>{database}"]['entry'], "00000000-0000-0000-0000-000000007054", "SOURCE", x['_subscriptionExternalId'], config['community_to_query'], x['name'])

        # schema
        schema = parts.pop()
        if f"{x['name']}>{database}>{schema}" not in entries[13]:
            entries[13][f"{x['name']}>{database}>{schema}"] = {
                "entry": importService.get_asset(config['community_to_query'], x['_subscriptionExternalId'], "Schema", f"{x['name']}>{database}>{schema}", schema),
                "relations": [],
                "attributes": []
            }

        if  f"{x['name']}>{database}" not in entries[13][f"{x['name']}>{database}>{schema}"]['relations']:
            entries[13][f"{x['name']}>{database}>{schema}"]['relations'].append(f"{x['name']}>{database}")
            importService.add_relations(entries[13][f"{x['name']}>{database}>{schema}"]['entry'], "00000000-0000-0000-0000-000000007024", "SOURCE", x['_subscriptionExternalId'], config['community_to_query'], f"{x['name']}>{database}")

        # table
        table = parts.pop()
        if f"{x['name']}>{database}>{schema}>{table}" not in entries[14]:
            entries[14][f"{x['name']}>{database}>{schema}>{table}"] = {
                "entry": importService.get_asset(config['community_to_query'], x['_subscriptionExternalId'], "Table", f"{x['name']}>{database}>{schema}>{table}", table),
                "relations": [],
                "attributes": []
            }

        if  f"{x['name']}>{database}>{schema}" not in entries[14][f"{x['name']}>{database}>{schema}>{table}"]['relations']:
            entries[14][f"{x['name']}>{database}>{schema}>{table}"]['relations'].append(f"{x['name']}>{database}>{schema}")
            importService.add_relations(entries[14][f"{x['name']}>{database}>{schema}>{table}"]['entry'], "00000000-0000-0000-0000-000000007043", "SOURCE", x['_subscriptionExternalId'], config['community_to_query'], f"{x['name']}>{database}>{schema}")


    
def do_all_findings(config, data_scan_resources_ready_df, data_scan_resources_exploded_df):
    logging.getLogger().debug("do all findings")

    runId = time.strftime("%Y%m%d")

    shutil.rmtree(f'./runs/{runId}', ignore_errors=True)

    _= [os.remove(f) for f in glob.glob(f'./runs/{runId}.json.*')]
    
    ctx = get_script_run_ctx()

    t = threading.Thread(target=show_progress, args=[runId], daemon=True)

    add_script_run_ctx(t, ctx)

    t.start()

    collibra = get_collibra(get_config())
    
    communities = {}

    response = collibra.get("session").get(f"{collibra.get('endpoint')}/communities")

    _ = [x(communities, community.get("name"), community) for community in response.json()["results"]]

    st.write("")

    if 'submitted' not in st.session_state or not st.session_state.submitted:
        if st.button("Start", type='primary'):
            show_dialog(communities)
        
        st.stop()


    # placeholder = st.empty()
    
    # with placeholder.container():   
    #     with open("Animation-1736352676693.json", "r") as f:
    #         st_lottie(json.load(f), height=200, width=300)
        

    community = st.session_state.resources_community

    config['community_to_query'] = community # (communities.get(community) if community else st.warning("Please specify.") & st.stop())

    importService = ImportService(runId, 1, 150000)

    entries = [{} for element in range(15)]

    data_scan_resources_ready_df.apply(lambda x: do_finding(importService, config, entries, x), axis=1)

    if 'do_finding_examples' in st.session_state and st.session_state.do_finding_examples:
        data_scan_resources_exploded_df.apply(lambda x: do_finding_example(importService, config, entries, x), axis=1)


    # each in it step file
    allEntries = [[] for element in range(15)]

    _= [allEntries[i].append(v['entry']) for i,e in enumerate(entries) for k,v in e.items()]

    _= [importService.save(e, "./runs", runId, i, True) for i,e in enumerate(allEntries)]

    
    # # all in one step file
    # allEntries = []
    # _= [allEntries.append(v['entry']) for i,e in enumerate(entries) for k,v in e.items()]
    # importService.save(allEntries, "./runs", runId, 0, True)

    HarvesterService().run(config, "./runs") 

    # placeholder.empty()

    t.join()


#main   
def main():
    logging.getLogger().setLevel(logging.INFO)

    try:
        show_dashboard(get_config())

    except Exception as error:
        raise Exception('Error: %s', error)
    


if __name__ == '__main__':
    st.set_page_config(layout="wide")

    main()    

    

#get collibra
#@st.cache_resource
# def get_collibra(config):
#     payload = f'client_id={config["collibra_client_id"]}&client_secret={config["collibra_client_secret"]}&grant_type=client_credentials'

#     headers = {'Content-Type': 'application/x-www-form-urlencoded'}

#     conn = http.client.HTTPSConnection(config["collibra_host"])

#     conn.request("POST", config['collibra_token_endpoint'], payload, headers)

#     res = conn.getresponse()

#     token = json.loads(res.read().decode("utf-8"))

#     collibra = {}

#     collibra = {"host": f"https://{config['collibra_host']}"}

#     collibra["endpoint"] = f"{collibra['host']}{config['collibra_api_endpoint']}"

#     collibra["session"] = requests.Session()
        
#     collibra.get("session").headers.update({'Authorization': f'Bearer {token["access_token"]}'})

#     return collibra

