import re
import sys
import csv
import time
import json
import codecs
import logging
import requests
import glob

import pandas as pd

from datetime import datetime

from contextlib import closing

import streamlit as st

import altair as alt

from requests.auth import HTTPBasicAuth

from services import ImportService, Identifier

from harvester import HarvesterService

import threading

from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx

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


#get projects query 
GET_PROJECTS_QUERY = (
    """
      query ProjectsTable(
        $filterBy: ProjectFilters
        $first: Int
        $after: String
        $orderBy: ProjectOrder
      ) {
        projects(
          filterBy: $filterBy
          first: $first
          after: $after
          orderBy: $orderBy
        ) {
          nodes {
            id
            name
            isFolder
            archived
            businessUnit
            description
          }
        }
      }
    """
)


#get isseus query
GET_ISSUES_QUERY = (
    """
    query IssuesTable($filterBy: IssueFilters, $first: Int, $after: String, $orderBy: IssueOrder) {
    issues: issuesV2(
        filterBy: $filterBy
        first: $first
        after: $after
        orderBy: $orderBy
    ) {
        nodes {
        id
        sourceRule {
            __typename
            ... on Control {
            id
            name
            controlDescription: description
            resolutionRecommendation
            securitySubCategories {
                title
                category {
                name
                framework {
                    name
                }
                }
            }
            risks
            }
            ... on CloudEventRule {
            id
            name
            cloudEventRuleDescription: description
            sourceType
            type
            risks
            securitySubCategories {
                title
                category {
                name
                framework {
                    name
                }
                }
            }
            }
            ... on CloudConfigurationRule {
            id
            name
            cloudConfigurationRuleDescription: description
            remediationInstructions
            serviceType
            risks
            securitySubCategories {
                title
                category {
                name
                framework {
                    name
                }
                }
            }
            }
        }
        createdAt
        updatedAt
        dueAt
        type
        resolvedAt
        statusChangedAt
        projects {
            id
            name
            slug
            businessUnit
            riskProfile {
            businessImpact
            }
        }
        status
        severity
        entitySnapshot {
            id
            type
            nativeType
            name
            status
            cloudPlatform
            cloudProviderURL
            providerId
            region
            resourceGroupExternalId
            subscriptionExternalId
            subscriptionName
            subscriptionTags
            tags
            createdAt
            externalId
        }
        serviceTickets {
            externalId
            name
            url
        }
        notes {
            createdAt
            updatedAt
            text
            user {
            name
            email
            }
            serviceAccount {
            name
            }
        }
        }
        pageInfo {
        hasNextPage
        endCursor
        }
    }
    }
    """
)


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


#get projects
def get_projects(config):
    logging.getLogger().debug("get projects")

    variables = {
        "first": 500,
        "filterBy": {
            "includeArchived": False,
            "isFolder": False,
            "root": False
        }
    }

    nodes = []

    while True:
        response = query(config, GET_PROJECTS_QUERY, variables)

        try:
            nodes = nodes + response['projects']['nodes']

            variables['after'] = response['projects']['pageInfo']['endCursor']

            if response['projects']['pageInfo']['hasNextPage'] == False:
                break

        except Exception as error:
            break

    return nodes


#get issues
def get_issues(config, project_id):
    logging.getLogger().debug("get issues")

    variables = {
        "first": 500,
        "filterBy": {
            "project": [
                project_id
            ],
        },
        "severity": ["CRITICAL"],
        "stackLayer": ["APPLICATION_AND_DATA", "DATA_STORES"]
    }
    
    nodes = []

    while True:
        response = query(config, GET_ISSUES_QUERY, variables)

        try:
            nodes = nodes + response['issues']['nodes']

            variables['after'] = response['issues']['pageInfo']['endCursor']

            if response['issues']['pageInfo']['hasNextPage'] == False:
                break

        except Exception as error:
            break
            
    return nodes


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
    logging.getLogger().debug("init dashboard")

    get_token(config)

    session = st.connection("snowflake").session()

    # resources = get_resources(config, config['wizio_project_id']) 
    
    # resources_df = pd.DataFrame(resources)

    # session.write_pandas(resources_df, "RESOURCES", auto_create_table=True, overwrite=True)

    # resources_df['externalId'] = resources_df['graphEntity'].apply(get_external_id)

    # session.write_pandas(resources_df, "RESOURCES_READY", auto_create_table=True, overwrite=True)

    # reports = get_report(config, config['wizio_project_id']) 

    # data_scan_df = reports['DATA_SCAN']

    # session.write_pandas(data_scan_df, "DATA_SCAN", auto_create_table=True, overwrite=True)

    # data_scan_resources_df = data_scan_df.set_index('Resource External ID') .join(resources_df.set_index('externalId'))

    # data_scan_resources_df.reset_index(inplace=True)

    # session.write_pandas(data_scan_resources_df, "DATA_SCAN_RESOURCES", auto_create_table=True, overwrite=True)

    # data_scan_resources_ready_df = pd.json_normalize(data_scan_resources_df['graphEntity'])

    # data_scan_resources_ready_df[['Finding ID', 'Category', 'Classifier', 'Unique Matches', 'Total Matches', 'Severity', 'Finding Examples']] = data_scan_resources_df[['ID', 'Category', 'Classifier', 'Unique Matches', 'Total Matches', 'Severity', 'Finding Examples']]

    # data_scan_resources_ready_df = data_scan_resources_ready_df.rename(lambda x: x.replace('properties.','_'), axis='columns')

    # data_scan_resources_ready_df['Examples Count'] = data_scan_resources_ready_df['Finding Examples'].apply(get_number_of_findings)

    # data_scan_resources_ready_df['_creationYYMM']=data_scan_resources_ready_df['_creationDate'].str[0:7]

    # session.write_pandas(data_scan_resources_ready_df, "DATA_SCAN_RESOURCES_READY", auto_create_table=True, overwrite=True)

    data_scan_resources_ready_df = session.table("DATA_SCAN_RESOURCES_READY").to_pandas()

    # data_scan_resources_ready_df.to_csv('datascanresourcesready.csv', index=False)

    # data_scan_resources_ready_df = pd.read_csv('datascanresourcesready.csv')  

    return data_scan_resources_ready_df


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

        data_scan_resources_ready_df = get_data_findings(config)

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
        
        columns=['name','Classifier','Unique Matches','Total Matches','Severity']
        
        st.dataframe(data_scan_resources_ready_df[columns].pivot_table(values=["Unique Matches","Total Matches"], index=["name","Severity"], columns="Classifier", aggfunc="sum"))

        #st.write(data_scan_resources_ready_df[columns].pivot_table(values=["Unique Matches","Total Matches"], index=["name","Severity"], columns="Classifier", aggfunc="sum").to_html())
        
        #group 6
        with st.expander("See details"):
            columns=['id','name','type','_externalId','_nativeType','_kind','_cloudPlatform','_subscriptionExternalId','_region','__environments','_isManaged','_isPaaS','_creationDate','Finding ID','Category','Classifier','Severity','Unique Matches','Total Matches','Finding Examples','Examples Count']

            st.data_editor(data_scan_resources_ready_df[columns],hide_index=True,column_config={"id":"Resource Id","name":"Resource Name","type":"Resource Type","_externalId":"Resource External Id","_nativeType":"Resource Native Type","_kind":"Resource Kind","_cloudPlatform":"Resource Platform","_subscriptionExternalId":"Resource Account","_region":"Resource Region","__environments":"Resource Environment","_isManaged":"Is Resource Managed","_isPaaS":"Is Resource PaaS","_creationDate": "Resource Creation Date","Finding ID": "Finding Id","Category": "Finding Category","Classifier": "Finding Classifier","Severity": "Finding Severity"})
            
        st.write("")

        #do stuff
        do_stuff(config, data_scan_resources_ready_df)

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


#do classifier
def do_classifier(classifier, category, severity, entries, importService):
    if classifier not in entries:
        entries[classifier] = {
            "entry": importService.get_asset("Data Architects community", "Business Data Models", "Data Concept", classifier, classifier),
            "relations": [],
            "attributes": []
        }

    if category not in entries[classifier]['relations']:
        entries[classifier]['relations'].append(category)
        importService.add_relations(entries[classifier]['entry'], "c0e00000-0000-0000-0000-000000007316", "SOURCE", "Data categories", "Privacy and Risk community", category)

    if severity not in entries[classifier]['attributes']:
        entries[classifier]['attributes'].append(severity)
        importService.add_attributes(entries[classifier]['entry'], 'Severity', severity, 'string')
    

#do system
def do_system(system, platform, account, domain, community, entries, importService):
    if system not in entries:

        entries[system] = {
            "entry": importService.get_asset(community, domain, "System", system, system),
            "relations": [],
            "attributes": []
        }

    if platform not in entries[system]['attributes']:
        entries[system]['attributes'].append(platform)
        importService.add_attributes(entries[system]['entry'], 'Platform', platform, 'string')

    if account not in entries[system]['attributes']:
        entries[system]['attributes'].append(account)
        importService.add_attributes(entries[system]['entry'], 'Account Name', account, 'string')


#do file storage
def do_filestorage(bucket, region, cdate, system, domain, community, entries, importService):
    if bucket not in entries:
        entries[bucket] = {
            "entry": importService.get_asset(community, domain, "S3 File System", bucket, bucket),
            "relations": [],
            "attributes": []
        }

    if system not in entries[bucket]['relations']:
        entries[bucket]['relations'].append(system)
        importService.add_relations(entries[bucket]['entry'], "00000000-0000-0000-0000-000000007054", "SOURCE", domain, community, system)

    if region not in entries[bucket]['attributes']:
        entries[bucket]['attributes'].append(region)
        importService.add_attributes(entries[bucket]['entry'], 'Region', region, 'string')

    if cdate not in entries[bucket]['attributes']:
        entries[bucket]['attributes'].append(cdate)
        importService.add_attributes(entries[bucket]['entry'], 'Created At', cdate, 'string')


#do storage container
def do_storagecontainer(bucket, region, cdate, system, category, classifier, domain, community, entries, importService):
    if bucket not in entries:
        entries[bucket] = {
            "entry": importService.get_asset(community, domain, "S3 Bucket", bucket, bucket),
            "relations": [],
            "attributes": []
        }

    if system not in entries[bucket]['relations']:
        entries[bucket]['relations'].append(system)
        importService.add_relations(entries[bucket]['entry'], "00000000-0000-0000-0001-002600000000", "SOURCE", domain, community, system)

    if category not in entries[bucket]['relations']:
        entries[bucket]['relations'].append(category)
        importService.add_relations(entries[bucket]['entry'], "01930192-86fb-77b0-8baf-30a80dccb864", "TARGET", "Data categories", "Privacy and Risk community", category)

    if classifier not in entries[bucket]['relations']:
        entries[bucket]['relations'].append(classifier)
        importService.add_relations(entries[bucket]['entry'], "01930192-f332-70fc-8572-9f7283c4cfd4", "TARGET",  "Business Data Models", "Data Architects community", classifier)

    if region not in entries[bucket]['attributes']:
        entries[bucket]['attributes'].append(region)
        importService.add_attributes(entries[bucket]['entry'], 'Region', region, 'string')

    if cdate not in entries[bucket]['attributes']:
        entries[bucket]['attributes'].append(cdate)
        importService.add_attributes(entries[bucket]['entry'], 'Created At', cdate, 'string')


#do directory
def do_directory(bucket, region, cdate, system, category, classifier, domain, community, entries, importService):
    if bucket not in entries:
        entries[bucket] = {
            "entry": importService.get_asset(community, domain, "Directory", bucket, "/"),
            "relations": [],
            "attributes": []
        }

    if system not in entries[bucket]['relations']:
        entries[bucket]['relations'].append(system)
        importService.add_relations(entries[bucket]['entry'], "00000000-0000-0000-0001-002600000001", "SOURCE", domain, community, system)

    if category not in entries[bucket]['relations']:
        entries[bucket]['relations'].append(category)
        importService.add_relations(entries[bucket]['entry'], "01930192-86fb-77b0-8baf-30a80dccb864", "TARGET", "Data categories", "Privacy and Risk community", category)

    if classifier not in entries[bucket]['relations']:
        entries[bucket]['relations'].append(classifier)
        importService.add_relations(entries[bucket]['entry'], "01930192-f332-70fc-8572-9f7283c4cfd4", "TARGET",  "Business Data Models", "Data Architects community", classifier)

    if region not in entries[bucket]['attributes']:
        entries[bucket]['attributes'].append(region)
        importService.add_attributes(entries[bucket]['entry'], 'Region', region, 'string')

    if cdate not in entries[bucket]['attributes']:
        entries[bucket]['attributes'].append(cdate)
        importService.add_attributes(entries[bucket]['entry'], 'Created At', cdate, 'string')


#do measure
def do_measure(bucket, classifier, unique, total, domain, community, entries, importService):
    entries[f"{bucket}:{classifier}"] = {
        "entry": [
            importService.get_asset("Governance council", "New Data Findings Metrics", "Measure", f"{bucket}:{classifier}:Unique Matches", f"{classifier} Unique Matches"),
            importService.get_asset("Governance council", "New Data Findings Metrics", "Measure", f"{bucket}:{classifier}:Total Matches", f"{classifier} Total Matches"),
        ]
    }

    importService.add_attributes(entries[f"{bucket}:{classifier}"]['entry'][0], 'Count', unique, 'string')
    importService.add_attributes(entries[f"{bucket}:{classifier}"]['entry'][1], 'Count', total, 'string')

    importService.add_relations(entries[f"{bucket}:{classifier}"]['entry'][0], "01930b23-1a84-7d44-b817-275206442bf6", "TARGET",  "Business Data Models", "Data Architects community", classifier)
    importService.add_relations(entries[f"{bucket}:{classifier}"]['entry'][0], "01930b24-2617-722b-9502-8c30d4b3818c", "SOURCE",  domain, community, bucket)

    importService.add_relations(entries[f"{bucket}:{classifier}"]['entry'][1], "01930b23-1a84-7d44-b817-275206442bf6", "TARGET",  "Business Data Models", "Data Architects community", classifier)
    importService.add_relations(entries[f"{bucket}:{classifier}"]['entry'][1], "01930b24-2617-722b-9502-8c30d4b3818c", "SOURCE",  domain, community, bucket)


#do dimension
def do_dimension(classifier, entries, importService):
    if classifier not in entries:
        entries[classifier] = {
            "entry": importService.get_asset("Governance council", "Data Findings Dimensions", "Data Findings Dimension", classifier, classifier)
        }


#do metric
def do_metric(bucket, classifier, unique, total, domain, community, entries, importService):
    entries[f"{bucket}:{classifier}"] = {
        "entry": [
            importService.get_asset("Governance council", "Data Findings Rules", "Data Findings Rule", f"{bucket}:{classifier}:Unique Matches", f"{classifier} Unique Matches"),
            importService.get_asset("Governance council", "Data Findings Rules", "Data Findings Rule", f"{bucket}:{classifier}:Total Matches", f"{classifier} Total Matches"),

            importService.get_asset("Governance council", "Data Findings Metrics", "Data Findings Metric", f"{bucket}:{classifier}:Unique Matches", f"{classifier} Unique Matches"),
            importService.get_asset("Governance council", "Data Findings Metrics", "Data Findings Metric", f"{bucket}:{classifier}:Total Matches", f"{classifier} Total Matches"),
        ]
    }
    
    # #rule applies to asset directory
    importService.add_relations(entries[f"{bucket}:{classifier}"]['entry'][0], "00000000-0000-0000-0000-000000007018", "SOURCE",  domain, community, bucket)
    importService.add_relations(entries[f"{bucket}:{classifier}"]['entry'][1], "00000000-0000-0000-0000-000000007018", "SOURCE",  domain, community, bucket)

    # # metric passing fraction
    importService.add_attributes(entries[f"{bucket}:{classifier}"]['entry'][2], 'Passing Fraction', unique, 'string')
    importService.add_attributes(entries[f"{bucket}:{classifier}"]['entry'][3], 'Passing Fraction', total, 'string')

    # # metric classified by dimension
    importService.add_relations(entries[f"{bucket}:{classifier}"]['entry'][2], "01931f87-3dca-7b65-a03c-dce0146ade76", "TARGET",  "Data Findings Dimensions", "Governance council", classifier)
    importService.add_relations(entries[f"{bucket}:{classifier}"]['entry'][3], "01931f87-3dca-7b65-a03c-dce0146ade76", "TARGET",  "Data Findings Dimensions", "Governance council", classifier)

    # # metric executes rule
    importService.add_relations(entries[f"{bucket}:{classifier}"]['entry'][2], "01931feb-4b9a-7b6b-a456-e1a2759ceca4", "SOURCE",  "Data Findings Rules", "Governance council", f"{bucket}:{classifier}:Unique Matches")
    importService.add_relations(entries[f"{bucket}:{classifier}"]['entry'][3], "01931feb-4b9a-7b6b-a456-e1a2759ceca4", "SOURCE",  "Data Findings Rules", "Governance council", f"{bucket}:{classifier}:Total Matches")


#show dialog
@st.dialog("Choose")
def show_dialog(communities):
    option = st.selectbox(
        label="Select the community where you want to find your storage on ",
        options=sorted([f"{k}" for k, v in communities.items()]),
        index=None
    )
    
    if not option:
        st.warning("Please specify.") & st.stop()

    if st.button("Submit"):
        st.session_state.submitted = True
        st.session_state.option = option
        st.rerun()


#show progress
def show_progress():
    progress = 20

    bar = st.progress(progress)
    
    while progress != 100:
        files = list(filter(lambda x: 'beam' not in x.lower(), glob.glob(f'./runs/*.json.step.*')))
        
        progress = len(files)*10 +20

        bar.progress(progress, text=f'{progress//10} of 10')

        time.sleep(1)

    bar.progress(100)

    time.sleep(1)



#do stuff
def do_stuff(config, data_scan_resources_ready_df):
    ctx = get_script_run_ctx()

    t = threading.Thread(target=show_progress,daemon=True)

    add_script_run_ctx(t, ctx)

    t.start()


    logging.getLogger().debug("do stuff")

    collibra = get_collibra(get_config())
    
    communities = {}

    response = collibra.get("session").get(f"{collibra.get('endpoint')}/communities")

    _ = [x(communities, community.get("name"), community) for community in response.json()["results"]]

    st.write("")

    if 'submitted' not in st.session_state or not st.session_state.submitted:
        if st.button("Start", type='primary'):
            show_dialog(communities)
        
        st.stop()


    option = st.session_state.option

    communityToQuery = (communities.get(option) if option else st.warning("Please specify.") & st.stop())

    bucketsFindings = data_scan_resources_ready_df.query("type == 'BUCKET'")

    importService = ImportService(time.strftime("%Y%m%d"), 1, 150000)

    #categories
    categoryEntries = []

    _= [categoryEntries.append(importService.get_asset("Privacy and Risk community", "Data categories", "Data Category", e, e)) for e in bucketsFindings['Category'].drop_duplicates()]

    #classifiers
    entries = {}

    _= [do_classifier(e[0], e[1], e[2], entries, importService) for e in bucketsFindings[['Classifier', 'Category', 'Severity']].drop_duplicates().itertuples(index=False)]

    classifierEntries = []

    _= [classifierEntries.append(v['entry']) for k,v in entries.items()]
    
    importService.save(categoryEntries+classifierEntries, "./runs", "classifier", 0, True)

    #domains
    domainEntries = []

    domains = data_scan_resources_ready_df['_subscriptionExternalId'].drop_duplicates()

    _= [domainEntries.append(importService.get_domain(communityToQuery['name'], "Technology Asset Domain", str(e))) for e in domains]

    #systems 
    entries = {}

    _= [do_system(str(e[0]), e[1], str(e[2]), str(e[3]), communityToQuery['name'], entries, importService) for e in bucketsFindings[['_subscriptionExternalId', '_cloudPlatform', '_subscriptionExternalId', '_subscriptionExternalId']].drop_duplicates().itertuples(index=False)]

    systemEntries = []

    _= [systemEntries.append(v['entry']) for k,v in entries.items()]
    
    importService.save(systemEntries, "./runs", "system", 1, True)

    #file storages
    entries = {}

    _= [do_filestorage(e[0], e[1], e[2], str(e[3]), str(e[4]), communityToQuery['name'], entries, importService) for e in bucketsFindings[['_externalId', '_region', '_creationDate', '_subscriptionExternalId', '_subscriptionExternalId']].drop_duplicates().itertuples(index=False)]

    fileStorageEntries = []

    _= [fileStorageEntries.append(v['entry']) for k,v in entries.items()]
    
    importService.save(fileStorageEntries, "./runs", "fileStorage", 2, True)

    #storage containers
    entries = {}

    _= [do_storagecontainer(f"s3://{e[0]}", e[1], e[2], str(e[3]), e[4], e[5], str(e[6]), communityToQuery['name'], entries, importService) for e in bucketsFindings[['_externalId', '_region', '_creationDate', '_externalId', 'Category', 'Classifier', '_subscriptionExternalId']].drop_duplicates().itertuples(index=False)]

    storageContainerEntries = []

    _= [storageContainerEntries.append(v['entry']) for k,v in entries.items()]

    importService.save(storageContainerEntries, "./runs", "storageContainer", 3, True)
    
    #directories
    entries = {}

    _= [do_directory(f"s3://{e[3]}/", e[1], e[2], f"s3://{e[3]}", e[4], e[5], str(e[6]), communityToQuery['name'], entries, importService) for e in bucketsFindings[['_externalId', '_region', '_creationDate', '_externalId', 'Category', 'Classifier', '_subscriptionExternalId']].drop_duplicates().itertuples(index=False)]

    directoryEntries = []

    _= [directoryEntries.append(v['entry']) for k,v in entries.items()]

    importService.save(directoryEntries, "./runs", "directory", 4, True)

    #measures
    entries = {}

    _= [do_measure(f"s3://{e[0]}/", e[1], e[2], e[3], str(e[4]), communityToQuery['name'], entries, importService) for e in bucketsFindings[['_externalId', 'Classifier', 'Unique Matches', 'Total Matches', '_subscriptionExternalId']].drop_duplicates().itertuples(index=False)]

    measureEntries = []

    _= [measureEntries.append(v['entry']) for k,v in entries.items()]

    importService.save(measureEntries, "./runs", "measure", 5, True)

    #dimensions
    entries = {}

    _= [do_dimension(e[0], entries, importService) for e in bucketsFindings[['Classifier']].drop_duplicates().itertuples(index=False)]

    dimensionEntries = []

    _= [dimensionEntries.append(v['entry']) for k,v in entries.items()]
    
    importService.save(dimensionEntries, "./runs", "dimension", 6, True)

    #metrics
    entries = {}

    _= [do_metric(f"s3://{e[0]}/", e[1], e[2], e[3], str(e[4]), communityToQuery['name'], entries, importService) for e in bucketsFindings[['_externalId', 'Classifier', 'Unique Matches', 'Total Matches', '_subscriptionExternalId']].drop_duplicates().itertuples(index=False)]

    metricEntries = []

    _= [metricEntries.append(v['entry']) for k,v in entries.items()]
    
    importService.save(metricEntries, "./runs", "metric", 7, True)

    # TODO: data finding rule and metric: rule connected with directory and metric, metric connected with dimension and with passing fraction

    # TODO: add finding examples

    HarvesterService().run(config, "./runs")

    t.join()

    with st.expander("See results"):
        file = sorted(glob.glob(f'./runs/*.results'))[-1] 

        with open(file, "r") as f:
            results = json.load(f)

            parts = [(p['step_number'], p['file_name'], p['part_number'], p['job']['id'], p['job']['result']) for k,v in results['steps'].items() for p in v]

            st.dataframe(pd.DataFrame(parts, columns=['Step Number','File Name','Part Number','Job Id','Job Result']))

            st.markdown("[take a closer look](https://print.collibra.com/profile/9693d5ce-9fb4-4e97-b46e-7218526eda14/activities)")
            

    st.stop()


#main   
def main():
    logging.getLogger().setLevel(logging.INFO)

    try:
        show_dashboard(get_config())

    except Exception as error:
        raise Exception('Error: %s', error)
    


if __name__ == '__main__':
    st.set_page_config(layout="wide")
    
    #main()    

    session = st.connection("snowflake").session()

    data_scan_resources_ready_df = session.table("DATA_SCAN_RESOURCES_READY").to_pandas()

    

    
    data_scan_resources_exploded_df = data_scan_resources_ready_df.query("type == 'BUCKET'")

    data_scan_resources_exploded_df['Finding Examples'] = data_scan_resources_exploded_df['Finding Examples'].apply(lambda x: eval(x) if x is not None else None)

    columns=['id', 'name', 'type', '_subscriptionExternalId', 'Category', 'Classifier', 'Finding Examples']

    data_scan_resources_exploded_df = data_scan_resources_exploded_df[columns].explode('Finding Examples', ignore_index=True)

    exploded_df = pd.json_normalize(data_scan_resources_exploded_df['Finding Examples'])

    columns=['id', 'name', 'type', '_subscriptionExternalId', 'Category', 'Classifier']
        
    data_scan_resources_exploded_df = pd.concat([data_scan_resources_exploded_df[columns], exploded_df[['key', 'path']]], axis=1)

    session.write_pandas(data_scan_resources_exploded_df, "DATA_SCAN_RESOURCES_EXPLODED", auto_create_table=True, overwrite=True)

    st.dataframe(data_scan_resources_exploded_df)
    


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

