# %%
import re
import sys
import csv
import time
import json
import codecs
import logging
import requests

import pandas as pd

from datetime import datetime

from contextlib import closing

import streamlit as st

import altair as alt



# %%
# graphql conf
MAX_RETRIES_FOR_QUERY = 5
MAX_RETRIES_FOR_DOWNLOAD_REPORT = 5
RETRY_TIME_FOR_QUERY = 2
RETRY_TIME_FOR_DOWNLOAD_REPORT = 60
CHECK_INTERVAL_FOR_DOWNLOAD_REPORT = 20

# %%
# authentication     
AUTH0_URLS = ['https://auth.wiz.io/oauth/token', 'https://auth0.gov.wiz.io/oauth/token']
COGNITO_URLS = ['https://auth.app.wiz.io/oauth/token', 'https://auth.gov.wiz.io/oauth/token']

# %%
# get projects query 
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

# %%
# get isseus query
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

# %%
# get resources query
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

# %%
# get report query
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

# %%

# create report mutation
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

# %%
# rerun report mutation
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

# %%
# report download query
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

# %%
# set logging
def set_logging():
    logging.getLogger().setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stderr)

    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

    handler.setFormatter(formatter)

    logging.getLogger().handlers = [handler]

# %%
# get config
def get_config():
    logging.getLogger().debug("get config")

    with open('config.json', "r") as f:
        config = json.load(f)

    return config

# %%
# get auth params
def generate_authentication_params(config):
    if config['token_url'] in AUTH0_URLS:
        return {
            'grant_type': 'client_credentials',
            'audience': 'beyond-api',
            'client_id': config['client_id'],
            'client_secret': config['client_secret']
        }
    
    elif config['token_url'] in COGNITO_URLS:
        return {
            'grant_type': 'client_credentials',
            'audience': 'wiz-api',
            'client_id': config['client_id'],
            'client_secret': config['client_secret']
        }
    
    else:
        raise Exception('Error: wrong token url')

# %%
# get token
def get_token(config):
    response = requests.post(
        config['token_url'],
        headers = {'Content-Type': 'application/x-www-form-urlencoded'},
        data = generate_authentication_params(config)
    )

    if response.status_code != requests.codes.ok:
        raise Exception(f'Error: {response.text}') 

    if not response.json().get('access_token'):
        raise Exception(f'Error: {response.json().get("message")}')

    config['token'] = response.json().get('access_token')

    return config

# %%
# send request
def send_request(config, query, variables):    
    if config['token']:
        return requests.post(
            config['api_endpoint_url'],
            headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + config['token']},
            json = {'query': query, 'variables': variables}
        )
    
    raise Exception('Error: token not found')

# %%
# query
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

# %%
# get projects
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

# %%
# get issues
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

# %%
# get resources
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

# %%
# create report
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

# %%
# rerun report
def rerun_report(config, report_id):
    variables = {
        'reportId': report_id
    }

    response = query(config, RERUN_REPORT_MUTATION, variables)

    report_id = response['rerunReport']['report']['id']

    return report_id

# %%
# get report url and status
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

# %%
# get report content
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

# %%
# get report content to dataframe
def get_report_content_to_dataframe(download_url):
    return pd.read_csv(download_url)    

# %%
# get report
def get_report(config, project_id):    
    logging.getLogger().debug("get report")
    
    report_data = {}
    
    report_type= "DATA_SCAN"

    report_id = create_report(config, project_id, report_type, report_type)

    report_url = get_report_url_and_status(config, report_id)
    
    #report_data[report_type] = get_report_content(report_url)
    report_data[report_type] = get_report_content_to_dataframe(report_url)

    return report_data

# %%
# get external id
def get_external_id(x):
    try:
        return x['properties']['externalId']
    
    except Exception as error:
        try:
            return x['externalId']
        
        except Exception as error:
            return None

# %%
def replaceDot(x):
    return(x.replace('properties.','_'))


def get_number_of_findings(x):
    try:
        return len(json.loads(x))
    
    except Exception as error:
        return None
    
# %%
# main
def main():
    global projects
    global issues_df
    global resources_df
    global data_scan_df
    global data_scan_issues_df
    global data_scan_resources_df
    global data_scan_resources_df2

    try:
        set_logging()

        config = get_token(get_config())

        #resources = get_resources(config, config['project_id']) 
        
        #resources_df = pd.DataFrame(resources)

        #resources_df['externalId'] = resources_df['graphEntity'].apply(get_external_id)

        #reports = get_report(config, config['project_id']) 

        #data_scan_df = reports['DATA_SCAN']

        #data_scan_resources_df = data_scan_df.set_index('Resource External ID') .join(resources_df.set_index('externalId'))


        #data_scan_resources_df.reset_index(inplace=True)

        #data_scan_resources_df2=pd.json_normalize(data_scan_resources_df['graphEntity'])

        #data_scan_resources_df2[['Finding ID', 'Category', 'Classifier', 'Unique Matches', 'Total Matches', 'Severity', 'Finding Examples']]= data_scan_resources_df[['ID', 'Category', 'Classifier', 'Unique Matches', 'Total Matches', 'Severity', 'Finding Examples']]

        #data_scan_resources_df2.to_csv('datascanresources2.csv')


        data_scan_resources_df2 = pd.read_csv('datascanresources2.csv')  
    
        data_scan_resources_df2 = data_scan_resources_df2.rename(replaceDot, axis='columns')

        data_scan_resources_df2['Examples Count'] = data_scan_resources_df2['Finding Examples'].apply(get_number_of_findings)

        data_scan_resources_df2['_creationYYMM']=data_scan_resources_df2['_creationDate'].str[0:7]
        

        resources_per_cloud_platform = data_scan_resources_df2[['_cloudPlatform','id']].drop_duplicates().groupby(by=['_cloudPlatform']).count().reset_index().rename(columns={"id": "count"})

        resources_per_environment = data_scan_resources_df2[['__environments','id']].drop_duplicates().groupby(by=['__environments']).count().reset_index().rename(columns={"id": "count"})

        resources_per_status = data_scan_resources_df2[['_status','id']].drop_duplicates().groupby(by=['_status']).count().reset_index().rename(columns={"id": "count"})


        resources_per_region = data_scan_resources_df2[['_region','id']].drop_duplicates().groupby(by=['_region']).count().reset_index().rename(columns={"id": "count"})

        resources_per_type = data_scan_resources_df2[['type','id']].drop_duplicates().groupby(by=['type']).count().reset_index().rename(columns={"id": "count"})
    
        resources_per_creation_date = data_scan_resources_df2[['_creationYYMM','id']].drop_duplicates().groupby(by=['_creationYYMM']).count().reset_index().rename(columns={"id": "count"})

        resources_per_category = data_scan_resources_df2[['Category','id']].drop_duplicates().groupby(by=['Category']).count().reset_index().rename(columns={"id": "count"})

        resources_per_severity = data_scan_resources_df2[['Severity','id']].drop_duplicates().groupby(by=['Severity']).count().reset_index().rename(columns={"id": "count"})


        findings_per_region = data_scan_resources_df2[['_region','Finding ID']].groupby(by=['_region']).count().reset_index().rename(columns={"Finding ID": "count"})

        findings_per_type = data_scan_resources_df2[['type','Finding ID']].groupby(by=['type']).count().reset_index().rename(columns={"Finding ID": "count"})
    
        #findings_per_severity = data_scan_resources_df2[['Severity','Finding ID']].groupby(by=['Severity']).count().reset_index().rename(columns={"Finding ID": "count"})

        findings_per_classifier = data_scan_resources_df2[['Classifier','Finding ID']].groupby(by=['Classifier']).count().reset_index().rename(columns={"Finding ID": "count"})

        findings_per_type_and_severity = data_scan_resources_df2[['type', 'Severity', 'Finding ID']].groupby(by=['type','Severity']).count().reset_index().rename(columns={"Finding ID": "count"})

        findings_per_type_and_classifier = data_scan_resources_df2[['type', 'Classifier', 'Finding ID']].groupby(by=['type','Classifier']).count().reset_index().rename(columns={"Finding ID": "count"})


        unique_matches_per_region = data_scan_resources_df2[['_region','Unique Matches']].groupby(by=['_region']).sum().reset_index().rename(columns={"Unique Matches": "count"})

        unique_matches_per_type = data_scan_resources_df2[['type','Unique Matches']].groupby(by=['type']).sum().reset_index().rename(columns={"Unique Matches": "count"})

        unique_matches_per_classifier = data_scan_resources_df2[['Classifier','Unique Matches']].groupby(by=['Classifier']).sum().reset_index().rename(columns={"Unique Matches": "count"})

        #unique_matches_per_severity = data_scan_resources_df2[['Severity','Unique Matches']].groupby(by=['Severity']).sum().reset_index().rename(columns={"Unique Matches": "count"})

        unique_matches_per_type_and_severity = data_scan_resources_df2[['type', 'Severity', 'Unique Matches']].groupby(by=['type','Severity']).sum().reset_index().rename(columns={"Unique Matches": "count"})

        unique_matches_per_type_and_classifier = data_scan_resources_df2[['type', 'Classifier', 'Unique Matches']].groupby(by=['type','Classifier']).sum().reset_index().rename(columns={"Unique Matches": "count"})

        
        total_matches_per_region = data_scan_resources_df2[['_region','Total Matches']].groupby(by=['_region']).sum().reset_index().rename(columns={"Total Matches": "count"})

        total_matches_per_type = data_scan_resources_df2[['type','Total Matches']].groupby(by=['type']).sum().reset_index().rename(columns={"Total Matches": "count"})

        total_matches_per_classifier = data_scan_resources_df2[['Classifier','Total Matches']].groupby(by=['Classifier']).sum().reset_index().rename(columns={"Total Matches": "count"})

        #total_matches_per_severity = data_scan_resources_df2[['Severity','Total Matches']].groupby(by=['Severity']).sum().reset_index().rename(columns={"Total Matches": "count"})

        total_matches_per_type_and_severity = data_scan_resources_df2[['type', 'Severity', 'Total Matches']].groupby(by=['type','Severity']).sum().reset_index().rename(columns={"Total Matches": "count"})

        total_matches_per_type_and_classifier = data_scan_resources_df2[['type', 'Classifier', 'Total Matches']].groupby(by=['type','Classifier']).sum().reset_index().rename(columns={"Total Matches": "count"})

    
        st.set_page_config(layout="wide")

        style = """
            <style>
                .stMetric {
                    background-color: #EEEEEE;
                    border: 1px solid #DCDCDC;
                    padding: 10px;
                    border-radius: 10px; 
                }

                .stVegaLiteChart {
                    background-color: #EEEEEE;
                }
            </style>
        """

        st.markdown(style, unsafe_allow_html=True)

        st.subheader("General Dashboard")

        col1, col2, col3, col4 = st.columns(4)

        col1.metric("AWS", resources_per_cloud_platform.iloc[0]['count'])

        col2.metric("Production", resources_per_environment.iloc[0]['count'])

        col3.metric("Active", resources_per_status.iloc[0]['count'])

        col4.metric("Inactive", resources_per_status.iloc[1]['count'])

        st.write("#")

        # group 1
        st.subheader("Resources Summary")

        # group 1.1
        c = (alt.Chart(resources_per_creation_date)
              .encode(alt.X('_creationYYMM:O', axis=alt.Axis(labels=True, labelAngle=0)).timeUnit("yearmonth").title('Resource creation'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="lightgreyteal", reverse=False), alt.Text('count'), tooltip=["_creationYYMM:T", "count"])
              .properties(title='Number of resources per date')
         )

        st.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True) 

        st.write("#")

        # group 1.2
        col1, col2, col3 = st.columns([1,1,1])

        col1.markdown(
"""## Calling us-east-1

The analysis provides a breakdown of the resources identified across different regions and their types. As illustrated in the graphs on the right, more than 70% of the resources with data findings are located in the us-east-1 region, approximately 50% are categorized as buckets, while around 10% are classified as databases.
"""
        )

        c = (
            alt.Chart(resources_per_region)
            .encode(alt.X('_region', axis=alt.Axis(labels=True, labelAngle=0)).title('Resource region'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="lightgreyteal", reverse=False), alt.Text('count'), tooltip=["_region", "count"])
            .properties(title='Number of resources per region')
        )

        col2.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True)

        c = (alt.Chart(resources_per_type)
            .encode(alt.X('type', axis=alt.Axis(labels=True, labelAngle=0)).title('Resource type'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="lightgreyteal", reverse=False), alt.Text('count'), tooltip=["type", "count"])
            .properties(title='Number of resources per type')
        )

        col3.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True)

        st.write("#")

        # group 1.3
        col1, col2, col3 = st.columns([1,1,1])

        c = (alt.Chart(resources_per_severity)
            .encode(alt.X('Severity', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding severity'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="lightgreyteal", reverse=False), alt.Text('count'), tooltip=["Severity", "count"])
            .properties(title='Number of resources per severity')
        )
                
        col1.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True) 

        c = (alt.Chart(resources_per_category)
              .encode(alt.X('Category', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding classifier'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="lightgreyteal", reverse=False), alt.Text('count'), tooltip=["Category", "count"])
              .properties(title='Number of resources per classifier')
         )
                
        col2.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True) 

        col3.markdown(
"""## Should we worry about it

The analysis offers a comprehensive overview of the identified resources, highlighting their severity and classifications. As shown in the graphs on the left, 47 out of the 96 (50%) resources exhibit significant findings, categorized as high and critical data with Personally Identifiable, Financial, and Digital Identity information being in the top 5 categories.
"""
        )

        st.write("#")

        # group 2
        st.subheader("Data Findinds Summary")

        # group 2.1
        col1, col2, col3 = st.columns([1,1,1])

        col1.markdown(
"""## Houston, we have a problem

The analysis offers a detailed overview of the unique findings discovered across various regions and their classifications. As demonstrated in the graphs on the right, over 75% of the resources containing data findings are situated in the us-east-1 region. Additionally, approximately 75% of these resources are categorized as buckets, while around 18% are identified as databases. This reinforces our earlier observations that buckets and databases are the most critical components.
"""            
        )

        c = (alt.Chart(findings_per_region)
             .encode(alt.X('_region', axis=alt.Axis(labels=True, labelAngle=0)).title('Resource region'), alt.Y('count', axis=alt.Axis(labels=False)).title(''), alt.Color('count', legend=None).scale(scheme="lightorange", reverse=False), alt.Text('count'), tooltip=["_region", "count"])
             .properties(title='Number of findings per region')
        )
        
        col2.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True)

        c = (alt.Chart(findings_per_type)
             .encode(alt.X('type', axis=alt.Axis(labels=True, labelAngle=0)).title('Resource type'), alt.Y('count', axis=alt.Axis(labels=False)).title(''), alt.Color('count', legend=None).scale(scheme="lightorange", reverse=False), alt.Text('count'), tooltip=["type", "count"])
             .properties(title='Number of findings per type')
        )
        
        col3.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True)

        st.write("#")

        # group 2.2
        col1, col2 = st.columns([2,1])

        # c = (alt.Chart(findings_per_severity)
        #     .encode(alt.X('Severity', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding severity'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="lightorange", reverse=False), alt.Text('count'), tooltip=["Severity", "count"])
        #     .properties(title='Number of findings per severity')
        # )
                
        # col1.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True) 

        c = (alt.Chart(findings_per_classifier)
              .encode(alt.X('Classifier', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding classifier'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="lightorange", reverse=False), alt.Text('count'), tooltip=["Classifier", "count"])
              .properties(title='Number of findings per classifier')
         )
                
        col1.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True) 

        col2.markdown(
"""## Who you gonna call, today 

The analysis provides a thorough overview of the identified resources and their classifications. The graph on the left illustrates that key data points, including names, emails, phone numbers, addresses, gender, and transaction details, are prominently featured.
"""
        )

        st.write("#")

        # group 2.3
        col1, col2 = st.columns([1,2])
 
        col1.markdown(
"""## The most bang for the buck
"""            
        )

        c = (alt.Chart(findings_per_type_and_classifier)
               .encode(alt.X('Classifier', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding classifier'), alt.Y('type', axis=alt.Axis(labels=False, labelAngle=0)).title('Resource type'), alt.Color('count', legend=None).scale(scheme="orangered", reverse=False), alt.Text('count'), tooltip=["Classifier","type","count"])
               .properties(title='Number of findings per resource type and classifier')
        )

        col2.altair_chart(c.mark_rect(), use_container_width=True) 

        # group 2.4
        col1, col2 = st.columns([1,2])

        c = (alt.Chart(findings_per_type_and_severity)
               .encode(alt.X('Severity', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding severity'), alt.Y('type', axis=alt.Axis(labels=False, labelAngle=0)).title('Resource type'), alt.Color('count', legend=None).scale(scheme="orangered", reverse=False), alt.Text('count'), tooltip=["Severity","type","count"])
               .properties(title='Number of findings per resource type and severity')
        )

        col1.altair_chart((c.mark_rect() + c.mark_text(baseline="middle", fontWeight="bold").encode(color=alt.value("white"))), use_container_width=True) 


        col2.markdown(
"""
When spending time or money, it is essential to insist on getting the most bang for the buck.
"""
        )

        st.write("#")


        # # group 3
        # st.subheader("Unique Matches Summary")

        # # group 3.1
        # col1, col2, col3 = st.columns([1,1,1])

        # col1.markdown("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.")

        # c = (alt.Chart(unique_matches_per_region)
        #      .encode(alt.X('_region', axis=alt.Axis(labels=True, labelAngle=0)).title('Resource region'), alt.Y('count', axis=alt.Axis(labels=False)).title(''), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["_region", "count"])
        #      .properties(title='Number of unique matches per region')
        # )
        
        # col2.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True)

        # c = (alt.Chart(unique_matches_per_type)
        #      .encode(alt.X('type', axis=alt.Axis(labels=True, labelAngle=0)).title('Resource type'), alt.Y('count', axis=alt.Axis(labels=False)).title(''), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["type", "count"])
        #      .properties(title='Number of unique matches per type')
        # )
        
        # col3.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True)

        # st.write("#")

        # # group 3.2
        # col1, col2 = st.columns([2,1])

        # c = (alt.Chart(unique_matches_per_classifier)
        #       .encode(alt.X('Classifier', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding classifier'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["Classifier", "count"])
        #       .properties(title='Number of unique matches per classifier')
        #  )
                
        # col1.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True) 

        # # c = (alt.Chart(unique_matches_per_severity)
        # #     .encode(alt.X('Severity', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding severity'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["Severity", "count"])
        # #     .properties(title='Number of unique matches per severity')
        # # )
                
        # # col2.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True) 

        # col2.markdown("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.")

        # st.write("#")

        # # group 3.3
        # col1, col2 = st.columns([1,2])
 
        # # c = (alt.Chart(unique_matches_per_type_and_severity)
        # #        .encode(alt.X('Severity', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding severity'), alt.Y('type', axis=alt.Axis(labels=False, labelAngle=0)).title('Resource type'), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["Severity","type","count"])
        # #        .properties(title='Number of unique matches per resource type and severity')
        # # )

        # # l[0].altair_chart(c.mark_rect(), use_container_width=True) 

        # col1.markdown("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.")

        # c = (alt.Chart(unique_matches_per_type_and_classifier)
        #        .encode(alt.X('Classifier', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding classifier'), alt.Y('type', axis=alt.Axis(labels=False, labelAngle=0)).title('Resource type'), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["Classifier","type","count"])
        #        .properties(title='Number of unique matches per resource type and classifier')
        # )

        # col2.altair_chart(c.mark_rect(), use_container_width=True) 


        # # group 3.4
        # col1, col2 = st.columns([1,2])

        # c = (alt.Chart(unique_matches_per_type_and_severity)
        #        .encode(alt.X('Severity', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding severity'), alt.Y('type', axis=alt.Axis(labels=False, labelAngle=0)).title('Resource type'), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["Severity","type","count"])
        #        .properties(title='Number of unique matches per resource type and severity')
        # )

        # col1.altair_chart((c.mark_rect() + c.mark_text(baseline="middle", fontWeight="bold").encode(color=alt.value("white"))), use_container_width=True) 

        # col2.markdown("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.")

        # st.write("#")

        # group 4
        st.subheader("Total Matches Summary")

        # group 4.1
        col1, col2, col3 = st.columns([1,1,1])

        col1.markdown(
"""## In all its magnitude

The analysis provides a comprehensive overview of the total matches identified across different regions and their classifications. As illustrated in the graphs on the right, more than 80% of the resources containing data findings are located in the us-east-1 region. Furthermore, around 97% of these resources are classified as buckets, while merely 2% are recognized as databases. If you're looking to begin your work, start with your buckets..
"""            
        )

        c = (alt.Chart(total_matches_per_region)
             .encode(alt.X('_region', axis=alt.Axis(labels=True, labelAngle=0)).title('Resource region'), alt.Y('count', axis=alt.Axis(labels=False)).title(''), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["_region", "count"])
             .properties(title='Number of total matches per region')
        )
        
        col2.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True)

        c = (alt.Chart(total_matches_per_type)
             .encode(alt.X('type', axis=alt.Axis(labels=True, labelAngle=0)).title('Resource type'), alt.Y('count', axis=alt.Axis(labels=False)).title(''), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["type", "count"])
             .properties(title='Number of total matches per type')
        )
        
        col3.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True)

        st.write("#")

        # group 4.2
        col1, col2 = st.columns([2,1])

        c = (alt.Chart(total_matches_per_classifier)
              .encode(alt.X('Classifier', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding classifier'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["Classifier", "count"])
              .properties(title='Number of total matches per classifier')
         )
                
        col1.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True) 

        # c = (alt.Chart(total_matches_per_severity)
        #     .encode(alt.X('Severity', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding severity'), alt.Y('count', axis=alt.Axis(labels=False)).title('Resources'), alt.Color('count', legend=None).scale(scheme="blues", reverse=False), alt.Text('count'), tooltip=["Severity", "count"])
        #     .properties(title='Number of total matches per severity')
        # )
                
        # col2.altair_chart((c.mark_bar() + c.mark_text(align='center', dy=-10)).configure_axis(grid=False).configure_view(strokeWidth=0), use_container_width=True) 

        col2.markdown(
"""## Kill 'Em All
.. and get rid of mushrooms in your yard. 
"""        )

        st.write("#")

        # group 4.3
        col1, col2 = st.columns([1,2])
 
        col1.markdown(
"""## Allow me
"""        )

        c = (alt.Chart(total_matches_per_type_and_classifier)
               .encode(alt.X('Classifier', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding classifier'), alt.Y('type', axis=alt.Axis(labels=False, labelAngle=0)).title('Resource type'), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["Classifier","type","count"])
               .properties(title='Number of total matches per resource type and classifier')
        )

        col2.altair_chart(c.mark_rect(), use_container_width=True) 

        # group 4.4
        col1, col2 = st.columns([1,2])

        c = (alt.Chart(total_matches_per_type_and_severity)
               .encode(alt.X('Severity', axis=alt.Axis(labels=True, labelAngle=0)).title('Finding severity'), alt.Y('type', axis=alt.Axis(labels=False, labelAngle=0)).title('Resource type'), alt.Color('count', legend=None).scale(scheme="reds", reverse=False), alt.Text('count'), tooltip=["Severity","type","count"])
               .properties(title='Number of total matches per resource type and severity')
        )

        col1.altair_chart((c.mark_rect() + c.mark_text(baseline="middle", fontWeight="bold").encode(color=alt.value("white"))), use_container_width=True) 

        col2.markdown(
"""
Prioritize addressing the critical findings first, followed by the high findings.
"""            
        )

        st.write("#")

        # group 5
        with st.expander("See details"):
            columns=['id','name','type','_externalId','_nativeType','_kind','_cloudPlatform','_subscriptionExternalId','_region','__environments','_isManaged','_isPaaS','_creationDate','Finding ID','Category','Classifier','Unique Matches','Total Matches','Severity','Examples Count','Finding Examples']

            st.dataframe(data_scan_resources_df2[columns])
        

        logging.getLogger().debug('done')

    except Exception as error:
        raise Exception('Error: %s', error)




# %%
if __name__ == '__main__':
    main()    


