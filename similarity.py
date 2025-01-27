
import json
import logging
import requests
import math

from requests import Request, Session

from requests.auth import HTTPBasicAuth

import pandas as pd
import numpy as np

import altair as alt

import streamlit as st

alt.data_transformers.enable("vegafusion")



def get_config():
    logging.getLogger().debug("get config")

    with open('config.json', "r") as f:
        config = json.load(f)

    return config


def get_collibra(config):
    logging.getLogger().debug("get collibra")

    collibra = {}

    collibra["host"] = f"https://{config['collibra_host']}"

    collibra["username"] = config['collibra_username']

    collibra["password"] = config['collibra_password']

    collibra["endpoint"] = f"{collibra['host']}{config['collibra_api_endpoint']}"

    collibra["session"] = Session()

    collibra.get("session").auth = HTTPBasicAuth(collibra.get("username"), collibra.get("password"))

    return collibra


def get_response(method, url, data, limit, session):
    logging.getLogger().debug("send request")

    offset = 0
    
    results = []

    while True:
        request = Request(
            method=method,
            url=f"{url}&offset={offset}&limit={limit}",
            headers = {'accept': 'application/json', 'Content-Type': 'application/json'},
            data = data
        )

        request = session.prepare_request(request)
   
        response = session.send(request)

        if response.status_code != requests.codes.ok: raise Exception(f'Error: {response.text}') 

        if not response.json()['results']: break

        results = results + response.json()['results']

        offset+=limit

    return results


def get_names_per_database(names_per_database, names):
    dbname = f"{names[0]}>{names[1]}"

    if dbname not in names_per_database:
        names_per_database[dbname] = []

    if names[2].upper() not in names_per_database[dbname]:
        names_per_database[dbname].append(names[2].upper())

    if names[3].upper() not in names_per_database[dbname]:
        names_per_database[dbname].append(names[3].upper())

    return names_per_database



def jaccard_similarity(x, y):
  intersection_cardinality = len(set.intersection(*[set(x), set(y)]))

  union_cardinality = len(set.union(*[set(x), set(y)]))

  #return round(intersection_cardinality/float(union_cardinality) *100)
  return intersection_cardinality/float(union_cardinality)


def update_matrix(matrix, x, y, v):
    matrix[x][y] = v



def main():
    config = get_config()

    collibra = get_collibra(config)

    response = get_response("GET", f"{collibra.get('endpoint')}/assets?typePublicIds=Table&typeInheritance=true&sortField=NAME&sortOrder=ASC", None, 1000, collibra.get("session"))

    all_table_names = list(map(lambda x: f"{x['domain']['id']}>{x['name']}".split('>'), filter(lambda x: x['name'].count('>')==3 , response))) # only tables complying with the new edge naming convention c>d>s>t

    names_per_database = {}

    _=list(map(lambda x: get_names_per_database(names_per_database, x), all_table_names)) 

    # build empty matrix 
    n = len(names_per_database.keys())
    arr = np.array([None] *n*n, dtype=float)

    matrix = arr.reshape(n, n)

    # update matrix with similarity
    l=list(names_per_database.keys())
    
    _=[update_matrix(matrix, i, ii, jaccard_similarity(l[i],l[ii])) for i, n in enumerate(matrix) for ii, nn in enumerate(matrix[i])]

    similarity_df = pd.DataFrame(matrix, columns=list(names_per_database.keys()), index=list(names_per_database.keys()))
    
    similarity_df_melted = pd.melt(similarity_df.reset_index(), id_vars=['index'], value_vars=similarity_df.columns)


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

    st.markdown(style, unsafe_allow_html=True)

    st.subheader("Jaccard index")

    st.write("Jaccard index is a statistic used for gauging the similarity and diversity of sample sets. It is defined in general taking the ratio of two sizes, the intersection size divided by the union size, also called intersection over union (IoU).")

    col1, col2 = st.columns([1,2])

    with col1:
        with st.form("my_form"):
            database_A = st.selectbox("Select the database you want to check", [""] + list(names_per_database.keys()))

            database_B = st.selectbox("Choose the database to query against", [""] + list(names_per_database.keys()))

            st.write("#")
            
            number = st.slider(label="Select the similarity index you want to start query with", min_value=0.0, max_value=1.0, value=0.6, format="%0.2f")

            submitted = st.form_submit_button("Submit")

            if not submitted:
                st.stop()
        

    with col2:
        mask = f'Similarity > {number}' 

        if database_A != "": mask = f"{mask} and Domain_A == '{database_A.split('>')[0]}' and Database_A == '{database_A.split('>')[1]}'"

        if database_B != "": mask = f"{mask} and Domain_B == '{database_B.split('>')[0]}' and Database_B == '{database_B.split('>')[1]}'"

        similarity_df_melted_transpose = pd.DataFrame(similarity_df_melted.apply(lambda x: [x['index'].split('>')[0], x['index'].split('>')[1], x['variable'].split('>')[0], x['variable'].split('>')[1], x['value']], axis=1).to_dict(), index=["Domain_A","Database_A","Domain_B","Database_B","Similarity"]).transpose()
        
        st.dataframe(similarity_df_melted_transpose.query(mask), hide_index=True)

    st.write("#")

    mask = f'value > {number}'  
    if database_A != "": mask = f"{mask} and index == '{database_A}'"

    if database_B != "": mask = f"{mask} and variable == '{database_B}'"

    c = (alt.Chart(similarity_df_melted.query(mask))
        .encode(alt.X('variable', axis=alt.Axis(labels=False, labelAngle=0)).title('Database'), alt.Y('index', axis=alt.Axis(labels=False, labelAngle=0)).title('Database'), alt.Color('value', legend=None).scale(scheme="orangered", reverse=False), alt.Text('value'), tooltip=["variable","index","value"])
        .properties(title='Check it out')
    )

    st.altair_chart(c.mark_rect(), use_container_width=True)             


if __name__ == '__main__':
    st.set_page_config(layout="wide")

    main() 


# done









