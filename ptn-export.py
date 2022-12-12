import json
import requests
import boto3
import pandas as pd
import streamlit as st
import hashlib

def main():

    ### Config lookup 
    config_lookup = {
        "staging": {
            "gql": st.secrets["STAGING_GQL"],
            "aws_id": st.secrets["AWS_ID"],
            "aws_secret": st.secrets["AWS_SECRET"],
            "client_id": st.secrets["STAGING_CLIENT_ID"],
            "region_name": st.secrets["STAGING_REGION"]
            },
        "prod": {
            "gql": st.secrets["PROD_GQL"],
            "aws_id": st.secrets["AWS_ID"],
            "aws_secret": st.secrets["AWS_SECRET"],
            "client_id": st.secrets["PROD_CLIENT_ID"],
            "region_name": st.secrets["PROD_REGION"]
            }
        }

    ### pw hasher
    def sha512hash(password):
        hashed_password = hashlib.sha512(password.encode('utf-8'))
        return hashed_password.hexdigest()

    ### Title
    st.title("PtN Data Export")

    ### Section 1: Upload source file
    with st.container() as container_1:
        st.header("Step 1: Enter Config Details")
        with st.form(key='run_export'):
            active_config = st.selectbox("Select config", config_lookup.keys(), 0)
            basic_pw = st.text_input(label='Service Password', type='password')
            user_name = st.text_input(label='username')
            user_pw = st.text_input(label='password', type='password')
            submit_button = st.form_submit_button(label='Run Export')

    ### Execute
    if submit_button:
        st.session_state['last_update_reference'] = ''
        st.session_state['follow_up_query'] = True
        st.session_state['query_counter'] = 0

        if sha512hash(basic_pw) == st.secrets["SERVICE_PASSWORD_HASH"]:
        # print(active_config)
            try:
                st.success(f"Configuration Accepted\n\n")
                st.text("")
                st.markdown("***")

                ### Set config based on active_config
                _aws_access_key_id = config_lookup[active_config]['aws_id']
                _aws_secret_access_key = config_lookup[active_config]['aws_secret']
                _app_username = user_name
                _app_pw = user_pw
                _app_client = config_lookup[active_config]['client_id']
                _app_region = config_lookup[active_config]['region_name']
                endpoint = config_lookup[active_config]['gql']
                # print(_aws_access_key_id, _aws_secret_access_key,_app_client,endpoint)

                ### Client + Query
                cognito = boto3.client('cognito-idp', aws_access_key_id=_aws_access_key_id, aws_secret_access_key=_aws_secret_access_key, region_name=_app_region)

                while 'idtoken' not in locals():
                    st.text("processing...")    
                    idtoken = cognito.initiate_auth(AuthFlow='USER_PASSWORD_AUTH', 
                                    AuthParameters={'USERNAME': _app_username,'PASSWORD': _app_pw},
                                    ClientId=_app_client)['AuthenticationResult']['IdToken'] 
                
                st.header("idtoken")
                st.code(idtoken)
                    
                # print(idtoken)
                # print("starting query")
                headers = {'Authorization': idtoken, 'Content-Type':'application/json'}
                combined_list = []

                def run_and_parse_request(last_update):
                    prod_query = "query GetAnswersSheets{my{id activities{id dateStart dateEnd timeStart timeEnd state answersSheets{id state completedDate completedTime encodedResults questionnaire{id version name}}}}}"

                    staging_query = "query GetAnswersSheets{my{id activities(pagination: {limit: 30 orderBy: \"lastUpdateReference\" afterReference: \"%s\"}){id dateStart dateEnd timeStart timeEnd lastUpdateReference state answersSheets{id state completedDate completedTime encodedResults questionnaire{id version name}}}}}" % (last_update)

                    if active_config == "staging":
                        query = staging_query
                    elif active_config == "prod":
                        query = staging_query
                    else :
                        query = prod_query

                    r = requests.post(endpoint, json={'query': query}, headers=headers)
                    # print(r.content)
                    # print(r.status_code)
                    
                    # print(r.url, " ", r.body)
                    if r.status_code == 200:
                        activities = json.loads(r.content)['data']['my']['activities']
                        # print (len(activities))
                        if len(activities) > 0:
                            st.session_state['last_update_reference'] = activities[-1]['lastUpdateReference']

                            ### Aggregate Data
                            for a in activities: 
                                if len(a['answersSheets']) > 0:
                                    for ans in a['answersSheets']:
                                        out = {}
                                        out['activityId'] = a['id']
                                        out['activity.dateStart'] = a['dateStart']
                                        out['activity.dateEnd'] = a['dateEnd']
                                        out['activity.timeStart'] = a['timeStart']
                                        out['activity.timeEnd'] = a['timeEnd']
                                        out['activity.state'] = a['state']
                                        
                                        ### New in Staging
                                        if active_config == "staging":
                                            out['activity.lastUpdateReference'] = a['lastUpdateReference']
                                        elif active_config == "prod":
                                            out['activity.lastUpdateReference'] = a['lastUpdateReference']
                                        else:
                                            pass

                                        ### Answersheet Data
                                        out['asId'] = ans['id']
                                        out['as.completedDate'] = ans['completedDate']
                                        out['as.completedTime'] = ans['completedTime']
                                        out['as.state'] = ans['state']
                                        out['as.questionnaire'] = ans['questionnaire']['name']
                                        combined_list.append(out)
                                else:
                                    pass
                        else:
                            st.session_state['follow_up_query'] = False
                            st.success(f"Activities Exhausted: {r.status_code}")
                    else:
                        st.session_state['follow_up_query'] = False
                        st.error(f"HTTP Status Code: {r.status_code}")

                ### Run Loop
                while st.session_state['follow_up_query'] == True:
                    # print("start loop")
                    # print(st.session_state['last_update_reference'])
                    # print(st.session_state['follow_up_query'])
                    # print(st.session_state['query_counter'])
                    run_and_parse_request(st.session_state['last_update_reference'])
                    st.session_state['query_counter'] += 1
                    # print("before restart loop")
                    # print(st.session_state['last_update_reference'])
                    # print(st.session_state['follow_up_query'])
                    # print(st.session_state['query_counter'])
                    # print(len(combined_list))
                else:
                    st.text(f"Done! {st.session_state['query_counter']} Query Loops Complete")
                    # print(combined_list)
                
                ### Output Functions
                def output_csv(dataframe, has_header=True): 
                    """Takes a dataframe and has header argument to produce csv file to buffer for download button to use"""
                    return dataframe.to_csv(sep=",", index=False, header=has_header).encode('utf-8')
                
                def output_json(json_object): 
                    """Takes a dataframe and has header argument to produce csv file to buffer for download button to use"""
                    return json.dumps(json_object, ensure_ascii=False, indent=4)
                
                def download_success():
                    """Success message on download"""
                    st.success("Download Successful!")
                    st.empty()

                ### Download Files
                df = pd.DataFrame(combined_list)
                out_csv = output_csv(df, True)
                out_json = output_json(combined_list)

                st.header("\n\nStep 2: Download Files")
                
                st.download_button(
                    label = "Download CSV",
                    data = out_csv,
                    file_name = f"ptn-export-{user_name}.csv",
                    mime='text/csv', 
                    on_click=download_success)
                
                st.download_button(
                    label = "Download JSON",
                    data = out_json,
                    file_name = f"ptn-export-{user_name}.json",
                    mime='text/plain', 
                    on_click=download_success)
                
            except AttributeError:
                st.error("Please select a file before continuing")

        else:
            st.error("You're not allowed access")


if __name__ == '__main__':
    main()




    
    
    
