# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title("Secure Audience Manager :snowflake: :bar_chart: :closed_lock_with_key:")

# Get the current credentials
session = get_active_session()

# Get current app DB to remove from DB list
current_db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]

# Functions to retrieve databases, schemas, tables, and columns, based on selections of each
# remove the current app, the app package, and the provider's database that houses the private (and inaccessible) customer data
def get_databases():
    databases = session.sql("SHOW DATABASES").collect()
    database_list = [row[1] for row in databases]
    database_list.remove(current_db)
    database_list.remove("SAM_APP")
    database_list.remove("RMN_DEV")
    return database_list

def get_schemas(database):
    schemas = session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect()
    return [row[1] for row in schemas]

def get_tables(database, schema):
    tables = session.sql(f"SHOW TABLES IN SCHEMA {database}.{schema}").collect()
    return [row[1] for row in tables]

def get_columns(database,schema,table):
    columns = session.sql(f"DESC TABLE {database}.{schema}.{table}").collect()
    return [row[0] for row in columns]

#create dynamic dropdowns for database, schema, table, as well as the main ID column, and the column type (currently only email and phone supported)
selected_database = st.selectbox("Select a Database", get_databases(), key="database")
if not selected_database:
    st.write("You'll need to grant read access of your data to the app - execute the commands below with your database, schema, and table and then refresh this page. <APP_NAME> should be " + current_db)
    code = '''
    GRANT USAGE ON DATABASE <DATABASE> TO APPLICATION <APP_NAME>;
    GRANT USAGE ON SCHEMA <DATABASE.SCHEMA> TO APPLICATION <APP_NAME>;
    GRANT SELECT ON TABLE <DATABASE.SCHEMA.TABLE> TO APPLICATION <APP_NAME>;
    '''
    st.code(code, language='sql')

selected_schema = st.selectbox("Select a Schema", get_schemas(selected_database), key="schema")
selected_table = st.selectbox("Select a Table", get_tables(selected_database, selected_schema), key="table")
id_col = st.selectbox("Select your primary identifier", get_columns(selected_database,selected_schema,selected_table), key="column")
id_type = st.selectbox("Select your ID type", ["email", "phone", "ip address", "RampID"], key = "id_type")


#buttons for match rate and audiences, each button calls a stored procedure
match_button, global_audiences = st.columns(2)

#return overall match rate
if match_button.button("Run Match Overlap"):
    match_result = session.call("code_schema.get_match_rate",selected_database+'.'+selected_schema+'.'+selected_table, id_col, id_type)
    st.write("Match rate is " + str(match_result) + "%!")

#TODO: stored proc to return table of relevant audiences with a minimum of 10k customers in each
if global_audiences.button("Display standard audiences"):
    st.write("Still have to implement this!")

    
# Custom audience section
st.subheader("Create Custom Audience")

# specify audience type
audience_type = st.selectbox("Select Audience Type", ["audience","suppression list","lookalike"], key="audience_type")

# lookalike options
if audience_type == "lookalike":
    lookalike_type = st.selectbox("Confidence or audience size?", ["size","confidence level"], key="lookalike_type")

    if lookalike_type == "size":
        lookalike_count = st.slider(
        "Requested lookalike audience count",
        min_value=10000,
        max_value=99000,
        value=60,
        help="Number of unknown prospects to target",
        )
    else:
        lookalike_count = st.slider(
        "Requested lookalike confidence interval",
        min_value=0,
        max_value=99,
        value=1,
        help="Confidence level needed",
        )

# create input fields for name, sql, and frequency
audience_name = st.text_input("Enter the audience name")
advertiser_sql = st.text_area("SQL Input")

if audience_type != "lookalike":
    update_frequency = st.selectbox("Select Update Frequency", ["manual","monthly","hourly","daily"], key="update_frequency")
else:
    update_frequency = "manual"
    st.write("Manual frequency required for lookalike")

# TODO: 
# write tasks as part of app setup.sql
# create a way to resume or stop tasks when audiences are created or when the last audience for a cadence is "deleted"

# function and button to create audience 
def create():
    audience_id = str(hash(audience_name))[-9:]
    
    if audience_type == "audience" or audience_type == "suppression":
        result = session.call("code_schema.update_audience",audience_type[0].upper()+audience_id,audience_name,update_frequency,advertiser_sql,id_col)
    else:
        # TODO: create lookalike udtf
        result = 0
    return result

if st.button("Create Audience"):
    st.write(create())
    st.experimental_rerun()

# Query metadata table and convert it into a Pandas dataframe (streamlit requires pandas for display)
created_audiences = session.table("customer.audience_metadata").to_pandas()

# show dataframe
st.subheader("Created Audiences")
st.dataframe(created_audiences, use_container_width=True)


