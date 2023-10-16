CREATE APPLICATION ROLE app_public;
create schema customer;

-- main table for audience data, will be shared back to provider
CREATE OR REPLACE TABLE customer.custom_audience
(
    audience_id varchar,
    audience_name varchar,
    provider_id varchar
)
cluster by (audience_id);

-- metadata for audiences for app, not shared
CREATE OR REPLACE TABLE customer.audience_metadata
(
    audience_id varchar,
    audience_name varchar,
    audience_type varchar,
    audience_count number(11,0),
    update_frequency varchar,
    sql_text varchar,
    last_updated timestamp_tz,
    deleted number(1,0)
)
cluster by (audience_id);

-- view of metadata, will be shared back to provider
CREATE OR REPLACE VIEW customer.vw_audience_metadata
AS
select
    audience_id,
    audience_name,
    audience_type,
    audience_count,
    update_frequency,
    last_updated
from customer.audience_metadata;

GRANT USAGE on SCHEMA customer to APPLICATION ROLE app_public;
GRANT SELECT, INSERT, UPDATE, DELETE on table customer.custom_audience to APPLICATION ROLE app_public;
GRANT SELECT, INSERT, UPDATE, DELETE on table customer.audience_metadata to APPLICATION ROLE app_public;
GRANT SELECT on view customer.vw_audience_metadata to APPLICATION ROLE app_public;

CREATE OR ALTER VERSIONED SCHEMA code_schema;
GRANT USAGE ON SCHEMA code_schema TO APPLICATION ROLE app_public;

CREATE OR REPLACE SECURE FUNCTION code_schema.get_ids_email(advertiser_id varchar)
-- secure function to match 1PD tables
-- TODO: refactor this to return advertiser ID, or create a separate function
    RETURNS TABLE(provider_id VARCHAR)
    AS
    $$
        select c_customer_id
        from provider.customer pc
        where c_email_address = advertiser_id
    $$;

CREATE OR REPLACE SECURE PROCEDURE code_schema.get_match_rate(advertiser_customer_table string, col string, col_type string)
-- calls secure function to return overlap %
-- TODO: implement phone number, ip adddress, rampID
    RETURNS FLOAT
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.8
    HANDLER = 'match'
    PACKAGES = ('snowflake-snowpark-python')
AS
$$

def match(session, advertiser_customer_table, col, col_type):
    adv_count = session.table(advertiser_customer_table).count()

    sql_text = "with adv_table as (select * from " + advertiser_customer_table + ") select provider_id from adv_table inner join table(code_schema.get_ids_email(" + col + "))"

    if col_type == "email":
        df = session.sql(sql_text)
    elif col_type == "phone":
        return 1.1
    else:
        return 2.2

    result = df.count()

    return round((result/adv_count)*100,2)

$$;

CREATE OR REPLACE SECURE PROCEDURE code_schema.update_audience(audience_id string, audience_name string, updated_frequency string, advertiser_sql string, col string)
-- takes the inputs from streamlit and creates/updates an audiencs
-- update_audience is used when creating a new audience or for updating an existing audience
-- TODO: error handling, add phone number, ip address, and rampID matching
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.8
    HANDLER = 'create'
    PACKAGES = ('snowflake-snowpark-python')
AS
$$
import datetime

def create(session, audience_id, audience_name, update_frequency, advertiser_sql, col):
    audience_type = "unspecified"

    if audience_id[0] == "A":
        audience_type = "audience"
    elif audience_id[0] == "S":
        audience_type = "suppression"
    elif audience_id[0] == "L":
        audience_type = "lookalike"

    if ";" in advertiser_sql:
        return "error - unsuccessful: semi-colons not allowed in SQL text"

    sql_text = "with adv_table as (" + advertiser_sql + ") select \'" + audience_id + "\' as audience_id, \'" + audience_name + "\' as audience_name, provider_id from adv_table inner join table(code_schema.get_ids_email(" + col + "))"

    new_data = session.sql(sql_text)
    row_count = new_data.count()

    if (audience_type == "A" or audience_type == "S") and row_count < 10000:
        return "error - unsuccessful: audience or suppression count must be at least 10,000 customers"

    delete_old = session.sql("delete from customer.custom_audience where audience_id = '" + audience_id + "'").collect()
    write_new = new_data.write.mode("append").save_as_table("customer.custom_audience")

    delete_metadata = session.sql("delete from customer.audience_metadata where audience_id = '" + audience_id + "'").collect()
    new_metadata = session.create_dataframe([[audience_id,audience_name,audience_type,row_count,update_frequency,advertiser_sql,datetime.datetime.now(),0]], schema=["audience_id", "audience_name","audience_type","audience_count","sql_text","last_updated","deleted"])
    write_metadata = new_metadata.write.mode("append").save_as_table("customer.audience_metadata")

    return "created audience " + audience_name + " with count " + str(row_count)

$$;

-- allow app to run functions
GRANT USAGE ON FUNCTION code_schema.get_ids_email(varchar) TO APPLICATION ROLE app_public;
GRANT USAGE ON PROCEDURE code_schema.get_match_rate(string, string, string) TO APPLICATION ROLE app_public;
GRANT USAGE ON PROCEDURE code_schema.update_audience(string, string, string, string, string) TO APPLICATION ROLE app_public;

CREATE STREAMLIT code_schema.sam_app
  FROM '/streamlit'
  MAIN_FILE = '/sam_app.py'
;

GRANT USAGE ON STREAMLIT code_schema.sam_app TO APPLICATION ROLE app_public;
