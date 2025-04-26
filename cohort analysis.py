#cohort analysis 

#assumptions/process
# 14 days ince install, counting day of dsi=0 as day 0.
# for each isntall date we have in silver_layer app_events_cleaned table, the uid's of people who installed app
#on that install_date were 
#connecting to google cloud big query
import os #- necessary when dealing with opearting systems
from google.cloud import bigquery
import pandas as pd
from datetime import timedelta
from google.cloud.bigquery import SchemaField, Table
from google.cloud.exceptions import GoogleCloudError, NotFound

relative_path = "../Boardible/private_key.json"  # Example relative path
full_path = os.path.abspath(relative_path)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = full_path  #this can't be a table name 

client = bigquery.Client()

sql="""SELECT
  session_date,
  install_date,
  uid,
  dsi,
  event_name,
  event_params.key AS param_key,
  event_params.value.string_value AS string_value,
  event_params.value.int_value AS param_value_int
FROM
  `boardible-e039c.silver_layer.app_events_cleaned`,
  UNNEST(event_params) AS event_params
WHERE
 event_name = 'EditAvatar'
  AND event_params.key = 'NewValue'"""

df = client.query(sql).to_dataframe()
df

session_date_new = pd.to_datetime(df["session_date"])
install_date_new = pd.to_datetime(df["install_date"])

df_proper = df.copy()
df_proper["session_date"] =session_date_new
df_proper["install_date"]= install_date_new
df_proper.drop(["param_value_int", "event_name", "param_key", "param_value_int"], axis=1)

#finding all uid with dsi =0 on all install dates
sql = """ SELECT  install_date, uid --COUNT (DISTINCT uid)--, dsi --event_name, uid, event_params
    FROM `boardible-e039c.silver_layer.app_events_cleaned`
    WHERE dsi = 0
    GROUP BY install_date, uid
    ORDER BY install_date """

dsi_zero = client.query(sql).to_dataframe()
dsi_zero

#all install dates
sql = """SELECT  DISTINCT install_date
    FROM `boardible-e039c.silver_layer.app_events_cleaned`"""
all_install_dates_to_check = client.query(sql).to_dataframe()
all_install_dates_to_check

#extracting uid's that installed app for each install date
uid_on_day0 = []
for date in all_install_dates_to_check["install_date"]:
    uids = dsi_zero.loc[dsi_zero["install_date"] == date, "uid"]
    uid_on_day0.append(uids.tolist())

#the uid's that did generate an avatar edit customization within 14 days of install date 
#consdiered dsi =0 to be day 0 and checked the next 14 days since dsi =0 
#creating a dataframe
semi_final_frequency_df=pd.DataFrame()
semi_final_frequency_df["install_date"]=0
semi_final_frequency_df["uid"]=0

for date in range(len(all_install_dates_to_check["install_date"])): #
    dates_to_check = [all_install_dates_to_check["install_date"][date]+pd.Timedelta(days=i) for i in range(1,15) ]
    for uid in uid_on_day0[date]:
        if df_proper.loc[ (df_proper["uid"] == uid)  & (df_proper["session_date"].isin(dates_to_check)) ,:].empty == False:
            semi_final_frequency_df.loc[date, "install_date" ] =all_install_dates_to_check["install_date"][date]
            semi_final_frequency_df.loc[date, "uid" ] =uid

#need to convert date type to string such that the json file handles it
semi_final_frequency_df['install_date'] = semi_final_frequency_df['install_date'].astype(str) 

#uplodaing to big query
dataset_id = "gold_layer"
table_id = "edit_avatar_cohort_14_day_since_install"
temporary_table_id = "temporary_edit_avatar_cohort_14_day_since_install"

schema = [
    SchemaField("install_date", "DATE"),
    SchemaField("uid", "STRING"),
]

table_ref = client.dataset(dataset_id).table(table_id)

try:
    client.get_table(table_ref) #this creates the table but doesn't populate it 
    print(f"Table {dataset_id}.{table_id} already exists.")
except NotFound:
    table = Table(table_ref, schema=schema)
    client.create_table(table)
    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

temporary_table_ref = client.dataset(dataset_id).table(temporary_table_id)

try:
    client.get_table(temporary_table_ref)
    print(f"Temporary table {dataset_id}.{temporary_table_id} already exists.")
except NotFound:
    temp_table = Table(temporary_table_ref, schema=schema)
    client.create_table(temp_table)
    print(
        f"Created temporary table {temp_table.project}.{temp_table.dataset_id}.{temp_table.table_id}"
    )

json_data = semi_final_frequency_df.to_dict(orient='records') #this line job = client.load_table_from_json only takes in a json