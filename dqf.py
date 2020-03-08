
################## USAGE ##################
/usr/lib/spark/bin/spark-submit --num-executors 6 --executor-cores 5 --executor-memory 24g --driver-memory 5g --conf spark.yarn.executor.memoryOverhead=6144 s3://bex-analytics-softwares/builds/ede-arm-clv.git/39f3aeb6b790907fea482af3ccd4b9ca2f6c66a9-EDE-25573-update-mcltv-blended-spend/scripts/DQ/POC/dq_clv_clicks_dqfr.py 2019-11-13 test 'v-dhoncharuk@expedia.com,srkatta@expedia.com,j4b4a7c1e7j0u0m5@expedia.slack.com' 'embidetl' 'embidetl' 'MCLTV' 'mcltv_click_counts' 'edemeta-dq-test.cmguqnu4wehw.us-west-2.rds.amazonaws.com' 'https://ede-django-dqf-api.us-west-2.test.expedia.com/dqf_api/ReceiptLog/'
#################################################

# loading libs
import sys
import datetime
import MySQLdb
import os
import pandas
import boto3
import requests
import time
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, count
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from io import StringIO

####################################################################
# Checking and assigning params
####################################################################
team_name='DP2BRAVO'
start_time = datetime.datetime.now().replace(microsecond=0)
process_day = str(sys.argv[1])
environment=str(sys.argv[2])
mysql_login=str(sys.argv[4])
mysql_pass=str(sys.argv[5])
partitions = 200
bucket='big-data-analytics-'+environment
path_to_files = 'tmp/dq_checks'
path_folder=os.path.join('s3://',bucket, path_to_files)
suite_name=str(sys.argv[6])
case_name=str(sys.argv[7])
host = str(sys.argv[8])
api_endpoint = str(sys.argv[9])
db_name = 'dqmetastore'
runid=time.time()
sender="s-qubole-edemeta-dq-framework-{0}@expedia.com".format(environment)
email=str(sys.argv[3])


#####################################################################
# Functions definition
#####################################################################

#Function to write df to csv file on S3
def df_to_csv(dataframes, bucket, path_to_files):
    filename_dict={}
    for key, value in dataframes.items():
        filename_dict[key]=key+'_result.csv'
        csv_buffer = StringIO()
        value.toPandas().to_csv(csv_buffer, index=False)

        s3_key = os.path.join(path_to_files,filename_dict[key])
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, s3_key).put(Body=csv_buffer.getvalue())
        
    return filename_dict

# Function to fetch commands from DQF metastore
def fetch_commands(suite, case_name):
    dq_commands={}
    db=MySQLdb.connect(user=mysql_login,
                       passwd=mysql_pass,
                       host=host,
                       db=db_name)
    comm_cur = db.cursor()
    query = ("select command_name, command_value from test_case_command where suite_name='{0}' and case_name='{1}';".format(suite_name, case_name))

    comm_cur.execute(query)
    
    for (command_name, command_value) in comm_cur:
        dq_commands[command_name]=command_value
    
    comm_cur.close()
    db.close()
    return dq_commands
  
# Function to create df
def df_creation(commands):
    df_dict={}
    for key, value in dq_commands.items():
        value=value.replace('$DATE$', process_day)
        print(key)
        print(value)
        df_dict[key]=spark.sql(value)
        df_dict[key].show()
    return df_dict

#Function to evaluate results
def eval_results(dataframe):
    results_dict={}
    for key, value in dataframe.items():
        dq_check=value.filter(col("result")=='FAIL').count()
        if dq_check>0:
          results_dict[key]='FAIL'
        else:
          results_dict[key]='PASS'
    return  results_dict

#Function to generate HTML for message body
def html_gen(final_status, check_statuses, date, runid, start_time, end_time, case_name):
    colspan=len(check_statuses)
    
    if final_status=='FAIL':
        f_color='red'
    else:
        f_color='green'
        
    header="""<html>
                <head></head>
                <body>
                <p>Validation:  DQ for {0} table has been completed</p>
                <p> DQ local date = {1}</p>
                <p> DQ runid = {2}</p>
                <p>DQ process started at: {3}, process ended at: {4}</p>
                <p>Result of the DQ run  - {5}</p>
                <p>Plese look for result of failed  DQ  checks in the attached files</p></n>
                <table border="1" class="dataframe">
                    <tr>
                        <th colspan="{6}" bgcolor="{7}">{8}</th>
                    </tr>
                <tr style="text-align: center;">""".format(case_name, date, runid, start_time, end_time, final_status, colspan, f_color, final_status)
    
    checks_names=""" """
    
    checks_statuses=""" """
    
    for key, value in check_statuses.items():
        checks_names=checks_names+'<th>'+key+'</th>'
        if value=='PASS':
            checks_statuses=checks_statuses+'<td bgcolor="green">'+value+'</td>'
        else:
            checks_statuses=checks_statuses+'<td bgcolor="red">'+value+'</td>'
    
    footer="""</tr>
              </tbody>
              </table>
              </body>
              </html>"""
    
    html_string=header+checks_names+'</tr><tr>'+checks_statuses+footer
    return html_string

#Function to send raw email with attachments

def send_email_att(sender, email, attachments, subject, body_text, body_html):
    # This address must be verified with Amazon SES.
    
    RECIPIENT = email
    MSG_RECIPIENT = email.split(',')

    AWS_REGION = "us-east-1"

    # The character encoding for the email.
    CHARSET = "utf-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses',region_name=AWS_REGION)

    # Create a multipart/mixed parent container.
    msg = MIMEMultipart('mixed')
    # Add subject, from and to lines.
    msg['Subject'] = subject 
    msg['From'] = sender 
    msg['To'] = RECIPIENT

    # Create a multipart/alternative child container.
    msg_body = MIMEMultipart('alternative')

    # Encode the text and HTML content and set the character encoding. This step is
    # necessary if you're sending a message with characters outside the ASCII range.
    textpart = MIMEText(body_text.encode(CHARSET), 'plain', CHARSET)
    htmlpart = MIMEText(body_html.encode(CHARSET), 'html', CHARSET)

    # Add the text and HTML parts to the child container.
    msg_body.attach(textpart)
    msg_body.attach(htmlpart)

    # Attach the multipart/alternative child container to the multipart/mixed
    # parent container.
    msg.attach(msg_body)

    # Add the attachment to the parent container.
    for key, value in attachments.items():
        msg.attach(value)
    
    try:
        #Provide the contents of the email.
        response = client.send_raw_email(
            Source=sender,
            Destinations=MSG_RECIPIENT,
            RawMessage={
                'Data':msg.as_string(),
            },
        )
    # Display an error if something goes wrong.    
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])
    
#Function for loging the results to metastore
def logging_results(result, team_name, suite_name, case_name, runid, start_time, end_time):
    for key, value in result.items():
        receipt_payload = {
                                "team_name": team_name,
                                "suite_name": suite_name,
                                "case_name": case_name,
                                "command_name": key,
                                "command_run_id": runid,
                                "run_start": start_time,
                                "run_end": end_time,
                                "result": value,
                                "run_status": "done",
                                "rule_name": key
                            }
             
        response_post = requests.post(api_endpoint,data=receipt_payload)

#Function to split email string for general mailing and slack alerts
def email_split(email):
    email_split=email.split(',')
    email_out=''
    email_slack=''
    for key in email_split:
        if key.find('slack') is not -1:
            email_slack=email_slack+key+','
        else:
            email_out=email_out+key+','
    email_slack=email_slack[:-1]
    email_out=email_out[:-1]
    
    return email_out, email_slack

        
############################################
#Removing old status files from s3
############################################
#os.system("hadoop fs -rm -r 's3://{0}/{1}/dq*'|| echo 'The files does not exist.'".format(bucket, path_to_files))
if os.path.isdir(path_folder):
    os.system("hadoop fs -rm -r 's3://{0}/{1}/dq*'|| echo 'The files does not exist.'".format(bucket, path_to_files))
else:
    os.system("hadoop fs -mkdir {0}".format(path_folder))
       
#Starting spark seesion
    
spark = SparkSession \
    .builder \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set("spark.sql.crossJoin.enabled", 'true')

############################################
# Execution part
############################################

# Fetching commands
print("Fetching commands from metastore")
dq_commands = fetch_commands(suite_name, case_name)

#Qerying data and writing it to dataframes
print("Creating dataframes from commands")
df_dict = df_creation(dq_commands)

#Writing files to s3 location
print("Writing files to s3")
files = df_to_csv(df_dict, bucket, path_to_files)

#Evaluating results based on dataframes columns
print("Create dict with results for dq checks")
results=eval_results(df_dict)

#Evaluating final result
print("Evaluating final result")
final_status='PASS'
for key, value in results.items():
    if value=='FAIL':
        final_status='FAIL'

##########################################
# Template for email
##########################################

#Subject line    
SUBJECT = "DQ CLV {0} check: {1}".format(case_name, final_status)

#Text for non-html clients
BODY_TEXT = "DQ check status - {0}. Look for details of failed DQ checks in the attached files.".format(final_status)

end_time = datetime.datetime.now().replace(microsecond=0)

#Content for HTML clients
BODY_HTML=html_gen(final_status, results, process_day, runid, start_time, end_time, case_name)

# Define the attachment part and encode it using MIMEApplication

dq_att={}

for key, value in results.items():
    if(value=='FAIL'):
        filename='{0}_result.csv'.format(key)
        path_file=os.path.join(path_to_files,filename)
        s3_object = boto3.client('s3')
        s3_object = s3_object.get_object(Bucket=bucket, Key=path_file)
        body = s3_object['Body'].read()

        dq_att[key] = MIMEApplication(body, _subtype='octet-stream')
        dq_att[key].add_header("Content-Disposition", 'attachment', filename=filename)

#Email splitting

email_out, email_slack = email_split(email)
        
#########################################################################
# Mailing part
#########################################################################

#Sending email to regular address
send_email_att(sender, email_out, dq_att, SUBJECT, BODY_TEXT, BODY_HTML)

#Sending email to slack in case of failures
if final_status=='FAIL':
    send_email_att(sender, email_slack, dq_att, SUBJECT, BODY_TEXT, BODY_HTML)
    
######################################################
# Loging results to DQF metastore
######################################################

log_res=logging_results(results, team_name, suite_name, case_name, runid, start_time, end_time)

print("DQ process ended:")
print (datetime.datetime.now().replace(microsecond=0))
