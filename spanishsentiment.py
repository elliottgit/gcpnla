# Imports the Google Cloud client library
from google.cloud import bigquery
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types

import time

# BQ information
PROJECT_ID = 'elliottn-env'
DATASET_ID = 'SurveyMonkeyPOC'
source_table_id = 'SpanishCustOps'
target_table_id = 'SpanishCustOpsTest'
TIMEOUT = 30
client = language.LanguageServiceClient()

# This is the column to pull data from to run NLP
source_target_table_id = 'CaseID'
source_target_table_column_name = 'Message'

# Schema for target table
SCHEMA = [
        bigquery.SchemaField('id', 'STRING', mode='required'),
        bigquery.SchemaField('message', 'STRING', mode='required'),
        bigquery.SchemaField('sentiment_score', 'FLOAT', mode='required'),
        bigquery.SchemaField('sentiment_magnitude', 'FLOAT', mode='required'),
]

# NLP information
nlp_salience_threshold = 0.001
#print("here")

# Initialize Clients
bq = bigquery.Client()
language = language.LanguageServiceClient()

dataset_ref = bq.dataset(DATASET_ID)
dataset = bigquery.Dataset(dataset_ref)

source_table_ref = dataset.table(source_table_id)
source_table = bigquery.Table(source_table_ref)

#query_job=bq.query('select  Respondent__, '+ source_target_table_column_name+' frowhere Please_comment_on_how_well_you_understood_the_company_s_business_strategy_after_attending_New_Hire_Onboarding_ is NOT NULL LIMIT 500')
query_job = bq.query('SELECT ' + source_target_table_id + ',' + source_target_table_column_name + ' FROM `' + PROJECT_ID + '.' + DATASET_ID + '.' + source_table_id + '` where Message is NOT NULL ')

iterator = query_job.result(timeout=TIMEOUT)
rows = list(iterator)
length = len(rows)

start_time = time.time()
count = 0

target_table_ref = dataset.table(target_table_id)
target_table = bigquery.Table(target_table_ref, schema=SCHEMA)
target_table = bq.create_table(target_table)	

results = []
for row in rows:
	sample = row[source_target_table_column_name]
	document = types.Document(content=sample, type=enums.Document.Type.PLAIN_TEXT)
	sentiment = client.analyze_sentiment(document).document_sentiment
#	try:
#	entities = language.analyze_sentiment(document).document_sentiment
#	for entity in document_sentiment:
#		if(entity.salience > nlp_salience_threshold):
	results.append((row[source_target_table_id],document.content, sentiment.score, sentiment.magnitude ));

#	except:
#		print 'error'
bq.create_rows(target_table, results)