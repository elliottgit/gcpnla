# Imports the Google Cloud client library
from google.cloud import bigquery
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types

import time

# BQ information
PROJECT_ID = 'elliottn-env'
DATASET_ID = 'SurveyMonkeyPOC'
source_table_id = 'NewHire'
target_table_id = 'NewHiewCommentEntity'
TIMEOUT = 30

# This is the column to pull data from to run NLP
source_target_table_id = 'Respondent'
source_target_table_column_name = 'BusinessStrategyComment'

# Schema for target table
SCHEMA = [
        bigquery.SchemaField('id', 'INTEGER', mode='required'),
        bigquery.SchemaField('BusinessStrategyComment', 'STRING', mode='required'),
        bigquery.SchemaField('entity', 'STRING', mode='required'),
        bigquery.SchemaField('salience', 'FLOAT', mode='required'),
        bigquery.SchemaField('sentiment_score', 'FLOAT', mode='required'),
        bigquery.SchemaField('sentiment_magnitude', 'FLOAT', mode='required'),
]

# NLP information
nlp_salience_threshold = 0.001

# Initialize Clients
bq = bigquery.Client()
language = language.LanguageServiceClient()

dataset_ref = bq.dataset(DATASET_ID)
dataset = bigquery.Dataset(dataset_ref)

source_table_ref = dataset.table(source_table_id)
source_table = bigquery.Table(source_table_ref)

query_job = bq.query('SELECT ' + source_target_table_id + ',' + source_target_table_column_name + ' FROM `' + PROJECT_ID + '.' + DATASET_ID + '.' + source_table_id + '` WHERE ' + source_target_table_column_name + ' IS NOT NULL')

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
	
#	try:
	entities = language.analyze_entity_sentiment(document).entities
	for entity in entities:
		if(entity.salience > nlp_salience_threshold):
#			results.append((entity.name, row[source_target_table_id], entity.salience, entity.sentiment.score, entity.sentiment.magnitude ));	
			results.append((row[source_target_table_id], row[source_target_table_column_name], entity.name, entity.salience, entity.sentiment.score, entity.sentiment.magnitude ));	
#	except:
#		print 'error'
bq.create_rows(target_table, results)