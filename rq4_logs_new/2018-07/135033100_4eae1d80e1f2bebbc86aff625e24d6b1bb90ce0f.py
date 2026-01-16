############################################################################################
## Extract, transform and load script | Fetches data from Amazon S3 and loads it to Hive. ##
############################################################################################

import boto3
import pathlib

def run_etl(hiveContext, bucket_name, path_prefix, dataset_dir, hive_table_name):
	
	# Get all target directory keys
	s3_client = boto3.resource('s3')
	s3_bucket = s3_client.Bucket(bucket_name)
	s3_objects = s3_bucket.objects.filter(Prefix=path_prefix+'/', Delimiter='/')

	# Select data files keys only
	data_keys = []
	for s3_object in s3_objects:

		if s3_object.key.endswith('.json'):
			data_keys.append(s3_object.key)
	
	# Report the number of data files
	print('Fetched the keys of %s data file(s).\n' % len(data_keys))

	# Prepare output directory
	pathlib.Path(dataset_dir).mkdir(exist_ok=True) 

	# Fetch data
	for index, data_key in enumerate(data_keys):

		# Notify
		output_file_name = '{}/part_{}.json'.format(dataset_dir,index+1)
		print('Downloading part ({}/{})\nKey: {}\nSaving to: {}\n'.format(index+1, len(data_keys), data_key, output_file_name))
		
		# Check if data file already exists
		output_file_path = pathlib.Path(output_file_name)
		
		# Download data file if missing
		if not(output_file_path.is_file()):
			s3_bucket.download_file(data_key, output_file_name)
	
	# Save fetched data to Hive
	fetched_data = hiveContext.read.json(dataset_dir)
	fetched_data.createOrReplaceTempView('dataset_temp') 
	hql_statement = 'CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM dataset_temp'.format(hive_table_name)
	hiveContext.sql(hql_statement);
	print('Fetched and loaded dataset to Hive.\nTable name: {}\nNumber of records: {}\n'.format(hive_table_name,fetched_data.count()))


	

	



