import time
import boto3
import pandas as pd
import io
from vars import config

class QueryAthena:

    def __init__(self, query, database):
        self.database = database
        self.folder = f'brsaords01/'
        self.bucket = 'athena-query-results-dest'
        self.s3_input = 's3://' + self.bucket + '/my_folder_input'
        self.s3_output = 's3://' + self.bucket + '/' + self.folder
        self.region_name = 'sa-east-1'
        self.query = query
        self.client = boto3.client('athena', region_name=self.region_name)
        self.resource = boto3.resource('s3', region_name=self.region_name)
        self.filename = None

    def load_conf(self, q):
        try:
            response = self.client.start_query_execution(
                QueryString=q,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={'OutputLocation': self.s3_output}
            )
            self.filename = response['QueryExecutionId']
            print('Execution ID: ' + self.filename)
            return response
        except Exception as e:
            print(f"Error starting query execution: {e}")
            return None  # Return None if the query fails

    def run_query(self):
        queries = [self.query]
        for q in queries:
            res = self.load_conf(q)
            if res is None:  # If load_conf fails, return early
                return pd.DataFrame()  # Return an empty DataFrame as a fallback

        try:
            query_status = None
            while query_status in ['QUEUED', 'RUNNING', None]:
                response = self.client.get_query_execution(QueryExecutionId=res['QueryExecutionId'])
                query_status = response['QueryExecution']['Status']['State']
                print(f"Query Status: {query_status}")
                
                if query_status in ['FAILED', 'CANCELLED']:
                    raise Exception(f"Athena query failed or was cancelled: {self.query}")
                
                time.sleep(1)  # Delay between checks

            print(f'Query "{self.query}" finished.')
            return self.obtain_data()

        except Exception as e:
            print(f"Error during query execution: {e}")
            return pd.DataFrame()  # Return an empty DataFrame in case of failure

    def obtain_data(self):
        try:
            response = self.resource.Bucket(self.bucket).Object(key=self.folder + self.filename + '.csv').get()
            return pd.read_csv(io.BytesIO(response['Body'].read()), encoding='utf8')
        except Exception as e:
            print(f"Error obtaining data from S3: {e}")
            return pd.DataFrame()  # Return an empty DataFrame if reading the CSV fails


def run_query(query, database):
    qa = QueryAthena(query=query, database=database)
    dataframe = qa.run_query()
    return dataframe
