from json2parquet import convert_json
import requests
from fastparquet import write
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import boto3 


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):

    result = None
    action = event.get("action")
    
    nsi_weather()
        

def nsi_weather():

# hit API and get the past 31 days weather data for lat/long hourly temperature_2m,rain,showers,visibility

    url = "https://api.open-meteo.com/v1/forecast?latitude=51.5085&longitude=-0.1257&hourly=temperature_2m,rain,showers,visibility&past_days=31"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    print(response.text)

    input_filename = response
    output_filename = ""

    # Infer Schema (requires reading dataset for column names)
    convert_json(input_filename, output_filename)

    # Given columns
    convert_json(input_filename, output_filename, ["my_column", "my_int"])

    # Given columns and custom field names
    field_aliases = {'my_column': 'my_updated_column_name', "my_int": "my_integer"}
    convert_json(input_filename, output_filename, ["my_column", "my_int"], field_aliases=field_aliases)


    # Given PyArrow schema
    schema = pa.schema([
        pa.field('my_column', pa.string),
        pa.field('my_int', pa.int64),
    ])
    convert_json(input_filename, output_filename, schema)

    # write to file


    write('outfile.parq', df)
    write('outfile2.parq', df, row_group_offsets=[0, 10000, 20000],
        compression='GZIP', file_scheme='hive')
    
    data = {0: {"data1": "value1"}}
    df = pd.DataFrame.from_dict("outfile.parq", orient="index")
    write_pandas_parquet_to_s3(
        df, "bucket", "folder/test/file.parquet", ".tmp/file.parquet")
    
    
def write_pandas_parquet_to_s3(df, bucketName, keyName, fileName):
    # dummy dataframe
    table = pa.Table.from_pandas(df)
    pq.write_table(table, fileName)

    # upload to s3
    s3 = boto3.client("s3")
    BucketName = bucketName
    with open(fileName) as f:
       object_data = f.read()
       s3.put_object(Body=object_data, Bucket=BucketName, Key=keyName)

