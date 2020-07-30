#### Description
A high level Python wrapper using pandas.
It is meant to provide a point-in-time json data handling for redshift load Job.

#### Installation
Supports Python 3.6+
```bash
pip install jsonfriendly-redshift
```
##### Features
<li> Automatic schema creation, if schema doesn't exist
<li> Automatic table creation, if table doesn't exist
<li> Automatic column addition to existing table, if column doesn't exist
<li> Dict utils helper functions to create flat json
<li> Dict utils helper functions to rename keys as per mapping provided and format datetime string to standard ISO formt.
<li> Utils helper functions to create New Line Delimited Json required for s3 load job with json

##### Refer Below Example For More Details
    import os
    import json
    import pandas as pd
    import psycopg2 as pg
    from jsonfriendly_redshift.handler import RedShiftHandler
    from jsonfriendly_redshift.dict_utils import flatten_dict_for_redshift, flatten_and_fix_timestamp, fix_keys
    from jsonfriendly_redshift.utils import generate_json_for_copy_query
    
    # you can use environment variable or direct assignment
    db_name = os.environ["DB_NAME"]
    db_password = os.environ["DB_PASSWORD"]
    db_user = os.environ["DB_USER"]
    db_port = os.environ["DB_PORT"]
    db_schema = os.environ["DB_SCHEMA"]
    db_host = os.environ["DB_HOST"]
    db_iam_arn = os.environ["REDSHIFT_IAM"]
    write_bucket = os.environ["WRITE_BUCKET"]
    
    # pg connection object
    connection = pg.connect(host=db_host,
                            user=db_user,
                            password=db_password,
                            database=db_name,
                            port=db_port)
    
    
    def lambda_handler(event, context):
        # s3 file retrieval logic goes here
        # Create a file object using the bucket and object key.
        # replace file_obj with yours
        file_obj = {}
    
        s3_data = file_obj['Body'].read().decode("utf-8")
        s3_data = json.loads(s3_data)
    
        final_data = {}
    
        # example for s3_data data of type list(dict, dict, dict)
    
        for dict_data in s3_data:
            ################
            # # fix_keys # #
            ################
            # replaces all the special char in key name of dictionary and replaces it
            # using re.sub(r"[^A-Za-z0-9_]+", "", key_name) and it does it recursively
            processed_data = fix_keys(dict_data)
    
            #################################
            # # flatten_and_fix_timestamp # #
            #################################
            # makes a flat dictionary object, excluding key with array, they are converted in a json string
            # for eg: converts {"first" {"second": "abc", "third": [{..},{..}]}}
            # to {"first_second": "abc", "first_third": "[{..},{..}]"} first_third = json.dumps object
            # and if it encounters datetime string which can be parsed from python date utils it
            # converts it to iso formatted datetime string
            # Optional if mapping dict is provided, it maps the flat key name to the name provided
            # eg: mapper = {"first_second": "my_key_name"} => {"my_key_name": data}
            # usage flatten_and_fix_timestamp(dict_obj, mapper)
            mapper_dict = {}  # provide your mappings for long column name
            processed_data = flatten_and_fix_timestamp(dict_data, mapper_dict)
    
            #################################
            # # flatten_dict_for_redshift # #
            #################################
            # its not recommended it does the same thing as flatten_and_fix_timestamp
            # but it doesn't takes any mapper
            # for safe side it strips the key name with last 127 chars
            # as redshift supports only 127 bytes of length for column name
            # and it may grow in size due to flattening of dict
            processed_data = flatten_dict_for_redshift(dict_data)
    
            data_df = pd.read_json(json.dumps(s3_data))
            # test data
            table_names = ["foo", "foo1", "foo2"]
            for table in table_names:
                ###################################
                # # initialize redshift handler # #
                ###################################
                rs_handler = RedShiftHandler(df=data_df, schema=db_schema,
                                             table=table, conn=connection)
    
                # handles the schema creation, table creation and column creation automatically
                # by checking the metadata of table already present, if not, it creates it with
                # most suitable column data types
    
                ##########################################################################################
                # # Highly suggested to use good amount of data for initial load job (500 rows minimum)# #
                ##########################################################################################
                
                handle_successful, handle_error = rs_handler.handle_schema()
                if handle_successful and not handle_error:
                    # you can create json format supported by redshift for load operation by using
                    list_of_rows = [{}, {}, {}]  # your row data
                    new_line_delimited_json = generate_json_for_copy_query(list_of_rows)
                    # save this json to s3 bucket and provide the bucket_name and object key along 
                    # with IAM Role to execute_json_copy_query method of rs_handler
                    ##################################################################################
                    # # you logic for saving data to s3 goes here, if you want to use copy command # #
                    ##################################################################################
                    # or else you can write your own insert query  and execute it using 
                    # rs_handler.execute_query(query_string) goes here
                    # s3_object_path is where you have sored you file object
                    s3_object_path = "bucket_name/your_object_key"  # you dont need to provide s3:// initials
                    success, error = rs_handler.execute_json_copy_query(s3_object_path, db_iam_arn)
                        if error:
                            # Do somthing to manage

#### Troubleshooting
If you are facing any issue with psycopg2 installation i would suggest installing it using conda<br>
```bash
conda install psycopg2
```
PR's are welcome.
#### Contact
Email: mishraamrish.asm@gmail.com
#### Questions or suggestions?
Simple, just send me an email or open an issue :)
#### License
[MIT License](https://github.com/mishraamrish/jsonfriendly_redshift/blob/master/LICENSE)
