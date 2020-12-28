# importing packages
import pandas as pd
import glob
import sqlite3
from io import StringIO
import boto3

# Defining file paths
directory = '/Users/ravisingamsetty/Downloads/output' 
json_files = glob.glob(directory + "/business_composition_final*.json")
csv_files = glob.glob(directory + "/reviews[0-9]*.csv")
sqllite_file = glob.glob(directory + "/user.sqlite")


# Read through json files and create a dataframe by appending each file
def get_business_composition(json_files):
    lst = []
    for filename in json_files:
        df = pd.read_json(filename)
        df = df.T      #transpose 
        lst.append(df)
        
    return(pd.concat(lst, axis=0, ignore_index=True))

 
# Read through csv files and create a dataframe by appending each file
def get_business_reviews(csv_files):
    lst = []
    for filename in csv_files:
        df = pd.read_csv(filename, index_col=0, header=0)
        lst.append(df)
        
    return(pd.concat(lst, axis=0, ignore_index=True))

# Read the sqllite file and read table content to Database
def get_business_user_attributes(sqllite_file):
    con = sqlite3.connect(sqllite_file[0])
    df_bsns_attr = pd.read_sql_query("SELECT * from business_attributes;", con)
    df_user = pd.read_sql_query("SELECT distinct `User - Id`,`User - Name` from Users2", con)
    con.close()

    # Remove the duplicates
    return(df_bsns_attr.drop_duplicates(),df_user)
    

#Upload the files to AWS S3 Bucket
def upload_dataframe_to_aws_s3(data_frame,name):
    bucket = 'myawsbucketslalom'  # already created on S3
    csv_buffer = StringIO()
    data_frame.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, f"{name}.csv").put(Body=csv_buffer.getvalue())

    
    

def execute_pgm():
    
    df_bsns_comp = get_business_composition(json_files)
          
    df_reviews =  get_business_reviews(csv_files)
    
    df_bsns_attr,df_user = get_business_user_attributes(sqllite_file)
    
# 1) Creation of base summarized dataset
    
    # renaming key column to be able to join with other dataset
    df_bsns_comp = df_bsns_comp.rename(columns={"business_id":"Business - Id"})
    
    # Joining business composition with business attributes
    df_dim_business = pd.merge(df_bsns_comp,df_bsns_attr,on=['Business - Id','Business - Name'])
    
    # Extract Zip code from address to be later used for aggregation
    df_dim_business['zip_code'] = df_bsns_comp['Business - Address'].str[-5:]

    # Join user_reviews to user to get username
    df_user_reviews = pd.merge(df_reviews,df_user,on='User - Id',how='left')

    # Create a final summarized data set
    df_base_summarized = pd.merge(df_user_reviews,df_dim_business,on='Business - Id',how='left')
    
    # Upload to S3
    #upload_dataframe_to_aws_s3(df_base_summarized,'base_summarized')
    
    
# 2) Calculate the mean reviews by business

    df_mean_review_bsns = df_base_summarized.groupby(['Business - Id','Business - Name']).agg(mean_review=('Review - Stars','mean')).reset_index().round(2)
    
    # Upload to S3
    #upload_dataframe_to_aws_s3(df_mean_review_bsns,'mean_review_by_business')

# 3) Calculate the mean reviews by zipcode for the Top 5 most business dense zipcodes

    # Find top 5 business dense zip code(based on number of business units per zipcode)
    df_zip_cnts = df_dim_business.groupby(['zip_code']).size().reset_index(name='counts')
    df_top_bsns_zip = df_zip_cnts.nlargest(5,'counts').reset_index(drop=True)

    # Finding the mean reaviews by zipcodes for the top 5 business desnse zip codes
    df_mean_review_top_bsns_zip = pd.merge(df_top_bsns_zip,df_base_summarized,on='zip_code').groupby(['zip_code']).agg(mean_review=('Review - Stars','mean')).reset_index().round(2)

    # Upload to S3
    #upload_dataframe_to_aws_s3(df_mean_review_top_bsns_zip,'mean_review_by_zipcode')


    
# 4) Calculate the Top 10 most active reviewers

    # Considering the number of times the user gave review as the criteria for active reviewrs
    df_user_review_cnts = df_base_summarized.groupby(['User - Id','User - Name']).size().reset_index(name='no_reviews')
    df_top_reviewers = df_user_review_cnts.nlargest(10,'no_reviews').reset_index(drop=True)

    # Upload to S3
    #upload_dataframe_to_aws_s3(df_top_reviewers,'active_reviewers')
   
    print('Program Executed Successfull')


if __name__== '__main__':
    print('Executing the Program')
    execute_pgm()

