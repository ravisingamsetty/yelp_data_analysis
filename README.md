# Yelp Data Analysis

# Overview
This application is about analyzing and providing some insights on businesses around Arizona by analysing the yelp reviews data

# Features
This applications does the following:

1) Extracts the business attributes and user reviews from the respective files and cleanses it as per the requirement

2) Produces the following datasets
  a) Base summarized dataset
  b) Mean reviews by business dataset
  c) Mean reviews by zipcode for the Top 5 most business dense zipcodes
  d) Top 10 most active reviewers

3) Uploads all the above datasets programatically to S3 bucket. Below are url's for the same 
    
    [Base summarized dataset](https://myawsbucketslalom.s3.amazonaws.com/base_summarized.csv)
    
    [Mean reviews by business dataset](https://myawsbucketslalom.s3.amazonaws.com/mean_review_by_business.csv)
    
    [Mean reviews by zipcode](https://myawsbucketslalom.s3.amazonaws.com/mean_review_by_zipcode.csv)

    [Top 10 Active Reviewers](https://myawsbucketslalom.s3.amazonaws.com/active_reviewers.csv)

# Running The Project

Download the **yelp_data_analysis.py** and run the command *python yelp_data_analysis.py* after updating the paths of the files. 

# Dependencies

Create a S3 bucket prior to running the script and make sure access key and secure access key are updated in the environment variables

# To Do

Removing all the manual interventions,like 
1) Unzipping the yelp dataset programatically 
2) Parameterizing the S3 bucket name in the script
