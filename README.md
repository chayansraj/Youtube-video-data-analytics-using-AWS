# Data Engineering Youtube video analytics using AWS (End-to-End)
This project aims to leverage Amazon Web Services to create a youtube trending video analytics service. The project contains different data engineering, data analysis, and data science sections.

# Project Goals

1. Data Ingestion - Create a data ingestion pipeline to extract new incoming data into the AWS data lake.
2. Data Lake - Create a centralized repository to store data from multiple sources and of different formats.
3. Data ETL Jobs - Create Extract, Transform, and Load jobs to preprocess raw data into usable proper versions.
4. Data Analysis - Create SQL queries to analyze data to create key insights about the product.
5. Data Reporting/Analytics - Create dashboards to answer questions and communicate insights to the Stakeholders.


# Data Architecture

The architecture (Data flow) used in this project uses different AWS functionalities.

<p align="center">
  <img width="1000" height="600" src="https://github.com/chayansraj/Youtube-data-analytics-using-AWS/assets/22219089/c7c91075-5578-47ce-bf1c-2ded09a8ba0e">
  <h6 align = "center" > Source: google </h6>
</p>

# Dataset Used 
The dataset contains daily statistics for trending YouTube videos with up to 200 trending videos per day. It includes several months of data on daily trending YouTube videos. Major nations covered in this dataset are US, GB, DE, CA, JP, IN, and FR regions (USA, Great Britain, Germany, Canada, Japan, India, and France, respectively). 
* There are two parts to this dataset:
  1. Each region’s trending video data is in a separate .csv file. Data includes both numerical and categorical columns and some of them are the video title, channel title, publish time, tags, views, likes and dislikes, description, and comment count.
  2. Each video is also associated with its video category which is stored in a separate .json file that varies between regions.

Dataset link: https://www.kaggle.com/datasets/datasnaek/youtube-new

# Implementation

* Step 1 - 
