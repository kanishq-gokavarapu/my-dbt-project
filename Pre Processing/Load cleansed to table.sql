load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/data_cleansed_report.csv' 
into table retail_analytics.amazon_sale_report
fields terminated by ','
ENCLOSED BY '"'
lines terminated by '\n'
ignore 1 lines ;