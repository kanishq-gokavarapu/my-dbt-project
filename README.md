As part of PreProcessing, used pyspark script to cleanse the initial csv shared. 
  Row level dupliactes are removed
  cast the date column to mysql supported format
  remove unnamed column
  City column is enriched by removing unwanted symbols, spaces and pincodes present in it. Empty strings are updated to constant value
  Courier status is enriched with Cancelled if order_status is cancelled ,  qty is null and courier status is null. Other Null values are updated with constant value.
  columns qty, amount, currency, promotion-ids, fulfilled by , ship-city, ship-state, ship-postal, ship-country nulls are handled by updating constant values.
Once Preprocessing is completed, Required table structure is created in Mysql and cleansed data is loaded into mysql table. Required SQL scripts are added in preprocessing folder.

As part of staging process, created a model which picks the records for which Order_id is not null and will remove duplicates if any ( additional layer check)
Using the staging layer, Fact and Dimensional models are created subsequently created a model to feed the right format of data to BI (Power BI) 

To ensure data quality, 21 tests are created across facts and dimensional models.

Performed visualizations using PowerBI. Visualizations along with DBT run and test stats are captured in Output folder.
