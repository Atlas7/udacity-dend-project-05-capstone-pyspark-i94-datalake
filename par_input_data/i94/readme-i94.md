I94 Immigration Data: Udacity claims the underlying i94 Immigration dataset comes from: https://travel.trade.gov/research/reports/i94/historical/2016.html

(Note: as at Dec 2022, it appears that this URL is now re-directed to another homepage: https://www.trade.gov/national-travel-and-tourism-office - unfortunately I can no longer identify where the original dataset was downloaded - possibly from private contact?)

Nevertheless, this is what Udacity's Data Engineering Nanodegree Capstone project page says: 

"This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. [This](https://travel.trade.gov/research/reports/i94/historical/2016.html) is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.""

Sample CSV location (I've moved it to): `raw_input_data/i94_sample_csv/immigration_data_sample.csv` <-- it appears to contains 1001 sample rows (1 header + 1000 records) from the year 2016 and month April.

Full SAS Datasets (in sas7bdat binary formats): on Udacity virtual server environment: `/data/18-83510-I94-Data-2016/`. It contains 12 SAS Dataset (1 for each month in year 2016). Each SAS Dataset has a naming convention like this: `i94_<mmm>16_sub.sas7bdat`, where `mmm` is in this list `['apr','aug','dec','feb','jan','jul','jun','mar','may','nov','oct','sep']`

The Strategy is to use PySpark to read in these 12 SAS Datasets, and output in one consolidated Parquet file (using suitable partition, such as year and month). The expected local output path of the parquet file will go to (according to my architecture): `par_input_data/i94/i94_parquet`

We will eventually upload the parquet files to S3.

We keep the i94 data dictionary (as SAS Code via a PROC FORMAT statement) provided by Udacity at `par_input_data/i94/I94_SAS_Labels_Descriptions.SAS`.
