# boston_crimes_report

Generates a report with statistics of the crimes in Boston.
## Columns
	* district: code of a district in Boston
	* crimes_total: total count of crimes in a district
	* crimes_monthly: average (todo median?) count of crimes monthly in a district
	* frequent_crime_types: top three crime_type happened in a district for the entire history
	* crime_type: first part of the 'name' from 'offense_codes' table
	* lat: average latitude of all incidents in a district
	* lng: average longtitude of all incidents in a district

The source data is Kaggle's datasets:  https://www.kaggle.com/AnalyzeBoston/crimes-in-boston

# Usage
spark-submit crimes_report.py <crimes.csv> <offense_codes.csv> <output_dir>
