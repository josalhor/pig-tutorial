-- Load data from a CSV file
data = LOAD 'data.csv' USING PigStorage(',') AS (name:chararray, age:int, city:chararray);

-- Filter data to get only records with age greater than 18
filtered_data = FILTER data BY age > 18;

-- Group data by city
grouped_data = GROUP filtered_data BY city;

-- Calculate average age for each city
aggregated_data = FOREACH grouped_data GENERATE group AS city, AVG(filtered_data.age) AS avg_age;

-- Join data with another dataset
other_data = LOAD 'other_data.csv' USING PigStorage(',') AS (city:chararray, population:int);
joined_data = JOIN aggregated_data BY city, other_data BY city;

-- Project only specific fields from the joined data
projected_data = FOREACH joined_data GENERATE aggregated_data::city AS city, (float)avg_age AS avg_age, population;

-- Sort data by city in ascending order
sorted_data = ORDER projected_data BY city ASC;

-- Clean and transform data using string functions
cleaned_data = FOREACH sorted_data GENERATE city, (chararray)REPLACE((chararray)avg_age, '\\.', ',') AS avg_age;

-- Store cleaned data to a local CSV file
STORE cleaned_data INTO 'cleaned_data.csv' USING PigStorage(',');
