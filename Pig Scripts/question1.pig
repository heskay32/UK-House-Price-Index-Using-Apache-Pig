
--Load Dataset

REGISTER /home/training/pig/lib/piggybank.jar;

data = LOAD 'UK-HPI-full-file-2022-08.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage  AS 
			(date:chararray, 
			region_name:chararray,
			area_code:chararray,
			average_price:float,
			index:float,
			index_sa:float,
			one_m_change:float,
			twelve_change:float,
			average_price_sa:float,
			sales_volume:int,
			detached_price:float,
			detached_index:float,
			detached_one_m_change:float,
			detached_twelve_change:float,
			s_detached_price:float,
			s_detached_index:float,
			s_detached_one_m_change:float,
			s_detached_twelve_change:float,
			terraced_price:float,
			terraed_index:float,
			terraced_one_m_change:float,
			terraced_twelve_change:float,
			flat_price:float,
			flat_index:float,
			flat_one_m_change:float,
			flat_twelve_change:float,
			cash_price:float,
			cash_index:float,
			cash_one_m_change:float,
			cash_twelve_change:float,
			cash_sales_volume:float,
			mortgage_price:float,
			mortgage_index:float,
			mortgage_one_m_change:float,
			mortgage_twelve_change:float,
			mortgage_sales_volume:float,
			ftb_price:float,
			ftb_index:float,
			ftb_one_m_change:float,
			ftb_twelve_change:float,
			foo_price:float,
			foo_index:float,
			foo_1m_change:float,
			foo_12_change:float,
			new_price:float,
			new_index:float,
			new_one_m_change:float,
			new_twelve_change:float,
			new_sales_volume:int,
			old_price:float,
			old_index:float,
			old_one_m_change:float,
			old_twelve_change:float,
			old_sales_volume:int
			);

--Create needed Relation and preprocess year column
process_year = foreach data generate SUBSTRING(date,6,10) AS year,area_code,region_name,average_price;

-- Filter out data related to a year Before BREXIT
before = FILTER process_year BY year=='2019';

-- Filter out data related to a year After BREXIT
after = FILTER process_year BY year=='2021';

--Group both before and after brexit relations according to regions.
group_before = GROUP before BY region_name;

group_after = GROUP after BY region_name;


--Calculate Average of house prices for each region (Before and After BREXIT)
avg_before = FOREACH group_before GENERATE group, FLATTEN(AVG(before.average_price)) AS average_price;

avg_after = FOREACH group_after GENERATE group, FLATTEN(AVG(after.average_price)) AS average_price;

--Join the two dataset on region.
joined = JOIN avg_before BY group, avg_after BY group;

-- Flatten the relation and create a new schema
relation = FOREACH joined GENERATE 
	avg_before::group AS region, 
	avg_before::average_price AS bf_price,
	avg_after::average_price AS af_price;

--Calculate Percentage Increase/Decrease for each region.
perc_change = FOREACH relation { 
			change = bf_price - af_price; 
			pchange =  ROUND_TO((change/af_price) * 100,2); 
			GENERATE region,pchange AS percentage;};

--Order in Descending and ascending using the percentage change
--ordered1 = ORDER perc_change BY percentage;
ordered = ORDER perc_change BY percentage DESC;

--Sort only buttom 10
--last_10 = LIMIT ordered1 10;

--Sort only top 10
top_10 = LIMIT ordered 10;

--output result
--dump last_10;
dump top_10;





