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

-- create relation and extract months from date field	
relation = foreach data generate SUBSTRING(date,3,5) AS month,average_price,sales_volume;

-- group according to month
grouped = group relation by month;

-- calculate average for sales volume and average price in each month
month_av = foreach grouped generate group, 
		ROUND_TO(AVG(relation.sales_volume),2) AS sales_volume, 
		ROUND_TO(AVG(relation.average_price),2) AS average_price;


--order in descending
ordered = ORDER month_av BY sales_volume DESC;


dump ordered;





















