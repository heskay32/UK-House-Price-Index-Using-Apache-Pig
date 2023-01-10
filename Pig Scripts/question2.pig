
--Load dataset
REGISTER /home/training/pig/lib/piggybank.jar;

data = LOAD 'Cash-mortgage-sales-2022-08.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage  AS 
			(date:chararray, 
			region_name:chararray,
			area_code:chararray,
			cash_average:float,
			cash_index:float,
			cash_monthly_change:float,
			cash_ann_change:float,
			cash_sales_volume:int,
			mortgage_average:float,
			mortgage_index:float,
			mortgage_monthly_change:float,
			mortgage_ann_change:float,
			mortgage_sales_volume:int
			);
			
			
--create relation needed and preprocess date field to extract year			
process_year = foreach data generate SUBSTRING(date,0,4) AS year,cash_sales_volume,mortgage_sales_volume;

--remove null data
clean = FILTER process_year BY cash_sales_volume IS NOT NULL AND mortgage_sales_volume IS NOT NULL;

--group according to year
group_by_year = GROUP clean BY year;

--Add up the volume of mortgage sale and cash sales for each year 
sum_sales = FOREACH group_by_year GENERATE 
		group, 
		FLATTEN(SUM(clean.cash_sales_volume)) AS cash, 
		FLATTEN(SUM(clean.mortgage_sales_volume)) AS mortgage;
		
-- calculate percentage ratio		
morg_to_cash = FOREACH sum_sales GENERATE group,ROUND_TO(((double)mortgage/(double)cash)*100,2) AS percent;

--order by percentage
ordered = ORDER morg_to_cash BY percent DESC;

result = FOREACH ordered GENERATE group, CONCAT((chararray)(percent), '%');

dump result;


