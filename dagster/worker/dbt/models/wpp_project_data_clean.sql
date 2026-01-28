with datasource as (
	select distinct
		monthyear
	    ,business_level
	    ,category
	    ,brand
	    ,variable
	    ,case 
	    	when "Amount" is null then 0
	    	else "Amount" 
	    end as "Amount" 
	from {{ source('raw', 'wpp_project_data_xlsx') }} 
)
, cleanstep1 as (
select 
    monthyear, 
    business_level, 
    category, 
    brand,
    -- Melakukan pivot untuk setiap nilai unik dalam kolom 'variable'
    coalesce(sum("Amount") filter (where variable = 'Sales'), 0) as "sales",
    coalesce(sum("Amount") filter (where variable = 'Spend on meta'), 0) as "spend_on_meta",
    coalesce(sum("Amount") filter (where variable = 'Spend on tiktok'), 0) as "spend_on_tiktok",
    coalesce(sum("Amount") filter (where variable = 'Spend on tv'), 0) as "spend_on_tv", 
    coalesce(sum("Amount") filter (where variable = 'Spend on ucontent'), 0) as "spend_on_ucontent",
    coalesce(sum("Amount") filter (where variable = 'Spend on youtube'), 0) as "spend_on_youtube"
from 
    datasource
group by 
    1, 2, 3, 4
)
,cleanstep2 as (
	select 
		monthyear 
	    ,business_level
	    ,category
	    ,brand
	    ,cast("sales" as bigint) as "sales"
	    ,cast("spend_on_meta" as bigint) as "spend_on_meta"
	    ,cast("spend_on_tiktok" as bigint) as "spend_on_tiktok"
	    ,cast("spend_on_tv" as bigint) as "spend_on_tv"
	    ,cast("spend_on_ucontent" as bigint) as "spend_on_ucontent"
	    ,cast("spend_on_youtube" as bigint) as "spend_on_youtube"
	from cleanstep1
)select * from cleanstep2
   
