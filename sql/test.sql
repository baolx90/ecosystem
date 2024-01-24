with
get_histories_1 AS
(
select 		id, app_name, shop_id, event, occurred_at
from 			hive_shopify_histories
WHERE			app_name not in ('social_publish','social_reply','chatalyst','messent','alihunter','salesbox')

)
   select DISTINCT
				id row_id, app_name, shop_id
				, case 	when lag(event,1) OVER (PARTITION by app_name, shop_id ORDER BY occurred_at) = 'RELATIONSHIP_DEACTIVATED'
								and event = 'RELATIONSHIP_UNINSTALLED' then 1
								end caution_flag
   from get_histories_1 where 	event in ('RELATIONSHIP_DEACTIVATED', 'RELATIONSHIP_REACTIVATED', 'RELATIONSHIP_INSTALLED', 'RELATIONSHIP_UNINSTALLED')