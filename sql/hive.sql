with
get_histories_1 AS
(
select 		id, app_name, shop_id, event, occurred_at
from 			shopify_histories
WHERE			app_name not in ('social_publish','social_reply','chatalyst','messent','alihunter','salesbox')

)
,elm_id_1 AS
(
select
				DISTINCT
				id row_id, app_name, shop_id
				, case 	when lag(event,1) OVER (PARTITION by app_name, shop_id ORDER BY occurred_at) = 'RELATIONSHIP_DEACTIVATED'
								and event = 'RELATIONSHIP_UNINSTALLED' then 1
								end caution_flag

from 		get_histories_1
where 	event in ('RELATIONSHIP_DEACTIVATED', 'RELATIONSHIP_REACTIVATED', 'RELATIONSHIP_INSTALLED', 'RELATIONSHIP_UNINSTALLED')
)
, get_histories AS
(
select 		*
from 			shopify_histories
WHERE			app_name not in ('social_publish','social_reply','chatalyst','messent','alihunter','salesbox')
					and from_utc_timestamp(occurred_at, 'GMT+7') >= from_utc_timestamp(from_unixtime(unix_timestamp()), 'GMT+7')
)
, get_transactions AS
(
select 		*
from 			shopify_transactions
WHERE			app_name not in ('social_publish','social_reply','chatalyst','messent','alihunter','salesbox')
					and from_utc_timestamp(occurred_at, 'GMT+7') >= from_utc_timestamp(from_unixtime(unix_timestamp()), 'GMT+7')
)
, caution_shop_id AS
(
select 		*
from  elm_id_1
where 		caution_flag =1
)
, pre_histories AS
(select 	id, shop_id, shopify_domain, app_name, event, occurred_at
FROM			get_histories h
WHERE 		event in ('RELATIONSHIP_DEACTIVATED', 'RELATIONSHIP_REACTIVATED', 'RELATIONSHIP_INSTALLED', 'RELATIONSHIP_UNINSTALLED')
					--and h.app_name not in ('social_publish','social_reply','chatalyst','messent','alihunter','salesbox')
)
, union_all AS
(
		select
							0 as earning_flag
							, h.shop_id, shopify_domain, h.app_name, event

							, from_utc_timestamp(occurred_at, 'GMT+7') as date_event_itc
							, null as gross_earning_amount
							, null AS net_earning_amt
		from 			pre_histories h
		LEFT JOIN caution_shop_id c ON h.id = c.row_id
		WHERE 		c.row_id is null

		UNION ALL

		SELECT 		1 as earning_flag
							, shop_id, shopify_domain, app_name,  event

							, from_utc_timestamp(occurred_at, 'GMT+7') as date_event_itc
							, gross_amount as gross_earning_amount
							, net_amount as net_earning_amt
		FROM			get_transactions
		where 		--app_name not in ('social_publish','social_reply','chatalyst','messent','alihunter','salesbox')
							gross_amount >= 0

		)
 , get_flag AS
(
select
					date_format(from_utc_timestamp(date_event_itc, 'GMT+7'), 'yyyy-MM-dd') AS date_event
				, app_name
				, shop_id
				, shopify_domain
				, event
				, case 	when 	event='RELATIONSHIP_INSTALLED' then 1 else 0
								end 	ins_flag
				, case 	when 	event='RELATIONSHIP_REACTIVATED' then 1 else 0
								end   reopend_flag
				, case	when 	event='RELATIONSHIP_UNINSTALLED' then 1 else 0
								end 	unst_flag
				, case  when 	event='RELATIONSHIP_DEACTIVATED' then 1 else 0
								end 	closed_flag

				, earning_flag

				, case	when 	gross_earning_amount is null then 0 else gross_earning_amount
								end 	gross_earning_amount

				, case	when 	net_earning_amt is null then 0 else net_earning_amt
								end 	net_earning_amt
FROM 			union_all u
)
SELECT 			 from_utc_timestamp(from_unixtime(unix_timestamp()), 'GMT+7') as run_date
					, date_format(date_event, 'yyyy') as year_event
					, date_format(date_event, 'MM') as month_event
					, date_event
					, app_name

					, sum(ins_flag) as 	installed
					, sum(reopend_flag) as reopened

					, sum(unst_flag) as un_installed
					, sum(closed_flag) as closed

					, case when earning_flag = 0
								then sum (sum(ins_flag) + sum(reopend_flag) - sum(unst_flag) - sum(closed_flag))
									OVER (PARTITION BY app_name ORDER BY date_event) end sql_cumulative_merchant

					, earning_flag
					, sum(gross_earning_amount) as gross_earning_amount
					, sum(net_earning_amt) as net_earning_amt

FROM 			get_flag
GROUP BY  date_event, earning_flag, app_name;