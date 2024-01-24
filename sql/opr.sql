select event.event_date,sum(ins_flag) as installed,sum(reopend_flag) as reopened,sum(unst_flag) as uninstalled,sum(closed_flag) as closed,
       sum (sum(ins_flag) + sum(reopend_flag) - sum(unst_flag) - sum(closed_flag))
									OVER (ORDER BY event_date) as cumulative_merchant
from (select event,
             date_format(from_utc_timestamp(occurred_at, 'GMT+7'), 'yyyy-MM-dd') as                    event_date,
             case when event = 'RELATIONSHIP_INSTALLED' then 1 else 0 end   ins_flag,
             case when event = 'RELATIONSHIP_REACTIVATED' then 1 else 0 end reopend_flag,
             case when event = 'RELATIONSHIP_UNINSTALLED' then 1 else 0 end unst_flag,
             case when event = 'RELATIONSHIP_DEACTIVATED' then 1 else 0 end closed_flag
      from shopify_histories
      where app_name = 'social_shop' and occurred_at is not null
        and id not in (select sub.id
                       from (select DISTINCT id,
                                             case
                                                 when
                                                     lag(event) OVER (PARTITION by app_name, shop_id ORDER BY occurred_at) =
                                                     'RELATIONSHIP_DEACTIVATED'
                                                         and event = 'RELATIONSHIP_UNINSTALLED' then 1
                                                 end flag
                             from shopify_histories
                             where app_name = 'social_shop'
                               and event in
                                   ('RELATIONSHIP_INSTALLED', 'RELATIONSHIP_UNINSTALLED', 'RELATIONSHIP_DEACTIVATED',
                                    'RELATIONSHIP_REACTIVATED')) as sub
                       where sub.flag = 1)) as event
    group by event.event_date
    order by event.event_date ASC