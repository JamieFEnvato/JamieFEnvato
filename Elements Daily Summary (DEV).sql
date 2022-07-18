
WITH elements_coupons_prep as (  SELECT
        a.dim_subscription_key,
        a.dim_elements_coupon_key,
        case
                when b.discount_percent=100 then 'full free'
                when lower(b.name) like '%free%' then 'full free'
                when b.dim_elements_coupon_key=0 then 'no coupon'
                else 'partial free'
            end as coupon_type,
        row_number() over (partition by a.dim_subscription_key order by dim_date_key asc) as invoice_number
    FROM
        elements.fact_elements_subscription_transactions a
        join elements.dim_elements_coupon b on (a.dim_elements_coupon_key=b.dim_elements_coupon_key)
    WHERE 1=1 and a.dim_elements_coupon_key>0),

    MaxSub as ( select  max(cast(subscription_start_date as date)) as max_date from elements.dim_elements_subscription),

     SubPrep AS (


    SELECT
        res1.dim_subscription_key,
        cast(res1.first_successful_payment_date_aet as date) as first_successful_payment_date_aet,
        cast(res1.subscription_start_date as date) as subscription_start_date,
        cast(res1.termination_date as date) as termination_date,
        cast(res1.first_successful_non_trivial_payment_date_aet as date) as first_successful_non_trivial_payment_date_aet,
        res1.dim_elements_channel_key,
        f.country as geonetwork_country,
        res1.initial_plan,
        res1.subscription_platform,
        row_number() over (partition by res1.recurly_subscription_id order by res1.subscription_start_date desc) recurly_subscription_id_index,
        case when res1.subscription_started_on_trial is true and res1.subscription_start_date::date>='2021-02-08' and res1.subscription_platform='recurly' then 'free trial' else coalesce(ec.coupon_type,'no coupon') end as coupon_type_first_invoice,
        case when res1.first_successful_non_trivial_payment_date_aet is not null then 'paying sub >$1'
            when res1.first_successful_payment_date_aet is not null and res1.first_successful_non_trivial_payment_date_aet is null then 'paying sub $1 coupon or less'
            when res1.first_successful_payment_date_aet is null then 'on free trial/free coupon' end as has_paying_subscription,
         res1.plan_change,
         res1.is_first_subscription
    FROM
    elements.dim_elements_subscription as res1
    LEFT JOIN market.dim_geo_network f on (res1.dim_geo_network_key=f.dim_geo_network_key)
    LEFT JOIN elements_coupons_prep ec on (res1.dim_subscription_key=ec.dim_subscription_key and ec.invoice_number=1)
    WHERE 1=1 AND res1.subscription_start_date::date< (select max_date from MaxSub)),

Current_year_payment as (
Select  'C' as Period,
        first_successful_non_trivial_payment_date_aet as calendar_date,
        c.channel,
        c.sub_channel,
        c.channel_detail,
        geonetwork_country,
        initial_plan,
        coupon_type_first_invoice,
        has_paying_subscription,
        0 sessions,
        0 visitors,
        0 signups,
        0 terminations,
        0 sessions_ly,
        0 visitors_ly,
        0 signups_ly,
        0 terminations_ly,
        0 first_subs,
        0 return_subs,
        0 total_subs,
        0 first_subs_annual,
        0 return_subs_annual,
        0 total_subs_annual,
        0 first_subs_ly,
        0 return_subs_ly,
        0 total_subs_ly,
        0 first_subs_annual_ly,
        0 return_subs_annual_ly,
        0 total_subs_annual_ly,
        count(distinct case when is_first_subscription then dim_subscription_key end) as first_subs_payment,
        count(distinct case when not is_first_subscription then dim_subscription_key end) as return_subs_payment,
        count(distinct dim_subscription_key) as total_subs_payment,
        count(distinct case when is_first_subscription and res1.initial_plan like '%_annual'  then dim_subscription_key end) as first_subs_annual_payment,
        count(distinct case when not is_first_subscription and res1.initial_plan like '%_annual'   then dim_subscription_key end) as return_subs_annual_payment,
        count(distinct  case when res1.initial_plan like '%_annual'  then dim_subscription_key end ) as total_subs_annual_payment,
        0 first_subs_payment_ly,
        0 return_subs_payment_ly,
        0 total_subs_payment_ly,
        0 first_subs_annual_payment_ly,
        0 return_subs_annual_payment_ly,
        0 total_subs_annual_payment_ly
FROM SubPrep res1
LEFT JOIN elements.dim_elements_channel c on (res1.dim_elements_channel_key=c.dim_elements_channel_key)
    WHERE 1=1
        AND res1.first_successful_non_trivial_payment_date_aet::date<(select max_date from MaxSub)
        AND res1.first_successful_non_trivial_payment_date_aet::date>=dateadd('year',-2,date_trunc('year',(select max_date from MaxSub)))
    AND NOT res1.plan_change
GROUP BY 1,2,3,4,5,6,7,8,9),


last_year_payment as
(
Select  'L' as Period,
        date_add('year',+1,calendar_date) as calendar_date,
        channel,
        sub_channel,
        channel_detail,
        geonetwork_country,
        initial_plan,
        coupon_type_first_invoice,
        has_paying_subscription,
        0 sessions,
        0 visitors,
        0 signups,
        0 terminations,
        0 sessions_ly,
        0 visitors_ly,
        0 signups_ly,
        0 terminations_ly,
        0 first_subs,
        0 return_subs,
        0 total_subs,
        0 first_subs_annual,
        0 return_subs_annual,
        0 total_subs_annual,
        0 first_subs_ly,
        0 return_subs_ly,
        0 total_subs_ly,
        0 first_subs_annual_ly,
        0 return_subs_annual_ly,
        0 total_subs_annual_ly,
        0 first_subs_payment,
        0 return_subs_payment,
        0 total_subs_payment,
        0 first_subs_annual_payment,
        0 return_subs_annual_payment,
        0 total_subs_annual_payment,
        first_subs_payment first_subs_payment_ly,
        return_subs_payment return_subs_payment_ly,
        total_subs_payment total_subs_payment_ly,
        first_subs_annual_payment first_subs_annual_payment_ly,
        return_subs_annual_payment return_subs_annual_payment_ly,
        total_subs_annual_payment total_subs_annual_payment_ly
FROM Current_year_payment),


Current_Year_Sub as (
Select  'C' as Period,
        subscription_start_date as calendar_date,
        c.channel,
        c.sub_channel,
        c.channel_detail,
        geonetwork_country,
        initial_plan,
        coupon_type_first_invoice,
        has_paying_subscription,
        0 sessions,
        0 visitors,
        0 signups,
        0 terminations,
        0 sessions_ly,
        0 visitors_ly,
        0 signups_ly,
        0 terminations_ly,
        count(distinct case when is_first_subscription then dim_subscription_key end) as first_subs,
        count(distinct case when not is_first_subscription then dim_subscription_key end) as return_subs,
        count(distinct dim_subscription_key) as total_subs,
        count(distinct case when is_first_subscription and res1.initial_plan like '%_annual'  then dim_subscription_key end) as first_subs_annual,
        count(distinct case when not is_first_subscription and res1.initial_plan like '%_annual'   then dim_subscription_key end) as return_subs_annual,
        count(distinct  case when res1.initial_plan like '%_annual'  then dim_subscription_key end ) as total_subs_annual,
        0 first_subs_ly,
        0 return_subs_ly,
        0 total_subs_ly,
        0 first_subs_annual_ly,
        0 return_subs_annual_ly,
        0 total_subs_annual_ly,
        0 first_subs_payment,
        0 return_subs_payment,
        0 total_subs_payment,
        0 first_subs_annual_payment,
        0 return_subs_annual_payment,
        0 total_subs_annual_payment,
        0 first_subs_payment_ly,
        0 return_subs_payment_ly,
        0 total_subs_payment_ly,
        0 first_subs_annual_payment_ly,
        0 return_subs_annual_payment_ly,
        0 total_subs_annual_payment_ly
FROM SubPrep res1
LEFT JOIN elements.dim_elements_channel c on (res1.dim_elements_channel_key=c.dim_elements_channel_key)
    WHERE 1=1
        AND res1.subscription_start_date::date>=dateadd('year',2,date_trunc('year',(select max_date from MaxSub)))
    AND NOT res1.plan_change
GROUP BY 1,2,3,4,5,6,7,8,9),

Last_Year_Sub as (
Select  'L' as Period,
        date_add('year',+1,calendar_date) as calendar_date,
        channel,
        sub_channel,
        channel_detail,
        geonetwork_country,
        initial_plan,
        coupon_type_first_invoice,
        has_paying_subscription,
        0 sessions,
        0 visitors,
        0 signups,
        0 terminations,
        0 sessions_ly,
        0 visitors_ly,
        0 signups_ly,
        0 terminations_ly,
        0 first_subs,
        0 return_subs,
        0 total_subs,
        0 first_subs_annual,
        0 return_subs_annual,
        0 total_subs_annual,
        first_subs first_subs_ly,
        return_subs return_subs_ly,
        total_subs total_subs_ly,
        first_subs_annual first_subs_annual_ly,
        return_subs_annual return_subs_annual_ly,
        total_subs_annual total_subs_annual_ly,
        0 first_subs_payment,
        0 return_subs_payment,
        0 total_subs_payment,
        0 first_subs_annual_payment,
        0 return_subs_annual_payment,
        0 total_subs_annual_payment,
        0 first_subs_payment_ly,
        0 return_subs_payment_ly,
        0 total_subs_payment_ly,
        0 first_subs_annual_payment_ly,
        0 return_subs_annual_payment_ly,
        0 total_subs_annual_payment_ly
FROM Current_Year_Sub),

Current_year_Sessions as (
 select
         'C' as period,
        a.date_aest::date as calendar_date,
        c.channel,
        c.sub_channel,
        c.channel_detail,
        a.geonetwork_country,
        null as initial_plan,
        null as coupon_type_first_invoice,
        null as has_paying_subscription,
        count(distinct a.sessionid) as sessions,
        count(distinct a.fullvisitorid) as visitors,
        0 signups,
        0 terminations,
        0 sessions_ly,
        0 visitors_ly,
        0 signups_ly,
        0 terminations_ly,
        0 first_subs,
        0 return_subs,
        0 total_subs,
        0 first_subs_annual,
        0 return_subs_annual,
        0 total_subs_annual,
        0 first_subs_ly,
        0 return_subs_ly,
        0 total_subs_ly,
        0 first_subs_annual_ly,
        0 return_subs_annual_ly,
        0 total_subs_annual_ly,
        0 first_subs_payment,
        0 return_subs_payment,
        0 total_subs_payment,
        0 first_subs_annual_payment,
        0 return_subs_annual_payment,
        0 total_subs_annual_payment,
        0 first_subs_payment_ly,
        0 return_subs_payment_ly,
        0 total_subs_payment_ly,
        0 first_subs_annual_payment_ly,
        0 return_subs_annual_payment_ly,
        0 total_subs_annual_payment_ly
     FROM
        webanalytics.ds_bq_sessions_elements a
        left join elements.rpt_elements_session_channel c on (a.sessionid=c.sessionid)
    where 1=1
	and a.date_aest::date>=dateadd('year', -2, date_trunc('year',getdate_aest()))::date
    and a.date<to_char(getdate_aest(),'YYYYMMDD')::INT
    group by 1,2,3,4,5,6),

    Last_Year_Sessions as (

        SELECT
         'L' as period,
        date_add('year',+1,calendar_date) as calendar_date,
        channel,
        sub_channel,
        channel_detail,
        geonetwork_country,
        initial_plan,
        coupon_type_first_invoice,
        has_paying_subscription,
        0 sessions,
        0 visitors,
        0 signups,
        0 terminations,
        sessions sessions_ly,
        visitors visitors_ly,
        0 signups_ly,
        0 terminations_ly,
        0 first_subs,
        0 return_subs,
        0 total_subs,
        0 first_subs_annual,
        0 return_subs_annual,
        0 total_subs_annual,
        0 first_subs_ly,
        0 return_subs_ly,
        0 total_subs_ly,
        0 first_subs_annual_ly,
        0 return_subs_annual_ly,
        0 total_subs_annual_ly,
        0 first_subs_payment,
        0 return_subs_payment,
        0 total_subs_payment,
        0 first_subs_annual_payment,
        0 return_subs_annual_payment,
        0 total_subs_annual_payment,
        0 first_subs_payment_ly,
        0 return_subs_payment_ly,
        0 total_subs_payment_ly,
        0 first_subs_annual_payment_ly,
        0 return_subs_annual_payment_ly,
        0 total_subs_annual_payment_ly
        from Current_year_Sessions),

Current_year_signups as (
    select

        'C' as Period,
        cast(a.signup_date as date) as calendar_date,
        null channel,
        null sub_channel,
        null channel_detail,
        null geonetwork_country,
        null initial_plan,
        null coupon_type_first_invoice,
        null has_paying_subscription,
        0 sessions,
        0 visitors,
        count(*) as signups,
        0 terminations,
        0 sessions_ly,
        0 visitors_ly,
        0 signups_ly,
        0 terminations_ly,
        0 first_subs,
        0 return_subs,
        0 total_subs,
        0 first_subs_annual,
        0 return_subs_annual,
        0 total_subs_annual,
        0 first_subs_ly,
        0 return_subs_ly,
        0 total_subs_ly,
        0 first_subs_annual_ly,
        0 return_subs_annual_ly,
        0 total_subs_annual_ly,
        0 first_subs_payment,
        0 return_subs_payment,
        0 total_subs_payment,
        0 first_subs_annual_payment,
        0 return_subs_annual_payment,
        0 total_subs_annual_payment,
        0 first_subs_payment_ly,
        0 return_subs_payment_ly,
        0 total_subs_payment_ly,
        0 first_subs_annual_payment_ly,
        0 return_subs_annual_payment_ly,
        0 total_subs_annual_payment_ly

    FROM
        elements.rpt_elements_user_signup_session a
        LEFT JOIN webanalytics.ds_bq_sessions_elements b on a.sessionid=b.sessionid
        LEFT JOIN elements.rpt_elements_session_channel c on a.sessionid=c.sessionid
        --if a channel exists in factors table we take channel level factor
        WHERE 1=1
        AND a.signup_date::date<getdate_aest()::date
        AND a.signup_date::date>=dateadd('year',-2,date_trunc('year',getdate_aest()))::date
    group by 1,2),

   Last_year_signups as (

    select 'L' as Period,
        dateadd('year', +1, calendar_date)  as calendar_date,
        channel,
        sub_channel,
        channel_detail,
        geonetwork_country,
        initial_plan,
        coupon_type_first_invoice,
        has_paying_subscription,
        0 sessions,
        0 visitors,
        0 signups,
        0 terminations,
        0 sessions_ly,
        0 visitors_ly,
        signups signups_ly,
        0 terminations_ly,
        0 first_subs,
        0 return_subs,
        0 total_subs,
        0 first_subs_annual,
        0 return_subs_annual,
        0 total_subs_annual,
        0 first_subs_ly,
        0 return_subs_ly,
        0 total_subs_ly,
        0 first_subs_annual_ly,
        0 return_subs_annual_ly,
        0 total_subs_annual_ly,
        0 first_subs_payment,
        0 return_subs_payment,
        0 total_subs_payment,
        0 first_subs_annual_payment,
        0 return_subs_annual_payment,
        0 total_subs_annual_payment,
        0 first_subs_payment_ly,
        0 return_subs_payment_ly,
        0 total_subs_payment_ly,
        0 first_subs_annual_payment_ly,
        0 return_subs_annual_payment_ly,
        0 total_subs_annual_payment_ly

    FROM Current_year_signups),

Current_Year_Terminations as

 (
                /*NB: This query will only return queries if the dates go back to 2019 or less*/
            select
            'C' as period,
            cast(termination_date as date) as calendar_date,
            b.channel as channel,
            b.sub_channel as sub_channel,
            b.channel_detail AS channel_detail,
            geonetwork_country,
            initial_plan,
            coupon_type_first_invoice,
            has_paying_subscription,
            0 sessions,
            0 visitors,
            0 as signups,
            sum(1) terminations,
            0 sessions_ly,
            0 visitors_ly,
            0 signups_ly,
            0 terminations_ly,
            0 first_subs,
            0 return_subs,
            0 total_subs,
            0 first_subs_annual,
            0 return_subs_annual,
            0 total_subs_annual,
            0 first_subs_ly,
            0 return_subs_ly,
            0 total_subs_ly,
            0 first_subs_annual_ly,
            0 return_subs_annual_ly,
            0 total_subs_annual_ly,
            0 first_subs_payment,
            0 return_subs_payment,
            0 total_subs_payment,
            0 first_subs_annual_payment,
            0 return_subs_annual_payment,
            0 total_subs_annual_payment,
            0 first_subs_payment_ly,
            0 return_subs_payment_ly,
            0 total_subs_payment_ly,
            0 first_subs_annual_payment_ly,
            0 return_subs_annual_payment_ly,
            0 total_subs_annual_payment_ly
                    --If Recurly - need to examine the lastest record by start date, and check if this one has a termination date or not
            from
                    SubPrep a
                    join elements.dim_elements_channel b on (a.dim_elements_channel_key=b.dim_elements_channel_key)
            where
                    subscription_platform='braintree'
                    and termination_date is not null
                    AND termination_date::date <envato.getdate_aest()::date
                    AND termination_date::date>=dateadd('year',-2,date_trunc('year',envato.getdate_aest()))::date
            group by 1,2,3,4,5,6,7,8,9

            union all
            --Recurly terminations
            --If Recurly - need to examine the lastest record by start date, and check if this one has a termination date or not
            select
            'C' as period,
            cast(termination_date as date) as calendar_date,
            b.channel as channel,
            b.sub_channel as sub_channel,
            b.channel_detail AS channel_detail,
            geonetwork_country,
            initial_plan,
            coupon_type_first_invoice,
            has_paying_subscription,
            0 sessions,
            0 visitors,
            0 as signups,
            sum(1) terminations,
            0 sessions_ly,
            0 visitors_ly,
            0 signups_ly,
            0 terminations_ly,
            0 first_subs,
            0 return_subs,
            0 total_subs,
            0 first_subs_annual,
            0 return_subs_annual,
            0 total_subs_annual,
            0 first_subs_ly,
            0 return_subs_ly,
            0 total_subs_ly,
            0 first_subs_annual_ly,
            0 return_subs_annual_ly,
            0 total_subs_annual_ly,
            0 first_subs_payment,
            0 return_subs_payment,
            0 total_subs_payment,
            0 first_subs_annual_payment,
            0 return_subs_annual_payment,
            0 total_subs_annual_payment,
            0 first_subs_payment_ly,
            0 return_subs_payment_ly,
            0 total_subs_payment_ly,
            0 first_subs_annual_payment_ly,
            0 return_subs_annual_payment_ly,
            0 total_subs_annual_payment_ly
            from SubPrep a
                    join elements.dim_elements_channel b on (a.dim_elements_channel_key=b.dim_elements_channel_key)
            where   recurly_subscription_id_index=1
                    AND subscription_platform<>'braintree'
                    AND termination_date is not null
                    AND termination_date::date <envato.getdate_aest()::date
                    AND termination_date::date>=dateadd('year',-2,date_trunc('year',envato.getdate_aest()))::date
            group by 1,2,3,4,5,6,7,8,9
    )
,

Last_Year_Terminations as
    (   SELECT
        'L' as Period,
        date_add('year',+1,calendar_date) as calendar_date,
        channel,
        sub_channel,
        channel_detail,
        geonetwork_country,
        initial_plan,
        coupon_type_first_invoice,
        has_paying_subscription,
        0 sessions,
            0 visitors,
            0 as signups,
            0 as terminations,
            0 sessions_ly,
            0 visitors_ly,
            0 signups_ly,
            terminations terminations_ly,
            0 first_subs,
            0 return_subs,
            0 total_subs,
            0 first_subs_annual,
            0 return_subs_annual,
            0 total_subs_annual,
            0 first_subs_ly,
            0 return_subs_ly,
            0 total_subs_ly,
            0 first_subs_annual_ly,
            0 return_subs_annual_ly,
            0 total_subs_annual_ly,
            0 first_subs_payment,
            0 return_subs_payment,
            0 total_subs_payment,
            0 first_subs_annual_payment,
            0 return_subs_annual_payment,
            0 total_subs_annual_payment,
            0 first_subs_payment_ly,
            0 return_subs_payment_ly,
            0 total_subs_payment_ly,
            0 first_subs_annual_payment_ly,
            0 return_subs_annual_payment_ly,
            0 total_subs_annual_payment_ly
    FROM Current_Year_Terminations a
)



select *
    from Current_year_payment
UNION ALL
select *
    from Last_year_payment
UNION ALL
select *
    from Current_year_sub
UNION ALL
select *
    from Last_year_sub
UNION ALL
select *
    from Current_year_Sessions
UNION ALL
select *
    from Last_year_Sessions
UNION ALL
select *
    from Current_year_Signups
UNION ALL
select *
    from Last_year_Signups
UNION ALL
select *
    from Current_Year_Terminations
UNION ALL
select *
    from Last_Year_Terminations
