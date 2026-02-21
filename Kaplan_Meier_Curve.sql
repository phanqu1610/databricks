
----Activities
 WITH months
AS (
 SELECT 
 left(DATE, 7) AS month_key
  ,min(DATE) AS month_start
  ,max(DATE) AS month_end
  from `hh_eu_uat_adp_global`.`market_access`.vw_dim_calendar
  where DATE BETWEEN DATE '2024-01-01' AND LAST_DAY(current_date())
 GROUP BY left(DATE, 7) 
 )
 ,infusion
AS (
 SELECT to_date(expected_infusion_date,'yyyy-mm-dd') expected_infusion_date
  ,invoice_date
  ,fpcao.patient_id
  ,fpcao.program_id
  ,fpcao.program_team_member_id
	,fpcao.discount_scheme_id
  ,fpcao.STATUS
  ,fpcao.country_code
  ,fpcao.clinical_cycle_number
  ,fpcao.order_date
  ,fpcao.source
  ,fpcao.source_system
  ,m.month_key
  ,m.month_end
  ,m.month_start
  ,CASE 
   WHEN fpcao.expected_infusion_date::DATE <= m.month_end
    THEN DATEDIFF(day, to_date(expected_infusion_date,'yyyy-mm-dd')::DATE, m.month_end)
   ELSE NULL
   END AS indays
  ,CASE 
   WHEN order_date::DATE BETWEEN DATEADD(day, - 120, m.month_end)
     AND m.month_end
    THEN DATEDIFF(day, order_date::DATE, m.month_end)
   END AS indays_ordered
	 ,CASE 
   WHEN invoice_date::DATE BETWEEN DATEADD(day, - 120, m.month_end)
     AND m.month_end
    THEN DATEDIFF(day, invoice_date::DATE, m.month_end)
   END AS indays_invoiced
 FROM `hh_eu_uat_adp_global`.`market_access`.`vw_fpp__fct_program_cycles_and_orders` fpcao
 CROSS JOIN months m
 WHERE 1 = 1
  AND to_date(expected_infusion_date,'yyyy-mm-dd')::DATE >= '2024-01-01'
  AND to_date(expected_infusion_date,'yyyy-mm-dd') IS NOT NULL
  AND nvl(reason_description, '') NOT IN (
   'Quarantined'
   ,'Discarded as the infusion postponed and vial crossed date of usage'
   ,'Re-assigned'
   )
  AND fpcao.clinical_cycle_number > 0
 )
SELECT infusion.patient_id
 ,infusion.program_id
 ,program_team_member_id
 ,discount_scheme_id
 ,country_code
 ,month_key
 ,month_start
 ,source
 ,case when min(indays) <= 120 then 1 ELSE 0 end events
 ,MIN(indays) AS indays
 ,min(indays_ordered) AS indays_ordered
 ,min(indays_invoiced) AS indays_invoiced
FROM infusion
WHERE indays IS NOT NULL
GROUP BY infusion.patient_id
 ,infusion.program_id
 ,discount_scheme_id
 ,program_team_member_id
 ,source
 ,country_code
 ,month_key
 ,month_start
  ;

