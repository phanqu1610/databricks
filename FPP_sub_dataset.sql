----Infusions
WITH infusions
AS (
	SELECT order_id
		,program_id
		,patient_id
		,discount_scheme_id
		,product_name
		,country_code
		,delivered_quantity
		,order_quantity
		,dosage
		,clinical_cycle_number
		,to_date(expected_infusion_date,'yyyy-mm-dd') expected_infusion_date
    ,to_date(infusion_form_intake_date,'yyyy-mm-dd') actual_infusion_date
		,order_date
		,invoice_date
		,delivery_date
		,infusion_form_intake_date
		,invoice_number
		,order_type
		,program_team_member_id
		,status
		,reason_description
		,source
		,source_system
		,data_refresh_dttm
		,CASE 
			WHEN EXTRACT(YEAR FROM to_date(expected_infusion_date,'yyyy-MM-dd')) IN (2024,2025,2026)
				AND STATUS IN ('Infusion Confirmed','Pending Infusion Confirmation')
				THEN cycle_quantity
			WHEN reason_description IN (
					'Approved by MSD (retain approval)'
					,'Technical issue (Unable to upload form)'
					,'Collected but Pending Form'
					,'Completed Treatment'
					,'Patient complete treatment, but Acknowledgement form unreconciled'
					,'ICF Received but not uploaded'
					,'ICF Received but not uploaded in system'
					,'Received - Incomplete'
					,'Technical issue (Unable to upload form)'
					)
				THEN invoice_quantity
			ELSE cycle_quantity
			END AS cycle_quantity
		,CASE 
			WHEN EXTRACT(YEAR FROM to_date(expected_infusion_date,'yyyy-MM-dd')) IN (2024,2025,2026)
				AND reason_description IN (
					'Technical issue (Unable to upload form)'
					,'Collected but Pending Form'
					,'Completed Treatment'
					,'Patient complete treatment, but Acknowledgement form unreconciled'
					,'ICF Received but not uploaded'
					,'ICF Received but not uploaded in system'
					,'Received - Incomplete'
					,'Technical issue (Unable to upload form)'
					,'Completed'            
					)
				THEN 'Infusion Confirmed'
			ELSE STATUS
			END AS updated_status
		,invoice_quantity
  FROM `hh_eu_uat_adp_global`.`market_access`.`vw_fpp__fct_program_cycles_and_orders` 
	WHERE EXTRACT(YEAR FROM to_date(expected_infusion_date,'yyyy-MM-dd')) IN (2024,2025,2026) 
	AND country_code = 'KR'
	)
SELECT order_id
	,program_id
	,patient_id
	,discount_scheme_id
	,product_name
	,country_code
	,delivered_quantity
	,order_quantity
	,dosage
	,clinical_cycle_number
	,expected_infusion_date
  ,infusion_form_intake_date
	,order_date
	,invoice_date
	,delivery_date
	,actual_infusion_date
	,invoice_number
	,order_type
	,program_team_member_id
	,status
	,reason_description
	,source
	,source_system
	,data_refresh_dttm
	,cycle_quantity
	,invoice_quantity
	,updated_status
	,TO_CHAR(expected_infusion_date, 'yyyy-MM') AS period
	,CASE 
		WHEN updated_status IN (
				'Infusion Confirmed'
				,'Pending Infusion Confirmation'
				)
			THEN cycle_quantity
		END AS confirmed_quantity
	,CASE 
		WHEN updated_status NOT IN ('Pending Initiation')
			THEN invoice_quantity
		END AS ordered_quantity
FROM infusions;


----Patients
with pt as
(
select *
,row_number() over (partition by patient_id order by discontinuation_date desc)  rk
from `hh_eu_uat_adp_global`.`market_access`.vw_fpp__fct_patient_details
)
select * from pt where rk=1;



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

----------Avg Cycle
WITH infusions
AS (
	SELECT country_code
		,to_date(expected_infusion_date,'yyyy-mm-dd') expected_infusion_date
		,clinical_cycle_number
		,patient_id
		,program_id
		,discount_scheme_id
		,program_team_member_id
		,STATUS
		,reason_description
		,cycle_quantity
		,invoice_quantity
		,source
		,CASE WHEN STATUS IN ('Infusion Confirmed','Pending Infusion Confirmation') THEN cycle_quantity
			WHEN reason_description IN (
					'Approved by MSD (retain approval)'
					,'Technical issue (Unable to upload form)'
					,'Collected but Pending Form'
					,'Completed Treatment'
					,'Patient complete treatment, but Acknowledgement form unreconciled'
					,'ICF Received but not uploaded'
					,'ICF Received but not uploaded in system'
					,'Received - Incomplete'
					)
				THEN invoice_quantity
			ELSE cycle_quantity
			END AS cycle_quantity_adj
		, CASE 
			WHEN reason_description IN (
					'Technical issue (Unable to upload form)'
					,'Collected but Pending Form'
					,'Completed Treatment'
					,'Patient complete treatment, but Acknowledgement form unreconciled'
					,'ICF Received but not uploaded'
					,'ICF Received but not uploaded in system'
					,'Received - Incomplete'
					)
				THEN 'Infusion Confirmed'
			ELSE STATUS
			END AS updated_status
	FROM `hh_eu_uat_adp_global`.`market_access`.`vw_fpp__fct_program_cycles_and_orders` fpcao
	)
	,patients_in_scope
AS (
	SELECT DISTINCT country_code || '-' || patient_id AS KEY
		,country_code
		,patient_id
	FROM infusions
	WHERE STATUS IN (
			'Infusion Confirmed'
			,'Pending Infusion Confirmation'
			)
		AND EXTRACT(YEAR FROM expected_infusion_date) IN (2024,2025,2026)
	)
	,months
AS (
 SELECT 
 left(DATE, 7) AS month_key
  ,min(DATE) AS month_start
  ,max(DATE) AS month_end
  from `hh_eu_uat_adp_global`.`market_access`.vw_dim_calendar
  where DATE BETWEEN DATE '2024-01-01' AND LAST_DAY(current_date())
 GROUP BY left(DATE, 7) 
	)
	,patient_cohort_by_month
AS (
	SELECT m.month_key
		,m.month_start
		,i.country_code
		,i.patient_id
		,i.program_id
		,i.discount_scheme_id
		,i.program_team_member_id
		,i.source
		,MAX(i.clinical_cycle_number) AS max_cycle
	FROM infusions i
	INNER JOIN patients_in_scope p ON i.country_code || '-' || i.patient_id = p.KEY
	CROSS JOIN months m
	WHERE i.expected_infusion_date <= m.month_end
		AND i.updated_status IN (
			'Infusion Confirmed'
			,'Pending Infusion Confirmation'
			)
	GROUP BY m.month_key
		,i.country_code
		,i.patient_id
		,i.program_id
		,i.discount_scheme_id
		,i.source
		,i.program_team_member_id
		,m.month_start
	)
SELECT *
FROM patient_cohort_by_month;


-----TOT
WITH cycles
AS (
	SELECT fpcao.patient_id
		,fpcao.clinical_cycle_number
		,fpcao.program_id
		,fpcao.program_team_member_id
		,fpcao.discount_scheme_id
		,to_date(fpcao.expected_infusion_date,'yyyy-MM-dd') as expected_infusion
		,expected_infusion_date
		,fpcao.country_code
		,to_char(to_date(expected_infusion_date,'yyyy-MM-dd'),'yyyy-MM')  months
		,max(fpcao.clinical_cycle_number) OVER (PARTITION BY fpcao.patient_id) AS last_cycle
	FROM `hh_eu_uat_adp_global`.`market_access`.`vw_fpp__fct_program_cycles_and_orders` fpcao
	WHERE 1 = 1
		AND fpcao.STATUS <> 'Infusion Cancelled'
		AND fpcao.cycle_quantity IS NOT NULL
		AND fpcao.expected_infusion_date IS NOT NULL
		AND fpcao.expected_infusion_date > DATE '2024-01-01' 
	GROUP BY 1,2,3,4,5,6,7,8
	)
SELECT c1.patient_id
	,c1.last_cycle
	,c1.months
	,c1.program_id
	,c1.program_team_member_id
	,c1.discount_scheme_id
	,c1.country_code
	,sum(datediff(c2.expected_infusion_date,c1.expected_infusion_date)) as days_bw_cycles
FROM cycles c1
JOIN cycles c2 ON c1.patient_id = c2.patient_id
	AND c1.clinical_cycle_number + 1 = c2.clinical_cycle_number
	AND datediff(c2.expected_infusion_date,c1.expected_infusion_date) <= 120
GROUP BY 1,2,3,4,5,6,7;


select distinct program_name
from hh_eu_uat_adp_global.market_access.vw_fpp__dim_care_program
where program_id = 'PROG_2025_0348';

select distinct product_name
FROM `hh_eu_uat_adp_global`.`market_access`.`vw_fpp__fct_program_cycles_and_orders` fpcao where country_code = 'KR'
where program_id = 'PROG_2025_0348';


select *
from hh_eu_uat_adp_global.market_access.vw_fpp__fct_patient_team_details
