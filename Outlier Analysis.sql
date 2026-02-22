--RISK 1: patients with more than 35 cycles  
SELECT *
FROM hh_eu_uat_adp_global.market_access.vw_fpp__fct_patient_journey_risk_details 
WHERE risk_id = 1 ;


--RISK 2: patients with more than 2 years of treatment.
SELECT *
FROM hh_eu_uat_adp_global.market_access.vw_fpp__fct_patient_journey_risk_details 
WHERE risk_id = 2 ;


--RISK 3: Duplicate cycles.
SELECT *
FROM hh_eu_uat_adp_global.market_access.vw_fpp__fct_patient_journey_risk_details 
WHERE risk_id = 3 ;

