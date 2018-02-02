-- *******************************************************************************************************
-- STEP 6
--		Pull all selected data entities into temp tables before final export
-- ******************************************************************************************************* 
--------------------------------------------------------------------------------
--	STEP 6.1: Create Labs table
--------------------------------------------------------------------------------  
DROP TABLE xdr_Wherry_preg_lab PURGE;
CREATE TABLE xdr_Wherry_preg_lab AS 
SELECT 	DISTINCT coh.pat_id,
                o.pat_enc_csn_id, 
                o.order_proc_id, 
                p.proc_id, 
                p.proc_code, 
                p.description, 
                o.component_id, 
                cc.name component_name, 
                p.order_time, 
                p.result_time, 
                o.result_date, 
                trim(o.ord_value) as ord_value, 
                o.ord_num_value, 
                o.reference_unit, 
                o.ref_normal_vals, 
                o.reference_low, 
                o.reference_high,
                p.order_status_c, 
                p.order_type_c,
                o.RESULT_FLAG_C,
                op2.specimn_taken_time,
		--If there is a relevant operator in this field ('%','<','>','='), it gets captured in its own field
                case when regexp_like(ord_value,'[%<>]=*','i') then regexp_substr(o.ord_value,'[><%]=*') else null end as harm_sign,
                trim(o.ord_value) as harm_text_val,
		/*
		In the following case statement, the code identifies three different value patterns and applies different strategies to clean the data:
		-If the result includes ':', or text, ':' it replaces with a default value. Ex 'NEGATIVE' or '12-19-08 6:45AM' --> '9999999'
		-If the result includes '<','>',or'=', the code strips that character and formats the number accordingly. Ex '<0.04' --> '0.04')
		-If the result includes '%', the code strips that character and formats the number accordingly. Ex. '28%' --> '28'
		
		All formatting shall respect decimal values
		*/
                case when regexp_like(ord_value,':','i')
                  or regexp_substr(ord_value,'[1-9]\d*(\.\,\d+)?') is null
                       then ord_num_value
                  when regexp_like(ord_value,'[<>]=*','i')
                       then to_number(regexp_substr(ord_value,'-?[[:digit:],.]*$'),'9999999999D9999999999', 'NLS_NUMERIC_CHARACTERS = ''.,''' )
                  when regexp_like(ord_value,'%','i') 
                       then to_number(regexp_substr(ord_value,'[1-9]\d*(\.\,\d+)?'),'9999999999D9999999999', 'NLS_NUMERIC_CHARACTERS = ''.,''' )
                  else ord_num_value end as harm_num_val,
                cc.common_name
              FROM clarity.order_results        o
              JOIN XDR_WHERRY_preg_pat          coh ON o.pat_id = coh.pat_id
              JOIN clarity.order_proc           p   ON p.order_proc_id = o.order_proc_id 
              JOIN clarity.clarity_component    cc  ON o.component_id = cc.component_id
              LEFT JOIN clarity.order_proc_2    op2 ON p.ORDER_PROC_ID = op2.ORDER_PROC_ID
              where p.order_type_c in (7, 26, 62, 63)			--doulbe check this codes
                      and p.ordering_date between to_date('10/01/2014','mm/dd/yyyy') and to_date('10/01/2016','mm/dd/yyyy')
                      and o.ord_value is not null
                      and o.order_proc_id is not null
;

CREATE INDEX xdr_Wherry_preg_lab_ENCIDIX ON xdr_Wherry_preg_lab(pat_enc_csn_id);

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_Wherry_preg_LAB' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	--    3,736(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 		--2,555,351(9/5/17)
FROM XDR_Wherry_preg_LAB;
COMMIT;
                       

--------------------------------------------------------------------------------
--	STEP 6.2: Create Meds table
-------------------------------------------------------------------------------- 
DROP TABLE XDR_Wherry_preg_med PURGE;
CREATE TABLE XDR_Wherry_preg_med as
select DISTINCT med1.*,
	cm.pharm_class_c,
	zpc.name as pharm_class,
	cm.thera_class_c,
	ztc.name as thera_class,
	cm.pharm_subclass_c,
	zsc.name as pharm_subclass,
  cm.name medication_name, 
	cm.generic_name
FROM (
SELECT  m.pat_id,
	m.pat_enc_csn_id, 
	m.order_med_id, 
	/*m.ordering_date, 
	m.start_date,
	m.end_date,*/
	m.ORDER_INST,
	m.ORDER_START_TIME,
	m.ORDER_END_TIME,
	/*
	In some circumstances, for example when Intelligent Medication Selection selects an IMS mixture, this column may contain template records that do not represent real
medications. For this reason, it is recommended to use ORDER_MEDINFO. DISPENSABLE_MED_ID when reporting on medication orders.
Additionally, in some cases where dispensable_med_id is not populated, user_sel_med_id is the field form where to obtain the medication_id
*/
  case when m.medication_id != 800001 then m.medication_id
       else coalesce(omi.dispensable_med_id, m.user_sel_med_id) end as used_med_id,        
	m.medication_id, 
  --omi.dispensable_med_id,
  --m.user_sel_med_id,
	m.hv_discrete_dose,
	zmu.name as dose_unit,
    m.MED_DIS_DISP_QTY,
    zmudis.name as dis_dose_unit,
    zos.name as order_status,
    zom.name as ordering_mode,
    zoc.name as order_class,
	omi.last_admin_inst,
	m.sig,
	m.quantity,
	ipf.freq_name,
	m.refills,
	rou.NAME                    AS route_name,
    rou.abbr                    AS route_abbreviation,
	mar.INFUSION_RATE,
	mar.MAR_INF_RATE_UNIT_C,
	zmudis.name as inf_rate_dose_unit
FROM clarity.order_med m 
JOIN XDR_WHERRY_preg_pat            coh ON m.pat_id = coh.pat_id
--JOIN XDR_Wherry_preg_COH  coh on m.pat_enc_csn_id = coh.pat_enc_csn_id
LEFT JOIN clarity.order_medinfo     omi on m.order_med_id = omi.order_med_id
LEFT JOIN clarity.mar_admin_info    mar   ON m.order_med_id = mar.order_med_id
LEFT JOIN clarity.zc_admin_route    rou   ON mar.route_c = rou.MED_ROUTE_C
left join clarity.ip_frequency      ipf on m.hv_discr_freq_id = ipf.freq_id
left join clarity.zc_med_unit       zmu on m.hv_dose_unit_c = zmu.disp_qtyunit_c
left join clarity.zc_order_status   zos on m.order_status_c = zos.order_status_c
left join clarity.zc_ordering_mode  zom on m.ordering_mode_c = zom.ordering_mode_c
left join clarity.zc_med_unit       zmudis on m.MED_DIS_DISP_UNIT_C = zmudis.disp_qtyunit_c
left join clarity.zc_med_unit       zmudis2 on mar.MAR_INF_RATE_UNIT_C = zmudis2.disp_qtyunit_c
left join clarity.zc_order_class zoc on m.order_class_C = zoc.order_class_c
WHERE m.ordering_date is not null
	AND m.start_date is not null
) med1
LEFT JOIN clarity.clarity_medication cm on med1.used_med_id = cm.medication_id
left join clarity.zc_pharm_class zpc on cm.pharm_class_c = zpc.pharm_class_c
left join clarity.zc_thera_class ztc on cm.thera_class_c = ztc.thera_class_c
left join clarity.zc_pharm_subclass zsc on cm.pharm_subclass_c = zsc.pharm_subclass_c
;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_Wherry_preg_MED' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	--  3,734(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 		--249,042(9/5/17)
FROM XDR_Wherry_preg_MED;
COMMIT;

--------------------------------------------------------------------------------
--	STEP 6.3: Create Allergies table
--------------------------------------------------------------------------------
DROP TABLE xdr_Wherry_preg_alg PURGE;
CREATE TABLE xdr_Wherry_preg_alg as
SELECT DISTINCT pat.pat_id 
               ,alg.allergy_id
               ,alg.allergen_id
               ,alg.description
               ,alg.reaction
               ,alg.date_noted
               ,alg.severity_c
               ,xsv.name                  AS severity
               ,alg.allergy_severity_c
               ,xas.name                  AS allergy_severity
               ,alg.alrgy_status_c
               ,xst.name                  AS allergy_status
               ,alg.alrgy_dlet_rsn_c
               ,xdr.name                  AS delete_reason
               ,alg.alrgy_dlt_cmt
               ,alg.alrgy_entered_dttm
  FROM XDR_Wherry_preg_PAT                 pat
  JOIN clarity.allergy                  alg ON pat.pat_id = alg.pat_id
  LEFT JOIN clarity.zc_severity         xsv ON alg.severity_c = xsv.severity_c
  LEFT JOIN clarity.zc_allergy_severit  xas ON alg.allergy_severity_c = xas.allergy_severity_c
  LEFT JOIN clarity.zc_alrgy_status     xst ON alg.alrgy_status_c = xst.alrgy_status_c
  LEFT JOIN clarity.zc_alrgy_dlet_rsn   xdr ON alg.alrgy_dlet_rsn_c = xdr.alrgy_dlet_rsn_c
  WHERE
      alg.ALRGY_STATUS_C = 1 		--'Active'                       --allergy_status can be 'deleted' too
;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_Wherry_preg_ALG' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 		
FROM XDR_Wherry_preg_ALG;
COMMIT;
--------------------------------------------------------------------------------
--	STEP 6.4: Create Diagnoses table
--------------------------------------------------------------------------------
	--------------------------------------------------------------------------------
	--	STEP 6.4.1: Create destination table
	--------------------------------------------------------------------------------
DROP TABLE XDR_Wherry_preg_DX PURGE;
 CREATE TABLE XDR_Wherry_preg_DX
   (	"PAT_ID" VARCHAR2(18 BYTE), 
	"PAT_ENC_CSN_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"CONTACT_DATE" DATE, 
	"ICD_CODE" VARCHAR2(254 BYTE), 
	"ICD_TYPE" NUMBER, 
	"PRIMARY_SEC_FLAG" CHAR(1 BYTE), 
	"ADMIT_DX_FLAG" CHAR(1 BYTE), 
	"POA_FLAG" VARCHAR2(50 BYTE), 
	"HSP_FINAL_DX_FLAG" CHAR(1 BYTE)
   );
   
	--------------------------------------------------------------------------------
	--	STEP 6.4.2: Initial load from pat_enc_dx table
	--------------------------------------------------------------------------------
insert into XDR_Wherry_preg_DX
SELECT coh.pat_id, 
	   dx.pat_enc_csn_id, 
	   dx.contact_date, 
	   edg.code as icd_code,
     9 as icd_type,
     'P' as primary_sec_flag,
     null as admit_dx_flag,
     null as poa_flag,
     null as hsp_final_dx_flag
FROM XDR_Wherry_preg_PAT coh
JOIN clarity.pat_enc_dx dx ON coh.pat_id = dx.pat_id
JOIN clarity.edg_current_icd9 edg ON dx.dx_id = edg.dx_id
WHERE dx.primary_dx_yn = 'Y' 
  and edg.code not like 'IMO0%'
UNION
SELECT coh.pat_id, 
	   dx.pat_enc_csn_id, 
	   dx.contact_date, 
	   edg.code as icd_code,
	   10 as icd_type,
     'P' as primary_sec_flag,
     null as admit_dx_flag,
     null as poa_flag,
     null as hsp_final_dx_flag
FROM XDR_Wherry_preg_PAT coh
JOIN clarity.pat_enc_dx dx ON coh.pat_id = dx.pat_id
JOIN clarity.edg_current_icd10 edg ON dx.dx_id = edg.dx_id
WHERE dx.primary_dx_yn = 'Y' 
  and edg.code not like 'IMO0%'
;
commit;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_Wherry_preg_DX' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	--
	,COUNT(*) AS TOTAL_COUNT 		--10,939 rows inserted.(9/5/17)
FROM XDR_Wherry_preg_DX;
COMMIT;

	--------------------------------------------------------------------------------
	--	STEP 6.4.3: Now Load Secondary Dx if they don't already exist.
	--------------------------------------------------------------------------------
merge into XDR_Wherry_preg_DX lcd
using
(SELECT coh.pat_id, 
	   dx.pat_enc_csn_id, 
	   dx.contact_date, 
	   edg.code as icd_code,
     9 as icd_type
FROM XDR_Wherry_preg_PAT coh
JOIN clarity.pat_enc_dx dx ON coh.pat_id = dx.pat_id
JOIN clarity.edg_current_icd9 edg ON dx.dx_id = edg.dx_id
WHERE (dx.primary_dx_yn is null or  dx.primary_dx_yn != 'Y')
    and edg.code not like 'IMO0%'
UNION
SELECT coh.pat_id, 
	   dx.pat_enc_csn_id,
	   dx.contact_date, 
	   edg.code as icd_code,
	   10 as icd_type
FROM XDR_Wherry_preg_PAT coh
JOIN clarity.pat_enc_dx dx ON coh.pat_id = dx.pat_id
JOIN clarity.edg_current_icd10 edg ON dx.dx_id = edg.dx_id
WHERE (dx.primary_dx_yn is null or  dx.primary_dx_yn != 'Y')
    and edg.code not like 'IMO0%'
) adm 
on (lcd.pat_id = adm.pat_id
  and lcd.pat_enc_csn_id = adm.pat_enc_csn_id
  and lcd.contact_date = adm.contact_date
  and lcd.icd_code = adm.icd_code)
when not matched then
  insert (pat_id, pat_enc_csn_id, contact_date, icd_code, icd_type, primary_sec_flag, admit_dx_flag, poa_flag, hsp_final_dx_flag)
  values (adm.pat_id, adm.pat_enc_csn_id, adm.contact_date, adm.icd_code, adm.icd_type, 'S', null, null, null)
;
commit;
--195,913 rows merged.(9/5/17)

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_Wherry_preg_DX' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	--
	,COUNT(*) AS TOTAL_COUNT 		--206,852(9/5/17)
FROM XDR_Wherry_preg_DX;
COMMIT;

	--------------------------------------------------------------------------------
	--	STEP 6.4.4: Now Load Admit Dx
	--------------------------------------------------------------------------------
MERGE INTO XDR_Wherry_preg_DX lcd
using
  (select
		  coh.pat_id,
		  hd.pat_enc_csn_id,
		  trunc(dt.calendar_dt) as contact_date,
		  edg.code as icd_code,
		  9 as icd_type
    FROM XDR_Wherry_preg_PAT coh
    join clarity.hsp_admit_diag hd on coh.pat_id = hd.pat_id
    JOIN clarity.edg_current_icd9 edg ON hd.dx_id = edg.dx_id
    LEFT JOIN CLARITY.DATE_DIMENSION  dt ON hd.PAT_ENC_DATE_REAL = dt.epic_dte
      where hd.PAT_ENC_DATE_REAL is not null
      and hd.dx_id is not null
      and edg.code not like 'IMO0%'
  UNION
  select
		  coh.pat_id,
		  hd.pat_enc_csn_id,
		  trunc(dt.calendar_dt) as contact_date,
		  edg.code as icd_code,
		  10 as icd_type
  FROM XDR_Wherry_preg_PAT coh
  join clarity.hsp_admit_diag hd on coh.pat_id = hd.pat_id
  JOIN clarity.edg_current_icd10 edg ON hd.dx_id = edg.dx_id
  LEFT JOIN CLARITY.DATE_DIMENSION  dt ON hd.PAT_ENC_DATE_REAL = dt.epic_dte
  where hd.PAT_ENC_DATE_REAL is not null
    and hd.dx_id is not null
    and edg.code not like 'IMO0%'
  ) adm
on (lcd.pat_id = adm.pat_id
  and lcd.pat_enc_csn_id = adm.pat_enc_csn_id
  and lcd.contact_date = adm.contact_date
  and lcd.icd_code = adm.icd_code
  and lcd.icd_type = adm.icd_type)
when matched then 
  update set admit_dx_flag = 'A'
when not matched then
  insert (pat_id, pat_enc_csn_id, contact_date, icd_code, icd_type, primary_sec_flag, admit_dx_flag, poa_flag, hsp_final_dx_flag)
  values (adm.pat_id, adm.pat_enc_csn_id, adm.contact_date, adm.icd_code, adm.icd_type, null, 'A', null, null)
;
commit;
--35,401 rows merged.(9/5/17)

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_Wherry_preg_DX' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	--
	,COUNT(*) AS TOTAL_COUNT 		--206,852(9/5/17)
FROM XDR_Wherry_preg_DX;
COMMIT;

	--------------------------------------------------------------------------------
	--	STEP 6.4.5: Now Load HSP_ACCT_DX_LIST
	--				This table contains hospital account final diagnosis list information from the HAR master file  
	--      		final DX, Present on admission
	--  			Process line 1 (Primary final dx) first
	--------------------------------------------------------------------------------
MERGE INTO XDR_Wherry_preg_DX lcd
using
    (select
        coh.pat_id,
        coh.pat_enc_csn_id,
        trunc(t.hosp_admsn_time) as contact_date,
        edg.code as icd_code,
        9 as icd_type
    from XDR_Wherry_preg_PAT coh
    JOIN XDR_Wherry_preg_IP_ENC t ON coh.PAT_ENC_CSN_ID = t.PAT_ENC_CSN_ID
    join clarity.hsp_acct_dx_list hd on t.hsp_account_id = hd.hsp_account_id
    JOIN clarity.edg_current_icd9 edg ON hd.dx_id = edg.dx_id
    where t.hosp_admsn_time is not null
      and hd.line = 1
      and edg.code not like 'IMO0%'
    UNION
    select
        coh.pat_id,
        coh.pat_enc_csn_id,
        trunc(t.hosp_admsn_time) as contact_date,
        edg.code as icd_code,
        10 as icd_type
    from XDR_Wherry_preg_PAT coh
    JOIN XDR_Wherry_preg_IP_ENC t ON coh.PAT_ENC_CSN_ID = t.PAT_ENC_CSN_ID
    join clarity.hsp_acct_dx_list hd on t.hsp_account_id = hd.hsp_account_id
    JOIN clarity.edg_current_icd10 edg ON hd.dx_id = edg.dx_id
    left join clarity.ZC_DX_POA zdp on hd.final_dx_poa_c = zdp.dx_poa_c
    where t.hosp_admsn_time is not null
      and hd.line = 1
      and edg.code not like 'IMO0%'
    ) hsp
on (lcd.pat_id = hsp.pat_id
  and lcd.pat_enc_csn_id = hsp.pat_enc_csn_id
  and lcd.contact_date = hsp.contact_date
  and lcd.icd_code = hsp.icd_code
  and lcd.icd_type = hsp.icd_type)
when matched then 
  update set hsp_final_dx_flag = 1,
      primary_sec_flag = 'P'
when not matched then
  insert (pat_id, pat_enc_csn_id, contact_date, icd_code, icd_type, primary_sec_flag, admit_dx_flag, poa_flag, hsp_final_dx_flag)
  values (hsp.pat_id, hsp.pat_enc_csn_id, hsp.contact_date, hsp.icd_code, hsp.icd_type,'P', null, null, 1)
;
commit;
--6,525 rows merged.(9/5/17)

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_Wherry_preg_DX' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 		
FROM XDR_Wherry_preg_DX;
COMMIT;
	--------------------------------------------------------------------------------
	--	STEP 6.4.6: Process line 2-end, Secondary dx next
	--  			Don't update primary secondary flag
	--------------------------------------------------------------------------------
MERGE INTO XDR_Wherry_preg_DX lcd
using
    (select
        coh.pat_id,
        coh.pat_enc_csn_id,
        trunc(t.hosp_admsn_time) as contact_date,
        edg.code as icd_code,
        9 as icd_type
    from XDR_Wherry_preg_PAT coh
    JOIN XDR_Wherry_preg_IP_ENC t ON coh.PAT_ENC_CSN_ID = t.PAT_ENC_CSN_ID
    join clarity.hsp_acct_dx_list hd on t.hsp_account_id = hd.hsp_account_id
    JOIN clarity.edg_current_icd9 edg ON hd.dx_id = edg.dx_id
    left join clarity.ZC_DX_POA zdp on hd.final_dx_poa_c = zdp.dx_poa_c
    where t.hosp_admsn_time is not null
      and hd.line > 1
      and edg.code not like 'IMO0%'
    UNION
    select
        coh.pat_id,
        coh.pat_enc_csn_id,
        trunc(t.hosp_admsn_time) as contact_date,
        edg.code as icd_code,
        10 as icd_type
    from XDR_Wherry_preg_PAT coh
    JOIN XDR_Wherry_preg_IP_ENC t ON coh.PAT_ENC_CSN_ID = t.PAT_ENC_CSN_ID
    join clarity.hsp_acct_dx_list hd on t.hsp_account_id = hd.hsp_account_id
    JOIN clarity.edg_current_icd10 edg ON hd.dx_id = edg.dx_id
    left join clarity.ZC_DX_POA zdp on hd.final_dx_poa_c = zdp.dx_poa_c
    where t.hosp_admsn_time is not null
      and hd.line > 1
      and edg.code not like 'IMO0%'
    ) hsp
on (lcd.pat_id = hsp.pat_id
  and lcd.pat_enc_csn_id = hsp.pat_enc_csn_id
  and lcd.contact_date = hsp.contact_date
  and lcd.icd_code = hsp.icd_code
  and lcd.icd_type = hsp.icd_type)
when matched then 
  update set hsp_final_dx_flag = 1
when not matched then
  insert (pat_id, pat_enc_csn_id, contact_date, icd_code, icd_type, primary_sec_flag, admit_dx_flag, poa_flag, hsp_final_dx_flag)
  values (hsp.pat_id, hsp.pat_enc_csn_id, hsp.contact_date, hsp.icd_code, hsp.icd_type,'S', null, null, 1)
;
commit;
--151,767 rows merged.(9/5/17)

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_Wherry_preg_DX' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 		
FROM XDR_Wherry_preg_DX;
COMMIT;
	--------------------------------------------------------------------------------
	--	STEP 6.4.7: Last but not least, update the POA flag if it's = 'Yes'
	--------------------------------------------------------------------------------
UPDATE XDR_Wherry_preg_DX lcd 
SET lcd.poa_flag = 'Y'
WHERE EXISTS (SELECT hsp.pat_id
              ,hsp.pat_enc_csn_id
              ,hsp.contact_date
              ,hsp.icd_code
              ,hsp.icd_type
            FROM 
            (select
        coh.pat_id,
        coh.pat_enc_csn_id,
        trunc(t.hosp_admsn_time) as contact_date,
        edg.code as icd_code,
        9 as icd_type
    from XDR_Wherry_preg_PAT coh
    JOIN XDR_Wherry_preg_IP_ENC t ON coh.PAT_ENC_CSN_ID = t.PAT_ENC_CSN_ID
    join clarity.hsp_acct_dx_list hd on t.hsp_account_id = hd.hsp_account_id
    JOIN clarity.edg_current_icd9 edg ON hd.dx_id = edg.dx_id
    left join clarity.ZC_DX_POA zdp on hd.final_dx_poa_c = zdp.dx_poa_c
    where t.hosp_admsn_time is not null
      and hd.line > 1
      and edg.code not like 'IMO0%'
      and zdp.name = 'Yes'
    UNION
    select
        coh.pat_id,
        coh.pat_enc_csn_id,
        trunc(t.hosp_admsn_time) as contact_date,
        edg.code as icd_code,
        10 as icd_type
    from XDR_Wherry_preg_PAT coh
    JOIN XDR_Wherry_preg_IP_ENC t ON coh.PAT_ENC_CSN_ID = t.PAT_ENC_CSN_ID
    join clarity.hsp_acct_dx_list hd on t.hsp_account_id = hd.hsp_account_id
    JOIN clarity.edg_current_icd10 edg ON hd.dx_id = edg.dx_id
    left join clarity.ZC_DX_POA zdp on hd.final_dx_poa_c = zdp.dx_poa_c
    where t.hosp_admsn_time is not null
      and hd.line > 1
      and edg.code not like 'IMO0%'
      and zdp.name = 'Yes'
    ) hsp
            WHERE lcd.pat_id = hsp.pat_id
  and lcd.pat_enc_csn_id = hsp.pat_enc_csn_id
  and lcd.contact_date = hsp.contact_date
  and lcd.icd_code = hsp.icd_code
  and lcd.icd_type = hsp.icd_type);
commit;
--103,882 rows updated.(9/5/17)

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_Wherry_preg_DX' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	--  3,738(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 		--328,354(9/5/17)
FROM XDR_Wherry_preg_DX;
COMMIT;

select * from XDR_Wherry_preg_DX;



----------------------------------------------------------------------------
-- STEP 6.5: Create Procedures table
--------------------------------------------------------------------------------
	--------------------------------------------------------------------------------
	--	STEP 6.5.1: Create destination table
	--------------------------------------------------------------------------------
DROP TABLE xdr_Wherry_preg_prc PURGE;
CREATE TABLE xdr_Wherry_preg_prc
   (	"PAT_ID" VARCHAR2(18 BYTE), 
	"PAT_ENC_CSN_ID" NUMBER(18,0), 
	"PROC_DATE" DATE, 
	"PROCEDURE_NAME" VARCHAR2(254 BYTE), 
	"PX_CODE" VARCHAR2(254 BYTE), 
	"ICD_CODE_SET" VARCHAR2(254 BYTE), 
	"PROC_PERF_PROV_ID" VARCHAR2(20 BYTE)
   );

    --------------------------------------------------------------------------------
    -- STEP 6.5.2: Insert ICD Procedures
    --------------------------------------------------------------------------------
insert into xdr_Wherry_preg_prc
SELECT distinct t.pat_id, 
		t.pat_enc_csn_id, 
		p.proc_date, 
		i.procedure_name        as px_name, 
		i.ref_bill_code         as px_code,
		zhcs.name               as code_type,
		p.proc_perf_prov_id as prov_id
FROM clarity.hsp_acct_px_list p  
      JOIN clarity.cl_icd_px i ON p.final_icd_px_id = i.icd_px_id 
      JOIN XDR_WHERRY_preg_ENC t ON p.hsp_account_id = t.hsp_account_id 
      --JOIN XDR_Wherry_preg_PAT   coh on t.PAT_ENC_CSN_ID = coh.PAT_ENC_CSN_ID
      join clarity.ZC_HCD_CODE_SET zhcs on i.REF_BILL_CODE_SET_C = zhcs.CODE_SET_C
      WHERE p.proc_date >= '01/01/2006';
--552,841 rows inserted.(01/18/18)
COMMIT;
--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_Wherry_preg_PRC' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	-- 3,238(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 		--31,709(9/5/17)
FROM XDR_Wherry_preg_PRC;
COMMIT;

    --------------------------------------------------------------------------------
    -- STEP 6.5.3: Insert CPT Procedures - Professional
    --------------------------------------------------------------------------------
insert into xdr_Wherry_preg_prc
SELECT arpb.patient_id 								    AS pat_id
                      ,arpb.pat_enc_csn_id
                      ,arpb.service_date                AS proc_date 
                      ,null as px_name
                      ,arpb.cpt_code                    AS px_code
                      ,'CPT-Professional'               AS code_type
                      ,arpb.SERV_PROVIDER_ID            AS prov_id
        FROM clarity.arpb_transactions  arpb 
        join XDR_WHERRY_preg_ENC        enc on arpb.pat_enc_csn_id = enc.pat_enc_csn_id
        --join i2b2.lz_clarity_patient    pat on arpb.patient_id = pat.pat_id
        WHERE --patient_id is not null AND 
          tx_type_c = 1					-----  Charges only
          AND void_date is null
          AND arpb.service_date >= '01/01/2006'; 


--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_Wherry_preg_PRC' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	-- 3,238(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 		--31,709(9/5/17)
FROM XDR_Wherry_preg_PRC;
COMMIT;
    --------------------------------------------------------------------------------
    -- STEP 6.5.4: Insert CPT Procedures - Hospital
    --------------------------------------------------------------------------------
insert into xdr_Wherry_preg_prc
SELECT hsp.pat_id
                ,hspt.pat_enc_csn_id
                ,hspt.service_date                                      AS proc_date
                ,eap.proc_name                                          AS px_name
                ,substr(coalesce(hspt.hcpcs_code,hspt.cpt_code),1,5)    AS px_code
                ,'CPT-Hospital'                                         AS code_type   
                ,hspt.PERFORMING_PROV_ID                                AS prov_id
            FROM  hsp_account       hsp   
            JOIN clarity.hsp_transactions           hspt  ON hsp.hsp_account_id = hspt.hsp_account_id
            LEFT JOIN clarity.f_arhb_inactive_tx    fait on hspt.tx_id = fait.tx_id
            join XDR_WHERRY_preg_ENC                enc on hspt.pat_enc_csn_id = enc.pat_enc_csn_id
            LEFT JOIN clarity.CLARITY_EAP           eap ON hspt.proc_id = eap.proc_id
            --join i2b2.lz_clarity_patient pat on hsp.pat_id = pat.pat_id
          where hspt.tx_type_ha_c = 1  
          and (length(hspt.cpt_code) = 5 or hspt.hcpcs_code is not null)
          and fait.tx_id is null;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_Wherry_preg_PRC' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	-- 3,238(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 		--31,709(9/5/17)
FROM XDR_Wherry_preg_PRC;
COMMIT;


select code_type,count(*) 
from xdr_Wherry_preg_prc
group by ICD_CODE_SET;
--ICD-10-PCS	18656
--ICD-9-CM Volume 3	13053



--------------------------------------------------------------------------------
-- STEP 6.6: Create Flowsheet table
--------------------------------------------------------------------------------
DROP TABLE xdr_Wherry_preg_flo PURGE;
CREATE TABLE xdr_Wherry_preg_flo AS 
SELECT DISTINCT coh.pat_id
                       ,enc.pat_enc_csn_id
                       ,enc.INPATIENT_DATA_ID
                       ,meas.flt_id
                       ,meas.flo_meas_id
                       ,dta.display_name      AS template_name
                       ,gpd.disp_name         AS measure_name
                       ,gpd.flo_meas_name     
                       ,meas.recorded_time
                       ,meas.meas_value       AS measure_value
          FROM XDR_Wherry_preg_PAT           coh 
          --Needs encounter table
          --JOIN XDR_Wherry_preg_IP_ENC        enc   ON coh.pat_enc_csn_id = enc.pat_enc_csn_id
          JOIN clarity.ip_flwsht_rec      rec   ON enc.inpatient_data_id = rec.inpatient_data_id
          JOIN clarity.ip_flwsht_meas     meas  ON rec.fsd_id = meas.fsd_id
          JOIN clarity.ip_flo_gp_data     gpd   ON meas.flo_meas_id = gpd.flo_meas_id
          JOIN clarity.ip_flt_data        dta   ON meas.flt_id = dta.template_id
          WHERE meas.recorded_time IS NOT NULL 
            AND meas.meas_value IS NOT NULL
            AND meas.flo_meas_id IN ('11'         --Height
                                    ,'14'         --Weight
                                    ,'5'          --Blood Pressure  
                                    ,'8'          --Pulse
                                    ,'6'          --Temperature
                                    ,'9'          --Respiratory Rate 
                                    ,'301070'     --BMI
                                    ,'10'         --Pulse Oximetry (SpO2)
                                    )
;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_Wherry_preg_FLO' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	-- 3,238(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 		--31,709(9/5/17)
FROM XDR_Wherry_preg_FLO;
COMMIT;

--------------------------------------------------------------------------------
-- STEP 6.7: Create Social History table
--------------------------------------------------------------------------------
DROP TABLE XDR_Wherry_preg_SOC PURGE;
CREATE TABLE XDR_Wherry_preg_SOC AS
SELECT DISTINCT coh.pat_id 
               ,coh.study_id
               ,soc.pat_enc_csn_id            
               ,soc.pat_enc_date_real
               ,pat.birth_date
               ,trunc(months_between(CURRENT_DATE, pat.birth_date)/12) AS age
               ,xsx.NAME                                               AS gender
            --Get sexual history below
               ,xsa.NAME                                               AS sexually_active
               ,soc.female_partner_yn                                           --never nulls; defaults to "N" when unchecked
               ,soc.male_partner_yn                                             --never nulls; defaults to "N" when unchecked
               ,soc.condom_yn
               ,soc.pill_yn
               ,soc.diaphragm_yn
               ,soc.iud_yn
               ,soc.surgical_yn
               ,soc.spermicide_yn
               ,soc.implant_yn
               ,soc.rhythm_yn
               ,soc.injection_yn
               ,soc.sponge_yn
               ,soc.inserts_yn
               ,soc.abstinence_yn
            --Get tobacco history below
               ,soc.tobacco_pak_per_dy
               ,soc.tobacco_used_years
               ,xtb.NAME                                               AS tobacco_user
               ,soc.cigarettes_yn
               ,soc.pipes_yn 
               ,soc.cigars_yn
               ,soc.snuff_yn
               ,soc.chew_yn 
               ,xsm.NAME                                               AS smoking_tob_status
               ,soc.smoking_start_date
               ,soc.smoking_quit_date
            --Get alcohol history below
               ,soc.alcohol_comment                                    AS alcohol_comments 
               ,xal.NAME                                               AS alcohol_user
               ,soc.alcohol_oz_per_wk
               ,xdt.NAME                                               AS alcohol_type
               ,soa.alcohol_drinks_wk 
            --Get drug history below
               ,soc.iv_drug_user_yn 
               ,soc.illicit_drug_freq  
               ,soc.illicit_drug_cmt 
  FROM XDR_Wherry_preg_PAT                              coh
  JOIN clarity.patient                    pat ON coh.pat_id = pat.pat_id
  LEFT JOIN clarity.social_hx           soc ON pat.pat_id = soc.pat_id
  LEFT JOIN clarity.social_hx_alc_use   soa ON soc.pat_enc_csn_id = soa.pat_enc_csn_id
  LEFT JOIN clarity.zc_sexually_active  xsa ON soc.sexually_active_c = xsa.sexually_active_c
  LEFT JOIN clarity.zc_sex              xsx ON pat.sex_c = xsx.rcpt_mem_sex_c
  LEFT JOIN clarity.zc_tobacco_user     xtb ON soc.tobacco_user_c = xtb.tobacco_user_c
  LEFT JOIN clarity.zc_smoking_tob_use  xsm ON soc.smoking_tob_use_c = xsm.smoking_tob_use_c
  LEFT JOIN clarity.zc_alcohol_use      xal ON soc.alcohol_use_c = xal.alcohol_use_c
  LEFT JOIN clarity.zc_hx_drink_types   xdt ON soa.hx_drink_types_c = xdt.hx_drink_types_c
  WHERE soc.pat_enc_date_real = (SELECT MAX(soc.pat_enc_date_real) FROM social_hx soc WHERE soc.pat_id = coh.pat_id)
;
--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_Wherry_preg_SOC' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	-- 3,238(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 		--31,709(9/5/17)
FROM XDR_Wherry_preg_SOC;
COMMIT;


--------------------------------------------------------------------------------
-- STEP 6.8: Create Family History table
--------------------------------------------------------------------------------
DROP TABLE xdr_Wherry_preg_fam PURGE;
CREATE TABLE xdr_Wherry_preg_fam AS
SELECT DISTINCT pat.pat_id 
               ,pat.study_id
               ,fam.pat_enc_csn_id       
               ,fam.line
               ,fam.medical_hx_c
               ,xmh.NAME                  AS medical_hx
               ,fam.relation_c
               ,xrc.NAME                  AS relation
  FROM XDR_Wherry_preg_PAT              pat
  JOIN clarity.family_hx                fam ON pat.pat_id = fam.pat_id
  LEFT JOIN clarity.zc_medical_hx       xmh ON fam.medical_hx_c = xmh.medical_hx_c
  LEFT JOIN clarity.zc_msg_caller_rel   xrc ON fam.relation_c = xrc.msg_caller_rel_c 
  WHERE fam.pat_enc_date_real = (SELECT MAX(fam.pat_enc_date_real) FROM clarity.family_hx fam WHERE fam.pat_id = pat.pat_id)
;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_Wherry_preg_FAM' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	-- 3,238(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 		--31,709(9/5/17)
FROM XDR_Wherry_preg_FAM;
COMMIT;

--------------------------------------------------------------------------------
-- STEP 6.9: Create Problem List table
--------------------------------------------------------------------------------
DROP TABLE xdr_Wherry_preg_pl purge;
CREATE TABLE xdr_Wherry_preg_pl AS
--SELECT DISTINCT enc.pat_id
--               ,enc.pat_enc_csn_id
--               ,enc.effective_date_dt          AS encounter_date
--               ,pl.problem_list_id
--               ,pl.hx_problem_id               AS dx_id
--               ,pl.hx_description              AS prob_desc 
--               ,pl.hx_date_noted               AS noted_date
--               ,pl.hx_date_of_entry            AS update_date
--               ,pl.hx_date_resolved            AS resolved_date
--               ,zps.name                       AS problem_status
--               --,pl.hx_comment                  AS problem_cmt     
--               ,zhp.name                       AS priority
--               ,pl.hx_is_hosp_yn               AS hospital_problem
--               ,pl.hx_principal_yn             AS principal_yn
--               ,pl.hx_chronic_yn               AS chronic_yn             
--               --,pl.PROBLEM_TYPE_C              --1-MEDICAL   2-NURSING   3-Hospitalization       (in progress)           
--               
--               --ICD9 CODES
--               --,'9'                            AS icd_type
--               --,cin9.code                      AS icd_code
--               --,icd9.ICD_DESC                  AS icd_desc
--  
--               --ICD10 CODES
--               --,'10'                           AS icd_type
--               --,cin10.code                     AS ICD_code
--               --,icd10.ICD_DESC                 AS ICD_DESC
--               --,DSC10.DESCRIPTION              AS ICD10_Diagnosis_desc               
--
--               ,ser.prov_id                           
--               ,ser.prov_type          
--               ,prv.primary_specialty         
--  FROM xdr_Wherry_preg_enc                      enc
--  JOIN clarity.problem_list_hx            pl    ON enc.pat_enc_csn_id = pl.hx_problem_ept_csn
--  LEFT JOIN clarity.clarity_ser           ser   ON pl.hx_entry_user_id = ser.user_id
--  LEFT JOIN clarity.v_cube_d_provider     prv   ON ser.prov_id = prv.provider_id
--  LEFT JOIN clarity.zc_problem_status     zps   ON pl.hx_status_c = zps.problem_status_c
--  LEFT JOIN clarity.zc_hx_priority        zhp   ON pl.hx_priority_c = zhp.hx_priority_c
  
  --ICD9 CODES
  --LEFT JOIN edg_current_icd9              cin9  ON pl.DX_ID = cin9.dx_id AND cin9.line = 1
  --LEFT JOIN i2b2.lz_dx_px_lookup          icd9   ON cin9.code = icd9.code
  --                                              AND icd9.code_type = 'DX' 
  --                                              AND icd9.icd_type = 9
  
  --ICD10 CODES
  --LEFT JOIN edg_current_icd10             cin10 ON pl.DX_ID = cin10.dx_id AND cin10.line = 1
  --LEFT JOIN i2b2.lz_dx_px_lookup          icd10 ON cin10.code = icd10.code
  --                                              AND icd10.code_type = 'DX' 
  --                                              AND icd10.icd_type = 10
--UNION
SELECT DISTINCT enc.pat_id
               ,enc.pat_enc_csn_id
               ,enc.effective_date_dt as encounter_date
               ,pl.problem_list_id
               ,pl.dx_id
               ,pl.description                AS prob_desc 
               ,pl.noted_date                 AS noted_date
               ,pl.date_of_entry              AS update_date
               ,pl.resolved_date              AS resolved_date
               ,zps.name                      AS problem_status        
               --,pl.problem_cmt                AS problem_cmt     
               ,zhp.name                      AS priority
               ,pl.hospital_pl_yn             AS hospital_problem
               ,PL.PRINCIPAL_PL_YN            AS principal_yn
               --,pl.PROBLEM_TYPE_C             --1-MEDICAL   2-NURSING   3-Hospitalization   (in progress)
               ,pl.chronic_yn                 AS chronic_yn
               
               --ICD9 CODES
               --,'9'                           AS icd_type
               --,cin9.code                     AS ICD_code
               --,icd9.ICD_DESC                 AS ICD_DESC
               --,DSC9.DESCRIPTION              AS ICD9_Diagnosis_desc
               
               --ICD10 CODES
               --,'10'                          AS icd_type
               --,cin10.code                    AS ICD_code
               --,icd10.ICD_DESC                AS ICD_DESC
               --,DSC10.DESCRIPTION             AS ICD10_Diagnosis_desc
                              
               ,ser.prov_id                           
               ,ser.prov_type          
               ,prv.primary_specialty      
  FROM xdr_Wherry_preg_enc                        enc
  JOIN clarity.problem_list                 pl    ON enc.pat_enc_csn_id = pl.problem_ept_csn AND rec_archived_yn = 'N'
  LEFT JOIN clarity.clarity_ser             ser   ON pl.entry_user_id = ser.user_id
  LEFT JOIN clarity.v_cube_d_provider       prv   ON ser.prov_id = prv.provider_id
  LEFT JOIN clarity.zc_problem_status       zps   ON pl.problem_status_c = zps.problem_status_c
  LEFT JOIN clarity.zc_hx_priority          zhp   ON pl.priority_c = zhp.hx_priority_c
  
  --ICD9 CODES
  --LEFT JOIN edg_current_icd9                cin9  ON pl.dx_id = cin9.dx_id AND cin9.line = 1
  --LEFT JOIN i2b2.lz_dx_px_lookup            icd9  ON cin9.code = icd9.code
  --                                                AND icd9.code_type = 'DX' 
  --                                                AND icd9.icd_type = 9
 
  --ICD10 CODES
  --LEFT JOIN edg_current_icd10               cin10 ON pl.dx_id = cin10.dx_id AND cin10.line = 1
  --LEFT JOIN i2b2.lz_dx_px_lookup            icd10 ON cin10.code = icd10.code
  --                                                AND icd10.code_type = 'DX' 
  --                                                AND icd10.icd_type = 10
;
--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'xdr_Wherry_preg_pl' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	-- 30855
	,COUNT(*) AS TOTAL_COUNT 		--387855
FROM xdr_Wherry_preg_pl;
COMMIT;




--------------------------------------------------------------------------------
-- STEP 6.10: Create Problem List Diagnosis table
--------------------------------------------------------------------------------
DROP TABLE xdr_Wherry_preg_pldx PURGE;
CREATE TABLE xdr_Wherry_preg_pldx AS
SELECT DISTINCT PAT_ID,
              PAT_ENC_CSN_ID,
              'PROBLEM_LIST'           AS diagnosis_source, 
              --DX_ID,								commented out on 6/9/17 to avoid confusion
              PROBLEM_LIST_ID,
              NOTED_DATE               AS diagnosis_date,
              PRINCIPAL_YN             AS primary_dx_yn, 
              --NULL                     AS SOURCE,
              --ICD CODES
              case when NOTED_DATE <= '01/01/2015' then icd9.icd_type else icd10.icd_type end icd_type,
              case when NOTED_DATE <= '01/01/2015' then icd9.code else icd10.code end icd_code,
              case when NOTED_DATE <= '01/01/2015' then icd9.icd_desc else icd10.icd_desc end icd_desc
FROM xdr_Wherry_preg_pl               pl
  --ICD9 CODES JOIN
  LEFT JOIN clarity.edg_current_icd9                cin9  ON pl.dx_id = cin9.dx_id AND cin9.line = 1
  LEFT JOIN XDR_Wherry_preg_DX_LOOKUP          icd9   ON cin9.code = icd9.CODE
                                                --AND icd9.code_type = 'DX' 
                                                AND icd9.icd_type = 9
  --ICD10 CODES JOIN
  LEFT JOIN clarity.edg_current_icd10             cin10 ON pl.DX_ID = cin10.dx_id AND cin10.line = 1
  LEFT JOIN XDR_Wherry_preg_DX_LOOKUP          icd10   ON cin10.code = icd10.CODE
                                                --AND icd10.code_type = 'DX' 
                                                AND icd10.icd_type = 10
;


--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'xdr_Wherry_preg_pldx' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	        --30855
	,COUNT(*) AS TOTAL_COUNT 		                --301122
FROM xdr_Wherry_preg_pldx;
COMMIT;


--------------------------------------------------------------------------------
-- STEP 6.11: Create Providers table
--------------------------------------------------------------------------------
DROP TABLE xdr_Wherry_preg_prov PURGE;
CREATE TABLE xdr_Wherry_preg_prov AS
SELECT rownum as prov_study_id
        ,x.*
FROM (SELECT DISTINCT prov.prov_id               AS provider_id,
                --Provider Gender(?)
                --uclaprov.DEID_PROV_ID,
                prv.provider_type,
                prv.primary_specialty,
                CASE WHEN   ser.ACTIVE_STATUS = 'Active'  AND  emp.USER_STATUS_C = 1 THEN 1
                    ELSE NULL 
                END active_providers,
                CASE WHEN   emp.user_id IS NOT NULL THEN 1
                    ELSE NULL
                END UC_provider
FROM 
    (--All provider from encounters + procedures
    select visit_prov_id as prov_id from xdr_Wherry_preg_enc
    UNION
    select PROC_PERF_PROV_ID as prov_id from xdr_Wherry_preg_prc
    ) prov
LEFT JOIN clarity.v_cube_d_provider       prv   ON prov.prov_id = prv.provider_id
--check for active providers
LEFT JOIN clarity.clarity_ser                     ser ON prov.prov_id = ser.PROV_ID
LEFT JOIN clarity.CLARITY_EMP                     emp ON prov.PROV_ID = emp.PROV_ID 
--LEFT JOIN I2B2.bip_PROVIDER_LINK                  uclaprov ON prov.PROV_ID = uclaprov.CLARITY_PROV_ID
) x
ORDER BY  dbms_random.value
;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'xdr_Wherry_preg_prov' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 		
FROM xdr_Wherry_preg_prov;
COMMIT;
