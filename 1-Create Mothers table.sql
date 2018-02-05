-- *******************************************************************************************************
-- STEP 1.1
--		Create table to capture all data counts
---------------------------------------------------------------------------------------------------------
--		This table will permit have a point of reference of the different datasets and could
--		help troubleshoot potential issues at a basic level	
-- *******************************************************************************************************
DROP TABLE XDR_WHERRY_preg_COUNTS PURGE;
CREATE TABLE XDR_WHERRY_preg_COUNTS
   (	TABLE_NAME VARCHAR2(30 BYTE), 
	PAT_COUNT NUMBER,
	TOTAL_COUNT NUMBER,
	LOAD_TIME timestamp default systimestamp);

	
-- *******************************************************************************************************
-- STEP 1.2
--   Create an initial Patient table with ALL mother records based on diagnoses codes
---------------------------------------------------------------------------------------------------------
--	The table i2b2.int_dx contains legacy diagnoses codes for the 2006-2013 period.
--  Each site has dealt with legacy data different and shall implement their own approach
-- *******************************************************************************************************
DROP TABLE xdr_wherry_prg_pregall PURGE;
CREATE TABLE xdr_wherry_prg_pregall AS
    SELECT DISTINCT mom.pat_id
            ,9 AS ICD_TYPE
            ,EDG.code as ICD_CODE
            ,dx.CONTACT_DATE
    FROM clarity.patient   mom
    JOIN clarity.pat_enc_dx         dx  ON mom.pat_id = dx.pat_id
    JOIN clarity.edg_current_icd9           edg ON dx.dx_id = edg.dx_id
    WHERE
        mom.sex_c = 1       --female
        AND (REGEXP_LIKE(edg.CODE,'^6[3-7][0-9]+')       --ICD-9: 630-679 (includes all subcategories)
        OR REGEXP_LIKE(EDG.CODE,'^V2(2|3)+')        --ICD-9: V22-V23 (includes all subcategories)
        )
        AND dx.CONTACT_DATE BETWEEN '01/01/2006' AND '01/18/2018'
UNION
    SELECT DISTINCT mom.pat_id
                ,10 AS ICD_TYPE
                ,EDG.code as ICD_CODE
                ,dx.CONTACT_DATE
    FROM clarity.patient   mom
    JOIN clarity.pat_enc_dx         dx  ON mom.pat_id = dx.pat_id
    JOIN clarity.edg_current_icd10           edg ON dx.dx_id = edg.dx_id
    WHERE
        mom.sex_c = 1       --female
        AND (REGEXP_LIKE(edg.code,'^Z3(4|A|7)+')         --ICD-10: Z34, Z3A, Z37, O categories
        OR REGEXP_LIKE(edg.code,'^O+')              --ICD-10: Z34, Z3A, Z37, O categories
        )
        AND dx.CONTACT_DATE BETWEEN '01/01/2006' AND '01/18/2018'
UNION
    SELECT DISTINCT mom.pat_id
        ,9 AS ICD_TYPE
        ,EDG.code as ICD_CODE
        ,enc.CONTACT_DATE
    FROM clarity.patient   mom
    JOIN clarity.pat_enc                    enc  ON mom.pat_id = enc.pat_id
    join clarity.hsp_acct_dx_list           dx on enc.hsp_account_id = dx.hsp_account_id
    JOIN clarity.edg_current_icd9           edg ON dx.dx_id = edg.dx_id
    WHERE
        mom.sex_c = 1       --female
        AND (REGEXP_LIKE(edg.CODE,'^6[3-7][0-9]+')       --ICD-9: 630-679 (includes all subcategories)
        OR REGEXP_LIKE(EDG.CODE,'^V2(2|3)+')       --ICD-9: V22-V23 (includes all subcategories)
        )
        AND enc.CONTACT_DATE BETWEEN '01/01/2006' AND '01/18/2018'
UNION
    SELECT DISTINCT mom.pat_id
        ,10 AS ICD_TYPE
        ,EDG.code as ICD_CODE
        ,enc.CONTACT_DATE
    FROM clarity.patient   mom
    JOIN clarity.pat_enc                    enc  ON mom.pat_id = enc.pat_id
    join clarity.hsp_acct_dx_list           dx on enc.hsp_account_id = dx.hsp_account_id
    JOIN clarity.edg_current_icd10          edg ON dx.dx_id = edg.dx_id
    WHERE
        mom.sex_c = 1       --female
        AND (REGEXP_LIKE(edg.code,'^Z3(4|A|7)+')       --ICD-10: Z34, Z3A, Z37, O categories
        OR REGEXP_LIKE(edg.code,'^O+')       --ICD-10: Z34, Z3A, Z37, O categories
        )
        AND enc.CONTACT_DATE BETWEEN '01/01/2006' AND '01/18/2018'
--legacy data
UNION
	select PAT_ID
        ,9 AS ICD_TYPE
        ,ICD9_CODE
        ,EFFECTIVE_DATE
	from i2b2.int_dx
	WHERE (REGEXP_LIKE(ICD9_CODE,'^6[3-7][0-9]+')       --ICD-9: 630-679 (includes all subcategories)
        OR REGEXP_LIKE(ICD9_CODE,'^V2(2|3)+')        --ICD-9: V22-V23 (includes all subcategories)
        )
        AND EFFECTIVE_DATE BETWEEN '01/01/2006' AND '01/25/2018'
;
--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'xdr_wherry_prg_pregall' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	--    3,736(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 		--5,953,931(9/5/17)
FROM xdr_wherry_prg_pregall;
COMMIT;



--CHECK DX CODES
SELECT DISTINCT dx.ICD_TYPE
,dx.icd_CODE 
,lk.icd_desc
FROM xdr_wherry_prg_cohdx dx
join i2b2.lz_dx_px_lookup lk on dx.icd_code = lk.code
                            and dx.icd_type = lk.ICD_TYPE
                            and lk.code_type = 'DX'
-- *******************************************************************************************************
-- STEP 1.3
--   Pull all encounter for Mothers
-- *******************************************************************************************************
DROP TABLE XDR_WHERRY_preg_ENC PURGE;
CREATE TABLE XDR_WHERRY_preg_ENC AS
SELECT e.pat_id, 
            e.pat_enc_csn_id, 
            e.hsp_account_id, 
            e.inpatient_data_id, 
            e.ip_episode_id,
            e.effective_date_dt,
            e.hosp_admsn_time, 
            e.hosp_dischrg_time,
            nvl(e.visit_fc, -999) prim_fc, 
            nvl(fc.fin_class_title, 'Unknown') financial_class,
            nvl(e.enc_type_c,'-999') enc_type_c, 
            nvl(enctype.name, 'Unknown') encounter_type,
            e.department_id,
            e.visit_prov_id,
            e.appt_status_c, 
            e.pcp_prov_id,
            --hsp.disch_disp_c, 
            --dd.name disposition, 
            --hsp.ed_disposition_c, 
            --edd.name ed_disposition,
            dep.department_name,
            dep.specialty
            --loc.loc_name       
		FROM clarity.pat_enc 				e
        JOIN (SELECT DISTINCT PAT_ID 
			FROM xdr_wherry_prg_pregall) 	pat 	ON e.pat_id = pat.pat_id		
        LEFT JOIN clarity.clarity_fc 		fc 		ON e.visit_fc = fc.financial_class
        LEFT JOIN clarity.ZC_DISP_ENC_TYPE 	enctype ON e.enc_type_c = enctype.disp_enc_type_c
        --LEFT JOIN clarity.pat_enc_hsp       hsp 	ON e.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
        --LEFT JOIN clarity.zc_disch_disp     dd  	ON hsp.disch_disp_c = dd.disch_disp_c
        --LEFT JOIN clarity.zc_ed_disposition edd 	ON hsp.ed_disposition_c = edd.ed_disposition_c
        LEFT JOIN clarity.clarity_dep       dep 	ON e.department_id = dep.department_id
        --LEFT JOIN XDR_WHERRY_preg_childall  cld ON 
        --left join clarity.clarity_loc               loc ON dep.rev_loc_id = loc.loc_id
        WHERE 
			--discard errors and dummy/test encounters
			e.enc_type_c not in (2532, 2534, 40, 2514, 2505, 2506, 2512, 2507)
			--the potential mother had a hospital encounter
			--AND enctype.name = 'Hospital Encounter'
			--Encounter took place on the same date where one of the children was born
--			AND e.effective_date_dt IN (SELECT DISTINCT birth_date
--										FROM XDR_WHERRY_preg_childall)
;
select count(*) , count(distinct pat_id)  from XDR_WHERRY_preg_ENC_pregall;  --1064076	69895
--There are 44,921 potential mothers with a hospital encounter in this period.
select count(*) , count(distinct enc.pat_id)  
from XDR_WHERRY_preg_ENC_pregall enc
join (select distinct pat_id  from XDR_WHERRY_PRG_PREGALL where CONTACT_DATE BETWEEN '01/01/2006' AND '03/01/2013') dx on enc.pat_id = dx.pat_id
where enc.effective_date_dt BETWEEN '01/01/2006' AND '03/01/2013'; --767055			44921


--CREATE INDEXES
CREATE INDEX XDR_WHERRY_preg_ENC_preg_DTIX ON XDR_WHERRY_preg_ENC(hosp_admsn_time);
CREATE INDEX XDR_WHERRY_preg_ENC_preg_DTIX ON XDR_WHERRY_preg_ENC(hosp_dischrg_time);
CREATE INDEX XDR_WHERRY_preg_childall_BDIX ON XDR_WHERRY_preg_ENC(BIRTH_DATE);



--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_WHERRY_preg_enc' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	--    3,736(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 		--5,953,931(9/5/17)
FROM XDR_WHERRY_preg_enc;
COMMIT;


