-- *******************************************************************************************************
-- STEP 0
--		Create table to capture all data counts
-- *******************************************************************************************************
DROP TABLE XDR_WHERRY_preg_COUNTS PURGE;
CREATE TABLE XDR_WHERRY_preg_COUNTS
   (	TABLE_NAME VARCHAR2(30 BYTE), 
	PAT_COUNT NUMBER,
	TOTAL_COUNT NUMBER,
	LOAD_TIME timestamp default systimestamp);

-- *******************************************************************************************************
-- STEP 1
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

	
-- *******************************************************************************************************
-- STEP 2
--   Create an initial table with ALL mother-child records available in Clarity 
--	 At UCLA, this data is available from the date that Care Connect launched on 03/2013 to the present time
-- *******************************************************************************************************
--caveat, sometimes children get assigned a pregnancy DX code. The PI will be discarding them on their end (2/1/18)
DROP TABLE xdr_wherry_all_mom_child PURGE;                                                  
CREATE TABLE xdr_wherry_all_mom_child AS
SELECT DISTINCT nb.pat_id                                               AS nb_pat_id
               --,nbp.pat_mrn_id                                          AS nb_mrn
               ,nbp.birth_date                                          AS nb_dob
               --,mom.child_enc_csn_id                                    AS nb_csn
               ,mom.line                                                AS nb_rank
               ,xsx.name                                                AS nb_sex
               ,nbp.ped_gest_age                                        AS nb_age
               ,nbp.zip                                                 AS nb_zip
               ,mom.pat_id                                              AS mom_pat_id
               --,mp.pat_mrn_id                                           AS mom_mrn
               ,mp.birth_date                                           AS mom_dob
               --,trunc(months_between(nbp.birth_date, mp.birth_date)/12) AS mom_age_at_delivery
               --,mom.pat_enc_csn_id                                      AS mom_csn
               ,MAX(line) OVER (PARTITION BY mom.pat_enc_csn_id)        AS number_of_babies
  FROM clarity.hsp_ld_mom_child mom
  LEFT JOIN clarity.pat_enc     nb  ON mom.child_enc_csn_id = nb.pat_enc_csn_id
  LEFT JOIN clarity.patient     nbp ON nb.pat_id = nbp.pat_id
  LEFT JOIN clarity.zc_sex      xsx ON nbp.sex_c = xsx.rcpt_mem_sex_c
  LEFT JOIN clarity.patient     mp  ON mom.pat_id = mp.pat_id
  WHERE 
        trunc(nbp.birth_date) BETWEEN '01/01/2006' AND '01/25/2018';
  
 select count(*), count(distinct NB_pat_id), count(distinct mom_pat_id) from xdr_wherry_all_mom_child;     --17087	17087	14837
--ALTER TABLE xdr_wherry_prg_pat ADD CONSTRAINT xdr_wherry_prg_pat_pk PRIMARY KEY (pat_id);

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'xdr_wherry_all_mom_child' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	--    3,736(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 		--5,953,931(9/5/17)
FROM xdr_wherry_all_mom_child;
COMMIT;



--CHECK DX CODES
SELECT DISTINCT dx.ICD_TYPE
,dx.icd_CODE 
,lk.icd_desc
FROM xdr_wherry_prg_cohdx dx
join i2b2.lz_dx_px_lookup lk on dx.icd_code = lk.code
                            and dx.icd_type = lk.ICD_TYPE
                            and lk.code_type = 'DX'

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'xdr_wherry_all_mom_child' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	--    3,736(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 		--5,953,931(9/5/17)
FROM xdr_wherry_all_mom_child;
COMMIT;


-- *******************************************************************************************************
-- STEP 3
--   Create an initial Patient table with all patients born in the period of prior to CC launch date 01/01/2006 - 03/01/2013
-- *******************************************************************************************************
--maybe reduce this to the '01/01/2006' AND '03/02/2013' period since 2013 to present is addressed by the mom-child link table
DROP TABLE XDR_WHERRY_preg_childall PURGE;
CREATE TABLE XDR_WHERRY_preg_childall AS
SELECT pat.*
FROM clarity.patient pat
WHERE birth_date BETWEEN   '01/01/2006' AND '01/25/2018';

--QA counts (to be removed?)
select count(*) , count(distinct pat_id)  from XDR_WHERRY_PREG_CHILDALL;        --168490	168490
select extract(year from birth_date) as y, count(distinct pat_id)  
from XDR_WHERRY_PREG_CHILDALL
group by extract(year from birth_date)
order by y;        --168490	168490


--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_WHERRY_preg_childall' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	--    3,736(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 		--5,953,931(9/5/17)
FROM XDR_WHERRY_preg_childall;
COMMIT;


-- *******************************************************************************************************
-- STEP 4
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


-- *******************************************************************************************************
-- STEP 5
--   		Create hospital encounters table for the '01/01/2006' - '03/01/2013' population
-- *******************************************************************************************************
DROP TABLE XDR_WHERRY_preg_HSP PURGE;
CREATE TABLE XDR_WHERRY_preg_HSP AS
SELECT * 
FROM XDR_WHERRY_preg_ENC
WHERE encounter_type = 'Hospital Encounter'
	AND 
		(hosp_admsn_time BETWEEN '01/01/2006' AND '03/01/2013'
		OR
		hosp_dischrg_time BETWEEN '01/01/2006' AND '03/01/2013')
;


--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_WHERRY_preg_hsp' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	--	  3,736(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 				--5,953,931(9/5/17)
FROM XDR_WHERRY_preg_hsp;
COMMIT;


-- *******************************************************************************************************
-- STEP 6
--   		Create table matching hospital encounter with children DOB for the '01/01/2006' - '03/01/2013' population
-- *******************************************************************************************************
--7:50 am: 40731 seconds
DROP TABLE XDR_WHERRY_preg_enc_dob PURGE;
CREATE TABLE XDR_WHERRY_preg_enc_dob AS
SELECT enc.PAT_ID		as mom_pat_id
		,ENC.effective_date_dt
		,enc.hosp_admsn_time 
		,enc.hosp_dischrg_time
		,cld.pat_id as child_pat_id
		,cld.BIRTH_DATE as child_birth_date
FROM XDR_WHERRY_PREG_CHILDALL	cld			
JOIN  XDR_WHERRY_preg_HSP		enc ON cld.birth_date between enc.hosp_admsn_time and enc.hosp_dischrg_time;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_WHERRY_preg_enc_dob' AS TABLE_NAME
	,COUNT(distinct mom_pat_id) AS PAT_COUNT	--    3,736(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 					--5,953,931(9/5/17)
FROM XDR_WHERRY_preg_enc_dob;
COMMIT;





-- *******************************************************************************************************
-- STEP 7
--   		Create mother and children tables to optimize matching query
-- *******************************************************************************************************
--start 15:40
--CREATE AD HOC TABLE FOR MOMS TO MATCH
DROP TABLE XDR_WHERRY_mom_matching PURGE;
CREATE TABLE XDR_WHERRY_mom_matching AS
SELECT DISTINCT enc.MOM_PAT_ID
,pat.add_line_1
,pat.city
,pat.zip
,pat.home_phone
,pat.email_address
FROM XDR_WHERRY_preg_enc_dob		enc
LEFT JOIN clarity.patient			pat on enc.MOM_PAT_ID = pat.pat_id;

--create indexex
create index XDR_WHERRY_mom_patidix on XDR_WHERRY_mom_matching(mom_pat_id);
create index XDR_WHERRY_mom_addix on XDR_WHERRY_mom_matching(add_line_1);
create index XDR_WHERRY_mom_phix on XDR_WHERRY_mom_matching(home_phone);
create index XDR_WHERRY_mom_emailix on XDR_WHERRY_mom_matching(email_address);

SELECT COUNT(*) COUNT_TOTAL, count(distinct mom_pat_id) AS COUNT_MOM FROM XDR_WHERRY_mom_matching;--SELECT COUNT(*) COUNT_TOTAL, count(distinct mom_pat_id) AS COUNT_MOM FROM XDR_WHERRY_mom_matching;--30077	30077	53724       53724	53724

--CREATE AD HOC TABLE FOR CHILDREN TO MATCH
--CREATE AD HOC TABLE FOR CHILDREN TO MATCH
DROP TABLE XDR_WHERRY_child_matching PURGE;
CREATE TABLE XDR_WHERRY_child_matching AS
SELECT DISTINCT enc.child_pat_id
,pat.add_line_1
,pat.city
,pat.zip
,pat.home_phone
,pat.email_address
FROM XDR_WHERRY_preg_enc_dob		enc
LEFT JOIN clarity.patient			pat on enc.child_pat_id = pat.pat_id;

create index XDR_WHERRY_cld_patidix on XDR_WHERRY_child_matching(child_pat_id);
create index XDR_WHERRY_cld_addix on XDR_WHERRY_child_matching(add_line_1);
create index XDR_WHERRY_cld_phix on XDR_WHERRY_child_matching(home_phone);
create index XDR_WHERRY_cld_emailix on XDR_WHERRY_child_matching(email_address);

SELECT COUNT(*) COUNT_TOTAL, count(distinct child_pat_id) AS COUNT_CHILD FROM XDR_WHERRY_child_matching;--112379	112379          53724	53724



-- *******************************************************************************************************
-- STEP 8
--   		Clean patent contact info to avoid low quality matches
-- *******************************************************************************************************

--Identify place holders, dummy, or mistaken contact information to include in the matching query


--it identifies some dummy/place holder records and some entries that belong to health institutions which shouldn't be user to pair patients
-- besides the obvious, it might require some manual checking to eliminate some of the entries with the higher number of records.
select add_line_1, count(*) AS C FROM
(
select add_line_1 from XDR_WHERRY_child_matching
UNION ALL
select add_line_1 from XDR_WHERRY_mom_matching
)
group by add_line_1  
order by c desc;

--it identifies some dummy/place holder records and some entries that belong to health institutions which shouldn't be user to pair patients
select HOME_PHONE, count(*) AS C FROM
(
select HOME_PHONE from XDR_WHERRY_child_matching
UNION ALL
select HOME_PHONE from XDR_WHERRY_mom_matching
)
group by HOME_PHONE  
order by c desc;
-- we use thris service to check some phone #s: https://www.411.com/phone
null        	8000
000-000-0000	1584
000-000-0001	860
999-999-9999	47
213-742-1335	40      we couldn't deternine where it belongs
310-000-0000	38
818-252-5863	34      Totally Kids Specialty Health Care
213-742-1339	31      we couldn't deternine where it belongs
661-298-8000	21      we couldn't deternine where it belongs
310-399-9536	17      we couldn't deternine where it belongs
818-999-9999	16
310-000-0001	16
805-000-0000	8
213-000-0001	8

--it generally only shows null and potentially real address
select EMAIL_ADDRESS, count(*) AS C FROM
(
select EMAIL_ADDRESS from XDR_WHERRY_child_matching
UNION ALL
select EMAIL_ADDRESS from XDR_WHERRY_mom_matching
)
group by EMAIL_ADDRESS  
order by c desc;


alter table XDR_WHERRY_mom_matching add address_yn char(1);
update XDR_WHERRY_mom_matching
set address_yn = 'n'
where
    --Default addresses
ADD_LINE_1 IN ('10920 Wilshire Blvd. Ste 1600',
                '545 S San Pedro St',
                '545 S. San Pedro Street ',
                '545 S SAN PEDRO ST ',
                '10716 LA TUNA CANYON ROAD',
                '10716 LA TUNA CANYON RD'
            )
OR
    (--incorrect ADDRESS LINE
            ADD_LINE_1 is null
            OR ADD_LINE_1 IN (' ','.',',','0','00','000','0000')
            OR REGEXP_LIKE(ADD_LINE_1,'(RETURN.MAIL|RETURNED.MAIL|MAIL.RETURNED|BAD.ADDRESS|NO.ADDRESS|NOT.KNOWN|NO.STREET|UNKNOWN|0000)','i')    
            --UPPER(ADD_LINE_1) LIKE '%NO ADDRESS%'
            --UPPER(ADD_LINE_1) LIKE '%NOT KNOWN%'
            --UPPER(ADD_LINE_1) LIKE '%NO STREET%'
            --UPPER(ADD_LINE_1) LIKE '%UNKNOWN%'
            --UPPER(ADD_LINE_1) LIKE '%0000%'
            --UPPER(ADD_LINE_1) IN ('RETURN MAIL','MAIL RETURNED','BAD ADDRESS')
            ) 
OR
    (-- PO boxes
            REGEXP_LIKE(ADD_LINE_1,'BOX','i')   --UPPER(ADD_LINE_1) like '%BOX%'
            AND
            REGEXP_LIKE(ADD_LINE_1,'PO','i')   --UPPER(ADD_LINE_1) like '%PO%'
            ) 
OR
    (-- HOMELESS
            UPPER(ADD_LINE_1) = 'HOMELESS'
            OR UPPER(CITY) = 'HOMELESS'
            ) 
OR
    (-- NO_CITY
            CITY IS NULL
            OR CITY IN (' ','.',',','0','00','000','0000')
            OR REGEXP_LIKE(CITY,'(RETURN.MAIL|MAIL.RETURNED|BAD.ADDRESS|NO.CITY|UNKNOWN|#)','i')     --UPPER(CITY) NOT IN ('RETURN MAIL','MAIL RETURNED','BAD ADDRESS')
            --UPPER(CITY) LIKE '%NO CITY%'
            --UPPER(CITY) LIKE '%UNKNOWN%'
            --UPPER(CITY) LIKE '%#%'
            ) 
OR
    (-- NO_ZIP
            ZIP IS NULL
            OR LENGTH(ZIP) < 5
            OR REGEXP_LIKE(ZIP,'###','i')
            ) 
;
commit;
--632 rows updated.

update XDR_WHERRY_mom_matching
set address_yn = 'y'
where address_yn is null;
commit;
--29,445 rows updated.


alter table XDR_WHERRY_child_matching add address_yn char(1);
update XDR_WHERRY_child_matching
set address_yn = 'n'
where
    --Default addresses
ADD_LINE_1 IN ('10920 Wilshire Blvd. Ste 1600',
                '545 S San Pedro St',
                '545 S. San Pedro Street ',
                '545 S SAN PEDRO ST ',
                '10716 LA TUNA CANYON ROAD',
                '10716 LA TUNA CANYON RD'
            )
OR
    (--incorrect ADDRESS LINE
            ADD_LINE_1 is null
            OR ADD_LINE_1 IN (' ','.',',','0','00','000','0000')
            OR REGEXP_LIKE(ADD_LINE_1,'(RETURN.MAIL|MAIL.RETURNED|BAD.ADDRESS|NO.ADDRESS|NOT.KNOWN|NO.STREET|UNKNOWN|0000)','i')    
            --UPPER(ADD_LINE_1) LIKE '%NO ADDRESS%'
            --UPPER(ADD_LINE_1) LIKE '%NOT KNOWN%'
            --UPPER(ADD_LINE_1) LIKE '%NO STREET%'
            --UPPER(ADD_LINE_1) LIKE '%UNKNOWN%'
            --UPPER(ADD_LINE_1) LIKE '%0000%'
            --UPPER(ADD_LINE_1) IN ('RETURN MAIL','MAIL RETURNED','BAD ADDRESS')
            ) 
OR
    (-- PO boxes
            REGEXP_LIKE(ADD_LINE_1,'BOX','i')   --UPPER(ADD_LINE_1) like '%BOX%'
            AND
            REGEXP_LIKE(ADD_LINE_1,'PO','i')   --UPPER(ADD_LINE_1) like '%PO%'
            ) 
OR
    (-- HOMELESS
            UPPER(ADD_LINE_1) = 'HOMELESS'
            OR UPPER(CITY) = 'HOMELESS'
            ) 
OR
    (-- NO_CITY
            CITY IS NULL
            OR CITY IN (' ','.',',','0','00','000','0000')
            OR REGEXP_LIKE(CITY,'(RETURN.MAIL|MAIL.RETURNED|BAD.ADDRESS|NO.CITY|UNKNOWN|#)','i')     --UPPER(CITY) NOT IN ('RETURN MAIL','MAIL RETURNED','BAD ADDRESS')
            --UPPER(CITY) LIKE '%NO CITY%'
            --UPPER(CITY) LIKE '%UNKNOWN%'
            --UPPER(CITY) LIKE '%#%'
            ) 
OR
    (-- NO_ZIP
            ZIP IS NULL
            OR LENGTH(ZIP) < 5
            OR REGEXP_LIKE(ZIP,'###','i')
            ) 
;
commit;
--3,928 rows updated.

update XDR_WHERRY_child_matching
set address_yn = 'y'
where address_yn is null;
commit;
--108,451 rows updated.




alter table XDR_WHERRY_mom_matching add phone_yn char(1);
update XDR_WHERRY_mom_matching
set phone_yn = 'n'
where
home_phone IS NULL
OR HOME_PHONE in ('000-000-0000',
'000-000-0001',
'999-999-9999',
'310-000-0000',
'818-252-5863',      -- Totally Kids Specialty Health Care
'818-999-9999',
'310-000-0001',
'805-000-0000',
'213-000-0001');
COMMIT;
--1,882 rows updated.


update XDR_WHERRY_mom_matching
set phone_yn = 'y'
where phone_yn is null;
commit;
--28,195 rows updated.




alter table XDR_WHERRY_child_matching add phone_yn char(1);
update XDR_WHERRY_child_matching
set phone_yn = 'n'
where
home_phone IS NULL
OR HOME_PHONE in ('000-000-0000',
'000-000-0001',
'999-999-9999',
'310-000-0000',
'818-252-5863',      -- Totally Kids Specialty Health Care
'818-999-9999',
'310-000-0001',
'805-000-0000',
'213-000-0001');
COMMIT;
--8,729 rows updated.


update XDR_WHERRY_child_matching
set phone_yn = 'y'
where phone_yn is null;
commit;
--103,650 rows updated.


------------------------------------------------------------------------------
--   Match mothers to children based on 
--		Proxy: pat_id
--		Address
--		Phone
--		Email		
-----------------------------------------------------------------------------
DROP TABLE XDR_WHERRY_preg_matching purge;
CREATE TABLE XDR_WHERRY_preg_matching AS
SELECT DISTINCT enc.pat_id as mom_pat_id
				,enc.child_pat_id
				,enc.effective_date_dt
				,enc.hosp_admsn_time 
				,enc.hosp_dischrg_time
				,enc.CHILD_BIRTH_DATE
				--,CASE WHEN enc.effective_date_dt = enc.CHILD_BIRTH_DATE THEN 1 ELSE 0 END DATES_MATCH
				--SOUNDEX ADDRESS MATCH
				,CASE WHEN (
							SOUNDEX(mom.add_line_1) = SOUNDEX(cld.add_line_1)
							AND mom.ADDRESS_YN = 'y' 
							AND cld.ADDRESS_YN = 'y'
							) THEN 1 ELSE 0 END SIMILAR_ADDRESS
				,CASE WHEN ( 
                            mom.add_line_1 = cld.add_line_1
                            AND mom.ADDRESS_YN = 'y' 
                            AND cld.ADDRESS_YN = 'y'
                            ) THEN 1 ELSE 0 END ADDRESS_MATCH
				,CASE WHEN ( 
                            mom.home_phone = cld.home_phone
                            AND mom.phone_YN = 'y' 
                            AND cld.phone_YN = 'y'
            			) THEN 1 ELSE 0 END PHONE_MATCH
				,CASE WHEN (
                            mom.email_address = cld.email_address
                            AND mom.email_address is not null
                            AND cld.email_address is not null
                            ) THEN 1 ELSE 0 END EMAIL_MATCH
				,CASE WHEN enc.pat_id = prx.proxy_pat_id THEN 1 ELSE 0 END PROXY_MATCH

FROM XDR_WHERRY_preg_dist			    enc
LEFT JOIN XDR_WHERRY_mom_matching		mom on enc.pat_id = mom.mom_pat_id
LEFT JOIN XDR_WHERRY_child_matching		cld on enc.child_pat_id = cld.child_pat_id
LEFT JOIN clarity.PAT_MYC_PRXY_HX		prx ON enc.child_pat_id = prx.pat_id
WHERE
    	enc.pat_id <> enc.child_pat_id      --sometimes, the child gets assigned a preganncy dx code and since it was also at the hospital, it can get tagged to herself
         and
        (
		--SOUNDEX ADDRESS MATCH
			( 
			SOUNDEX(mom.add_line_1) = SOUNDEX(cld.add_line_1)
			AND mom.ADDRESS_YN = 'y' 
            AND cld.ADDRESS_YN = 'y'
			)
        --EXACT ADDRESS MATCH
		OR ( 
			mom.add_line_1 = cld.add_line_1
			AND mom.ADDRESS_YN = 'y' 
            AND cld.ADDRESS_YN = 'y'
			)
		--home_phone match
		OR ( 
			mom.home_phone = cld.home_phone
			AND mom.phone_YN = 'y' 
            AND cld.phone_YN = 'y'
			)
		--email_address match
		OR (
			mom.email_address = cld.email_address
			AND mom.email_address is not null
            AND cld.email_address is not null
			)
		--mother is PROXY for the child
		OR (enc.pat_id = prx.proxy_pat_id)
		);
		
SELECT count(*) FROM XDR_WHERRY_preg_matching ;     --27524
SELECT COUNT(*) COUNT_TOTAL, count(distinct mom_pat_id) AS COUNT_MOM, count(distinct child_pat_id) AS COUNT_CHILD  FROM XDR_WHERRY_preg_matching;      --27524	17061	20234



--ELIMINATE CHILDREN ASSIGNED AS MOTHER TO OTHER CHILDREN
DROP TABLE XDR_WHERRY_preg_matching_FINAL PURGE;
CREATE TABLE XDR_WHERRY_preg_matching_FINAL AS
SELECT DISTINCT 
ORIG.PROXY_MATCH
,ORIG.PHONE_MATCH
,ORIG.MOM_PAT_ID
,ORIG.EMAIL_MATCH
--,ORIG.EFFECTIVE_DATE_DT
,ORIG.CHILD_PAT_ID
,ORIG.CHILD_BIRTH_DATE
,ORIG.ADDRESS_MATCH
,MOM.ZIP AS MOM_ZIP
,MOM.PHONE_YN AS MOM_PHONE_YN
,MOM.HOME_PHONE AS MOM_HOME_PHONE
,MOM.EMAIL_ADDRESS AS MOM_EMAIL_ADDRESS
,MOM.CITY AS MOM_CITY
,MOM.ADD_LINE_1 AS MOM_ADD_LINE_1
,MOM.ADDRESS_YN AS MOM_ADDRESS_YN
,PM.PAT_NAME AS MOM_NAME
,CLD.ZIP AS CHILD_ZIP
,CLD.PHONE_YN AS CHILD_PHONE_YN
,CLD.HOME_PHONE AS CHILD_HOME_PHONE
,CLD.EMAIL_ADDRESS AS CHILD_EMAIL_ADDRESS
,CLD.CITY AS CHILD_CITY
,CLD.ADD_LINE_1 AS CHILD_ADD_LINE_1
,CLD.ADDRESS_YN AS CHILD_ADDRESS_YN
,CM.PAT_NAME AS CHILD_NAME
FROM XDR_WHERRY_preg_matching       ORIG
LEFT JOIN XDR_WHERRY_mom_matching		mom on orig.mom_pat_id = mom.mom_pat_id
left join clarity.patient               pm on mom.mom_pat_id = pm.pat_id
LEFT JOIN XDR_WHERRY_child_matching		cld on ORIG.child_pat_id = cld.child_pat_id
left join clarity.patient               cm on cld.child_pat_id = cm.pat_id
WHERE
    --ELIMINATE CHILDREN ASSIGNED AS MOTHER TO OTHER CHILDREN
    --CALCUALTE AGE IN YEARS
 ROUND(MONTHS_BETWEEN(ORIG.EFFECTIVE_DATE_DT,pm.birth_date)/12) > 1;
 
 
SELECT COUNT(*) COUNT_TOTAL, count(distinct mom_pat_id) AS COUNT_MOM, count(distinct child_pat_id) AS COUNT_CHILD  FROM XDR_WHERRY_preg_matching_FINAL;      --20720	17037	20210     27524	17061	20234 
 
 
SELECT * FROM  XDR_WHERRY_preg_matching_FINAL;


--find children assigned 2 mothers and set them aside and remove them from table
create table XDR_WHERRY_preg_matching_ex as
select  orig.*
from (
select CHILD_PAT_ID,count(distinct MOM_PAT_ID) mom_count from XDR_WHERRY_preg_matching_final
group by CHILD_PAT_ID
) x
join XDR_WHERRY_preg_matching_final orig on x.CHILD_PAT_ID = orig.CHILD_PAT_ID
where x.mom_count > 1
--and orig.MOM_PAT_ID <> x.CHILD_PAT_ID
--AND ROUND(MONTHS_BETWEEN(SYSDATE,pm.birth_date)/12) > 10
order by orig.CHILD_PAT_ID, orig.MOM_PAT_ID;

select count(*) from XDR_WHERRY_preg_matching_ex;

delete from XDR_WHERRY_preg_matching_final
where CHILD_PAT_ID in (select distinct CHILD_PAT_ID from XDR_WHERRY_preg_matching_ex );

commit;


SELECT COUNT(*) COUNT_TOTAL, count(distinct mom_pat_id) AS COUNT_MOM, count(distinct child_pat_id) AS COUNT_CHILD  FROM XDR_WHERRY_preg_matching_FINAL;      --20720	17037	20210     27524	17061	20234 
/*************************************************************************************
--insert final 2006-2013 mom-child records into xdr_wherry_all_mom_child

************************************************************************************/
ALTER TABLE xdr_wherry_all_mom_child ADD PROXY_MATCH VARCHAR(2);
ALTER TABLE  xdr_wherry_all_mom_child ADD ADDRESS_MATCH VARCHAR(2);
ALTER TABLE xdr_wherry_all_mom_child ADD PHONE_MATCH VARCHAR(2);
ALTER TABLE xdr_wherry_all_mom_child ADD EMAIL_MATCH VARCHAR(2);
ALTER TABLE xdr_wherry_all_mom_child ADD SIMILAR_ADDRESS VARCHAR(2);

INSERT INTO xdr_wherry_all_mom_child (NB_pat_id,nb_dob,nb_rank,nb_sex,nb_zip,mom_pat_id,mom_dob
SELECT DISTINCT CHILD_PAT_ID
				,CHILD_BIRTH_DATE
				,NULL nb_rank
				,NULL nb_sex
				,CHILD_ZIP
				,mom_pat_id
				,NULL mom_dob
				,NULL number_of_babies
FROM XDR_WHERRY_preg_matching_final;
COMMIT;
SELECT COUNT(*) TOT_COUNT, COUNT(DISTINCT MOM_PAT_ID) AS MOM_COUNT, COUNT(DISTINCT NB_PAT_ID) AS NB_COUNT FROM xdr_wherry_all_mom_child;




nb.pat_id                                               AS nb_pat_id
               --,nbp.pat_mrn_id                                          AS nb_mrn
               ,nbp.birth_date                                          AS nb_dob
               --,mom.child_enc_csn_id                                    AS nb_csn
               ,mom.line                                                AS nb_rank
               ,xsx.name                                                AS nb_sex
               ,nbp.ped_gest_age                                        AS nb_age
               ,nbp.zip                                                 AS nb_zip
               ,mom.pat_id                                              AS mom_pat_id
               --,mp.pat_mrn_id                                           AS mom_mrn
               ,mp.birth_date                                           AS mom_dob
               --,trunc(months_between(nbp.birth_date, mp.birth_date)/12) AS mom_age_at_delivery
               --,mom.pat_enc_csn_id                                      AS mom_csn
               ,MAX(line) OVER (PARTITION BY mom.pat_enc_csn_id)        AS number_of_babies



select matching_vector,count(*) from (
SELECT distinct x.ADDRESS_MATCH ||  x.PHONE_MATCH ||  x.EMAIL_MATCH || x.PROXY_MATCH as matching_vector
,x.MOM_PAT_ID
,x.CHILD_PAT_ID
FROM XDR_WHERRY_preg_matching_final x )
group by matching_vector
order by matching_vector;


--what about mothers who were assigned more than one kid on the same data but they were not twins?
select x.* 
from (
select MOM_PAT_ID,CHILD_BIRTH_DATE
,EMAIL_MATCH
,count(distinct CHILD_PAT_ID ) CHILD_count from XDR_WHERRY_preg_matching_final
group by MOM_PAT_ID,CHILD_BIRTH_DATE,EMAIL_MATCH
) x
where x.CHILD_count > 1
ORDER BY X.MOM_PAT_ID;

--618 MOTHERS WITH TWINS OR TRIPLETS
--SAME FLAG ON PROXY 593
--SAME FLAG ON ADDRESS 516
--SAME FLAG ON PHONE 536
--SAME FLAG ON EMAIL 611