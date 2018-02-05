-- *******************************************************************************************************
--   The hsp_ld_mom_child table (Step 2.1) only covers a portion of the period cover by the study which means that
--	 will use two processes to compile all the children linked to the mothers in this study.
--  The second approach will look at:
--		Step 2.2 - Find children born during the period for this process (01/01/2006 - 03/01/203)
--		Step 2.3 - find hospital encounters for mothers identified in Step 1.1 (pregnancy dx codes)
--		Step 2.4 - Find children born during a mother hospitalization period
--		Step 2.5 - obtain patient contact information (address, phone, email, and proxy pat_id) for children and mothers
--		Step 2.6 - clean patient contact information
--		Step 2.7 - link children to potential mother based on the patient contact information --> create flags to categorized the linkages
-- *******************************************************************************************************

	
-- *******************************************************************************************************
-- STEP 2.1
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



--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'xdr_wherry_all_mom_child' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	--    3,736(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 		--5,953,931(9/5/17)
FROM xdr_wherry_all_mom_child;
COMMIT;


-- *******************************************************************************************************
-- STEP 2.2
--   Create an initial children table with all patients born in the period prior to CC launch date (01/01/2006 - 03/01/2013)
-- *******************************************************************************************************
--maybe reduce this to the '01/01/2006' AND '03/02/2013' period since 2013 to present is addressed by the mom-child link table
DROP TABLE XDR_WHERRY_preg_childall PURGE;
CREATE TABLE XDR_WHERRY_preg_childall AS
SELECT pat.*
FROM clarity.patient pat
WHERE birth_date BETWEEN   '01/01/2006' AND '03/01/2013';

--QA counts (to be removed?)
/*
select count(*) , count(distinct pat_id)  from XDR_WHERRY_PREG_CHILDALL;        --168490	168490
select extract(year from birth_date) as y, count(distinct pat_id)  
from XDR_WHERRY_PREG_CHILDALL
group by extract(year from birth_date)
order by y;        --168490	168490
*/

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_WHERRY_preg_childall' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	--    3,736(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 		--5,953,931(9/5/17)
FROM XDR_WHERRY_preg_childall;
COMMIT;


-- *******************************************************************************************************
-- STEP 2.3
--   		Create hospital encounters table for mother for the '01/01/2006' - '03/01/2013' period
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
-- STEP 2.4
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
	,COUNT(distinct mom_pat_id) AS PAT_COUNT	--
	,COUNT(*) AS TOTAL_COUNT 					--
FROM XDR_WHERRY_preg_enc_dob;
COMMIT;





-- *******************************************************************************************************
-- STEP 2.5
--   		Create mother and children specific tables to optimize matching query
-- *******************************************************************************************************

--	Mothers
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


-- Children
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
-- STEP 2.6
--   		Clean patent contact info to avoid low quality matches
--------------------------------------------------------------------------------------------------------
--		Identify place holders, dummy, or mistaken contact information to exclude from the matching query
--		The code already includes some common patterns found at UCLA that refer to this issues. However, 
--		the script is also looking at the particular datasets to identify other potential issues and includes them in the code
--		This portion requires some minor manual manipulation, as the issues will be different at each site but
-- 		it is important to executed in order to create an optimized dataset that reduce un-wanted matches.
-- *******************************************************************************************************

	--------------------------------------------------------------------------------------------------------
	--	Step 2.6.1: Address lookup
	--	 It identifies some dummy/place holder records and some entries that belong to health institutions which shouldn't be user to pair patients
	--	 Based on our findings, we manually enter some entries on the first portion of the WHERE clause and add a flag to be used [address_yn] later
	--	This is ran both in the XDR_WHERRY_mom_matching and XDR_WHERRY_child_matching tables
	--------------------------------------------------------------------------------------------------------
select add_line_1, count(*) AS C FROM
(
select add_line_1 from XDR_WHERRY_child_matching
UNION ALL
select add_line_1 from XDR_WHERRY_mom_matching
)
group by add_line_1  
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

	--------------------------------------------------------------------------------------------------------
	--	Step 2.6.2: Home phone lookup
	--	 It identifies some dummy/place holder records and some entries that belong to health institutions which shouldn't be user to pair patients
	-- 	we use this service to check some phone #s: https://www.411.com/phone
	--	 Based on our findings, we manually enter some entries on the first portion of the WHERE clause and add a flag to be used [phone_yn] later
	--	This is ran both in the XDR_WHERRY_mom_matching and XDR_WHERRY_child_matching tables
	--------------------------------------------------------------------------------------------------------
	--------------------------------------------------------------------------------------------------------
select HOME_PHONE, count(*) AS C FROM
(
select HOME_PHONE from XDR_WHERRY_child_matching
UNION ALL
select HOME_PHONE from XDR_WHERRY_mom_matching
)
group by HOME_PHONE  
order by c desc;

/* Example of records identified
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
*/




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

	--------------------------------------------------------------------------------------------------------
	--	Step 2.6.3: Email lookup
	--	 We did find dummy/place holders records. It only showed null and potentially real address
	--------------------------------------------------------------------------------------------------------

select EMAIL_ADDRESS, count(*) AS C FROM
(
select EMAIL_ADDRESS from XDR_WHERRY_child_matching
UNION ALL
select EMAIL_ADDRESS from XDR_WHERRY_mom_matching
)
group by EMAIL_ADDRESS  
order by c desc;





-- *******************************************************************************************************
-- STEP 2.7
--   		Match mothers to children based on 
--				Proxy: pat_id
--				Address
--				Phone
--				Email	
--------------------------------------------------------------------------------------------------------
-- *******************************************************************************************************
DROP TABLE XDR_WHERRY_preg_matching purge;
CREATE TABLE XDR_WHERRY_preg_matching AS
SELECT DISTINCT enc.pat_id as mom_pat_id
				,enc.child_pat_id
				,enc.effective_date_dt
				,enc.hosp_admsn_time 
				,enc.hosp_dischrg_time
				,enc.CHILD_BIRTH_DATE
				
				
				,CASE WHEN (	--SOUNDEX ADDRESS MATCH
							SOUNDEX(mom.add_line_1) = SOUNDEX(cld.add_line_1)
							AND mom.ADDRESS_YN = 'y' 
							AND cld.ADDRESS_YN = 'y'
							) THEN 1 ELSE 0 END SIMILAR_ADDRESS
				,CASE WHEN ( 	--EXACT ADDRESS MATCH
                            mom.add_line_1 = cld.add_line_1
                            AND mom.ADDRESS_YN = 'y' 
                            AND cld.ADDRESS_YN = 'y'
                            ) THEN 1 ELSE 0 END ADDRESS_MATCH
				,CASE WHEN ( 	--EXACT HOME PHONE MATCH
                            mom.home_phone = cld.home_phone
                            AND mom.phone_YN = 'y' 
                            AND cld.phone_YN = 'y'
            			) THEN 1 ELSE 0 END PHONE_MATCH
				,CASE WHEN (	--EXACT EMAIL MATCH
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




-- *******************************************************************************************************
-- STEP 2.8
--   		Create final matching table after removing some of the noise and duplication 
--				* Select distinct matches (this is done here to avoid running a DISTINCT on Step ___ that could affect performance)
--				* Sometimes children may received a pregnancy diagnoses that could set them up to be selected as potential mothers
--				* Remove children matched to two different mothers
-- *******************************************************************************************************

	--------------------------------------------------------------------------------------------------------
	--	Step 2.8.1: Select distinct matches (this is done here to avoid running a DISTINCT on Step ___ that could affect performance)
	--				Sometimes children may received a pregnancy diagnoses that could set them up to be selected as potential mothers
	--------------------------------------------------------------------------------------------------------
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
	ROUND(MONTHS_BETWEEN(ORIG.EFFECTIVE_DATE_DT,pm.birth_date)/12) > 1;
 
 
SELECT COUNT(*) COUNT_TOTAL, count(distinct mom_pat_id) AS COUNT_MOM, count(distinct child_pat_id) AS COUNT_CHILD  FROM XDR_WHERRY_preg_matching_FINAL;      --20720	17037	20210     27524	17061	20234 
 
 
SELECT * FROM  XDR_WHERRY_preg_matching_FINAL;

	--------------------------------------------------------------------------------------------------------
	--	Step 2.8.2: Remove children matched to two different mothers
	--------------------------------------------------------------------------------------------------------
DROP TABLE XDR_WHERRY_preg_matching_ex PURGE;
create table XDR_WHERRY_preg_matching_ex as
select  orig.*
from (
		select CHILD_PAT_ID
				,count(distinct MOM_PAT_ID) mom_count 
		from XDR_WHERRY_preg_matching_final
		group by CHILD_PAT_ID
	) x
join XDR_WHERRY_preg_matching_final orig on x.CHILD_PAT_ID = orig.CHILD_PAT_ID
where 
	x.mom_count > 1
order by orig.CHILD_PAT_ID, orig.MOM_PAT_ID;

select count(*) from XDR_WHERRY_preg_matching_ex;

delete from XDR_WHERRY_preg_matching_final
where CHILD_PAT_ID in (
						select distinct CHILD_PAT_ID 
						from XDR_WHERRY_preg_matching_ex 
						);

commit;


SELECT COUNT(*) COUNT_TOTAL, count(distinct mom_pat_id) AS COUNT_MOM, count(distinct child_pat_id) AS COUNT_CHILD  FROM XDR_WHERRY_preg_matching_FINAL;      --20720	17037	20210     27524	17061	20234 

-- *******************************************************************************************************
-- STEP 2.9
--   		insert final 2006-2013 mother-child records into xdr_wherry_all_mom_child (pending) 2/2/18
-- *******************************************************************************************************
--Add flags created duing ,atching process
ALTER TABLE xdr_wherry_all_mom_child ADD PROXY_MATCH VARCHAR(2);
ALTER TABLE  xdr_wherry_all_mom_child ADD ADDRESS_MATCH VARCHAR(2);
ALTER TABLE xdr_wherry_all_mom_child ADD PHONE_MATCH VARCHAR(2);
ALTER TABLE xdr_wherry_all_mom_child ADD EMAIL_MATCH VARCHAR(2);
ALTER TABLE xdr_wherry_all_mom_child ADD SIMILAR_ADDRESS VARCHAR(2);

--insert records 
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



			   
--QA queries and descriptive outomes
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