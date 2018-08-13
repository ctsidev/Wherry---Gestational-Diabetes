-- *******************************************************************************************************
--   The hsp_ld_mom_child table (Step 2.1) only covers a portion of the period of the study, which means that
--	 we will use two processes to compile all the children linked to the mothers in the cohort.
--	 At UCLA, the go live date for Care Connect was 03/01/2013 and it marks the date used in the threshold for the second approach. 
--	 (This might differ depending on your organization)
--   The second approach will look at:
--		Step 2.2 - Find children born during the following period (01/01/2006 - 03/01/203)
--		Step 2.3 - find hospital encounters for mothers identified in Step 1.1 (pregnancy DX codes)
--		Step 2.4 - Create potential matches for children born during a mother hospitalization period
--		Step 2.5 - Obtain patient contact information (address, phone, email, and proxy pat_id) for children and mothers
--		Step 2.6 - Clean patient contact information
--		Step 2.7 - Link children to potential mother based on the patient contact information --> create flags to categorized the linkages
-- *******************************************************************************************************

	
-- *******************************************************************************************************
-- STEP 2.1
--   Create an initial table with ALL mother-child records available in Clarity 
--	 At UCLA, this data is available from the date that Care Connect launched on 03/2013 to the present time
-- *******************************************************************************************************
DROP TABLE xdr_wherry_all_mom_child PURGE;                                                  
CREATE TABLE xdr_wherry_all_mom_child AS
SELECT DISTINCT nb.pat_id                                               AS child_pat_id
               ,nbp.birth_date                                          AS child_dob
               ,xsx.name                                                AS child_sex
               ,nbp.ped_gest_age                                        AS child_age
               ,nbp.zip                                                 AS child_zip
               ,mom.pat_id                                              AS mom_pat_id
               ,mp.birth_date                                           AS mom_dob
  FROM clarity.hsp_ld_mom_child mom
  LEFT JOIN clarity.pat_enc     nb  ON mom.child_enc_csn_id = nb.pat_enc_csn_id
  LEFT JOIN clarity.patient     nbp ON nb.pat_id = nbp.pat_id
  LEFT JOIN clarity.zc_sex      xsx ON nbp.sex_c = xsx.rcpt_mem_sex_c
  LEFT JOIN clarity.patient     mp  ON mom.pat_id = mp.pat_id
  WHERE 
        trunc(nbp.birth_date) BETWEEN '01/01/2006' AND '08/01/2018';


--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'xdr_wherry_all_mom_child' AS TABLE_NAME
	,COUNT(distinct child_pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
	,'Create a table with all the mother-children matches accoridng to hsp_ld_mom_child' AS DESCRIPTION	
FROM xdr_wherry_all_mom_child;
COMMIT;


-- *******************************************************************************************************
-- STEP 2.2
--   Create an initial children table with all patients born in the period prior to CC launch date (01/01/2006 - 03/01/2013)
-- *******************************************************************************************************
DROP TABLE XDR_WHERRY_preg_childall PURGE;
CREATE TABLE XDR_WHERRY_preg_childall AS
SELECT DISTINCT pat.pat_id
			,pat.pat_name
			,pat.ADD_LINE_1
			,pat.CITY
			,pat.ZIP
			,pat.HOME_PHONE
			,pat.EMAIL_ADDRESS
			,pat.BIRTH_DATE
			,pat.sex
FROM clarity.patient pat
WHERE birth_date BETWEEN   '01/01/2006' AND '03/01/2013';

--This index shall improve performance on future steps.
CREATE INDEX XDR_WHERRY_preg_childall_DTIX ON XDR_WHERRY_preg_childall(BIRTH_DATE);


--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_preg_childall' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 		
	,'Children with a dob in the pre-Care Connect period' AS DESCRIPTION					
FROM XDR_WHERRY_preg_childall;
COMMIT;

-- *******************************************************************************************************
-- STEP 2.3
--   		Create encounters table for the '01/01/2006' - '03/01/2013' period
-- *******************************************************************************************************
DROP TABLE XDR_WHERRY_preg_DELIV PURGE;
CREATE TABLE XDR_WHERRY_preg_DELIV AS
SELECT * 
FROM XDR_WHERRY_preg_enc
WHERE 
		trunc(effective_date_dt) BETWEEN '01/01/2006' AND '03/01/2013';
--This index shall improve performance on future steps.
CREATE INDEX XDR_WHERRY_preg_DELIV_DTIX ON XDR_WHERRY_preg_DELIV(effective_date_dt);

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_preg_DELIV' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT 				
	,'Encounters in the pre-Care Connect period' AS DESCRIPTION					
FROM XDR_WHERRY_preg_DELIV;
COMMIT;


-- *******************************************************************************************************
-- STEP 2.4
--   		Create table matching hospital encounter with children DOB for the '01/01/2006' - '03/01/2013' population
--			This step is tiem consuming as it took 22k seconds to run 
-- *******************************************************************************************************
DROP TABLE XDR_WHERRY_preg_enc_dob PURGE;
CREATE TABLE XDR_WHERRY_preg_enc_dob AS
SELECT enc.PAT_ID		as mom_pat_id
		,ENC.effective_date_dt
		,cld.pat_id as child_pat_id
		,cld.BIRTH_DATE as child_birth_date
FROM XDR_WHERRY_PREG_CHILDALL	cld			
JOIN  XDR_WHERRY_preg_DELIV		enc ON  cld.birth_date = enc.effective_date_dt;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_preg_enc_dob' AS TABLE_NAME
	,COUNT(distinct mom_pat_id) AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 
	,'Create table with a dob matching a mothers encounter' AS DESCRIPTION					
FROM XDR_WHERRY_preg_enc_dob;
COMMIT;


--optimize table by limiting to distinct records
DROP TABLE XDR_WHERRY_preg_enc_dob_dist PURGE;
CREATE TABLE XDR_WHERRY_preg_enc_dob_dist AS
select distinct x.* from XDR_WHERRY_preg_enc_dob x;


--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_preg_enc_dob_dist' AS TABLE_NAME
	,COUNT(distinct mom_pat_id) AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 					
	,'Create table with a dob matching a mothers encounter (distinct)' AS DESCRIPTION	
FROM XDR_WHERRY_preg_enc_dob_dist;
COMMIT;

-- *******************************************************************************************************
-- STEP 2.5
--   		Create mother and children specific tables to optimize matching query
-- *******************************************************************************************************
	---------------------------------------------------
	--	Step 2.5.1	Mothers
	---------------------------------------------------
DROP TABLE XDR_WHERRY_mom_matching PURGE;
CREATE TABLE XDR_WHERRY_mom_matching AS
SELECT DISTINCT enc.MOM_PAT_ID
			,pat.add_line_1
			,pat.city
			,pat.zip
			,pat.home_phone
			,pat.email_address
			,pat.birth_date
			,pat.sex
FROM XDR_WHERRY_preg_enc_dob_dist		enc
LEFT JOIN clarity.patient			pat on enc.MOM_PAT_ID = pat.pat_id
--WHERE --sometimes the child receives some of the DX codes used in step 1.1. to identify the mothers
	--ROUND(MONTHS_BETWEEN(ENC.EFFECTIVE_DATE_DT,PAT.birth_date)/12) > 1
;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_mom_matching' AS TABLE_NAME
	,COUNT(distinct mom_pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT 				
	,'Create Mothers table with contact information' AS DESCRIPTION
FROM XDR_WHERRY_mom_matching;
COMMIT;

--create indexes
create index XDR_WHERRY_mom_patidix on XDR_WHERRY_mom_matching(mom_pat_id);
create index XDR_WHERRY_mom_addix on XDR_WHERRY_mom_matching(add_line_1);
create index XDR_WHERRY_mom_phix on XDR_WHERRY_mom_matching(home_phone);
create index XDR_WHERRY_mom_emailix on XDR_WHERRY_mom_matching(email_address);
	---------------------------------------------------
	--	Step 2.5.2	Children
	---------------------------------------------------
DROP TABLE XDR_WHERRY_child_matching PURGE;
CREATE TABLE XDR_WHERRY_child_matching AS
SELECT DISTINCT enc.child_pat_id
			,pat.add_line_1
			,pat.city
			,pat.zip
			,pat.home_phone
			,pat.email_address
			,pat.birth_date
			,pat.sex
FROM XDR_WHERRY_preg_enc_dob_dist		enc
LEFT JOIN XDR_WHERRY_preg_childall			pat on enc.child_pat_id = pat.pat_id;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_child_matching' AS TABLE_NAME
	,COUNT(distinct child_pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
	,'Create children table with contact information' AS DESCRIPTION
FROM XDR_WHERRY_child_matching;
COMMIT;

--create indexes
create index XDR_WHERRY_cld_patidix on XDR_WHERRY_child_matching(child_pat_id);
create index XDR_WHERRY_cld_addix on XDR_WHERRY_child_matching(add_line_1);
create index XDR_WHERRY_cld_phix on XDR_WHERRY_child_matching(home_phone);
create index XDR_WHERRY_cld_emailix on XDR_WHERRY_child_matching(email_address);



-- *******************************************************************************************************
-- STEP 2.6
--   		Clean patent contact info to avoid low quality matches
--------------------------------------------------------------------------------------------------------
--		Identify place holders, dummy, or mistaken contact information records to exclude from the matching criteria
--		The code already includes some common patterns found at UCLA that refer to this issues. However, 
--		the script is also looking at the particular datasets to identify other potential issues and include them in the code.
--		This portion requires some minor manual manipulation, as the issues will be different at each site but
-- 		it is important to executed in order to create an optimized dataset that reduce un-wanted matches.
-- *******************************************************************************************************

	--------------------------------------------------------------------------------------------------------
	--	Step 2.6.1: Address clean-up
	--	 It identifies some dummy/place holder records and some entries that belong to health institutions which shouldn't be used to pair patients
	--	 Based on our findings, we manually enter some entries on the first portion of the WHERE clause and add a flag to be used [address_yn] later
	--	 This is ran both in the XDR_WHERRY_mom_matching and XDR_WHERRY_child_matching tables
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
--628 rows updated.
--632 rows updated.

update XDR_WHERRY_mom_matching
set address_yn = 'y'
where address_yn is null;
commit;
--29,602 rows updated.
--29,461 rows updated.

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_mom_matching' AS TABLE_NAME
	,COUNT(distinct mom_pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
	,'Mom Matching addresses'
FROM XDR_WHERRY_mom_matching
WHERE address_yn = 'y';
COMMIT;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_mom_matching' AS TABLE_NAME
	,COUNT(distinct mom_pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
	,'Mom not Matching addresses'
FROM XDR_WHERRY_mom_matching
WHERE address_yn = 'n';
COMMIT;


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
--3,917 rows updated.
--3,749 rows updated.

update XDR_WHERRY_child_matching
set address_yn = 'y'
where address_yn is null;
commit;
--109,821 rows updated.
--105,899 rows updated.

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_child_matching' AS TABLE_NAME
	,COUNT(distinct child_pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
	,'Children Matching addresses'
FROM XDR_WHERRY_child_matching
WHERE address_yn = 'y';
COMMIT;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_child_matching' AS TABLE_NAME
	,COUNT(distinct child_pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
	,'Children not Matching addresses'
FROM XDR_WHERRY_child_matching
WHERE address_yn = 'n';
COMMIT;

	--------------------------------------------------------------------------------------------------------
	--	Step 2.6.2: Home phone clean-up
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
310-000-0000	38
818-252-5863	34      Totally Kids Specialty Health Care
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
--2,055 rows updated.
--1,891 rows updated.


update XDR_WHERRY_mom_matching
set phone_yn = 'y'
where phone_yn is null;
commit;
--28,175 rows updated.
--28,202 rows updated.


--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_mom_matching' AS TABLE_NAME
	,COUNT(distinct mom_pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
	,'Mom Matching phone'
FROM XDR_WHERRY_mom_matching
WHERE phone_yn = 'y';
COMMIT;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_mom_matching' AS TABLE_NAME
	,COUNT(distinct mom_pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
	,'Mom not Matching phone not'
FROM XDR_WHERRY_mom_matching
WHERE phone_yn = 'n';
COMMIT;



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
--9,223 rows updated.
--8,729 rows updated.


update XDR_WHERRY_child_matching
set phone_yn = 'y'
where phone_yn is null;
commit;
--104,515 rows updated.
--101,111 rows updated.

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_child_matching' AS TABLE_NAME
	,COUNT(distinct child_pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
	,'Children Matching phone'
FROM XDR_WHERRY_child_matching
WHERE phone_yn = 'y';
COMMIT;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_child_matching' AS TABLE_NAME
	,COUNT(distinct child_pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
	,'Children not Matching phone'
FROM XDR_WHERRY_child_matching
WHERE phone_yn = 'n';
COMMIT;


	--------------------------------------------------------------------------------------------------------
	--	Step 2.6.3: Email clean-up
	--	 We didn't find dummy/place holders records. It only showed null and potentially real address
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
-- *******************************************************************************************************
DROP TABLE XDR_WHERRY_preg_matching purge;
CREATE TABLE XDR_WHERRY_preg_matching AS
SELECT DISTINCT enc.mom_pat_id
				,enc.child_pat_id
				,enc.effective_date_dt
				,enc.hosp_admsn_time 
				,enc.hosp_dischrg_time
				,enc.CHILD_BIRTH_DATE
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
				,CASE WHEN enc.mom_pat_id = prx.proxy_pat_id THEN 1 ELSE 0 END PROXY_MATCH
FROM XDR_WHERRY_preg_enc_dob_dist		enc
LEFT JOIN XDR_WHERRY_mom_matching		mom on enc.mom_pat_id = mom.mom_pat_id
LEFT JOIN XDR_WHERRY_child_matching		cld on enc.child_pat_id = cld.child_pat_id
LEFT JOIN clarity.PAT_MYC_PRXY_HX		prx ON enc.child_pat_id = prx.pat_id
WHERE
    	enc.mom_pat_id <> enc.child_pat_id      --sometimes, the child gets assigned a pregnancy dx code and since it was also at the hospital, it can get tagged to herself
         and
        (
        --EXACT ADDRESS MATCH
		 ( 
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
		OR (enc.mom_pat_id = prx.proxy_pat_id)
		);

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_preg_matching' AS TABLE_NAME
	,COUNT(distinct mom_pat_id) AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 					
    ,'Match mothers to children based on Proxy: pat_id, Address, Phone, or Email' as DESCRIPTION
FROM XDR_WHERRY_preg_matching;
COMMIT;



--------------------------------------------------------------------------------------------------------
--	2.7.1	Add similar address match
--------------------------------------------------------------------------------------------------------
ALTER TABLE XDR_WHERRY_preg_matching ADD SIMILAR_ADDRESS NUMBER;

MERGE INTO XDR_WHERRY_preg_matching pat
using
  (SELECT DISTINCT enc.mom_pat_id
				,enc.child_pat_id
				,enc.effective_date_dt
				,enc.CHILD_BIRTH_DATE
				,1 AS SIMILAR_ADDRESS
FROM XDR_WHERRY_preg_matching			mat
JOIN XDR_WHERRY_preg_enc_dob_dist		enc ON mat.mom_pat_id = enc.mom_pat_id AND mat.child_pat_id = enc.child_pat_id
LEFT JOIN XDR_WHERRY_mom_matching		mom on enc.mom_pat_id = mom.mom_pat_id
LEFT JOIN XDR_WHERRY_child_matching		cld on enc.child_pat_id = cld.child_pat_id
LEFT JOIN clarity.PAT_MYC_PRXY_HX		prx ON enc.child_pat_id = prx.pat_id
WHERE 
	SOUNDEX(mom.add_line_1) = SOUNDEX(cld.add_line_1)
		AND mom.ADDRESS_YN = 'y' 
        AND cld.ADDRESS_YN = 'y'
  ) r
  on (pat.mom_pat_id = r.mom_pat_id
	AND pat.child_pat_id = r.child_pat_id)
  when matched then
      update set SIMILAR_ADDRESS = r.SIMILAR_ADDRESS;
--23,843 rows merged.     




-- *******************************************************************************************************
-- STEP 2.8
--   		Create final matching table after removing some of the noise and duplication 
--				* Select distinct matches (this is done here to avoid running a DISTINCT on Step 2.7 that could affect performance)
--				* Sometimes children may received a pregnancy diagnoses that could set them up to be selected as potential mothers
--				* Remove children matched to two different mothers (the low number of cases doesn't justify the effort of the manual review)
-- *******************************************************************************************************

	--------------------------------------------------------------------------------------------------------
	--	Step 2.8.1: Select distinct matches (this is done here to avoid running a DISTINCT on Step 2.7 that could affect performance)
	--				Sometimes children may received a pregnancy diagnoses that could set them up to be selected as potential mothers
	--				The PHI in here is not to be released to the PI and only used for QA the matching process
	--------------------------------------------------------------------------------------------------------
DROP TABLE XDR_WHERRY_preg_matching_FINAL PURGE;
CREATE TABLE XDR_WHERRY_preg_matching_FINAL AS
SELECT DISTINCT ORIG.MOM_PAT_ID
			,ORIG.CHILD_PAT_ID
			,ORIG.EMAIL_MATCH
			,ORIG.PROXY_MATCH
			,ORIG.PHONE_MATCH
			,ORIG.ADDRESS_MATCH
			,ORIG.SIMILAR_ADDRESS
			,MOM.ZIP 			AS MOM_ZIP
			,MOM.PHONE_YN 		AS MOM_PHONE_YN
			,MOM.HOME_PHONE 	AS MOM_HOME_PHONE
			,MOM.EMAIL_ADDRESS 	AS MOM_EMAIL_ADDRESS
			,MOM.CITY 			AS MOM_CITY
			,MOM.ADD_LINE_1 	AS MOM_ADD_LINE_1
			,MOM.ADDRESS_YN 	AS MOM_ADDRESS_YN
			,MOM.BIRTH_DATE		AS MOM_BIRTH_DATE
			,MOM.SEX            as MOM_SEX
			,CLD.ZIP 			AS CHILD_ZIP
			,CLD.PHONE_YN 		AS CHILD_PHONE_YN
			,CLD.HOME_PHONE 	AS CHILD_HOME_PHONE
			,CLD.EMAIL_ADDRESS 	AS CHILD_EMAIL_ADDRESS
			,CLD.CITY 			AS CHILD_CITY
			,CLD.ADD_LINE_1 	AS CHILD_ADD_LINE_1
			,CLD.ADDRESS_YN 	AS CHILD_ADDRESS_YN
			,ORIG.CHILD_BIRTH_DATE
			,CLD.SEX            AS CHILD_SEX
FROM XDR_WHERRY_preg_matching       ORIG
LEFT JOIN XDR_WHERRY_mom_matching		mom on orig.mom_pat_id = mom.mom_pat_id
left join clarity.patient               pm on mom.mom_pat_id = pm.pat_id
LEFT JOIN XDR_WHERRY_child_matching		cld on ORIG.child_pat_id = cld.child_pat_id
left join clarity.patient               cm on cld.child_pat_id = cm.pat_id;

 
--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_preg_matching_FINAL' AS TABLE_NAME
	,COUNT(distinct mom_pat_id) AS PAT_COUNT	--
	,COUNT(*) AS TOTAL_COUNT 					--
	,'Create table with all potential matches' as DESCRIPTION
FROM XDR_WHERRY_preg_matching_FINAL;
COMMIT;

--manual QA
SELECT * FROM  XDR_WHERRY_preg_matching_FINAL;

	--------------------------------------------------------------------------------------------------------
	--	Step 2.8.2: Remove children matched to two different mothers
	--				Considering the low count of patients in this scenario and the fact that it will require
	--				a very time consuming process to review each case, the PI decided to simply exclude these patients from final cohort.
	--------------------------------------------------------------------------------------------------------
--select children assigned to two different mothers
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

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_preg_matching_ex' AS TABLE_NAME
	,COUNT(distinct CHILD_PAT_ID) AS PAT_COUNT	--
	,COUNT(*) AS TOTAL_COUNT 					--
	,'Create table with cases to be deleted -double mother-' as DESCRIPTION
FROM XDR_WHERRY_preg_matching_ex;
COMMIT;


	--remove selected children from final table
delete from XDR_WHERRY_preg_matching_final
where CHILD_PAT_ID in (
						select distinct CHILD_PAT_ID 
						from XDR_WHERRY_preg_matching_ex 
						);
commit;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_preg_matching_final' AS TABLE_NAME
	,COUNT(distinct MOM_PAT_ID) AS PAT_COUNT	--
	,COUNT(*) AS TOTAL_COUNT 					--
	,'Final counts for matching cases pre Care Connect' as DESCRIPTION
FROM XDR_WHERRY_preg_matching_final;
COMMIT;


-- *******************************************************************************************************
-- STEP 2.9
--   		insert final 2006-2013 mother-child records into xdr_wherry_all_mom_child
-- *******************************************************************************************************
--Add flags created during matching process
ALTER TABLE xdr_wherry_all_mom_child ADD PROXY_MATCH VARCHAR(2);
ALTER TABLE xdr_wherry_all_mom_child ADD ADDRESS_MATCH VARCHAR(2);
ALTER TABLE xdr_wherry_all_mom_child ADD PHONE_MATCH VARCHAR(2);
ALTER TABLE xdr_wherry_all_mom_child ADD EMAIL_MATCH VARCHAR(2);
ALTER TABLE xdr_wherry_all_mom_child ADD SIMILAR_ADDRESS VARCHAR(2);

--insert records 
INSERT INTO xdr_wherry_all_mom_child (child_pat_id,child_dob,child_sex,child_zip,mom_pat_id,mom_dob,PROXY_MATCH,ADDRESS_MATCH,PHONE_MATCH,EMAIL_MATCH,SIMILAR_ADDRESS)
SELECT DISTINCT CHILD_PAT_ID
				,CHILD_BIRTH_DATE
				,CHILD_SEX
				,CHILD_ZIP
				,mom_pat_id
				,MOM_BIRTH_DATE
				,PROXY_MATCH
				,ADDRESS_MATCH
				,PHONE_MATCH
				,EMAIL_MATCH
				,SIMILAR_ADDRESS
FROM XDR_WHERRY_preg_matching_final;
--13,335 rows inserted.
COMMIT;

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'xdr_wherry_all_mom_child' AS TABLE_NAME
	,COUNT(distinct CHILD_PAT_ID) AS PAT_COUNT	--
	,COUNT(*) AS TOTAL_COUNT 					--
	,'Final counts for matching cases for entire study' as DESCRIPTION
FROM xdr_wherry_all_mom_child;
COMMIT;


			   
--QA queries and descriptive outcomes
DROP TABLE XDR_WHERRY_PREG_MTCHING_VECTOR PURGE;
CREATE TABLE XDR_WHERRY_PREG_MTCHING_VECTOR AS
select matching_vector,count(*) as match_count from (
SELECT distinct x.ADDRESS_MATCH ||  x.PHONE_MATCH ||  x.EMAIL_MATCH || x.PROXY_MATCH as matching_vector
,x.MOM_PAT_ID
,x.CHILD_PAT_ID
FROM XDR_WHERRY_preg_matching_final x )
group by matching_vector
order by matching_vector;
