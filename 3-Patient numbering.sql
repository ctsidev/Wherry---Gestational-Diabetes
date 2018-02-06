--left join pat_acct_cvg 				pac on pat.pat_id = pac.pat_id AND pac.account_active_yn = 'Y'
--left join clarity_epp 				clepp on pac.plan_id = clepp.benefit_plan_id
--left join zc_financial_class 		fincls on pac.fin_class = fincls.financial_class

-- *******************************************************************************************************
--	STEP 2: Create patient table with the demographic information requested by the PI (section c of the criteria + pull demographics)
--				c. Patient age: 18 or older at the time of the admission
-- *******************************************************************************************************
--------------------------------------------------------------------------------
--	STEP 2.1: Create Race Rollup reference table
--------------------------------------------------------------------------------  
DROP TABLE XDR_WHERRY_RACE_ROLLUP PURGE;
CREATE TABLE "XDR_WHERRY_RACE_ROLLUP" 
   (	"PATIENT_RACE_C" NUMBER(5,0), 
	"PATIENT_RACE_NAME" VARCHAR2(50 BYTE), 
	"ROLLUP_RACE_C" NUMBER(5,0), 
	"ROLLUP_RACE_NAME" VARCHAR2(50 BYTE));
--------------------------------------------------------------------------------
--	Step 2.2: Load Race Rollup records
--------------------------------------------------------------------------------
-- The file called [XDR_WHERRY_RACE_ROLLUP.csv] contains the data to load in the table above.
-- You shall use the utility of your choice to load this file into XDR_WHERRY_RACE_ROLLUP
-- which is used on step 2.5 to add the appropiate context to these records.
-- The file shall be formatted as a CSV with double quotation marks as text identifier.
-- It's recommended to check that the 'PATIENT_RACE_C' value corresponds to the same 'PATIENT_RACE_NAME' in your environment.

--------------------------------------------------------------------------------
--	Step 2.3: Create Patient Table
--------------------------------------------------------------------------------
DROP TABLE XDR_WHERRY_preg_pat PURGE;
  CREATE TABLE XDR_WHERRY_preg_pat
   ("STUDY_ID" NUMBER,
	"PAT_ID" VARCHAR2(18 BYTE), 
    "MOM_PAT_ID" VARCHAR2(18 BYTE),
    "FIRST_ENC_DATE" DATE,
--    "PAT_ENC_CSN_ID" VARCHAR(11),
	"BIRTH_DATE" DATE, 
--	"DEATH_DATE" DATE, 
	"PATIENT_STATUS" VARCHAR2(254 BYTE), 
	"ETHNIC_GROUP_C" NUMBER(38,0), 
	"ETHNIC_GROUP" VARCHAR2(254 BYTE), 
	"SEX" VARCHAR2(254 BYTE), 
	"MAPPED_RACE_NAME" VARCHAR2(254 BYTE), 
	"RESTRICTED_YN" VARCHAR2(1 BYTE), 
	"MAPPED_RACE_C" NUMBER(38,0),
    "BENEFIT_PLAN_NAME" VARCHAR2(254 BYTE),
    "FINANCIAL_CLASS" VARCHAR2(254 BYTE),
    "MOM_CHILD_MC" VARCHAR2(1 BYTE)
  );  



INSERT INTO XDR_WHERRY_preg_pat
SELECT rownum as study_id,
	pat.* 
--,'M' MOM_CHILD_YN
from
	(SELECT DISTINCT coh.pat_id,
			null as mom_pat_id,
			null as FIRST_ENC_DATE,
			--coh.PAT_ENC_CSN_ID,
			--p.pat_mrn_id,
			p.birth_date,
			--p.death_date,
			ps.name patient_status,
			coalesce(p.ethnic_group_c, -999) ethnic_group_c, 
			coalesce(eg.name, 'Unknown') ethnic_group,
			coalesce(s.name, 'Unknown') sex,
			null as mapped_race_name,
			p.restricted_yn,
			null as mapped_race_c,
			null as BENEFIT_PLAN_NAME,
			null as FINANCIAL_CLASS
	FROM --(SELECT * 
		  --FROM 
			  (-- ALL WOMEN WITH A PREGANNCY RELATED DX
				SELECT DISTINCT PAT_ID as pat_id
						,'M' MOM_CHILD_YN
				FROM xdr_wherry_prg_pregall
				union
				--ALL CHILDREN LINKED TO A WOMAN WITH A PREGNANCY RELATED DX
				SELECT DISTINCT NB_PAT_ID as CHILD_pat_id
						,'C' MOM_CHILD_YN
					--,MOM_CSN
					--,HOSP_ADMSN_TIME
					--,MIN(HOSP_ADMSN_TIME) OVER (PARTITION BY pat_id) AS FIRST_HOSP_DATE
				FROM xdr_wherry_all_mom_child
			)coh
	join CLARITY.patient p      ON coh.pat_id = p.pat_id
	LEFT JOIN clarity.zc_ethnic_group eg 
		ON p.ethnic_group_c = eg.ethnic_group_c
	LEFT JOIN clarity.zc_patient_status ps 
		ON p.pat_status_c = ps.patient_status_c
	LEFT JOIN clarity.zc_sex s 
		ON p.sex_c = s.rcpt_mem_sex_c
	LEFT JOIN clarity.patient_3 on p.pat_id = patient_3.pat_id
	LEFT JOIN clarity.patient_fyi_flags flags on p.pat_id = flags.patient_id
	left join clarity.patient_race r on coh.pat_id = r.pat_id
	left JOIN clarity.zc_patient_race pr ON r.patient_race_c = pr.patient_race_c
	WHERE p.pat_mrn_id NOT LIKE '<%>'		--- remove patients with invalid MRNs (MRN Not <%>) and test patients.  
			AND p.birth_date is NOT NULL			--exclude patients w/o dob
			AND (patient_3.pat_id is null OR patient_3.is_test_pat_yn is null or patient_3.is_test_pat_yn = 'N')
			AND (flags.PAT_FLAG_TYPE_C is null OR flags.PAT_FLAG_TYPE_C not in (6,8,9,1018,1053))			------  Removed flagged restricted patients
			AND r.patient_race_c is not null
	) pat 
ORDER BY  dbms_random.value
;
--14,715 rows inserted.
COMMIT;

--Add counts for QA
INSERT INTO XDR_WHERRY_preg_COUNTS(TABLE_NAME,PAT_COUNT,TOTAL_COUNT)
SELECT 'XDR_WHERRY_preg_pat' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	-- 3,738(9/1/17)
	,COUNT(*) AS TOTAL_COUNT 		-- 3,738(9/1/17)
FROM XDR_WHERRY_preg_pat
where mom_child_mc = 'M';
COMMIT;

SELECT * FROM XDR_WHERRY_preg_pat ORDER BY PAT_ID;
--------------------------------------------------------------------------------
--	STEP 2.4: Create patient table with all the mapped race values that apply to each patient
--				This process is the one used for UCRex, therefore if you already have this in your datamart
--				you might be able to include it here instead of running this portion of the code.
--------------------------------------------------------------------------------
DROP TABLE XDR_WHERRY_preg_pat_ALLRACE PURGE;
CREATE TABLE XDR_WHERRY_preg_pat_ALLRACE AS 
SELECT r.pat_id, 
		r.patient_race_c, 
		pr.name
FROM XDR_WHERRY_preg_pat pat
join clarity.patient_race r on pat.pat_id = r.pat_id
JOIN clarity.zc_patient_race pr ON r.patient_race_c = pr.patient_race_c
WHERE r.patient_race_c is not null
        AND MOM_CHILD_MC = 'M';

--Add counts for QA
INSERT INTO XDR_WHERRY_preg_COUNTS
SELECT 'XDR_WHERRY_preg_pat_ALLRACE' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 		
	,SYSDATE AS LOAD_TIME
FROM XDR_WHERRY_preg_pat_ALLRACE;
COMMIT;

--------------------------------------------------------------------------------
--	STEP 2.5: Create NEW patient table where only one race category is applied to each patient
--------------------------------------------------------------------------------
DROP TABLE XDR_WHERRY_preg_pat_RACE PURGE;
CREATE TABLE XDR_WHERRY_preg_pat_RACE AS
SELECT DISTINCT pat_id, mapped_race_c, mapped_race_name FROM 
         (SELECT s1.pat_id, 900 AS mapped_race_c, 'Multiple Races' AS mapped_race_name       
            FROM (SELECT DISTINCT lz.pat_id
                    FROM (SELECT DISTINCT pr.pat_id
                                         ,zc.rollup_race_c    AS patient_race_c
                                         ,zc.rollup_race_name AS "NAME"
                            FROM XDR_WHERRY_preg_pat_ALLRACE pr
                            JOIN XDR_WHERRY_RACE_ROLLUP             zc ON pr.patient_race_c = zc.patient_race_c
                         ) lz
                    WHERE lz.patient_race_c NOT IN (7,8)                                      --7=PATIENT REFUSED, 8=UNKNOWN
                    GROUP BY lz.pat_id HAVING count(*) > 1) s1
          UNION ALL
          SELECT s1.pat_id, 7 AS mapped_race_c, 'Patient Refused' AS mapped_race_name         --PATREF AND UNKNOWN ONLY, USE PATREF (E.G. 7,8)
            FROM (SELECT DISTINCT lz.pat_id
                    FROM (SELECT DISTINCT pr.pat_id
                                         ,zc.rollup_race_c    AS patient_race_c
                                         ,zc.rollup_race_name AS "NAME"
                            FROM XDR_WHERRY_preg_pat_ALLRACE pr
                            JOIN XDR_WHERRY_RACE_ROLLUP             zc ON pr.patient_race_c = zc.patient_race_c
                         ) lz
                    WHERE EXISTS (SELECT DISTINCT lz2.pat_id FROM XDR_WHERRY_preg_pat_ALLRACE lz2 WHERE lz.pat_id = lz2.pat_id AND lz2.patient_race_c = 7)
                      AND EXISTS (SELECT DISTINCT lz2.pat_id FROM XDR_WHERRY_preg_pat_ALLRACE lz2 WHERE lz.pat_id = lz2.pat_id AND lz2.patient_race_c = 8)
                    GROUP BY lz.pat_id HAVING count(*) = 2) s1
          UNION ALL
          SELECT DISTINCT s1.pat_id                                                           --SINGLE ONLY (E.G. ANY SINGLE OCCURRENCE OF RACE)
                         ,zc.rollup_race_c     AS mapped_race_c
                         ,zc.rollup_race_name  AS mapped_race_name 
            FROM (SELECT DISTINCT lz.pat_id
                    FROM (SELECT DISTINCT pr.pat_id
                                         ,zc.rollup_race_c    AS patient_race_c
                                         ,zc.rollup_race_name AS "NAME"
                            FROM XDR_WHERRY_preg_pat_ALLRACE pr
                            JOIN XDR_WHERRY_RACE_ROLLUP             zc ON pr.patient_race_c = zc.patient_race_c
                         ) lz
                    GROUP BY lz.pat_id HAVING count(*) = 1) s1
            JOIN XDR_WHERRY_preg_pat_ALLRACE lz ON s1.pat_id = lz.pat_id
            JOIN i2b2.race_rollup             zc ON lz.patient_race_c = zc.patient_race_c
          UNION ALL 
          SELECT DISTINCT s1.pat_id                                                           --PATREF AND UNKNOWN WITH ANOTHER SINGLE RACE, USE SINGLE RACE (E.G. 1,7,8 or 2,7, 4,14,7)
                         ,zc.rollup_race_c     AS mapped_race_c
                         ,zc.rollup_race_name  AS mapped_race_name 
            FROM (SELECT DISTINCT lz.pat_id
                    FROM (SELECT DISTINCT pr.pat_id
                                         ,zc.rollup_race_c    AS patient_race_c
                                         ,zc.rollup_race_name AS "NAME"
                            FROM XDR_WHERRY_preg_pat_ALLRACE pr
                            JOIN XDR_WHERRY_RACE_ROLLUP             zc ON pr.patient_race_c = zc.patient_race_c
                         ) lz
                    WHERE lz.patient_race_c NOT IN (7,8)                                      --7=PATIENT REFUSED, 8=UNKNOWN
                    GROUP BY lz.pat_id HAVING count(*) = 1) s1
            JOIN XDR_WHERRY_preg_pat_ALLRACE lz ON s1.pat_id = lz.pat_id
            JOIN XDR_WHERRY_RACE_ROLLUP             zc ON lz.patient_race_c = zc.patient_race_c
            WHERE lz.patient_race_c NOT IN (7,8) 
         --) x WHERE x.pat_id = p.pat_id
      )order by pat_id
;

--Add counts for QA
INSERT INTO XDR_WHERRY_preg_COUNTS(TABLE_NAME,PAT_COUNT,TOTAL_COUNT)
SELECT 'XDR_WHERRY_preg_pat_RACE' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT		--3,738(9/1/17)
	,COUNT(*) AS TOTAL_COUNT 			--3,738(9/1/17)
FROM XDR_WHERRY_preg_pat_RACE;
COMMIT;
--------------------------------------------------------------------------------
--	STEP 2.6: Update race name accordingly
--------------------------------------------------------------------------------	  
MERGE INTO XDR_WHERRY_preg_pat pat
using
  (select pat_id
        ,MAPPED_RACE_NAME
  FROM XDR_WHERRY_preg_pat_RACE
  ) r
  on (pat.pat_id = r.pat_id)
  when matched then
      update set MAPPED_RACE_NAME = r.MAPPED_RACE_NAME;
COMMIT;      

select * from XDR_WHERRY_preg_pat;   
--------------------------------------------------------------------------------
--	Step 2.7: Insert children records
--------------------------------------------------------------------------------
--DROP TABLE XDR_WHERRY_CHILD PURGE;
--  CREATE TABLE XDR_WHERRY_CHILD
--   ("STUDY_ID" NUMBER,
--	"PAT_ID" VARCHAR2(18 BYTE), 
--    "MOM_PAT_ID" VARCHAR2(18 BYTE), 
----    "FIRST_HOSP_DATE" DATE,
----    "PAT_ENC_CSN_ID" VARCHAR(11),
----	"BIRTH_DATE" DATE, 
----	"DEATH_DATE" DATE, 
--	"PATIENT_STATUS" VARCHAR2(254 BYTE), 
--	"ETHNIC_GROUP_C" NUMBER(38,0), 
--	"ETHNIC_GROUP" VARCHAR2(254 BYTE), 
--	"SEX" VARCHAR2(254 BYTE), 
--	"MAPPED_RACE_NAME" VARCHAR2(254 BYTE), 
--	"RESTRICTED_YN" VARCHAR2(1 BYTE), 
--	"MAPPED_RACE_C" NUMBER(38,0),
--    "BENEFIT_PLAN_NAME" VARCHAR2(254 BYTE),
--    "FINANCIAL_CLASS" VARCHAR2(254 BYTE)
--  );  

/*

INSERT INTO XDR_WHERRY_preg_pat
SELECT rownum as study_id,
pat.*, 
'C' AS MOM_CHILD_MC 
from
(SELECT DISTINCT coh.pat_id,
coh.mom_pat_id,
null as FIRST_ENC_DATE,
--coh.PAT_ENC_CSN_ID,
--p.pat_mrn_id,
p.birth_date,
--p.death_date,
ps.name patient_status,
coalesce(p.ethnic_group_c, -999) ethnic_group_c, 
coalesce(eg.name, 'Unknown') ethnic_group,
coalesce(s.name, 'Unknown') sex,
null as mapped_race_name,
p.restricted_yn,
null as mapped_race_c,
null as BENEFIT_PLAN_NAME,
null as FINANCIAL_CLASS
FROM --(SELECT * 
      --FROM 
          (SELECT DISTINCT nb.NB_PAT_ID as pat_id
                ,mom.mom_pat_id
                --,MOM_CSN
                --,HOSP_ADMSN_TIME
                --,MIN(HOSP_ADMSN_TIME) OVER (PARTITION BY pat_id) AS FIRST_HOSP_DATE
            FROM xdr_wherry_prg_cohdx nb
            join  xdr_wherry_prg_cohdx mom on nb.NB_PAT_ID = mom.NB_PAT_ID
--            ) x
--        WHERE 
--            x.HOSP_ADMSN_TIME = FIRST_HOSP_DATE
        )coh
join CLARITY.patient p      ON coh.pat_id = p.pat_id
LEFT JOIN clarity.zc_ethnic_group eg 
	ON p.ethnic_group_c = eg.ethnic_group_c
LEFT JOIN clarity.zc_patient_status ps 
	ON p.pat_status_c = ps.patient_status_c
LEFT JOIN clarity.zc_sex s 
	ON p.sex_c = s.rcpt_mem_sex_c
LEFT JOIN clarity.patient_3 on p.pat_id = patient_3.pat_id
LEFT JOIN clarity.patient_fyi_flags flags on p.pat_id = flags.patient_id
left join clarity.patient_race r on coh.pat_id = r.pat_id
left JOIN clarity.zc_patient_race pr ON r.patient_race_c = pr.patient_race_c
WHERE p.pat_mrn_id NOT LIKE '<%>'		--- remove patients with invalid MRNs (MRN Not <%>) and test patients.  
AND p.birth_date is NOT NULL			--exclude patients w/o dob
AND (patient_3.pat_id is null OR patient_3.is_test_pat_yn is null or patient_3.is_test_pat_yn = 'N')
AND (flags.PAT_FLAG_TYPE_C is null OR flags.PAT_FLAG_TYPE_C not in (6,8,9,1018,1053))			------  Removed flagged restricted patients
AND r.patient_race_c is not null
--AND MONTHS_BETWEEN(coh.FIRST_HOSP_DATE,p.birth_date)/12 >= 18
) pat 
ORDER BY  dbms_random.value
;
--17,000 rows inserted.
COMMIT;

select count(*),mom_child_mc
from XDR_WHERRY_preg_pat
group by mom_child_mc;

--Add counts for QA
INSERT INTO XDR_WHERRY_preg_COUNTS(TABLE_NAME,PAT_COUNT,TOTAL_COUNT)
SELECT 'XDR_WHERRY_PAT' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	-- 3,738(9/1/17)
	,COUNT(*) AS TOTAL_COUNT 		-- 3,738(9/1/17)
FROM XDR_WHERRY_preg_pat;
COMMIT;

SELECT * FROM XDR_WHERRY_preg_pat ORDER BY PAT_ID;
--------------------------------------------------------------------------------
--	STEP 2.8: Create patient table with all the mapped race values that apply to each patient
--				This process is the one used for UCRex, therefore if you already have this in your datamart
--				you might be able to include it here instead of running this portion of the code.
--              Reutilize same table for all race
--------------------------------------------------------------------------------
DROP TABLE XDR_WHERRY_preg_pat_ALLRACE PURGE;
CREATE TABLE XDR_WHERRY_preg_pat_ALLRACE AS 
SELECT r.pat_id, 
r.patient_race_c, 
pr.name
FROM XDR_WHERRY_preg_pat pat
join clarity.patient_race r on pat.pat_id = r.pat_id
JOIN clarity.zc_patient_race pr ON r.patient_race_c = pr.patient_race_c
WHERE r.patient_race_c is not null
AND MOM_CHILD_MC = 'C';

--Add counts for QA
INSERT INTO XDR_WHERRY_preg_COUNTS(TABLE_NAME,PAT_COUNT,TOTAL_COUNT)
SELECT 'XDR_WHERRY_CHILD_ALLRACE' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 		
	,SYSDATE AS LOAD_TIME
FROM XDR_WHERRY_preg_pat_ALLRACE;
COMMIT;

--------------------------------------------------------------------------------
--	STEP 2.9: Create NEW patient table where only one race category is applied to each patient
--              Reutilize same table for race
--------------------------------------------------------------------------------
DROP TABLE XDR_WHERRY_preg_pat_RACE PURGE;
CREATE TABLE XDR_WHERRY_preg_pat_RACE AS
SELECT DISTINCT pat_id, mapped_race_c, mapped_race_name FROM 
         (SELECT s1.pat_id, 900 AS mapped_race_c, 'Multiple Races' AS mapped_race_name       
            FROM (SELECT DISTINCT lz.pat_id
                    FROM (SELECT DISTINCT pr.pat_id
                                         ,zc.rollup_race_c    AS patient_race_c
                                         ,zc.rollup_race_name AS "NAME"
                            FROM XDR_WHERRY_preg_pat_ALLRACE pr
                            JOIN XDR_WHERRY_RACE_ROLLUP             zc ON pr.patient_race_c = zc.patient_race_c
                         ) lz
                    WHERE lz.patient_race_c NOT IN (7,8)                                      --7=PATIENT REFUSED, 8=UNKNOWN
                    GROUP BY lz.pat_id HAVING count(*) > 1) s1
          UNION ALL
          SELECT s1.pat_id, 7 AS mapped_race_c, 'Patient Refused' AS mapped_race_name         --PATREF AND UNKNOWN ONLY, USE PATREF (E.G. 7,8)
            FROM (SELECT DISTINCT lz.pat_id
                    FROM (SELECT DISTINCT pr.pat_id
                                         ,zc.rollup_race_c    AS patient_race_c
                                         ,zc.rollup_race_name AS "NAME"
                            FROM XDR_WHERRY_preg_pat_ALLRACE pr
                            JOIN XDR_WHERRY_RACE_ROLLUP             zc ON pr.patient_race_c = zc.patient_race_c
                         ) lz
                    WHERE EXISTS (SELECT DISTINCT lz2.pat_id FROM XDR_WHERRY_CHILD_ALLRACE lz2 WHERE lz.pat_id = lz2.pat_id AND lz2.patient_race_c = 7)
                      AND EXISTS (SELECT DISTINCT lz2.pat_id FROM XDR_WHERRY_CHILD_ALLRACE lz2 WHERE lz.pat_id = lz2.pat_id AND lz2.patient_race_c = 8)
                    GROUP BY lz.pat_id HAVING count(*) = 2) s1
          UNION ALL
          SELECT DISTINCT s1.pat_id                                                           --SINGLE ONLY (E.G. ANY SINGLE OCCURRENCE OF RACE)
                         ,zc.rollup_race_c     AS mapped_race_c
                         ,zc.rollup_race_name  AS mapped_race_name 
            FROM (SELECT DISTINCT lz.pat_id
                    FROM (SELECT DISTINCT pr.pat_id
                                         ,zc.rollup_race_c    AS patient_race_c
                                         ,zc.rollup_race_name AS "NAME"
                            FROM XDR_WHERRY_preg_pat_ALLRACE pr
                            JOIN XDR_WHERRY_RACE_ROLLUP             zc ON pr.patient_race_c = zc.patient_race_c
                         ) lz
                    GROUP BY lz.pat_id HAVING count(*) = 1) s1
            JOIN XDR_WHERRY_CHILD_ALLRACE lz ON s1.pat_id = lz.pat_id
            JOIN i2b2.race_rollup             zc ON lz.patient_race_c = zc.patient_race_c
          UNION ALL 
          SELECT DISTINCT s1.pat_id                                                           --PATREF AND UNKNOWN WITH ANOTHER SINGLE RACE, USE SINGLE RACE (E.G. 1,7,8 or 2,7, 4,14,7)
                         ,zc.rollup_race_c     AS mapped_race_c
                         ,zc.rollup_race_name  AS mapped_race_name 
            FROM (SELECT DISTINCT lz.pat_id
                    FROM (SELECT DISTINCT pr.pat_id
                                         ,zc.rollup_race_c    AS patient_race_c
                                         ,zc.rollup_race_name AS "NAME"
                            FROM XDR_WHERRY_preg_pat_ALLRACE pr
                            JOIN XDR_WHERRY_RACE_ROLLUP             zc ON pr.patient_race_c = zc.patient_race_c
                         ) lz
                    WHERE lz.patient_race_c NOT IN (7,8)                                      --7=PATIENT REFUSED, 8=UNKNOWN
                    GROUP BY lz.pat_id HAVING count(*) = 1) s1
            JOIN XDR_WHERRY_preg_pat_ALLRACE lz ON s1.pat_id = lz.pat_id
            JOIN XDR_WHERRY_RACE_ROLLUP             zc ON lz.patient_race_c = zc.patient_race_c
            WHERE lz.patient_race_c NOT IN (7,8) 
         --) x WHERE x.pat_id = p.pat_id
      )order by pat_id
;

--Add counts for QA
INSERT INTO XDR_WHERRY_preg_COUNTS(TABLE_NAME,PAT_COUNT,TOTAL_COUNT)
SELECT 'XDR_WHERRY_CHILD_RACE' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT		--3,738(9/1/17)
	,COUNT(*) AS TOTAL_COUNT 			--3,738(9/1/17)
FROM XDR_WHERRY_CHILD_RACE;
COMMIT;

--------------------------------------------------------------------------------
--	STEP 2.10: Update race name accordingly
--------------------------------------------------------------------------------	  
MERGE INTO XDR_WHERRY_preg_pat pat
using
  (select pat_id
        ,MAPPED_RACE_NAME
  FROM XDR_WHERRY_preg_pat_RACE
  ) r
  on (pat.pat_id = r.pat_id)
  when matched then
      update set MAPPED_RACE_NAME = r.MAPPED_RACE_NAME;
COMMIT;      
*/
select * from XDR_WHERRY_preg_pat;   