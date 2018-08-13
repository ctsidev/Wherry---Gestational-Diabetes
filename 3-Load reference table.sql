-- *******************************************************************************************************
-- Step 3
--		Load reference table
-- *******************************************************************************************************
--------------------------------------------------------------------------------
--	Step 3.1: Create Race Roll-up reference table
--------------------------------------------------------------------------------  
DROP TABLE XDR_WHERRY_RACE_ROLLUP PURGE;
CREATE TABLE "XDR_WHERRY_RACE_ROLLUP" 
   (	"PATIENT_RACE_C" NUMBER(5,0), 
	"PATIENT_RACE_NAME" VARCHAR2(50 BYTE), 
	"ROLLUP_RACE_C" NUMBER(5,0), 
	"ROLLUP_RACE_NAME" VARCHAR2(50 BYTE));
--------------------------------------------------------------------------------
--	Step 3.2: Load Race Roll-up records
--------------------------------------------------------------------------------
-- The file called [XDR_WHERRY_RACE_ROLLUP.csv] contains the data to load in the table above.
-- You shall use the utility of your choice to load this file into XDR_WHERRY_RACE_ROLLUP
-- which is used on Step 3.5 to add the appropriate context to these records.
-- The file shall be formatted as a CSV with double quotation marks as text identifier.
-- It's recommended to check that the 'PATIENT_RACE_C' value corresponds to the same 'PATIENT_RACE_NAME' in your environment.
--------------------------------------------------------------------------------

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_WHERRY_RACE_ROLLUP' AS TABLE_NAME
	,NULL AS PAT_COUNT		
	,COUNT(*) AS TOTAL_COUNT 		--112,803(10/12/17)
	,'Load race driver table' as DESCRIPTION
FROM XDR_WHERRY_RACE_ROLLUP;
COMMIT;
--------------------------------------------------------------------------------
--	Step 3.3: Create Diagnoses reference table
--------------------------------------------------------------------------------  
DROP TABLE XDR_Wherry_preg_DX_LOOKUP PURGE;
CREATE TABLE XDR_BAGHDADI_DX_LOOKUP
   (	"CODE" VARCHAR2(20 BYTE), 
	"ICD_TYPE" NUMBER(*,0), 
	"ICD_DESC" VARCHAR2(254 BYTE));
--------------------------------------------------------------------------------
--	Step 3.4: Load Diagnosis records
--------------------------------------------------------------------------------
-- The file called [XDR_BAGHDADI_DX_LOOKUP.zip] contains the data to load in the table above.
-- You shall use the utility of your choice to load this file into XDR_BAGHDADI_DX_LOOKUP
-- which is used on step 7.5 to add the appropiate context to these records.
-- The file shall be formatted as a CSV with double quotation marks as text identifier.

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_Wherry_preg_DX_LOOKUP' AS TABLE_NAME
	,NULL AS PAT_COUNT		
	,COUNT(*) AS TOTAL_COUNT 		--112,803(10/12/17)
	,'Load diagnoses driver table' as DESCRIPTION
FROM XDR_Wherry_preg_DX_LOOKUP;
COMMIT;
