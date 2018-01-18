-- *******************************************************************************************************
-- STEP 4
--		Load reference table
-- *******************************************************************************************************
--------------------------------------------------------------------------------
--	STEP 4.1: Create Diagnoses reference table
--------------------------------------------------------------------------------  
DROP TABLE XDR_Wherry_preg_DX_LOOKUP PURGE;
CREATE TABLE XDR_BAGHDADI_DX_LOOKUP
   (	"CODE" VARCHAR2(20 BYTE), 
	"ICD_TYPE" NUMBER(*,0), 
	"ICD_DESC" VARCHAR2(254 BYTE));
--------------------------------------------------------------------------------
--	STEP 4.2: Load Diagnosis records
--------------------------------------------------------------------------------
-- The file called [XDR_BAGHDADI_DX_LOOKUP.zip] contains the data to load in the table above.
-- You shall use the utility of your choice to load this file into XDR_BAGHDADI_DX_LOOKUP
-- which is used on step 7.5 to add the appropiate context to these records.
-- The file shall be formatted as a CSV with double quotation marks as text identifier.

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_Wherry_preg_DX_LOOKUP' AS TABLE_NAME
	,NULL AS PAT_COUNT		
	,COUNT(*) AS TOTAL_COUNT 		--112,803(10/12/17)
FROM XDR_Wherry_preg_DX_LOOKUP;
COMMIT;
