-- *******************************************************************************************************
-- STEP 6
--		Pull all labs entries, generate a driver list to send to PI, and apply the selections made to the final pull.
-- ******************************************************************************************************* 
--------------------------------------------------------------------------------
--	STEP 6.1: Create Labs table
--------------------------------------------------------------------------------  
DROP TABLE xdr_Wherry_preg_laball PURGE;
CREATE TABLE xdr_Wherry_preg_laball AS 
SELECT 	DISTINCT coh.pat_id,
                coh.study_id,
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
                coh.mom_child_mc,
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
                      and p.ordering_date between to_date('01/01/2006','mm/dd/yyyy') and to_date('08/01/2018','mm/dd/yyyy')
                      and o.ord_value is not null
                      and o.order_proc_id is not null
;



--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'xdr_Wherry_preg_laball' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT    --3,736(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 				--2,555,351(9/5/17)
  ,'Create table with all lab results' as DESCRIPTION
FROM xdr_Wherry_preg_laball;
COMMIT;



--------------------------------------------------------------------------------
--	STEP 6.2: Export Labs driver to a file
--------------------------------------------------------------------------------  
SELECT proc_id, description, component_id, component_name, COUNT(*) AS total 
	FROM xdr_Wherry_preg_laball    
  GROUP BY proc_id, description, component_id, component_name
  ORDER BY component_name;


--------------------------------------------------------------------------------
--	STEP 6.3: Create Labs driver table to load the PI selection
--------------------------------------------------------------------------------  
DROP TABLE xdr_Wherry_preg_labdrv PURGE;
CREATE TABLE xdr_Wherry_preg_labdrv
   (	"PROC_ID" NUMBER(18,0), 
	"DESCRIPTION" VARCHAR2(254 BYTE), 
	"COMPONENT_ID" NUMBER(18,0), 
	"COMPONENT_NAME" VARCHAR2(75 BYTE));


--------------------------------------------------------------------------------
--	STEP 6.4: Load Labs driver table with selections made by PI
--------------------------------------------------------------------------------
-- Your site will receive a file with the layout used above that shall contain ONLY
-- the medication records selected by the PI. This selection is based on the output file
-- generated by step 6.2
-- You shall use the utility of your choice to load this file into xdr_Wherry_preg_labdrv
-- which is used on step 6.5 to pull the appropiate set of records.
-- The file shall be formatted as a CSV with double quotation marks as text identifier.


--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_Wherry_preg_LABDRV' AS TABLE_NAME
	,COUNT(*) AS TOTAL_COUNT 				--2,555,351(9/5/17)
  ,'Load PI lab driver selection' as DESCRIPTION
FROM XDR_Wherry_preg_LABDRV;
COMMIT;

--------------------------------------------------------------------------------
--	STEP 6.5: Apply Labs driver selection to final selection
--------------------------------------------------------------------------------
DROP TABLE xdr_Wherry_preg_lab PURGE;
CREATE TABLE xdr_Wherry_preg_lab AS 
SELECT DISTINCT lab.*
  FROM xdr_Wherry_preg_laball            lab  
  JOIN xdr_Wherry_preg_labdrv            drv ON lab.proc_id = drv.proc_id AND lab.component_id = drv.component_id
;

CREATE INDEX xdr_Wherry_preg_lab_ENCIDIX ON xdr_Wherry_preg_lab(pat_enc_csn_id);

--Add counts for QA
INSERT INTO XDR_Wherry_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_Wherry_preg_LAB' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT    --3,736(9/5/17)
	,COUNT(*) AS TOTAL_COUNT 				--2,555,351(9/5/17)
  ,'Create table with lab driver list' as DESCRIPTION
FROM XDR_Wherry_preg_LAB;
COMMIT;