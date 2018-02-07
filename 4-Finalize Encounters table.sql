-- *******************************************************************************************************
-- STEP 3
--		Finalize encounter table
-- *******************************************************************************************************

- --------------------------------------------------------------------------------
-- STEP 3.1
--		Insert children Encounters
--------------------------------------------------------------------------------
--      Exclude the following encounter types enc_type_c not in (2532, 2534, 40, 2514, 2505, 2506, 2512, 2507)
--      In your environment, these codes might differ. I have listed the details below for your convinience.
--          2505	Erroneous Encounter
--          2506	Erroneous Telephone Encounter
--          2507	Scanned Document
--          2512	Transcribed Document
--          2514	Other eSource Document
--          2532	SMBP Historical Scanned Document
--          2534	Scanned Document No Visit
--            40	Wait List 
--------------------------------------------------------------------------------
INSERT INTO XDR_WHERRY_preg_ENC
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
            hsp.disch_disp_c, 
            dd.name disposition, 
            hsp.ed_disposition_c, 
            edd.name ed_disposition,
            dep.department_name,
            dep.specialty,
            loc.loc_name
        FROM clarity.pat_enc e
        JOIN XDR_WHERRY_preg_pat pat on e.pat_id = pat.pat_id AND PAT.MOM_CHILD_MC ='C'			--mother encounters were pulled on step 1.3
        LEFT JOIN clarity.clarity_fc 		fc 		ON e.visit_fc = fc.financial_class
        LEFT JOIN clarity.ZC_DISP_ENC_TYPE 	enctype ON e.enc_type_c = enctype.disp_enc_type_c
        LEFT JOIN clarity.pat_enc_hsp       hsp 	ON e.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
        LEFT JOIN clarity.zc_disch_disp     dd  	ON hsp.disch_disp_c = dd.disch_disp_c
        LEFT JOIN clarity.zc_ed_disposition edd 	ON hsp.ed_disposition_c = edd.ed_disposition_c
        LEFT JOIN clarity.clarity_dep       dep 	ON e.department_id = dep.department_id
        left join clarity.clarity_loc               loc ON dep.rev_loc_id = loc.loc_id
        WHERE e.enc_type_c not in (2532, 2534, 40, 2514, 2505, 2506, 2512, 2507)
			AND e.effective_date_dt BETWEEN '01/01/2006' AND '02/05/2018';

--gather counts for QA       
SELECT COUNT(*) AS TOT_COUNT, COUNT(DISTINCT PAT_ID) AS PAT_COUNT FROM XDR_WHERRY_preg_ENC;         --2,067,185	    31,715 (1/18/18)        
SELECT EXTRACT(YEAR FROM effective_date_dt) AS YEAR, COUNT(*) AS TOT_COUNT, COUNT(DISTINCT PAT_ID) AS PAT_COUNT FROM XDR_WHERRY_preg_ENC GROUP BY EXTRACT(YEAR FROM effective_date_dt) ORDER BY YEAR;

--take a look at output
--select * from XDR_WHERRY_preg_ENC;

--look at the details of codes being excluded from encounters pull        
/*select 
* from 
clarity.ZC_DISP_ENC_TYPE enctype
        WHERE enctype.disp_enc_type_c  in (2532, 2534, 40, 2514, 2505, 2506, 2512, 2507)     */


--Add counts for QA
INSERT INTO XDR_WHERRY_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_WHERRY_preg_preg_ENC' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 		
FROM XDR_WHERRY_preg_preg_ENC;
COMMIT;


--------------------------------------------------------------------------------
--	STEP 3.2: Update FIRST_ENC_DATE and LAST_ENC_DATE in the patient table (only for mothers)
--------------------------------------------------------------------------------	  
MERGE INTO XDR_WHERRY_preg_pat pat
using
  (select  enc.pat_id
        ,MIN(enc.effective_date_dt) AS FIRST_ENC_DATE
		,MAX(enc.effective_date_dt) AS LAST_ENC_DATE
    from XDR_WHERRY_preg_ENC		enc
	JOIN XDR_WHERRY_preg_pat		pat on enc.pat_id = pat.pat_id
    GROUP BY enc.pat_id) r
  on (pat.pat_id = r.pat_id)
  when matched then
      update set FIRST_ENC_DATE = r.FIRST_ENC_DATE,
				 LAST_ENC_DATE  = r.LAST_ENC_DATE;
COMMIT;
