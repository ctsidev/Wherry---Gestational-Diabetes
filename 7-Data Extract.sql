-- ******************************************************************************************************* 
-- STEP 7
--   Export the investigator data
-- *******************************************************************************************************
--------------------------------------------------------------------------------
-- STEP 7.1: Create encounter ID key table 
--------------------------------------------------------------------------------
DROP TABLE XDR_WHERRY_preg_ENCKEY PURGE;
CREATE TABLE XDR_WHERRY_preg_ENCKEY AS
select rownum as encounter_id
      ,pat_enc_csn_id
FROM xdr_WHERRY_preg_enc
order by pat_enc_csn_id;

--Add counts for QA
INSERT INTO XDR_WHERRY_preg_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT)
SELECT 'XDR_WHERRY_preg_ENCKEY' AS TABLE_NAME
	,NULL AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 		
FROM XDR_WHERRY_preg_ENCKEY;
COMMIT;

SELECT COUNT(*) FROM XDR_WHERRY_preg_ENCKEY    ; --
--------------------------------------------------------------------------------
-- STEP 7.2: Demographics Pull  - Mothers
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id,
               ROUND(MONTHS_BETWEEN(SYSDATE,pat.birth_date)/12)         AS age,
               pat.sex                            	AS gender,
               pat.mapped_race_name               	AS race,
               pat.ethnic_group                   	AS ethnicity,
               pat.PATIENT_STATUS                 	AS vital_status,
			   extract(year from PAT.LAST_ENC_DATE) AS year_last_encounter,     
               pat.BENEFIT_PLAN_NAME,
               pat.FINANCIAL_CLASS,
			   pat.mom_child_mc
  FROM xdr_WHERRY_preg_pat 	                  pat
  WHERE pat.mom_child_mc = 'M'
  order by study_id;

--------------------------------------------------------------------------------
-- STEP 7.3: Demographics Pull  - Children
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id,
                mom.study_id                            AS mom_study_id,
                ROUND(mom.first_enc_date - pat.birth_date)  AS age_days_mom_first_enc,
                pat.sex                            		AS gender,
                pat.mapped_race_name               		AS race,
                pat.ethnic_group                   		AS ethnicity,
                pat.PATIENT_STATUS                 		AS vital_status,
			    --extract(year from PAT.LAST_ENC_DATE) as year_last_encounter,     
                pat.BENEFIT_PLAN_NAME,
                pat.FINANCIAL_CLASS,
			    pat.mom_child_mc
  FROM xdr_WHERRY_preg_pat 	                  pat
  JOIN xdr_WHERRY_preg_pat 	                  mom on pat.mom_pat_id = mom.pat_id
  WHERE pat.mom_child_mc = 'C'
  order by study_id;               
--------------------------------------------------------------------------------
-- STEP 7.4: Diagnoses Pull 
--			Use the reference table provided to map the ICD code to its description (lz_dx_px_lookup)
--------------------------------------------------------------------------------
select DISTINCT pat.study_id
            ,enck.encounter_id
            ,EXTRACT(YEAR FROM dx.CONTACT_DATE) year_dx_date
            ,CASE WHEN pat.mom_child_mc = 'M' THEN ROUND(dx.CONTACT_DATE - pat.first_enc_date) 
				WHEN pat.mom_child_mc = 'C'  THEN ROUND(dx.CONTACT_DATE - mom.first_enc_date) 
				ELSE 999999999
			END dx_dt_days_first_enc
            ,dx.icd_type
            ,dx.icd_code
            --,dx.ICD_DESC
            ,dx.PRIMARY_SEC_FLAG
            ,dx.poa_flag
            ,dx.hsp_final_dx_flag
            ,dx.ADMIT_DX_FLAG
            ,case when dx.icd_type = 9 then icd9.icd_desc
                else icd10.icd_desc
            end icd_description
            ,pat.mom_child_mc
from XDR_WHERRY_preg_DX     				dx
--JOIN XDR_WHERRY_preg_ENC    		enc on dx.pat_enc_csn_id = enc.pat_enc_csn_id
JOIN XDR_WHERRY_preg_pat    				pat on dx.pat_id = pat.pat_id
JOIN XDR_WHERRY_preg_ENCKEY    				enck on dx.pat_enc_csn_id = enck.pat_enc_csn_id
--LEFT JOIN xdr_wherry_all_mom_child 			lnk  on dx.pat_id = lnk.nb_pat_id AND pat.mom_child_mc = 'C'
LEFT JOIN xdr_WHERRY_preg_pat 	    		mom  on pat.mom_pat_id = mom.pat_id AND pat.pat_id = 'C' AND mom.mom_child_mc = 'M'
LEFT JOIN XDR_WHERRY_preg_DX_LOOKUP        	icd9  ON dx.icd_code = icd9.code
                                              AND icd9.icd_type = 9
LEFT JOIN XDR_WHERRY_preg_DX_LOOKUP        	icd10  ON dx.icd_code = icd10.code
                                              AND icd10.icd_type = 10
ORDER BY pat.study_id,enck.encounter_id;--------------------------------------------------------------------------------
-- STEP 7.5: Procedures Pull 
--------------------------------------------------------------------------------
select  DISTINCT pat.study_id
               ,enck.encounter_id
               ,EXTRACT(YEAR FROM PRC.PROC_DATE) year_proc_date
               --,ROUND(PRC.PROC_DATE - pat.first_enc_date) proc_dt_days_first_enc
			   
			   ,CASE WHEN pat.mom_child_mc = 'M' THEN ROUND(PRC.PROC_DATE - pat.first_enc_date) 
						WHEN pat.mom_child_mc = 'C' THEN ROUND(PRC.PROC_DATE - mom.first_enc_date) 
					ELSE 999999999
				END proc_dt_days_first_enc
			
               ,prc.code_type
               ,prc.PX_CODE as procedure_code
               ,prc.px_name as procedure_name
               ,pat.mom_child_mc
from xdr_WHERRY_preg_prc     		prc
JOIN XDR_WHERRY_preg_pat           	pat  on prc.pat_id = pat.pat_id
JOIN XDR_WHERRY_preg_ENCKEY        	enck on prc.pat_enc_csn_id = enck.pat_enc_csn_id
LEFT JOIN xdr_wherry_all_mom_child 	lnk  on PRC.pat_id = lnk.nb_pat_id AND pat.mom_child_mc = 'C'
LEFT JOIN xdr_WHERRY_preg_pat 	    mom  on lnk.mom_pat_id = mom.pat_id AND pat.pat_id = 'C' AND mom.mom_child_mc = 'M'
--JOIN XDR_WHERRY_preg_ENC           enc  on prc.pat_enc_csn_id = enc.pat_enc_csn_id
ORDER BY study_id, encounter_id;


--------------------------------------------------------------------------------
-- STEP 7.6: Flowsheets Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id
               ,enck.encounter_id
				,EXTRACT(YEAR FROM flo.recorded_time) dt
               --,ROUND(flo.recorded_time - pat.first_enc_date) rec_dt_days_first_enc
			   
			   ,CASE WHEN pat.mom_child_mc = 'M' THEN ROUND(flo.recorded_time - pat.first_enc_date) 
						WHEN pat.mom_child_mc = 'C' THEN ROUND(flo.recorded_time - mom.first_enc_date) 
					ELSE 999999999
				END rec_dt_days_first_enc
				
               ,flo.measure_name      AS vital_sign_type
               ,flo.measure_value     AS vital_sign_value
               ,pat.mom_child_mc
               --,     AS vital_sign_taken_time
FROM xdr_WHERRY_preg_flo          flo
JOIN XDR_WHERRY_preg_pat          pat  on flo.pat_id = pat.pat_id
JOIN XDR_WHERRY_preg_ENCKEY       enck on flo.pat_enc_csn_id = enck.pat_enc_csn_id
--LEFT JOIN xdr_wherry_all_mom_child 			lnk  on flo.pat_id = lnk.nb_pat_id AND pat.mom_child_mc = 'C'
LEFT JOIN xdr_WHERRY_preg_pat 	    		mom  on pat.mom_pat_id = mom.pat_id AND pat.pat_id = 'C' AND mom.mom_child_mc = 'M'
ORDER BY pat.study_id,enck.encounter_id,year_rec_da;

--------------------------------------------------------------------------------
-- STEP 7.7: Lab Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id
               ,enck.encounter_id
               ,lab.proc_id                
               ,lab.description           
               ,lab.component_id       
               ,lab.component_name      
               ,EXTRACT(YEAR FROM lab.order_time) year_order_date
			   ,CASE WHEN pat.mom_child_mc = 'M' THEN ROUND(lab.order_time - pat.first_enc_date) 
						WHEN pat.mom_child_mc = 'C' THEN ROUND(lab.order_time - mom.first_enc_date) 
					ELSE 999999999
				END order_dt_days_first_enc
		
               ,EXTRACT(YEAR FROM lab.SPECIMN_TAKEN_TIME) year_SPECIMN_date
			   ,CASE WHEN pat.mom_child_mc = 'M' THEN ROUND(lab.SPECIMN_TAKEN_TIME - pat.first_enc_date) 
						WHEN pat.mom_child_mc = 'C' THEN ROUND(lab.SPECIMN_TAKEN_TIME - mom.first_enc_date) 
					ELSE 999999999
				END SPECIMN_dt_days_first_enc

               ,EXTRACT(YEAR FROM lab.RESULT_time) year_RESULT_date
			   ,CASE WHEN pat.mom_child_mc = 'M' THEN ROUND(lab.RESULT_time - pat.first_enc_date) 
						WHEN pat.mom_child_mc = 'C' THEN ROUND(lab.RESULT_time - mom.first_enc_date) 
					ELSE 999999999
				END RESULT_dt_days_first_enc
               ,lab.ord_value               AS results
               ,lab.reference_unit          
			   ,lab.reference_low 
               ,lab.reference_high
               ,pat.mom_child_mc
FROM xdr_WHERRY_preg_lab          			lab 
JOIN XDR_WHERRY_preg_pat          			pat  on lab.pat_id = pat.pat_id
JOIN XDR_WHERRY_preg_ENCKEY       			enck on lab.pat_enc_csn_id = enck.pat_enc_csn_id
--LEFT JOIN xdr_wherry_all_mom_child 			lnk  on lab.pat_id = lnk.nb_pat_id AND pat.mom_child_mc = 'C'
LEFT JOIN xdr_WHERRY_preg_pat 	    		mom  on pat.mom_pat_id = mom.pat_id AND pat.pat_id = 'C' AND mom.mom_child_mc = 'M'
--JOIN XDR_WHERRY_preg_ENC          enc  on lab.pat_enc_csn_id = enc.pat_enc_csn_id
ORDER BY pat.study_id,enck.encounter_id,component_name,order_dt_days_first_enc;


--------------------------------------------------------------------------------
-- STEP 7.8: medications Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id
               ,enck.encounter_id
               ,med.order_med_id
               ,EXTRACT(YEAR FROM nvl(mar.taken_time, med.ORDER_INST))   AS taken_time_order_date
               ,CASE WHEN pat.mom_child_mc = 'M' THEN ROUND(nvl(mar.taken_time, med.ORDER_INST) - pat.first_enc_date) 
						WHEN pat.mom_child_mc = 'C' THEN ROUND(nvl(mar.taken_time, med.ORDER_INST)- mom.first_enc_date) 
					ELSE 999999999
				END order_dt_days_first_enc
                
               ,EXTRACT(YEAR FROM med.ORDER_INST) year_order_date
               --,ROUND(med.ORDER_INST - pat.first_enc_date) order_dt_days_first_enc
			   ,CASE WHEN pat.mom_child_mc = 'M' THEN ROUND(med.ORDER_INST - pat.first_enc_date) 
						WHEN pat.mom_child_mc = 'C' THEN ROUND(med.ORDER_INST - mom.first_enc_date) 
					ELSE 999999999
				END taken_dt_days_first_enc
				
				
               ,EXTRACT(YEAR FROM med.ORDER_START_TIME) year_start_date
               --,ROUND(med.ORDER_START_TIME - pat.first_enc_date) start_dt_days_first_enc
			   ,CASE WHEN pat.mom_child_mc = 'M' THEN ROUND(med.ORDER_START_TIME - pat.first_enc_date) 
						WHEN pat.mom_child_mc = 'C' THEN ROUND(med.ORDER_START_TIME - mom.first_enc_date) 
					ELSE 999999999
				END start_dt_days_first_enc
				
				
               ,EXTRACT(YEAR FROM med.ORDER_END_TIME) year_end_date
               --,ROUND(med.ORDER_END_TIME - pat.first_enc_date) end_dt_days_first_enc
			   ,CASE WHEN pat.mom_child_mc = 'M' THEN ROUND(med.ORDER_END_TIME - pat.first_enc_date) 
						WHEN pat.mom_child_mc = 'C' THEN ROUND(med.ORDER_END_TIME - mom.first_enc_date) 
					ELSE 999999999
				END end_dt_days_first_enc
               ,med.medication_name
               ,med.generic_name
               ,med.sig
               ,med.HV_DISCRETE_DOSE            AS dose
               ,med.DOSE_UNIT
               ,med.FREQ_NAME                   AS FREQUENCY        
                --,med.ROUTE_NAME
                --,med.INFUSION_RATE
                --,med.inf_rate_dose_unit
               ,med.pharm_class
               ,med.pharm_subclass
               ,pat.mom_child_mc
FROM xdr_WHERRY_preg_med          			med
JOIN XDR_WHERRY_preg_pat          			pat  on med.pat_id = pat.pat_id
JOIN XDR_WHERRY_preg_ENCKEY       			enck on med.pat_enc_csn_id = enck.pat_enc_csn_id
--LEFT JOIN xdr_wherry_all_mom_child 			lnk  on lab.pat_id = lnk.nb_pat_id AND pat.mom_child_mc = 'C'
LEFT JOIN xdr_WHERRY_preg_pat 	    		mom  on pat.mom_pat_id = mom.pat_id AND pat.pat_id = 'C' AND mom.mom_child_mc = 'M'
ORDER BY pat.study_id, medication_name 
;

--------------------------------------------------------------------------------
-- STEP 7.9: Allergies Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id
               ,alg.allergen_id
               ,alg.DESCRIPTION             AS allergen_name
               ,EXTRACT(YEAR FROM alg.DATE_NOTED) year_noted_date
               --,ROUND(med.ORDER_END_TIME - pat.first_enc_date) end_dt_days_first_enc
			   ,CASE WHEN pat.mom_child_mc = 'M' THEN ROUND(alg.DATE_NOTED - pat.first_enc_date) 
						WHEN pat.mom_child_mc = 'C' THEN ROUND(alg.DATE_NOTED - mom.first_enc_date) 
					ELSE 999999999
				END noted_dt_days_first_enc

               ,alg.reaction
               ,alg.allergy_status
               ,alg.severity
               ,pat.mom_child_mc
FROM xdr_WHERRY_preg_alg                alg
JOIN XDR_WHERRY_preg_pat    		    pat ON alg.pat_id = pat.pat_id
LEFT JOIN xdr_WHERRY_preg_pat 	    	mom ON pat.mom_pat_id = mom.pat_id AND pat.pat_id = 'C' AND mom.mom_child_mc = 'M'
ORDER BY pat.study_id, allergen_name;

--------------------------------------------------------------------------------
-- STEP 7.10: Family History Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT fam.study_id
                ,enck.encounter_id
                ,fam.line
                ,fam.medical_hx
                ,fam.relation
                ,pat.mom_child_mc
FROM xdr_Wherry_preg_fam        fam
JOIN XDR_WHERRY_preg_pat    	pat on fam.pat_id = pat.pat_id
JOIN XDR_WHERRY_preg_ENCKEY     enck on fam.pat_enc_csn_id = enck.pat_enc_csn_id
ORDER BY fam.study_id, fam.line;

--------------------------------------------------------------------------------
-- STEP 7.11: Social History Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id
               --,soc.sexually_active
               --,soc.female_partner_yn                                           --never nulls; defaults to "N" when unchecked
               --,soc.male_partner_yn                                             --never nulls; defaults to "N" when unchecked
               ,soc.tobacco_pak_per_dy
               ,soc.tobacco_used_years
               ,soc.tobacco_user            
               ,soc.cigarettes_yn
               ,soc.smoking_tob_status
               ,EXTRACT(YEAR FROM soc.smoking_start_date) year_smk_start_date
			   ,CASE WHEN pat.mom_child_mc = 'M' THEN ROUND(soc.smoking_start_date - pat.first_enc_date) 
						WHEN pat.mom_child_mc = 'C' THEN ROUND(soc.smoking_start_date - mom.first_enc_date) 
					ELSE 999999999
				END smk_start_days_after_first_enc
               ,EXTRACT(YEAR FROM soc.smoking_quit_date) year_smk_quit_date
               ,CASE WHEN pat.mom_child_mc = 'M' THEN ROUND(soc.smoking_quit_date - pat.first_enc_date) 
						WHEN pat.mom_child_mc = 'C' THEN ROUND(soc.smoking_quit_date - mom.first_enc_date) 
					ELSE 999999999
				END smk_quit_days_after_first_enc
			   ,soc.alcohol_user
               ,soc.alcohol_oz_per_wk
               ,soc.alcohol_type
               ,soc.iv_drug_user_yn 
               ,soc.illicit_drug_freq  
               --,soc.illicit_drug_cmt
               ,pat.mom_child_mc
  FROM xdr_WHERRY_preg_soc          soc
  JOIN XDR_WHERRY_preg_pat    	    pat on soc.pat_id = pat.pat_id
  --LEFT JOIN xdr_wherry_all_mom_child 			lnk  on lab.pat_id = lnk.nb_pat_id AND pat.mom_child_mc = 'C'
  LEFT JOIN xdr_WHERRY_preg_pat 	    		mom  on pat.mom_pat_id = mom.pat_id AND pat.pat_id = 'C' AND mom.mom_child_mc = 'M'
  ORDER BY pat.study_id;
  
--------------------------------------------------------------------------------
-- STEP 7.12: Providers table
--------------------------------------------------------------------------------
SELECT DISTINCT PROV_STUDY_ID
		--DEID_PROV_ID
		,PRIMARY_SPECIALTY
		,PROVIDER_TYPE
		,UC_PROVIDER
		,ACTIVE_PROVIDERS
FROM xdr_Wherry_preg_prov
ORDER BY PROV_STUDY_ID;

--------------------------------------------------------------------------------
-- STEP 7.13: Problem list Pull ---------------------------------------------------------------
--------------------------------------------------------------------------------
/*SELECT DISTINCT pat.study_id
               ,enck.encounter_id
               ,pl.encounter_date 
               ,EXTRACT(YEAR FROM pl.encounter_date) year_encounter_date
               ,(pl.encounter_date - pat.first_enc_date) enc_dt_days_first_enc
               ,pl.problem_list_id
               ,pl.prob_desc 
               ,EXTRACT(YEAR FROM pl.noted_date  ) year_noted_date
               ,(pl.noted_date - pat.first_enc_date) note_dt_days_first_enc
               ,EXTRACT(YEAR FROM pl.update_date  ) year_entry_date
               ,(pl.update_date - pat.first_enc_date) entry_dt_days_first_enc
               ,EXTRACT(YEAR FROM pl.resolved_date  ) year_resolved_date
               ,(pl.resolved_date - pat.first_enc_date) resolved_dt_days_first_enc
               ,pl.problem_status
               --,pl.problem_cmt             
               ,pl.priority
               --,pl.hospital_problem
               ,principal_yn
               --,pl.prov_id                           
               --,pl.prov_type          
               --,pl.primary_specialty   
  FROM xdr_Wherry_preg_pl                pl
  JOIN xdr_WHerry_preg_pat               pat on pl.pat_id = pat.pat_id
  LEFT JOIN XDR_WHERRY_preg_ENCKEY       enck on pl.pat_enc_csn_id = enck.pat_enc_csn_id;
*/
--------------------------------------------------------------------------------
-- STEP 7.14: Problem List Diagnosis Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id
               --,pdx.PROBLEM_LIST_ID
               ,enck.encounter_id
               ,pdx.diagnosis_source
               ,pdx.icd_type
               ,pdx.icd_code
               ,pdx.icd_desc
               ,EXTRACT(YEAR FROM pdx.diagnosis_date  ) year_diagnosis_date
               --,(pdx.diagnosis_date - pat.first_enc_date) diagnosis_dt_days_first_enc
			   ,CASE WHEN pat.mom_child_mc = 'M' THEN ROUND(pdx.diagnosis_date - pat.first_enc_date) 
						WHEN pat.mom_child_mc = 'C' THEN ROUND(pdx.diagnosis_date - mom.first_enc_date) 
					ELSE 999999999
				END diagnosis_dt_days_first_enc
				
               ,pdx.primary_dx_yn
			   ,pat.mom_child_mc
  FROM xdr_Wherry_preg_pldx              pdx
  JOIN xdr_WHerry_preg_pat               pat on pdx.pat_id = pat.pat_id
  LEFT JOIN XDR_WHERRY_preg_ENCKEY       enck on pdx.pat_enc_csn_id = enck.pat_enc_csn_id
  --LEFT JOIN xdr_wherry_all_mom_child 			lnk  on lab.pat_id = lnk.nb_pat_id AND pat.mom_child_mc = 'C'
  LEFT JOIN xdr_WHERRY_preg_pat 	    		mom  on pat.mom_pat_id = mom.pat_id AND pat.pat_id = 'C' AND mom.mom_child_mc = 'M'
  ORDER BY pat.study_id, encounter_id, icd_type, icd_code;

--------------------------------------------------------------------------------
-- STEP 7.16: Pull mother - child linkage
--------------------------------------------------------------------------------
SELECT mom.pat_id as mom_study_id
		,cld.study_id as child_study_id
		--flags
		,lkn.address_match
		,lkn.phone_match
		,lkn.email_match
		,lkn.proxy_match
		,lkn.similar_adddress
FROM xdr_wherry_all_mom_child				lkn
left JOIN xdr_WHerry_preg_pat               mom on lnk.mom_pat_id = mom.pat_id AND mom.mom_child_mc = 'M'
left JOIN xdr_WHerry_preg_pat               cld on pdx.nb_pat_id = cld.pat_id AND cld.mom_child_mc = 'M'
 
--------------------------------------------------------------------------------
-- STEP 7.16: Data counts 
--------------------------------------------------------------------------------
SELECT * FROM XDR_WHERRY_preg_COUNTS ;
