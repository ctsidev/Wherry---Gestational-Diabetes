
------------------------------------------------------------------------------
--      Find all patients (female) that had a pregnancy related DX code
------------------------------------------------------------------------------
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
UNION
--legacy data
select PAT_ID
        ,9 AS ICD_TYPE
        ,ICD9_CODE
        ,EFFECTIVE_DATE
from i2b2.int_dx
WHERE (REGEXP_LIKE(ICD9_CODE,'^6[3-7][0-9]+')       --ICD-9: 630-679 (includes all subcategories)
        OR REGEXP_LIKE(ICD9_CODE,'^V2(2|3)+')        --ICD-9: V22-V23 (includes all subcategories)
        )
        AND EFFECTIVE_DATE BETWEEN '01/01/2006' AND '01/18/2018'
;


select count(*) , count(distinct pat_id)  from XDR_WHERRY_PRG_PREGALL; --1770297	76950


------------------------------------------------------------------------------
--      Find all patients born in the period of analysis
------------------------------------------------------------------------------
DROP TABLE XDR_WHERRY_preg_childall PURGE;
CREATE TABLE XDR_WHERRY_preg_childall AS
SELECT pat.*
FROM clarity.patient pat
WHERE birth_date BETWEEN '01/01/2006' AND '01/18/2018';  --'01/01/2006' AND '03/02/2013'

select count(*) , count(distinct pat_id)  from XDR_WHERRY_PREG_CHILDALL;        --168490	168490
select extract(year from birth_date) as y, count(distinct pat_id)  
from XDR_WHERRY_PREG_CHILDALL
group by extract(year from birth_date)
order by y;        --168490	168490

------------------------------------------------------------------------------
--   Pull hospital encounters for mothers where the date matches the birth of one of the potential sons./daughters
-----------------------------------------------------------------------------
-- first pull all hospital encounters
DROP TABLE XDR_WHERRY_preg_ENC_pregall PURGE;
CREATE TABLE XDR_WHERRY_preg_ENC_pregall AS
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
			AND enctype.name = 'Hospital Encounter'
			--Encounter took place on the same date where one of the children was born
--			AND e.effective_date_dt IN (SELECT DISTINCT birth_date
--										FROM XDR_WHERRY_preg_childall)
;
select count(*) , count(distinct pat_id)  from XDR_WHERRY_preg_ENC_pregall;  --1064076	69895
--CREATE INDEXES


CREATE INDEX XDR_WHERRY_preg_ENC_preg_DTIX ON XDR_WHERRY_preg_ENC_pregall(effective_date_dt);
CREATE INDEX XDR_WHERRY_preg_childall_BDIX ON XDR_WHERRY_preg_childall(BIRTH_DATE);

-----------------------------------------------------------------------------
--find encounters that match children DOBs
-----------------------------------------------------------------------------
DROP TABLE XDR_WHERRY_preg_enc_dob PURGE;
CREATE TABLE XDR_WHERRY_preg_enc_dob AS
SELECT enc.PAT_ID
		,ENC.effective_date_dt
		,cld.pat_id as child_pat_id
		,cld.BIRTH_DATE as child_birth_date
FROM XDR_WHERRY_preg_ENC_pregall 	enc
JOIN XDR_WHERRY_PREG_CHILDALL 		cld ON enc.effective_date_dt between (cld.BIRTH_DATE -2) and (cld.BIRTH_DATE + 2)
WHERE enc.effective_date_dt BETWEEN '01/01/2006' and '03/01/2013'
;

select count(*), count(distinct PAT_ID) AS COUNT_MOM, count(distinct child_pat_id) AS COUNT_CHILD from XDR_WHERRY_preg_enc_dob
--188985791	53724	109325


--create index?
create index XDR_WHERRY_preg_mat_patidix on XDR_WHERRY_preg_enc_dob(pat_id);
create index XDR_WHERRY_preg_mat_cldidix on XDR_WHERRY_preg_enc_dob(child_pat_id);

------------------------------------------------------------------------------
--   Match mothers to children based on 
--		Proxy: pat_id
--		Address
--		Phone
--		Email		
-----------------------------------------------------------------------------
DROP TABLE XDR_WHERRY_preg_matching PURGE;
CREATE TABLE XDR_WHERRY_preg_matching AS
SELECT DISTINCT enc.pat_id as mom_pat_id
				,enc.child_pat_id
				
				,CASE WHEN enc.effective_date_dt = enc.CHILD_BIRTH_DATE THEN 1 ELSE 0 END DATES_MATCH
				,CASE WHEN mom.add_line_1 = cld.add_line_1 THEN 1 ELSE 0 END ADDRESS_MATCH
				,CASE WHEN mom.home_phone = cld.home_phone THEN 1 ELSE 0 END PHONE_MATCH
				,CASE WHEN mom.email_address = cld.email_address THEN 1 ELSE 0 END EMAIL_MATCH
				,CASE WHEN enc.pat_id = prx.proxy_pat_id THEN 1 ELSE 0 END PROXY_MATCH

FROM XDR_WHERRY_preg_enc_dob		enc
LEFT JOIN clarity.patient			mom on enc.pat_id = mom.pat_id
LEFT JOIN clarity.patient			cld on enc.child_pat_id = cld.pat_id
--mother-child link table
--LEFT JOIN clarity.hsp_ld_mom_child  lnk ON mom.pat_id = lnk.pat_id
--LEFT JOIN clarity.pat_enc     		nb  ON lnk.child_enc_csn_id = nb.pat_enc_csn_id
--LEFT JOIN clarity.patient     		nbp ON nb.pat_id = nbp.pat_id 

LEFT JOIN clarity.PAT_MYC_PRXY_HX			prx ON enc.child_pat_id = prx.pat_id
WHERE
	--mother-child link table
	--(lnk.pat_id IS NOT NULL and nb.pat_enc_csn_id IS NOT NULL AND nbp.pat_idIS NOT NULL)
	
	--cld.birth_date BETWEEN '01/01/2006' AND '03/02/2013'	AND
		(--hospital date match
        (enc.effective_date_dt = enc.CHILD_BIRTH_DATE)
		--address match
		OR (mom.add_line_1 = cld.add_line_1)
		--home_phone match
		OR (mom.home_phone = cld.home_phone)
		--email_address match
		OR (mom.email_address = cld.email_address)
		--mother is PROXY for the child
		OR (enc.pat_id = prx.proxy_pat_id)
		)

        
-- Create first encounter table
DROP TABLE XDR_WHERRY_preg_FENC;
CREATE TABLE XDR_WHERRY_preg_FENC AS
select distinct x.PAT_ID
            ,x.effective_date_dt
            ,x.
from 
    (select enc.*
        ,min(enc.effective_date_dt) over (partition by pat_id) as first_enc
    from XDR_WHERRY_preg_ENC enc
    ) x
where
    first_enc = x.effective_date_dt

	
	
--------------------------------------
-- investigation and QA
--------------------------------------
select *
from xdr_wherry_preg_pat
where mom_pat_id = 'Z4131377';

select *
from i2b2.lz_clarity_enc 
where pat_id = 'Z4131377'
order by effective_date_dt;--90003860386


select cld.pat_id
,cld.ETHNIC_GROUP
,cld.MAPPED_RACE_NAME
,cld.birth_date
,cld.sex
,pat.pat_last_name
,pat.pat_first_name
,pat.home_phone  
        ,pat.add_line_1 
               ,pat.add_line_2 
               ,pat.city 
               --,xst.abbr
               ,pat.zip 
               ,pat.email_address
from i2b2.lz_clarity_patient cld
join clarity.patient pat on cld.pat_id = pat.pat_id
where 
--cld.birth_date = '07/23/2013 01:10:00'
cld.birth_date between '07/22/2013' and '07/24/2013'
--cld.pat_id in ('Z4143108','Z4507610')

select mom.pat_id
,mom.ETHNIC_GROUP
,mom.MAPPED_RACE_NAME
,mom.birth_date
,pat.pat_last_name
,pat.pat_first_name
,pat.home_phone  
        ,pat.add_line_1 
               ,pat.add_line_2 
               ,pat.city 
               --,xst.abbr
               ,pat.zip 
               ,pat.email_address
from i2b2.lz_clarity_patient mom
join clarity.patient pat on mom.pat_id = pat.pat_id
where  mom.pat_id = 'Z4131377'






select * from i2b2.lz_clarity_patient 
where birth_date < '03/01/2013'
order by birth_date desc
extract(hour from birth_date) <> '12'
--cld.birth_date = '07/23/2013 01:10:00'