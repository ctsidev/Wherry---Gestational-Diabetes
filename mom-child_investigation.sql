
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
select count(*) , count(distinct pat_id)  from XDR_WHERRY_PRG_PREGALL where CONTACT_DATE BETWEEN '01/01/2006' AND '03/01/2013'; --451197	44936


------------------------------------------------------------------------------
--      Find all patients born in the period of analysis
------------------------------------------------------------------------------\
--maybe reduce this to the '01/01/2006' AND '03/02/2013' period since 2013 to present is addressed by the mom-child link table
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
-- perhaps pull all encounters for mothers since this is going to be part of the final selection regardless
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
--There are 44,921 potential mothers with a hospital encounter in this period.
select count(*) , count(distinct enc.pat_id)  
from XDR_WHERRY_preg_ENC_pregall enc
join (select distinct pat_id  from XDR_WHERRY_PRG_PREGALL where CONTACT_DATE BETWEEN '01/01/2006' AND '03/01/2013') dx on enc.pat_id = dx.pat_id
where enc.effective_date_dt BETWEEN '01/01/2006' AND '03/01/2013'; --767055			44921


--CREATE INDEXES

CREATE INDEX XDR_WHERRY_preg_ENC_preg_DTIX ON XDR_WHERRY_preg_ENC_pregall(effective_date_dt);
CREATE INDEX XDR_WHERRY_preg_childall_BDIX ON XDR_WHERRY_preg_childall(BIRTH_DATE);

-----------------------------------------------------------------------------
--find encounters that match children DOBs 
-----------------------------------------------------------------------------
--we include a +2/-2 day margin
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
--168481368


--create index?
create index XDR_WHERRY_preg_mat_patidix on XDR_WHERRY_preg_enc_dob(pat_id);
create index XDR_WHERRY_preg_mat_cldidix on XDR_WHERRY_preg_enc_dob(child_pat_id);

--create table for distinct records (I thought using distinct above would have been too much)
DROP table XDR_WHERRY_preg_dist PURGE;
create table XDR_WHERRY_preg_dist as
--select count(*) from 
select distinct PAT_ID
,CHILD_PAT_ID
,EFFECTIVE_DATE_DT
,CHILD_BIRTH_DATE
from XDR_WHERRY_preg_enc_dob;


select count(*), count(distinct PAT_ID) AS COUNT_MOM, count(distinct child_pat_id) AS COUNT_CHILD from  XDR_WHERRY_preg_dist;--168481368	53724	109325


--CREATE AD HOC TABLE FOR MOMS TO MATCH
DROP TABLE XDR_WHERRY_mom_matching PURGE;
CREATE TABLE XDR_WHERRY_mom_matching AS
SELECT DISTINCT enc.pat_id as mom_pat_id
,mom.add_line_1
,mom.home_phone
,mom.email_address
FROM XDR_WHERRY_preg_enc_dob		enc
LEFT JOIN clarity.patient			mom on enc.pat_id = mom.pat_id;
create index XDR_WHERRY_mom_patidix on XDR_WHERRY_mom_matching(mom_pat_id);
create index XDR_WHERRY_mom_addix on XDR_WHERRY_mom_matching(add_line_1);
create index XDR_WHERRY_mom_phix on XDR_WHERRY_mom_matching(home_phone);
create index XDR_WHERRY_mom_emailix on XDR_WHERRY_mom_matching(email_address);

SELECT COUNT(*) COUNT_TOTAL, count(distinct mom_pat_id) AS COUNT_MOM FROM XDR_WHERRY_mom_matching;--53724	53724

--CREATE AD HOC TABLE FOR CHILDREN TO MATCH
DROP TABLE XDR_WHERRY_child_matching PURGE;
CREATE TABLE XDR_WHERRY_child_matching AS
SELECT DISTINCT enc.child_pat_id
,cld.add_line_1
,cld.home_phone
,cld.email_address
FROM XDR_WHERRY_preg_enc_dob		enc
LEFT JOIN clarity.patient			cld on enc.child_pat_id = cld.pat_id;

create index XDR_WHERRY_cld_patidix on XDR_WHERRY_child_matching(child_pat_id);
create index XDR_WHERRY_cld_addix on XDR_WHERRY_child_matching(add_line_1);
create index XDR_WHERRY_cld_phix on XDR_WHERRY_child_matching(home_phone);
create index XDR_WHERRY_cld_emailix on XDR_WHERRY_child_matching(email_address);

SELECT COUNT(*) COUNT_TOTAL, count(distinct child_pat_id) AS COUNT_MOM FROM XDR_WHERRY_child_matching;      --109325	109325



--Identify dummy or mistaken contact information to include in the matching query
select add_line_1, count(*) AS C FROM
(
select add_line_1 from XDR_WHERRY_child_matching
UNION ALL
select add_line_1 from XDR_WHERRY_mom_matching
)
group by add_line_1  
order by c desc;

select HOME_PHONE, count(*) AS C FROM
(
select HOME_PHONE from XDR_WHERRY_child_matching
UNION ALL
select HOME_PHONE from XDR_WHERRY_mom_matching
)
group by HOME_PHONE  
order by c desc;

select EMAIL_ADDRESS, count(*) AS C FROM
(
select EMAIL_ADDRESS from XDR_WHERRY_child_matching
UNION ALL
select EMAIL_ADDRESS from XDR_WHERRY_mom_matching
)
group by EMAIL_ADDRESS  
order by c desc;
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
				,enc.CHILD_BIRTH_DATE
				--,CASE WHEN enc.effective_date_dt = enc.CHILD_BIRTH_DATE THEN 1 ELSE 0 END DATES_MATCH
				,CASE WHEN ( 
							(mom.add_line_1 = cld.add_line_1)
							AND (mom.add_line_1 is not null 
								and cld.add_line_1 is not null 
								and UPPER(mom.add_line_1) not in ('RETURN MAIL','RETURNED MAIL','HOMELESS','RETURNED MAIL/BAD ADDRESS','HOMELESS STREET','UNKNOWN','RETURN MAIL BAD ADDRESS','HOMELESS CITY','RETURNED MAIL BAD ADDRESS','FOREIGN ADDRESS') 
								and UPPER(cld.add_line_1) not in ('RETURN MAIL','RETURNED MAIL','HOMELESS','RETURNED MAIL/BAD ADDRESS','HOMELESS STREET','UNKNOWN','RETURN MAIL BAD ADDRESS','HOMELESS CITY','RETURNED MAIL BAD ADDRESS','FOREIGN ADDRESS') 
								)
							) THEN 1 ELSE 0 END ADDRESS_MATCH
				,CASE WHEN ( 
							(mom.home_phone = cld.home_phone) 
							AND (mom.home_phone is not null 
								and cld.home_phone is not null 
								and mom.home_phone not in ('000-000-0001','000-000-0000','310-000-0000','999-999-9999','818-999-9999','310-000-0001','213-000-0001','805-000-0000') 
								and cld.home_phone not in ('000-000-0001','000-000-0000','310-000-0000','999-999-9999','818-999-9999','310-000-0001','213-000-0001','805-000-0000')
								)
							) THEN 1 ELSE 0 END PHONE_MATCH
				,CASE WHEN (
							(mom.email_address = cld.email_address)
							AND (mom.email_address IS NOT NULL 
								and cld.email_address IS NOT NULL)
							) THEN 1 ELSE 0 END EMAIL_MATCH
				,CASE WHEN enc.pat_id = prx.proxy_pat_id THEN 1 ELSE 0 END PROXY_MATCH

FROM XDR_WHERRY_preg_dist			enc
--FROM XDR_WHERRY_preg_enc_dob		enc
--LEFT JOIN clarity.patient			mom on enc.pat_id = mom.pat_id
LEFT JOIN XDR_WHERRY_mom_matching		mom on enc.pat_id = mom.mom_pat_id
--LEFT JOIN clarity.patient			cld on enc.child_pat_id = cld.pat_id
LEFT JOIN XDR_WHERRY_child_matching		cld on enc.child_pat_id = cld.child_pat_id
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
        --(enc.effective_date_dt = enc.CHILD_BIRTH_DATE)
		--address match
--		OR
    		( 
			(mom.add_line_1 = cld.add_line_1)
			AND (mom.add_line_1 is not null 
				and cld.add_line_1 is not null 
				and UPPER(mom.add_line_1) not in ('RETURN MAIL','RETURNED MAIL','HOMELESS','RETURNED MAIL/BAD ADDRESS','HOMELESS STREET','UNKNOWN','RETURN MAIL BAD ADDRESS','HOMELESS CITY','RETURNED MAIL BAD ADDRESS','FOREIGN ADDRESS') 
				and UPPER(cld.add_line_1) not in ('RETURN MAIL','RETURNED MAIL','HOMELESS','RETURNED MAIL/BAD ADDRESS','HOMELESS STREET','UNKNOWN','RETURN MAIL BAD ADDRESS','HOMELESS CITY','RETURNED MAIL BAD ADDRESS','FOREIGN ADDRESS') 
				)
			)
		--home_phone match
		OR ( 
			(mom.home_phone = cld.home_phone) 
			AND (mom.home_phone is not null 
				and cld.home_phone is not null 
				and mom.home_phone not in ('000-000-0001','000-000-0000','310-000-0000','999-999-9999','818-999-9999','310-000-0001','213-000-0001','805-000-0000') 
				and cld.home_phone not in ('000-000-0001','000-000-0000','310-000-0000','999-999-9999','818-999-9999','310-000-0001','213-000-0001','805-000-0000')
				)
			)
		--email_address match
		OR (
			(mom.email_address = cld.email_address)
			AND (mom.email_address IS NOT NULL 
				AND cld.email_address IS NOT NULL)
			)
		--mother is PROXY for the child
		OR (enc.pat_id = prx.proxy_pat_id)
		);

SELECT count(*) FROM XDR_WHERRY_preg_matching ;     --27937
SELECT COUNT(*) COUNT_TOTAL, count(distinct mom_pat_id) AS COUNT_MOM, count(distinct child_pat_id) AS COUNT_CHILD  FROM XDR_WHERRY_preg_matching;      --27937	17404	20573




select matching_vector,count(*) from (
SELECT distinct x.ADDRESS_MATCH ||  x.PHONE_MATCH ||  x.EMAIL_MATCH || x.PROXY_MATCH as matching_vector
,x.* 
FROM XDR_WHERRY_preg_matching x )
group by matching_vector;
/*
0100	6940
0001	1069
1010	75
0110	130
0011	76
1100	11260
1000	3328
1001	787
1101	1912
0101	1792
1110	199
0111	97
0010	87
1111	117
1011	68
*/

        There are _____ potential mothers with a hospital encounter in this period.
		

select matching_vector,count(*) from (
SELECT distinct x.ADDRESS_MATCH ||  x.PHONE_MATCH ||  x.EMAIL_MATCH || x.PROXY_MATCH as matching_vector
,x.MOM_PAT_ID
,x.CHILD_PAT_ID
FROM XDR_WHERRY_preg_matching x )
group by matching_vector
order by matching_vector;


select distinct x.MOM_PAT_ID
,x.CHILD_PAT_ID
,x.CHILD_BIRTH_DATE
,x.EFFECTIVE_DATE_DT
,x.ADDRESS_MATCH 
,x.PHONE_MATCH
,x.EMAIL_MATCH
,x.PROXY_MATCH
,x.ADDRESS_MATCH ||  x.PHONE_MATCH ||  x.EMAIL_MATCH || x.PROXY_MATCH as matching_vector
from XDR_WHERRY_preg_matching x
order by MOM_PAT_ID;


select distinct CHILD_PAT_ID from XDR_WHERRY_preg_matching 

--there are 57 kids with 2 mothers
select x.*
from (
select CHILD_PAT_ID,count(distinct MOM_PAT_ID) mom_count from XDR_WHERRY_preg_matching 
group by CHILD_PAT_ID
) x
where x.mom_count > 1
;


select distinct x.*
,orig.ADDRESS_MATCH ||  orig.PHONE_MATCH ||  orig.EMAIL_MATCH || orig.PROXY_MATCH as matching_vector
,orig.*
,mom.add_line_1
,cld.add_line_1
,mom.home_phone
,cld.home_phone
,mom.email_address
,cld.email_address
from (
select CHILD_PAT_ID,count(distinct MOM_PAT_ID) mom_count from XDR_WHERRY_preg_matching 
group by CHILD_PAT_ID
) x
join XDR_WHERRY_preg_matching orig on x.CHILD_PAT_ID = orig.CHILD_PAT_ID
LEFT JOIN XDR_WHERRY_mom_matching		mom on orig.mom_pat_id = mom.mom_pat_id
LEFT JOIN XDR_WHERRY_child_matching		cld on x.child_pat_id = cld.child_pat_id
where x.mom_count > 1
order by x.CHILD_PAT_ID, orig.MOM_PAT_ID;



select count(*) , count(distinct pat_id)  from XDR_WHERRY_PRG_PREGALL where CONTACT_DATE BETWEEN '03/01/2013' and '1/19/2018';

select count(*),count(distinct mom_pat_id) ,count(distinct nb_pat_id)  from xdr_wherry_all_mom_child 
where nb_dob BETWEEN '03/01/2013' and '1/19/2018';--17073	14828	17073

		
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