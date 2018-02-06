-- ******************************************************************************************************* 
-- STEP 8
--   Drop all the tables created during this process
-- *******************************************************************************************************

drop table XDR_WHERRY_PREG_ALG purge;
drop table XDR_WHERRY_PREG_CHILDALL purge;
drop table XDR_WHERRY_PREG_COUNTS purge;
drop table XDR_WHERRY_PREG_DIST purge;
drop table XDR_WHERRY_PREG_DX purge;
drop table XDR_WHERRY_PREG_DX_LOOKUP purge;
drop table XDR_WHERRY_PREG_ENC_DOB purge;
drop table XDR_WHERRY_PREG_ENC_DOB_V2 purge;
drop table XDR_WHERRY_PREG_ENC_PREGALL purge;
drop table XDR_WHERRY_PREG_FAM purge;
drop table XDR_WHERRY_PREG_FENC purge;
drop table XDR_WHERRY_PREG_FLO purge;
drop table XDR_WHERRY_PREG_HSP purge;
drop table XDR_WHERRY_PREG_INTDX purge;
drop table XDR_WHERRY_PREG_LAB purge;
drop table XDR_WHERRY_PREG_MATCHING purge;
drop table XDR_WHERRY_PREG_MATCHING_EX purge;
drop table XDR_WHERRY_PREG_MATCHING_FINAL purge;
drop table XDR_WHERRY_PREG_MED purge;
drop table XDR_WHERRY_PREG_PAT_ALLRACE purge;
drop table XDR_WHERRY_PREG_PAT_RACE purge;
drop table XDR_WHERRY_PREG_PL purge;
drop table XDR_WHERRY_PREG_PLDX purge;
drop table XDR_WHERRY_PREG_PRC purge;
drop table XDR_WHERRY_PREG_PROV purge;
drop table XDR_WHERRY_PREG_SOC purge;


--These table contain the study_id and encounter_id and it must be deleted with caution
--drop table XDR_WHERRY_PREG_PAT purge;
--drop table XDR_WHERRY_PREG_ENCKEY purge;
