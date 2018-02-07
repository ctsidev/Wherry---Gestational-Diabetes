# Wherry - Gestational Diabetes
### Investigator Name: Dr Laura Wherry
###### Series of scripts to pull data for Dr. Wherry's research project using Clarity datamart structure

	Author: Javi Sanz
	Revision Date: 20180110
	Version: 1.0.1

	Background:
	Script to pull data for Dr. Wherry research project using Clarity datamart structure

	Instructions:
	The script will create a series of table to capture the data for different entities (encounters, medications, etc) 	Ultimately, there is a select query to be used to export the tables formatted accordingly into csv files to deliver to the PI.
	
	For any questions regarding the script, feel free to contact me at
	jsanz@mednet.ucla.edu

*******************************************************************************************************
### PROJECT DESCRIPTION
*******************************************************************************************************	
This project uses diagnosis cutoffs for gestational diabetes to examine the short- and long-term consequences of diagnosis for health care utilization and health outcomes for the mother and the infant. 
    
##### SELECTION CRITERIA
Two de-identified data extractions: one for the mother population and one for the child population:

1. All available records for any female with a pregnancy-related diagnosis code (ICD-9: 630-679, V22-V23; ICD-10: Z34, Z3A, Z37, O categories) at any time during the period 1/2006 to the present.
1. All available records for any children who were the result of the pregnancies of the women identified under (1) at any time during the period 1/2006 to the present.
	1. For the period 03/01/2013 - 02/05/2018, we can use the table hsp_ld_mom_child
	1. For the period 01/01/2006 - 03/01/2013, we need to find mothers from (1) with a hospital encounter and a child bron during that stay. Then we will use contant infomation (address, home phone number, email address, proxy pat_id) to find potential matches. It inclues a data cleaning step

##### ICD-9 codes
* 630-679, V22-V23 

##### ICD-10 Codes
* Z34, Z3A, Z37, O categories

### PI Element selection
The code allows to create certain counts for PI to choose from in order to reduce the amount of data pulled and produce a more efficient and accurate dataset.

*LIST OF DATA ELEMENTS FOR EXTRACTION*
* Demographics
* Encounter
* Diagnoses Encounter
* Diagnoses Problem List
* Procedures
* Medications
* Flowsheets
* Labs
* Social History
* Family History
* Allergy

Additionally, there are two reference tables to be used in order to add the proper context to the data pull, one for diagnoses pull and one for the race field. The data for these tables is included in the repository.
*******************************************************************************************************
### SITE SPECIFIC CODES:
*******************************************************************************************************
The code was originally written to run on the UCLA EPIC implementation and certain reference codes might vary from site to site. Adecuate commetns are included in the script to check for these differences and improve the output quality.

*******************************************************************************************************
### Study_id
*******************************************************************************************************
We create an alternate id for each patient in the study. We use the dbms_obfuscation_toolkit and a combination of tables and functions to execute this method of protection. However, for the purpose of simplicity, we have replaced such a method with dbms_random and rownum which generates a field that shall minimize the footprints of other identifiers (patient_num or MRN) from all additional datasets  when distributing the data to the PI.

Additionally, it allows us to limit the ability to link patients across studies without proper consent. Study_id can be linked back to the original patient by decoding it. This can be used in cases when studies are properly vetted to merge patients participating in more than one study. In the case of de-identified datasets, the PI would obtain different study_id for the same patient every time, preventing them from connecting the same patient across studies.

The only important issue to consider is that you must be able to recover the original pat_id from the study_id in case there is a repull or follow-up QA from the PI. You can simply save a table with the study_id/pat_it mapping, or use a study_id generator that can be replicated at will.

*******************************************************************************************************
### MSSQLSERVER:
*******************************************************************************************************
If you are using this RDBM, you might have to include certain modifications in certain portions of the script
