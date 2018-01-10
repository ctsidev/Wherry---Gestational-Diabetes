# Baghdadi-Sepsis
### Investigator Name: Dr Jonathan Baghdadi
###### Series of scripts to pull data for Dr. Baghdadi's research project using Clarity datamart structure

	Author: Javi Sanz
	Revision Date: 20170412
	Version: 1.0.1

	Background:
	Script to pull data for Dr. Baghdadi research project using Clarity datamart structure

	Instructions:
	The script will create a series of table to capture the data for different entities (encounters, medications, etc) 	Ultimately, there is a select query to be used to export the tables formatted accordingly into csv files to deliver to the PI.
	
	For any questions regarding the script, feel free to contact me at
	jsanz@mednet.ucla.edu

*******************************************************************************************************
### PROJECT DESCRIPTION
*******************************************************************************************************	
Patients who develop sepsis while hospitalized have higher mortality than those who present from the community with sepsis, even after controlling for comorbidities. We suspect that this disparity may be related to differential effectiveness of the sepsis bundle in this population—either difficulties with its implementation or else decreased efficacy. The purpose of this study is to evaluate clinical factors associated with successful implementation of the bundle and factors associated with its success or failure when implemented correctly. 
    
##### SELECTION CRITERIA
1. Encounter took place between 10/01/2014 and 10/01/2016 at UC hospitals other than UCLA.
2. Encounter type: hospitalization.
3. Patient age: 18 or older at time of admission
4. ICD-9/10 codes for the inpatient encounter includes codes for:

##### ICD-9 codes
* Septicemia (038.0, 038.10, 038.11, 038.12, 038.19, 038.2, 038.3, 038.40, 038.41, 038.42, 038.43, 038.44, 038.49, 038.8, 038.9), SIRS (995.90), sepsis (995.91), bacteremia (790.7), other fungal infection (117.9), systemic candidiasis (112.5), candidal endocarditis (112.81), acute and subacute bacterial endocarditis (421.0), acute endocarditis (421.9), salmonella septicemia (003.1), septicemic plague (020.2), anthrax septicemia (022.3), meningococcal septicemia (036.2), Waterhouse-Friedrichson syndrome (036.3), herpetic septicemia (054.5), gonococcemia (098.89), sepsis due to indwelling urinary catheter (996.64), infection due to central venous catheter (999.31, 999.32)
AND
* Organ dysfunction:
	* Diagnoses
		* respiratory (518.51, 518.81, 518.82, 518.84, 786.09, 799.1)
		* cardiovascular (458.0, 458.21, 458.29, 458.8, 458.9, 785.50, 785.51, 785.59, 796.3)
		* renal (584.5, 584.6, 584.7, 584.8, 584.9)
		* hepatic (570, 572.2, 573.4)
		* hematologic (286.6, 286.7, 286.9, 287.49, 287.5)
		* metabolic (276.2)
		* neurologic (293.0, 293.1, 293.9, 348.30, 348.31, 348.39, 357.82, 359.81, 780.09)
	* Procedures
		* respiratory (93.90, 96.70, 96.71, 96.72, 31.1, 33.21, 33.22, 33.23, 33.24, 33.27, 31.29)
		* cardiovascular (00.17, 88.72, 89.62, 89.64)
		* renal (39.95)
		* hematologic (99.04, 99.05, 99.06, 99.07)
		* neurologic (89.14)

	OR
* Severe sepsis (995.92), Septic shock (785.52) WITHOUT codes for organ dysfunction

##### ICD-10 Codes
* Sepsis: A02.1, A03.9, A04.7, A20.7, A21.7, A22.7, A23.9, A24.1, A26.7, A28.0, A28.2, A32.7, A39.2, A39.3, A39.4, A40.0, A40.1, A40.3, A40.8, A40.9, A41.01, A41.02, A41.1, A41.2, A41.3, A41.4, A41.50, A41.51, A41.52, A41.53, A41.59, A41.81, A41.89, A41.9, A42.7, A54.86, B00.7, B37.7, B95.4, B95.61, B95.620, J18.9, J44.0, N39.0
AND
* Organ dysfunction:
	* respiratory (J80, J96.00, J96.01, J96.02, J96.90, J96.91, J96.92, R09.2)
	* cardiovascular (R57.0, R57.1, R57.8, R57.9, I95.1, I95.9)
	* renal (N17.0, N17.1, N17.2, N17.8, N17.9)
	* hepatic (K72.0, K72.9, K76.3)
	* neurological (F05, F05.9, G93.1, G93.40, G93.41)
	* hematologic (D69.5, D69.6, D65)
	* procedures (0BH13EZ, 0BH17EZ, 0BH18EZ)	

OR
* Severe sepsis without septic shock (R65.20), Severe sepsis with septic shock (R65.21)

### PI Element selection
The code allows to create certain counts for PI to choose from in order to reduce the amount of data pulled and produce a more efficient and accurate dataset.

The M.O. is always the same, pull the elements for the cohort, create a set of counts to send to PI, apply his selection 	by loading into a driver table and using it on a join with the previous table.
The elements where this formula is used in this case are: 
* Labs
* Meds
* Allergies

*LIST OF DATA ELEMENTS FOR EXTRACTION*
* Patient Demographics
* Encounters
* ADT Information
* Diagnoses
* Procedures
* Vital Signs and other Flowsheet Data
* Laboratory Test Results
* Medication Orders (Prescriptions) or Med Administration
* Allergies
* Microbiology

Additionally, there are two reference tables to be used in order to add the proper context to the data pull, one for diagnoses pull and one for the race field. The data for these tables is included in the repository.
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
