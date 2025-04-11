SELECT 
	icu.subject_id as lifeTimeNumber
	, icu.hadm_id as encounterNumber
	, icu.stay_id as encounterId
	, icu.first_careunit as displayLabel
	, patient.gender as gender
	, patient.anchor_age as age
	, icu.inTime as utcInTime
	, icu.outTime as utcOutTime
	, ROUND(CAST((icu.los * 24) AS NUMERIC), 2) as lengthOfStay
	, adm.deathtime as dateOfDeath
	, CASE
		WHEN adm.discharge_location = 'DIED' THEN 'DECES'
		ELSE 'SURVIE'
	END AS isDeceased
FROM 
	MIMICIV_ICU.ICUSTAYS AS ICU
LEFT JOIN 
	MIMICIV_HOSP.PATIENTS AS PATIENT 
ON 
	PATIENT.SUBJECT_ID = ICU.SUBJECT_ID
LEFT JOIN 
	MIMICIV_HOSP.ADMISSIONS AS ADM 
ON 
	ADM.HADM_ID = ICU.HADM_ID