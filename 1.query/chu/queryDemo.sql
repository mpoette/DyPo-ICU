WITH censusCTE AS (
	SELECT c.encounterId
	,c.ptCensusId
	,CASE WHEN c.utcOutTime = MAX( ISNULL(c2.utcOUtTime, '20501231')) OR c.utcOutTime IS NULL THEN '20501231' ELSE c.utcOutTime END as utcOutTime
	, c.utcInTime, c.inTimeId, c.inDayId
FROM DAR.PtCensus c WITH (NOLOCK)
INNER JOIN DAR.PtCensus c2 WITH (NOLOCK) on c.encounterId = c2.encounterId
GROUP BY c.ptCensusId, c.encounterId, c.utcOutTime, c.utcInTime, c.inTimeId, c.inDayId
),
dateOfDeath AS (
	SELECT
		c.ptCensusId,
		d.valueDateTime as 'dateOfDeath'
	FROM
		PtDemographic d
	LEFT JOIN 
		ptCensus c
	ON
		c.encounterId = d.encounteriD
		AND c.ptCensusId = (
				SELECT 
					TOP 1 c2.ptCensusId
				FROM
					censusCTE c2
				WHERE
					c2.encounterId = c.encounterId
				AND
					(c2.utcOutTime >= d.utcChartTime)
				ORDER BY
					c2.inDayId ASC
					, c2.utcInTime

			)
	WHERE
		attributeId = 23893
),
isDeceased AS (
	SELECT
		c.ptCensusId,
		d.valueString as 'isDeceased'
	FROM
		PtDemographic d
	LEFT JOIN 
		ptCensus c
	ON
		c.encounterId = d.encounteriD
		AND c.ptCensusId = (
				SELECT 
					TOP 1 c2.ptCensusId
				FROM
					censusCTE c2
				WHERE
					c2.encounterId = c.encounterId
				AND
					(c2.utcOutTime >= d.utcChartTime)
				ORDER BY
					c2.inDayId ASC
					, c2.utcInTime

			)
	WHERE
		termKeyId = 18138
), adresse AS (
SELECT 
	lifeTimeNumber
	,cp
	,ville
	,adresse
FROM  
	[CISPmsiDB].[pmsi].[PMSI_Data]
)

SELECT 
	c.ptCensusId, 
	c.encounterId,
	e.encounterNumber,
	e.lifeTimeNumber,
	u.displayLabel,
	e.lastName,
	e.firstName,
	e.gender,
	e.dateOfBirth,
	e.age,
	adresse.cp,
	adresse.ville,
	adresse.adresse,
	c.inTime,
	c.utcInTime,
	c.outTime,  
	c.utcOutTime, 
	c.lengthOfStay,
	c.transferClinicalUnitId,
	dod.dateOfDeath,
	isD.isDeceased
FROM 
	dbo.PtCensus c
LEFT JOIN
	D_Encounter e
ON
	c.encounterId = e.encounterId
LEFT JOIN
	D_ClinicalUnit as u
ON
	u.clinicalUnitId = c.clinicalUnitId
LEFT JOIN
	dateOfDeath as dod
ON
	dod.ptCensusId = c.ptCensusId
LEFT JOIN
	isDeceased as isD
ON
	isD.ptCensusId = c.ptCensusId
LEFT JOIN
	adresse
ON 
	adresse.lifeTimeNumber = e.lifeTimeNumber
WHERE
	c.inTime < '2024-07-01'
ORDER BY inTime DESC
