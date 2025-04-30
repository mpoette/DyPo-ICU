WITH ids_from_dict AS (
	SELECT 
		* 
	FROM 
		M_dictionary 
	WHERE 
		attributeId IN {id}
),
censusCTE AS (
	SELECT c.encounterId
	,c.ptCensusId
	,CASE WHEN c.utcOutTime = MAX( ISNULL(c2.utcOUtTime, '20501231')) OR c.utcOutTime IS NULL THEN '20501231' ELSE c.utcOutTime END as utcOutTime
	, c.utcInTime, c.inTimeId, c.inDayId
FROM DAR.PtCensus c WITH (NOLOCK)
INNER JOIN DAR.PtCensus c2 WITH (NOLOCK) on c.encounterId = c2.encounterId
GROUP BY c.ptCensusId, c.encounterId, c.utcOutTime, c.utcInTime, c.inTimeId, c.inDayId
)

SELECT
	p.encounterId
	, c.ptCensusId
	, {feature} as feature
	, p.valueString
	, p.valueNumber
	, p.utcValueDateTime
	, p.utcChartTime
FROM
	{feature_table} as p WITH (NOLOCK)

LEFT JOIN
	censusCTE as c
ON 
	c.encounterId = p.encounterId 
AND 
	c.ptCensusId =  (
		SELECT 
			TOP 1 c2.PtCensusId 
		FROM 
			censusCTE c2 
		WITH (NOLOCK) 
		WHERE 
			c2.encounterId = c.encounterId AND 
			(c2.utcOutTime >= p.utcChartTime) ORDER BY c2.inDayId ASC, c2.inTimeId ASC, c2.utcInTime) 

WHERE
	attributeId IN (SELECT attributeId FROM ids_from_dict)
AND 
	interventionId IN (SELECT interventionId FROM ids_from_dict)
