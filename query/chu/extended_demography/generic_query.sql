WITH ids_from_dict AS (
	SELECT 
		* 
	FROM 
		M_dictionary 
	WHERE 
		dictionaryPropName IN {dictionaryPropName}
)

SELECT
	p.encounterId
	, {feature} as feature
	, p.valueString
	, p.valueNumber
	, p.utcValueDateTime
	, p.utcChartTime
	, p.storeTime
FROM
	{feature_table} as p
WHERE
	interventionId IN (SELECT interventionId FROM ids_from_dict) 
AND 
	attributeId IN (SELECT attributeId FROM ids_from_dict)
