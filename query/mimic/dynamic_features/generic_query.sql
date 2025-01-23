WITH ids_from_dict AS (
	SELECT 
		* 
	FROM 
		mimiciv_icu.d_items
	WHERE 
		label IN {dictionaryPropName}
)

SELECT
	p.stay_id as encounterId
	, {feature} as feature
	, p.value as valueString
	, p.valuenum as valueNumber
	, p.chartTime as utcChartTime
FROM
	{feature_table} as p
WHERE
	itemid IN (SELECT itemid FROM ids_from_dict)

