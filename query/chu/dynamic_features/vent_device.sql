SELECT encounterId, chartTime, rootDictionaryPropName FROM PtTreatment as T
LEFT JOIN M_Dictionary as D
ON D.interventionId = T.interventionId AND D.attributeId = T.attributeId
WHERE d.rootDictionaryPropName IN ('ETTplacementInt', 'TrachCareInt')
AND clinicalUnitId IN (SELECT clinicalUnitId FROM D_ClinicalUnit WHERE department = 'reanimations')
GROUP BY encounterId, chartTime, rootDictionaryPropName