SELECT 
	d.encounterNumber,
	igs.*

FROM 
	pmsi.PMSI_IGSDetails as igs
LEFT JOIN
	pmsi.PMSI_Data as d
ON
	d.ficheid = igs.ficheid 


