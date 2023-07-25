DROP TABLE IF EXISTS #nearest_interval_t1;
DROP TABLE IF EXISTS #nearest_interval_1;
DROP TABLE IF EXISTS #nearest_interval_2;
DROP TABLE IF EXISTS #nearest_interval_3;
DROP TABLE IF EXISTS #nearest_interval_4;
DROP TABLE IF EXISTS #nearest_interval_5;
DROP TABLE IF EXISTS #nearest_interval_6;


SELECT t.cohort_definition_id,
        t.subject_id,
        min(cohort_start_date) cohort_start_date,
        min(cohort_end_date) cohort_end_date
INTO #nearest_interval_t1
FROM {@source_cohort_database_schema != '' } ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table} t
WHERE cohort_definition_id IN (@target_cohort_id)
GROUP BY t.cohort_definition_id,
        t.subject_id;

SELECT DISTINCT i.subject_id,
	0 priority,
	i.cohort_start_date,
	i.cohort_end_date
INTO #nearest_interval_1
FROM #nearest_interval_t1 t
INNER JOIN {@source_cohort_database_schema != '' } ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table} i ON t.subject_id = i.subject_id
	AND t.cohort_start_date <= i.cohort_end_date
	AND t.cohort_end_date >= i.cohort_start_date
WHERE i.cohort_definition_id IN (@interval_cohort_ids);

SELECT DISTINCT i.subject_id,
	i.cohort_start_date,
	i.cohort_end_date,
	MIN(DATEDIFF(DAY, t.cohort_start_date, i.cohort_start_date)) st_st,
	MIN(DATEDIFF(DAY, t.cohort_start_date, i.cohort_end_date)) st_end,
	MIN(DATEDIFF(DAY, t.cohort_end_date, i.cohort_start_date)) end_st,
	MIN(DATEDIFF(DAY, t.cohort_end_date, i.cohort_end_date)) end_end
INTO #nearest_interval_2
FROM #nearest_interval_t1 t
INNER JOIN {@source_cohort_database_schema != '' } ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table} i ON t.subject_id = i.subject_id
WHERE i.cohort_definition_id IN (@interval_cohort_ids)
GROUP BY i.subject_id,
	i.cohort_start_date,
	i.cohort_end_date;

SELECT subject_id,
	cohort_start_date,
	cohort_end_date,
	CASE 
		WHEN st_st < 0
			THEN st_st * - 1
		ELSE st_st
		END st_st,
	CASE 
		WHEN st_end < 0
			THEN st_end * - 1
		ELSE st_end
		END st_end,
	CASE 
		WHEN end_st < 0
			THEN end_st * - 1
		ELSE end_st
		END end_st,
	CASE 
		WHEN end_end < 0
			THEN end_end * - 1
		ELSE end_end
		END end_end
INTO #nearest_interval_3
FROM #nearest_interval_2;

SELECT subject_id,
	cohort_start_date,
	cohort_end_date,
	min(priority) priority
INTO #nearest_interval_4
FROM (
	SELECT subject_id,
		cohort_start_date,
		cohort_end_date,
		st_st priority
	FROM #nearest_interval_3
	
	UNION
	
	SELECT subject_id,
		cohort_start_date,
		cohort_end_date,
		st_end priority
	FROM #nearest_interval_3
	
	UNION
	
	SELECT subject_id,
		cohort_start_date,
		cohort_end_date,
		end_st priority
	FROM #nearest_interval_3
	
	UNION
	
	SELECT subject_id,
		cohort_start_date,
		cohort_end_date,
		end_end priority
	FROM #nearest_interval_3
	
	UNION
	
	SELECT subject_id,
		cohort_start_date,
		cohort_end_date,
		priority
	FROM #nearest_interval_1
	) as a
GROUP BY subject_id,
	cohort_start_date,
	cohort_end_date;

SELECT subject_id,
	ROW_NUMBER() OVER (
		PARTITION BY subject_id ORDER BY priority ASC
		) priority,
	cohort_start_date,
	cohort_end_date
INTO #nearest_interval_5
FROM #nearest_interval_4;

SELECT DISTINCT a.subject_id,
	a.cohort_start_date,
	a.cohort_end_date
INTO #nearest_interval_6
FROM #nearest_interval_5 a
INNER JOIN (
	SELECT subject_id,
		min(priority) priority
	FROM #nearest_interval_5
	GROUP BY subject_id
	) b ON a.subject_id = b.subject_id
	AND a.priority = b.priority
ORDER BY a.subject_id,
	a.cohort_start_date,
	a.cohort_end_date;

INSERT INTO {@target_cohort_database_schema != '' } ? {@target_cohort_database_schema.@target_cohort_table} : {@target_cohort_table}
SELECT @new_cohort_id cohort_definition_id,
	subject_id,
	cohort_start_date,
	cohort_end_date
FROM #nearest_interval_6;

DROP TABLE IF EXISTS #nearest_interval_t1;
DROP TABLE IF EXISTS #nearest_interval_1;
DROP TABLE IF EXISTS #nearest_interval_2;
DROP TABLE IF EXISTS #nearest_interval_3;
DROP TABLE IF EXISTS #nearest_interval_4;
DROP TABLE IF EXISTS #nearest_interval_5;
DROP TABLE IF EXISTS #nearest_interval_6;
