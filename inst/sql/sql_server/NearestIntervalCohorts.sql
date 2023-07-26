DROP TABLE IF EXISTS #overlaps_ts;
DROP TABLE IF EXISTS #ends_before_ts;
DROP TABLE IF EXISTS #starts_after_ts;
DROP TABLE IF EXISTS #nearest;

-----------

SELECT DISTINCT t.subject_id,
  t.cohort_start_date target_cohort_start_date,
	i.cohort_start_date interval_cohort_start_date,
	i.cohort_end_date interval_cohort_end_date,
	0 DAYS_DIFF
INTO #overlaps_ts
FROM {@source_cohort_database_schema != '' } ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table} t
INNER JOIN {@source_cohort_database_schema != '' } ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table} i
	ON t.subject_id = i.subject_id
		AND i.cohort_start_date <= t.cohort_start_date
		AND i.cohort_end_date >= t.cohort_start_date
WHERE i.cohort_definition_id IN (@interval_cohort_ids)
	AND t.cohort_definition_id = @target_cohort_id;

-----------
SELECT subject_id,
	target_cohort_start_date,
	interval_cohort_start_date,
	interval_cohort_end_date,
	DATEDIFF(DAY, interval_cohort_end_date, target_cohort_start_date) DAYS_DIFF
INTO #ends_before_ts
FROM (
	SELECT t.subject_id,
		t.cohort_start_date target_cohort_start_date,
		ROW_NUMBER() OVER (
			PARTITION BY t.subject_id,
			t.cohort_start_date 
			ORDER BY i.cohort_end_date DESC
			) rn,
		i.cohort_start_date interval_cohort_start_date,
		i.cohort_end_date interval_cohort_end_date
	FROM {@source_cohort_database_schema != '' } ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table} t
	INNER JOIN {@source_cohort_database_schema != '' } ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table} i
		ON t.subject_id = i.subject_id
			AND i.cohort_end_date < t.cohort_start_date
	WHERE i.cohort_definition_id IN (@interval_cohort_ids)
	    AND t.cohort_definition_id IN (@target_cohort_id)
	) a
WHERE rn = 1;

-----------
SELECT subject_id,
	target_cohort_start_date,
	interval_cohort_start_date,
	interval_cohort_end_date,
	DATEDIFF(DAY, target_cohort_start_date, interval_cohort_start_date) DAYS_DIFF
INTO #starts_after_ts
FROM (
	SELECT t.subject_id,
		t.cohort_start_date target_cohort_start_date,
		ROW_NUMBER() OVER (
			PARTITION BY t.subject_id,
			t.cohort_start_date 
			ORDER BY i.cohort_start_date
			) rn,
		i.cohort_start_date interval_cohort_start_date,
		i.cohort_end_date interval_cohort_end_date
	FROM {@source_cohort_database_schema != '' } ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table} t
	INNER JOIN {@source_cohort_database_schema != '' } ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table} i
		ON t.subject_id = i.subject_id
			AND i.cohort_start_date > t.cohort_start_date
	WHERE i.cohort_definition_id IN (@interval_cohort_ids)
	    AND t.cohort_definition_id IN (@target_cohort_id)
	) a
WHERE rn = 1;

SELECT DISTINCT subject_id,
          interval_cohort_start_date cohort_start_date,
          interval_cohort_end_date cohort_end_date
INTO #nearest
FROM
(
  SELECT subject_id,
  	target_cohort_start_date,
  	interval_cohort_start_date,
  	interval_cohort_end_date,
  	ROW_NUMBER() OVER (
  		PARTITION BY subject_id,
  		target_cohort_start_date
  		ORDER BY days_diff
  		) rn
  FROM (
  
    SELECT subject_id,
  		target_cohort_start_date,
  		interval_cohort_start_date,
  		interval_cohort_end_date,
  		days_diff
  	FROM #overlaps_ts c1
  	
  	UNION
  	
  	SELECT subject_id,
  		target_cohort_start_date,
  		interval_cohort_start_date,
  		interval_cohort_end_date,
  		days_diff
  	FROM #ends_before_ts c2
  	
  	UNION
  	
  	SELECT subject_id,
  		target_cohort_start_date,
  		interval_cohort_start_date,
  		interval_cohort_end_date,
  		days_diff
  	FROM #starts_after_ts c3
  	) f
) ff
WHERE rn = 1;

DELETE
FROM {@target_cohort_database_schema != '' } ? {@target_cohort_database_schema.@target_cohort_table} : {@target_cohort_table}
WHERE cohort_definition_id = @new_cohort_id;

INSERT INTO {@target_cohort_database_schema != '' } ? {@target_cohort_database_schema.@target_cohort_table} : {@target_cohort_table}
SELECT DISTINCT @new_cohort_id cohort_definition_id,
	subject_id,
	cohort_start_date,
	cohort_end_date
FROM #nearest;

DROP TABLE IF EXISTS #overlaps_ts;
DROP TABLE IF EXISTS #ends_before_ts;
DROP TABLE IF EXISTS #starts_after_ts;
DROP TABLE IF EXISTS #nearest;
