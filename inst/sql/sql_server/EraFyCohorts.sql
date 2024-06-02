DROP TABLE IF EXISTS #cohort_rows;
DROP TABLE IF EXISTS #cohort_era;
DROP TABLE IF EXISTS #cte_ends;
DROP TABLE IF EXISTS #cte_end_dates;
DROP TABLE IF EXISTS #raw_data;

SELECT DISTINCT subject_id,
        cohort_start_date,
        cohort_end_date
INTO #cohort_rows
FROM {@source_cohort_database_schema != ''} ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table}
WHERE cohort_definition_id IN (@old_cohort_ids);

--HINT DISTRIBUTE ON KEY (subject_id)
SELECT subject_id,
	DATEADD(day, - 1 * @era_constructor_pad, event_date) AS cohort_end_date
INTO #cte_end_dates
FROM (
	SELECT subject_id,
		event_date,
		SUM(event_type) OVER (
			PARTITION BY subject_id ORDER BY event_date,
				event_type ROWS UNBOUNDED PRECEDING
			) AS interval_status
	FROM (
		SELECT subject_id,
			cohort_start_date AS event_date,
			- 1 AS event_type
		FROM #cohort_rows
		
		UNION ALL
		
		SELECT subject_id,
			DATEADD(day, @era_constructor_pad, Cohort_end_date) AS end_date,
			1 AS event_type
		FROM #cohort_rows
		) RAWDATA
	) e
WHERE interval_status = 0;

DROP TABLE IF EXISTS #raw_data;

--HINT DISTRIBUTE ON KEY (subject_id)
SELECT 
	source.subject_id,
	source.cohort_start_date,
	MIN(e.cohort_end_date) AS cohort_end_date
INTO #cte_ends
FROM #cohort_rows source
INNER JOIN #cte_end_dates e 
  ON source.subject_id = e.subject_id
	AND e.cohort_end_date >= source.cohort_start_date
GROUP BY source.subject_id,
	source.cohort_start_date;

--HINT DISTRIBUTE ON KEY (subject_id)
SELECT 
	subject_id,
	min(cohort_start_date) AS cohort_start_date,
	cohort_end_date
INTO #cohort_era
FROM #cte_ends
GROUP BY 
	subject_id,
	cohort_end_date;
	
DROP TABLE IF EXISTS #cte_ends;
DROP TABLE IF EXISTS #cohort_rows;

{@is_temp_table} ? {
  DROP TABLE IF EXISTS @target_cohort_table;
  
  SELECT  CAST(@new_cohort_id AS BIGINT) cohort_definition_id,
          CAST(subject_id AS BIGINT) subject_id,
          CAST(cohort_start_date AS DATE) cohort_start_date, 
          CAST(cohort_end_date AS DATE) cohort_end_date
  INTO @target_cohort_table
} : {
  DELETE FROM 
  {@target_cohort_database_schema != ''} ? {
    @target_cohort_database_schema.@target_cohort_table
  } : {@target_cohort_table} 
  WHERE cohort_definition_id = @new_cohort_id;
  	
  INSERT INTO 
  {@target_cohort_database_schema != ''} ? {
    @target_cohort_database_schema.@target_cohort_table
  } : {@target_cohort_table} 
  SELECT  CAST(@new_cohort_id AS BIGINT) cohort_definition_id,
          CAST(subject_id AS BIGINT) subject_id,
          CAST(cohort_start_date AS DATE) cohort_start_date, 
          CAST(cohort_end_date AS DATE) cohort_end_date
}

  FROM    
  --HINT DISTRIBUTE ON KEY (subject_id)	
{@cdm_database_schema != ''} ?
{
    (
      SELECT 
          ce.subject_id,
          CASE WHEN op.observation_period_start_date < ce.cohort_start_date THEN ce.cohort_start_date
              ELSE op.observation_period_start_date END AS cohort_start_date,
          CASE WHEN op.observation_period_end_date > ce.cohort_end_date then ce.cohort_end_date
              ELSE op.observation_period_end_date END AS cohort_end_date
      FROM #cohort_era ce
      INNER JOIN @cdm_database_schema.observation_period op
      ON ce.subject_id = op.person_id
      WHERE op.observation_period_start_date <= ce.cohort_start_date
            AND op.observation_period_end_date >= ce.cohort_start_date
            -- only returns overlapping periods
      ) f
} :
{ #cohort_era f}
WHERE cohort_start_date <= cohort_end_date
;
 
DROP TABLE IF EXISTS #cohort_era;
DROP TABLE IF EXISTS #cte_end_dates;
