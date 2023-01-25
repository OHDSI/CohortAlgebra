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

SELECT subject_id, 
        event_date,
        event_type,
        start_ordinal
INTO #raw_data
FROM
(
  SELECT 
  	CAST(f.subject_id AS BIGINT) subject_id,
  	CAST(cohort_start_date AS DATE) AS event_date,
  	CAST(- 1 AS INT) AS event_type,
  	CAST(
  	      ROW_NUMBER() OVER (PARTITION BY subject_id 
  	                    ORDER BY cohort_start_date
  		    ) AS BIGINT) AS start_ordinal
  
  FROM #cohort_rows f
  
  UNION
  
  SELECT 
  	CAST(subject_id AS BIGINT) subject_id,
  	CAST(DATEADD(day, @era_constructor_pad, cohort_end_date) AS DATE) AS cohort_end_date,
  	CAST(1 AS INT) event_type,
  	CAST(NULL AS BIGINT) AS start_ordinal
  FROM #cohort_rows
) f;
  
--HINT DISTRIBUTE ON KEY (subject_id)
SELECT 
	subject_id,
	DATEADD(day, - 1 * @era_constructor_pad, event_date) AS cohort_end_date
INTO #cte_end_dates
FROM (
	SELECT 
		subject_id,
		event_date,
		event_type,
		MAX(start_ordinal) OVER (
			PARTITION BY 
			subject_id ORDER BY event_date,
				event_type,
				start_ordinal ROWS UNBOUNDED PRECEDING
			) AS start_ordinal,
		ROW_NUMBER() OVER (
			PARTITION BY 
			subject_id ORDER BY event_date,
				event_type,
				start_ordinal
			) AS overall_ord
	FROM #raw_data
	) e
WHERE (2 * e.start_ordinal) - e.overall_ord = 0;
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

DELETE FROM 
{@target_cohort_database_schema != ''} ? {
  @target_cohort_database_schema.@target_cohort_table
} : {@target_cohort_table} 
WHERE cohort_definition_id = @new_cohort_id;
	
INSERT INTO 
{@target_cohort_database_schema != ''} ? {
  @target_cohort_database_schema.@target_cohort_table
} : {@target_cohort_table} 
SELECT @new_cohort_id cohort_definition_id,
      subject_id,
      cohort_start_date, 
      cohort_end_date
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
