DROP TABLE IF EXISTS @temp_table_1;
	
DROP TABLE IF EXISTS #cohort_era;
DROP TABLE IF EXISTS #cte_ends;
DROP TABLE IF EXISTS #cte_end_dates;
DROP TABLE IF EXISTS #raw_data;

SELECT x.*
INTO #raw_data
FROM (
	SELECT target.new_cohort_id cohort_definition_id,
		source.subject_id,
		source.cohort_start_date AS event_date,
		- 1 AS event_type,
		ROW_NUMBER() OVER (
			PARTITION BY target.new_cohort_id,
			source.subject_id ORDER BY cohort_start_date
			) AS start_ordinal
	FROM {@source_database_schema != ''} ? {@source_database_schema.@source_cohort_table} : {@source_cohort_table} source
  INNER JOIN #old_to_new_cohort_id target
  ON source.cohort_definition_id = target.old_cohort_id
	
	UNION ALL
	
	SELECT target.new_cohort_id cohort_definition_id,
		source.subject_id,
		DATEADD(day, @eraconstructorpad, source.cohort_end_date) AS cohort_end_date,
		1 AS event_type,
		NULL
	FROM {@source_database_schema != ''} ? {@source_database_schema.@source_cohort_table} : {@source_cohort_table} source
  INNER JOIN #old_to_new_cohort_id target
  ON source.cohort_definition_id = target.old_cohort_id
	) x;

-- the magic
SELECT cohort_definition_id,
	subject_id,
	DATEADD(day, - 1 * @eraconstructorpad, event_date) AS cohort_end_date
INTO #cte_end_dates
FROM (
	SELECT cohort_definition_id,
		subject_id,
		event_date,
		event_type,
		MAX(start_ordinal) OVER (
			PARTITION BY cohort_definition_id,
			subject_id ORDER BY event_date,
				event_type,
				start_ordinal ROWS UNBOUNDED PRECEDING
			) AS start_ordinal,
		ROW_NUMBER() OVER (
			PARTITION BY cohort_definition_id,
			subject_id ORDER BY event_date,
				event_type,
				start_ordinal
			) AS overall_ord
	FROM #raw_data
	) e
WHERE (2 * e.start_ordinal) - e.overall_ord = 0;

SELECT target.new_cohort_id cohort_definition_id,
	source.subject_id,
	source.cohort_start_date,
	MIN(e.cohort_end_date) AS cohort_end_date
INTO #cte_ends
FROM {@source_database_schema != ''} ? {@source_database_schema.@source_cohort_table} : {@source_cohort_table} source
INNER JOIN #old_to_new_cohort_id target
  ON source.cohort_definition_id = target.old_cohort_id
INNER JOIN #cte_end_dates e 
  ON target.new_cohort_id = e.cohort_definition_id
	AND source.subject_id = e.subject_id
	AND e.cohort_end_date >= source.cohort_start_date
GROUP BY target.new_cohort_id,
	source.subject_id,
	source.cohort_start_date;

SELECT cohort_definition_id,
	subject_id,
	min(cohort_start_date) AS cohort_start_date,
	cohort_end_date
INTO #cohort_era
FROM #cte_ends
GROUP BY cohort_definition_id,
	subject_id,
	cohort_end_date;
	
	
	
{@cdm_database_schema != ''} ?
{

  SELECT cohort_definition_id,
          subject_id,
          cohort_start_date, 
          cohort_end_date
    INTO @temp_table_1
    FROM 
    (SELECT ce.cohort_definition_id,
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
    WHERE cohort_start_date <= cohort_end_date

} :
{
  SELECT *
  INTO @temp_table_1
  FROM #cohort_era
  WHERE cohort_start_date <= cohort_end_date
}
;
 
DROP TABLE IF EXISTS #cohort_era;
DROP TABLE IF EXISTS #cte_ends;
DROP TABLE IF EXISTS #cte_end_dates;
DROP TABLE IF EXISTS #raw_data;
