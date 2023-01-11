DROP TABLE IF EXISTS #offset_start;

SELECT subject_id,
	cohort_start_date,
	MAX(CASE 
		WHEN DATEADD(DAY, @offset_cohort_start_date, cohort_start_date) < observation_period_end_date
			THEN DATEADD(DAY, @offset_cohort_start_date, cohort_start_date)
		ELSE observation_period_end_date
		END) cohort_end_date
INTO #offset_start
FROM {@source_cohort_database_schema != ''} ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table} t
INNER JOIN @cdm_database_schema.observation_period op ON t.subject_id = op.person_id
WHERE op.observation_period_start_date <= t.cohort_start_date
	AND op.observation_period_end_date >= t.cohort_start_date
      AND t.cohort_definition_id = @old_cohort_id
GROUP BY subject_id, cohort_start_date;

DELETE FROM {@target_cohort_database_schema != ''} ? {@target_cohort_database_schema.@target_cohort_table} : {@target_cohort_table} 
WHERE cohort_definition_id = @new_cohort_id;

INSERT INTO {@target_cohort_database_schema != ''} ? {@target_cohort_database_schema.@target_cohort_table} : {@target_cohort_table} (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
SELECT @new_cohort_id cohort_definition_id,
        subject_id,
        min(cohort_start_date) cohort_start_date,
        cohort_end_date
FROM #offset_start
GROUP BY subject_id, cohort_end_date;

DROP TABLE IF EXISTS #offset_start;