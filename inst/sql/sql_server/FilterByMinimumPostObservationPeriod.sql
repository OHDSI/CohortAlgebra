DELETE FROM {@target_cohort_database_schema != ''} ? {@target_cohort_database_schema.@target_cohort_table} : {@target_cohort_table} 
WHERE cohort_definition_id = @new_cohort_id;

INSERT INTO {@target_cohort_database_schema != ''} ? {@target_cohort_database_schema.@target_cohort_table} : {@target_cohort_table} (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
SELECT @new_cohort_id cohort_definition_id,
        subject_id,
  	   cohort_start_date,
  	   cohort_end_date
FROM {@source_cohort_database_schema != ''} ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table} t
INNER JOIN @cdm_database_schema.observation_period op
ON t.subject_id = op.person_id
    AND observation_period_start_date <= cohort_start_date
    AND observation_period_end_date >= cohort_start_date
WHERE DATEDIFF(DAY, cohort_end_date, observation_period_end_date) >= @min_days
    AND t.cohort_definition_id = @old_cohort_id;
