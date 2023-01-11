DELETE FROM {@target_cohort_database_schema != ''} ? {@target_cohort_database_schema.@target_cohort_table} : {@target_cohort_table} 
WHERE cohort_definition_id = @new_cohort_id;

INSERT INTO {@target_cohort_database_schema != ''} ? {@target_cohort_database_schema.@target_cohort_table} : {@target_cohort_table} (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
SELECT @new_cohort_id cohort_definition_id,
        subject_id,
  	   cohort_start_date,
  	   cohort_end_date
FROM {@source_cohort_database_schema != ''} ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table} t
WHERE t.cohort_definition_id = @old_cohort_id
    AND DATEDIFF(DAY, cohort_start_date, cohort_end_date) >= @min_days
;
