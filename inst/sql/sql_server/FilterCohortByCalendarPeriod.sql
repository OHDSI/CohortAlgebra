DELETE FROM {@target_cohort_database_schema != ''} ? {@target_cohort_database_schema.@target_cohort_table} : {@target_cohort_table} 
WHERE cohort_definition_id = @new_cohort_id;

INSERT INTO {@target_cohort_database_schema != ''} ? {@target_cohort_database_schema.@target_cohort_table} : {@target_cohort_table} (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
SELECT @new_cohort_id cohort_definition_id,
        subject_id,
  	   cohort_start_date,
  	   cohort_end_date
FROM {@source_cohort_database_schema != ''} ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table} t
WHERE cohort_definition_id = @old_cohort_id
  {@cohort_start_date_range_low != ''} ? {AND cohort_start_date >= CAST('@cohort_start_date_range_low' AS DATE)}
  {@cohort_start_date_range_high != ''} ? {AND cohort_start_date <= CAST('@cohort_start_date_range_high' AS DATE)}
  {@cohort_end_date_range_low != ''} ? {AND cohort_end_date >= CAST('@cohort_end_date_range_low' AS DATE)}
  {@cohort_end_date_range_high != ''} ? {AND cohort_end_date <= CAST('@cohort_end_date_range_high' AS DATE)};
