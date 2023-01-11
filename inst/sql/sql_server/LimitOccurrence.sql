DELETE FROM {@target_cohort_database_schema != ''} ? {@target_cohort_database_schema.@target_cohort_table} : {@target_cohort_table} 
WHERE cohort_definition_id = @new_cohort_id;

INSERT INTO {@target_cohort_database_schema != ''} ? {@target_cohort_database_schema.@target_cohort_table} : {@target_cohort_table} (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
SELECT @new_cohort_id cohort_definition_id,
        t1.subject_id,
  	    t1.cohort_start_date,
  	    t1.cohort_end_date
FROM {@source_cohort_database_schema != ''} ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table} t1
INNER JOIN 
  (
      SELECT subject_id
              {@first_occurrence} ? {, min(cohort_start_date) cohort_start_date}
              {@last_occurrence} ? {, max(cohort_start_date) cohort_start_date}
      FROM {@source_cohort_database_schema != ''} ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table} 
      WHERE cohort_definition_id = @old_cohort_id
      GROUP BY subject_id
  ) t2
ON t1.subject_id = t2.subject_id
  AND t1.cohort_start_date = t2.cohort_start_date
WHERE cohort_definition_id = @old_cohort_id;
