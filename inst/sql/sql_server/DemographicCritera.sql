DELETE FROM {@target_cohort_database_schema != ''} ? {@target_cohort_database_schema.@target_cohort_table} : {@target_cohort_table} 
WHERE cohort_definition_id = @new_cohort_id;

INSERT INTO {@target_cohort_database_schema != ''} ? {@target_cohort_database_schema.@target_cohort_table} : {@target_cohort_table} (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
SELECT @new_cohort_id cohort_definition_id,
        t1.subject_id,
  	    t1.cohort_start_date,
  	    t1.cohort_end_date
FROM {@source_cohort_database_schema != ''} ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table} t1
INNER JOIN
    @cdm_database_schema.person p
ON t1.subject_id = p.person_id
WHERE cohort_definition_id = @old_cohort_id
  {@gender_concept_id != ''} ? {AND gender_concept_id IN (@gender_concept_id)}
  {@age_lower != ''} ? {AND YEAR(t1.cohort_start_date) - p.year_of_birth >= @age_lower}
  {@age_higher != ''} ? {AND YEAR(t1.cohort_start_date) - p.year_of_birth <= @age_higher}
;
