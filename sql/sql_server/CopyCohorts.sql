INSERT INTO {@target_database_schema != ''} ? {@target_database_schema.@target_cohort_table} : {@target_cohort_table}
SELECT target.new_cohort_id cohort_definition_id,
        source.subject_id,
        source.cohort_start_date,
        source.cohort_end_date
FROM {@source_database_schema != ''} ? {@source_database_schema.@source_cohort_table} : {@source_cohort_table} source
INNER JOIN #old_to_new_cohort_id target
ON source.cohort_definition_id = target.old_cohort_id;

UPDATE STATISTICS  {@target_database_schema != ''} ? {@target_database_schema.@target_cohort_table} : {@target_cohort_table};

