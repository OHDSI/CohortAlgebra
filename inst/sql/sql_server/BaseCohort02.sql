DELETE FROM @target_database_schema.@target_cohort_table WHERE cohort_definition_id IN (@target_cohort_id);

-- 02 Visits all
INSERT INTO @target_database_schema.@target_cohort_table
SELECT DISTINCT @target_cohort_id cohort_definition_id,
        person_id subject_id,
        visit_start_date cohort_start_date,
        visit_end_date cohort_end_date
FROM @cdm_database_schema.visit_occurrence
ORDER BY person_id, visit_start_date, visit_end_date;

UPDATE STATISTICS  @target_database_schema.@target_cohort_table;
