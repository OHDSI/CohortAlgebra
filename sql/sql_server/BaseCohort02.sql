DELETE FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id IN (-2);

-- 02 Visits all
INSERT INTO @cohort_database_schema.@cohort_table
SELECT DISTINCT -2 cohort_definition_id,
        person_id subject_id,
        visit_start_date cohort_start_date,
        visit_end_date cohort_end_date
FROM @cdm_database_schema.visit_occurrence
ORDER BY person_id, visit_start_date, visit_end_date;

UPDATE STATISTICS  @cohort_database_schema.@cohort_table;
