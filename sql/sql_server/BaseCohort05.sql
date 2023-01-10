DELETE FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id IN (-5);

-- 05 Visits with 365 days post continuous observation days
INSERT INTO @cohort_database_schema.@cohort_table
SELECT DISTINCT -5 cohort_definition_id,
        person_id subject_id,
        visit_start_date cohort_start_date,
        visit_end_date cohort_end_date
FROM @cdm_database_schema.visit_occurrence v
INNER JOIN @cdm_database_schema.observation_period op
ON v.subject_id = op.person_id
    AND observation_period_start_date <= cohort_start_date
    AND observation_period_end_date >= cohort_start_date
WHERE DATEDIFF(DAY, observation_period_start_date, cohort_start_date) >= @min_period
ORDER BY person_id, visit_start_date, visit_end_date;

UPDATE STATISTICS  @cohort_database_schema.@cohort_table;
