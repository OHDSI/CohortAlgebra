DELETE FROM @target_database_schema.@target_cohort_table WHERE cohort_definition_id IN (@target_cohort_id);

-- 08 Visits with 365 days prior continuous observation days
INSERT INTO @target_database_schema.@target_cohort_table
SELECT DISTINCT @target_cohort_id cohort_definition_id,
        v.person_id subject_id,
        v.visit_start_date cohort_start_date,
        v.visit_end_date cohort_end_date
FROM @cdm_database_schema.visit_occurrence v
INNER JOIN @cdm_database_schema.observation_period op
ON v.person_id = op.person_id
    AND observation_period_start_date <= cohort_start_date
    AND observation_period_end_date >= cohort_start_date
WHERE DATEDIFF(DAY, cohort_start_date, observation_period_start_date) >= @min_period
ORDER BY v.person_id, v.visit_start_date, v.visit_end_date;

UPDATE STATISTICS  @target_database_schema.@target_cohort_table;
