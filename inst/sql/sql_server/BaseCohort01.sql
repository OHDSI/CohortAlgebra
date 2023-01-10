DELETE FROM @target_database_schema.@target_cohort_table WHERE cohort_definition_id IN (@target_cohort_id);

-- 01 Observation Period
INSERT INTO @target_database_schema.@target_cohort_table
SELECT  @target_cohort_id cohort_definition_id,
        person_id subject_id,
        observation_period_start_date cohort_start_date,
        observation_period_end_date cohort_end_date
FROM @cdm_database_schema.observation_period
ORDER BY person_id, observation_period_start_date, observation_period_end_date;

UPDATE STATISTICS  @target_database_schema.@target_cohort_table;
