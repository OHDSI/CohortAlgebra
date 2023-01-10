DELETE FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id IN (-1);

-- 01 Observation Period
INSERT INTO @cohort_database_schema.@cohort_table
SELECT -1 cohort_definition_id,
person_id subject_id,
observation_period_start_date cohort_start_date,
observation_period_end_date cohort_end_date
FROM @cdm_database_schema.observation_period
ORDER BY person_id, observation_period_start_date, observation_period_end_date;

UPDATE STATISTICS  @cohort_database_schema.@cohort_table;
