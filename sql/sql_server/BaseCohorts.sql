DELETE FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id IN (0, -1, -2, -3);

-- observation period
INSERT INTO @cohort_database_schema.@cohort_table
SELECT 0 cohort_definition_id,
        person_id subject_id,
        observation_period_start_date cohort_start_date,
        observation_period_end_date cohort_end_date
FROM @cdm_database_schema.observation_period
ORDER BY person_id, observation_period_start_date, observation_period_end_date;

-- all visits
INSERT INTO @cohort_database_schema.@cohort_table
SELECT DISTINCT -1 cohort_definition_id,
        person_id subject_id,
        visit_start_date cohort_start_date,
        visit_end_date cohort_end_date
FROM @cdm_database_schema.visit_occurrence
ORDER BY person_id, visit_start_date, visit_end_date;

-- inpatient
INSERT INTO @cohort_database_schema.@cohort_table
SELECT DISTINCT -2 cohort_definition_id,
        person_id subject_id,
        visit_start_date cohort_start_date,
        visit_end_date cohort_end_date
FROM @cdm_database_schema.visit_occurrence
WHERE visit_concept_id = 9201
ORDER BY person_id, visit_start_date, visit_end_date;

-- ER visits
INSERT INTO @cohort_database_schema.@cohort_table
SELECT DISTINCT -3 cohort_definition_id,
        person_id subject_id,
        visit_start_date cohort_start_date,
        visit_end_date cohort_end_date
FROM @cdm_database_schema.visit_occurrence
WHERE visit_concept_id = 9203
ORDER BY person_id, visit_start_date, visit_end_date;

update STATISTICS  @cohort_database_schema.@cohort_table;