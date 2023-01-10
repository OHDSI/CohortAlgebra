DELETE FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id IN (-6);

-- 06 Visits Inpatient with 365 days post continuous observation days
INSERT INTO @cohort_database_schema.@cohort_table
SELECT DISTINCT -6 cohort_definition_id,
        person_id subject_id,
        visit_start_date cohort_start_date,
        visit_end_date cohort_end_date
FROM @cdm_database_schema.visit_occurrence v
INNER JOIN 
    @vocabulary_database_schema.concept_ancestpr ca
ON descendant_concept_id = visit_concept_id
INNER JOIN @cdm_database_schema.observation_period op
ON v.subject_id = op.person_id
    AND observation_period_start_date <= cohort_start_date
    AND observation_period_end_date >= cohort_start_date
WHERE DATEDIFF(DAY, observation_period_start_date, cohort_start_date) >= @min_period
    AND ancestor_concept_id  IN c(262, 9201)
ORDER BY person_id, visit_start_date, visit_end_date;

UPDATE STATISTICS  @cohort_database_schema.@cohort_table;