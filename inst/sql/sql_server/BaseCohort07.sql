DELETE FROM @target_database_schema.@target_cohort_table WHERE cohort_definition_id IN (@target_cohort_id);

-- 07 Visits Emergency Room with 365 days post continuous observation days
INSERT INTO @target_database_schema.@target_cohort_table
SELECT DISTINCT @target_cohort_id cohort_definition_id,
        v.person_id subject_id,
        v.visit_start_date cohort_start_date,
        v.visit_end_date cohort_end_date
FROM @cdm_database_schema.visit_occurrence v
INNER JOIN 
    @vocabulary_database_schema.concept_ancestor ca
ON descendant_concept_id = visit_concept_id
INNER JOIN @cdm_database_schema.observation_period op
ON v.person_id = op.person_id
    AND observation_period_start_date <= visit_start_date
    AND observation_period_end_date >= visit_start_date
WHERE DATEDIFF(DAY, observation_period_start_date, visit_start_date) >= 365
    AND ancestor_concept_id IN (9203)
ORDER BY v.person_id, v.visit_start_date, v.visit_end_date;

UPDATE STATISTICS  @target_database_schema.@target_cohort_table;
