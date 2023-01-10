DELETE FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id IN (-3);

-- 03 Visits Inpatient or Inpatient Emergency
INSERT INTO @cohort_database_schema.@cohort_table
SELECT DISTINCT -3 cohort_definition_id,
        person_id subject_id,
        visit_start_date cohort_start_date,
        visit_end_date cohort_end_date
FROM @cdm_database_schema.visit_occurrence
INNER JOIN 
    @vocabulary_database_schema.concept_ancestpr ca
ON descendant_concept_id = visit_concept_id
WHERE ancestor_concept_id IN c(262, 9201)
ORDER BY person_id, visit_start_date, visit_end_date;

UPDATE STATISTICS  @cohort_database_schema.@cohort_table;