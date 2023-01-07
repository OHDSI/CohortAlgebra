SELECT DISTINCT c.*
FROM @cohort_database_schema.@cohort_table c
INNER JOIN 
    @cohort_database_schema.@cohort_table c2
ON c.subject_id = c2.subject_id
  AND 
            LEFT JOIN
                @temp_table_1 r
            ON c.subject_id = r.subject_id
              AND c.cohort_definition_id = r.cohort_definition_id
            WHERE c.cohort_definition_id IN (@given_cohort_ids)
                  AND r.subject_id IS NULL;"