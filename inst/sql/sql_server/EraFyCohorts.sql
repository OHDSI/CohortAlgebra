DROP TABLE IF EXISTS @temp_table_2;
                
                
with cteEndDates (cohort_definition_id, subject_id, cohort_end_date) AS -- the magic
(
	SELECT
	  cohort_definition_id
		, subject_id
		, DATEADD(day,-1 * @eraconstructorpad, event_date)  as cohort_end_date
	FROM
	(
		SELECT
		  cohort_definition_id
			, subject_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY cohort_definition_id, subject_id
			                            ORDER BY event_date, event_type, start_ordinal ROWS UNBOUNDED PRECEDING) AS start_ordinal
			, ROW_NUMBER() OVER (PARTITION BY cohort_definition_id, subject_id
			                      ORDER BY event_date, event_type, start_ordinal) AS overall_ord
		FROM
		(
			SELECT
			  cohort_definition_id
				, subject_id
				, cohort_start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY cohort_definition_id, subject_id ORDER BY cohort_start_date) AS start_ordinal
			FROM @temp_table_1

			UNION ALL


			SELECT
			  cohort_definition_id
				, subject_id
				, DATEADD(day,@eraconstructorpad,cohort_end_date) as cohort_end_date
				, 1 AS event_type
				, NULL
			FROM @temp_table_1
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date) AS
(
	SELECT
	  c. cohort_definition_id
		, c.subject_id
		, c.cohort_start_date
		, MIN(e.cohort_end_date) AS cohort_end_date
	FROM @temp_table_1 c
	JOIN cteEndDates e ON c.cohort_definition_id = e.cohort_definition_id AND
	                      c.subject_id = e.subject_id AND
	                      e.cohort_end_date >= c.cohort_start_date
	GROUP BY c.cohort_definition_id, c.subject_id, c.cohort_start_date
),
cohort_era as
(
select cohort_definition_id, subject_id, min(cohort_start_date) as cohort_start_date, cohort_end_date
from cteEnds
group by cohort_definition_id, subject_id, cohort_end_date
)
{@cdm_database_schema != ''} ?
{SELECT cohort_definition_id,
        subject_id,
        cohort_start_date, 
        cohort_end_date
  FROM 
  (SELECT ce.cohort_definition_id,
        ce.subject_id,
        CASE WHEN op.observation_period_start_date < ce.cohort_start_date THEN ce.cohort_start_date
            ELSE op.observation_period_start_date END AS cohort_start_date,
        CASE WHEN op.observation_period_end_date > ce.cohort_end_date then ce.cohort_end_date
            ELSE op.observation_period_end_date END AS cohort_end_date
  into @temp_table_2
  FROM cohort_era ce
  INNER JOIN @cdm_database_schema.observation_period op
  ON ce.subject_id = op.person_id
  WHERE op.observation_period_start_date <= ce.cohort_start_date
        AND op.observation_period_end_date >= ce.cohort_start_date
        -- only returns overlapping periods
    ) f
  WHERE cohort_start_date <= cohort_end_date
} :
{SELECT *
 INTO @temp_table_2
 FROM cohort_era
 WHERE cohort_start_date <= cohort_end_date
}
 ;
