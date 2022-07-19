DROP TABLE IF EXISTS @temp_table_2;

WITH cohort_dates
AS (
	SELECT subject_id,
		cohort_date,
		-- LEAD will ignore values that are same (e.g. if cohort_start_date = cohort_end_date)
		ROW_NUMBER() OVER(PARTITION BY subject_id
		                  ORDER BY cohort_date ASC) cohort_date_seq
	FROM (
		SELECT subject_id,
			cohort_start_date cohort_date
		FROM @temp_table_1
		WHERE cohort_definition_id IN (@first_cohort_id, -999)

		UNION ALL -- we need all dates, even if duplicates

		SELECT subject_id,
			cohort_end_date cohort_date
		FROM @temp_table_1
		WHERE cohort_definition_id IN (@first_cohort_id, -999)
		) all_dates
	),
candidate_periods
AS (
	SELECT
		subject_id,
		cohort_date candidate_start_date,
		cohort_date_seq,
		LEAD(cohort_date, 1) OVER (
			PARTITION BY subject_id ORDER BY cohort_date, cohort_date_seq ASC
			) candidate_end_date
	FROM cohort_dates
	GROUP BY subject_id,
		cohort_date,
		cohort_date_seq
	),
candidate_cohort_date
AS (
	SELECT DISTINCT cohort.*,
		candidate_start_date,
		candidate_end_date
	FROM @temp_table_1 cohort
	INNER JOIN candidate_periods candidate ON cohort.subject_id = candidate.subject_id
		AND candidate_start_date >= cohort_start_date
		AND candidate_end_date <= cohort_end_date
	)
SELECT
	subject_id,
	candidate_start_date,
	candidate_end_date
INTO @temp_table_2
FROM candidate_cohort_date
GROUP BY subject_id,
	candidate_start_date,
	candidate_end_date
HAVING COUNT(*) = 1;