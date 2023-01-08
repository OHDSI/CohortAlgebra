SELECT @new_cohort_id cohort_definition_id,
	f.subject_id,
	f.cohort_start_date,
	f.cohort_end_date
INTO @temp_table_1
FROM (
	SELECT c1.*,
		DATEDIFF(day, 
		          CASE WHEN 
		              DATEADD(DAY, @first_offset, c1.cohort_start_date) > c2.cohort_start_date 
		                  THEN DATEADD(DAY, @first_offset, c1.cohort_start_date) ELSE c2.cohort_start_date 
		          END, 
		          CASE WHEN 
		              DATEADD(DAY, @second_offset, c1.cohort_end_date) > c2.cohort_end_date 
		                  THEN c2.cohort_end_date ELSE DATEADD(DAY, @second_offset, c1.cohort_end_date) 
		          END
		        ) + 1 overlap_days
	FROM @cohort_database_schema.@cohort_table c1
	INNER JOIN @cohort_database_schema.@cohort_table c2 ON c1.subject_id = c2.subject_id
		AND DATEADD(DAY, @first_offset, c1.cohort_start_date) <= c2.cohort_end_date
		AND DATEADD(DAY, @second_offset, c1.cohort_end_date) >= c2.cohort_start_date
	WHERE c1.cohort_definition_id IN (@first_cohort_id)
		AND c2.cohort_definition_id IN (@second_cohort_id)
		{@second_cohort_start_before_first_cohort_start} ? {
		    AND c2.cohort_start_date < c1.cohort_start_date}
		{@second_cohort_start_after_first_cohort_start} ? {
		    AND c2.cohort_start_date > c1.cohort_start_date}
	) f
GROUP BY f.cohort_definition_id,
	f.subject_id,
	f.cohort_start_date,
	f.cohort_end_date
HAVING sum(overlap_days) >= @min_days_overlap;
