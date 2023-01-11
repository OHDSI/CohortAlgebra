DROP TABLE IF EXISTS #censort_dates;

SELECT @new_cohort_id cohort_definition_id,
  	  subject_id,
  	  {@cohort_start_left_censort_start_year != ''} ? {
  		CASE
  			WHEN DATEFROMPARTS(@cohort_start_left_censort_start_year,
  			                    @cohort_start_left_censort_start_month,
  			                    @cohort_start_left_censort_start_day) >= cohort_start_date
  				THEN DATEFROMPARTS(@cohort_start_left_censort_start_year,
  			                    @cohort_start_left_censort_start_month,
  			                    @cohort_start_left_censort_start_day)
  			ELSE cohort_start_date
  			END} : {cohort_start_date} cohort_start_date,
  		{@cohort_end_right_censort_start_year != ''} ? {
  		CASE
  			WHEN DATEFROMPARTS(@cohort_end_right_censort_start_year,
  			                    @cohort_end_right_censort_start_month,
  			                    @cohort_end_right_censort_start_day) <= cohort_end_date
  				THEN DATEFROMPARTS(@cohort_end_right_censort_start_year,
  			                    @cohort_end_right_censort_start_month,
  			                    @cohort_end_right_censort_start_day)
  			ELSE cohort_end_date
  			END} : {cohort_end_date} cohort_end_date
INTO #censort_dates
FROM {@source_cohort_database_schema != ''} ? {@source_cohort_database_schema.@source_cohort_table} : {@source_cohort_table}
WHERE cohort_definition_id = @old_cohort_id;

DELETE FROM {@target_cohort_database_schema != ''} ? {@target_cohort_database_schema.@source_cohort_table} : {@target_cohort_table}
WHERE cohort_definition_id = @new_cohort_id;

INSERT INTO {@target_cohort_database_schema != ''} ? {@target_cohort_database_schema.@source_cohort_table} : {@target_cohort_table}
SELECT @new_cohort_id cohort_definition_id,
        subject_id,
        cohort_start_date,
        cohort_end_date
FROM #censort_dates
WHERE cohort_start_date <= cohort_end_date;

DROP TABLE IF EXISTS #censort_dates;
