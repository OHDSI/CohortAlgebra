# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of CohortAlgebra
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Modify cohort
#'
#' @description
#' Modify cohort by censoring, padding, limiting cohorts periods.
#' Censoring: Provide a date for right, left, both censoring. All cohorts will be truncated to the given date.
#' Pad days: Add days to either cohort start or cohort end dates. Maybe negative numbers. Final cohort will not be outside the persons observation period.
#' Limit cohort periods: Filter the cohorts to a given date range of cohort start, or cohort end or both.
#'
#' cdmDataschema is required when eraConstructorPad is > 0. eraConstructorPad is optional. It is also required when checking
#' for minimum continuous prior or post observation period.
#'
#' `r lifecycle::badge("experimental")`
#'
#' @template ConnectionDetails
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @template CohortDatabaseSchema
#'
#' @template OldCohortId
#'
#' @template NewCohortId
#'
#' @template PurgeConflicts
#'
#' @template TempEmulationSchema
#'
#' @template CdmDatabaseSchema
#'
#' @param cohortStartCensorDate   the minimum date for the cohort. All rows with cohort start date before this date
#'                                will be censored to given date.
#'
#' @param cohortEndCensorDate     the maximum date for the cohort. All rows with cohort end date after this date
#'                                will be censored to given date.
#'
#' @param cohortStartFilterRange  A range of dates representing minimum to maximum to filter the cohort by its cohort start
#'                                date e.g c(as.Date('1999-01-01'), as.Date('1999-12-31'))
#'
#' @param cohortEndFilterRange    A range of dates representing minimum to maximum to filter the cohort by its cohort end date e.g c(as.Date('1999-01-01'), as.Date('1999-12-31'))
#'
#' @param cohortStartPadDays      An integer value to pad the cohort start date. Default is 0 - no padding. The final cohort will
#'                                have no days outside the observation period dates of the initial observation period. If negative
#'                                padding, then cohortStartDate will not shift to before corresponding observationPeriodStartDate,
#'                                it will be forced to be equal to observationPeriodStartDate. If positive padding, then
#'                                cohortStartDate will not shift beyond observationPeriodEndDate, it will be forced to be
#'                                equal to observationPeriodEndDate. Also cohortStartDate will not be more than
#'                                cohortEndDate - it will be forced to be equal to cohortEndDate.
#'
#' @param cohortEndPadDays        An integer value to pad the cohort start date. Default is 0 - no padding. The final cohort will
#'                                have no days outside the observation period dates of the initial observation period. If negative
#'                                padding, then cohortEndDate will not shift to before corresponding observationPeriodEndDate,
#'                                it will be forced to be equal to observationPeriodEndDate. If positive padding, then
#'                                cohortEndDate will not shift beyond observationPeriodStartDate, it will be forced to be
#'                                equal to observationPeriodStartDate. Also cohortEndDate will not be less than
#'                                cohortStartDate - it will be forced to be equal to cohortStartDate.
#'
#' @param filterGenderConceptId   Provide an array of integers corresponding to conceptId to look for in the gender_concept_id
#'                                field of the person table.
#'
#' @param filterByAgeRange        Provide an array of two values, where second value is >= first value to filter the persons age on cohort_start_date.
#'                                Age is calculated as YEAR(cohort_start_date) - person.year_of_birth
#'
#' @param firstOccurrence         Do you want to restrict the cohort to the first occurrence per person?
#'
#' @param filterByMinimumCohortPeriod Do you want to filter cohort records by minimum cohort period, i.e. cohort period is calculated
#'                                    as DATEDIFF(cohort_start_date, cohort_start_date). if cohort_start_date = cohort_end_date then days = 0
#'
#' @param filterByMinimumPriorObservationPeriod  Do you want to filter cohort records by minimum Prior continuous Observation period
#'
#' @param filterByMinimumPostObservationPeriod  Do you want to filter cohort records by minimum Post continous Observation period
#'
#' @return
#' NULL
#'
#'
#' @examples
#' \dontrun{
#' CohortAlgebra::modifyCohort(
#'   connection = connection,
#'   cohortDatabaseSchema = cohortDatabaseSchema,
#'   cohortTable = tableName,
#'   oldCohortId = 3,
#'   newCohortId = 2,
#'   cohortEndFilterRange = c(as.Date("2010-01-01"), as.Date("2010-01-09")),
#'   purgeConflicts = TRUE
#' )
#' }
#'
#' @export
modifyCohort <- function(connectionDetails = NULL,
                         connection = NULL,
                         cohortDatabaseSchema = NULL,
                         cdmDatabaseSchema = NULL,
                         cohortTable = "cohort",
                         oldCohortId,
                         newCohortId = oldCohortId,
                         cohortStartCensorDate = NULL,
                         cohortEndCensorDate = NULL,
                         cohortStartFilterRange = NULL,
                         cohortEndFilterRange = NULL,
                         cohortStartPadDays = NULL,
                         cohortEndPadDays = NULL,
                         filterGenderConceptId = NULL,
                         filterByAgeRange = NULL,
                         firstOccurrence = FALSE,
                         filterByMinimumCohortPeriod = NULL,
                         filterByMinimumPriorObservationPeriod = NULL,
                         filterByMinimumPostObservationPeriod = NULL,
                         tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                         purgeConflicts = TRUE) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertIntegerish(
    x = oldCohortId,
    min.len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = newCohortId,
    min.len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = cohortDatabaseSchema,
    min.chars = 1,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = cdmDatabaseSchema,
    min.chars = 1,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = cohortTable,
    min.chars = 1,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertLogical(
    x = purgeConflicts,
    any.missing = FALSE,
    min.len = 1,
    add = errorMessages
  )
  checkmate::assertDate(
    x = cohortStartCensorDate,
    any.missing = FALSE,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertDate(
    x = cohortEndCensorDate,
    any.missing = FALSE,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertDate(
    x = cohortStartFilterRange,
    any.missing = TRUE,
    min.len = 1,
    max.len = 2,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertDouble(
    x = filterByMinimumCohortPeriod,
    len = 1,
    null.ok = TRUE,
    lower = 0,
    add = errorMessages
  )
  checkmate::assertDouble(
    x = filterByMinimumPriorObservationPeriod,
    len = 1,
    null.ok = TRUE,
    lower = 0,
    add = errorMessages
  )
  checkmate::assertDouble(
    x = filterByMinimumPostObservationPeriod,
    len = 1,
    null.ok = TRUE,
    lower = 0,
    add = errorMessages
  )

  if (any(
    !is.null(filterByMinimumPriorObservationPeriod),
    !is.null(filterByMinimumPostObservationPeriod)
  )) {
    checkmate::assertCharacter(
      x = cdmDatabaseSchema,
      min.chars = 1,
      len = 1,
      null.ok = FALSE,
      add = errorMessages
    )
  }

  if (!is.null(cohortStartFilterRange)) {
    checkmate::assert_true(x = cohortStartFilterRange[1] <= cohortStartFilterRange[2])
  }
  checkmate::assertDate(
    x = cohortEndFilterRange,
    any.missing = TRUE,
    min.len = 1,
    max.len = 2,
    null.ok = TRUE,
    add = errorMessages
  )
  if (!is.null(cohortEndFilterRange)) {
    checkmate::assert_true(x = cohortEndFilterRange[1] <= cohortEndFilterRange[2])
  }
  checkmate::assertIntegerish(
    x = cohortStartPadDays,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = cohortEndPadDays,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = filterGenderConceptId,
    min.len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = filterByAgeRange,
    min.len = 2,
    null.ok = TRUE,
    add = errorMessages
  )
  if (!is.null(filterByAgeRange)) {
    checkmate::assert_true(x = filterByAgeRange[1] <= filterByAgeRange[2])
  }

  checkmate::reportAssertions(collection = errorMessages)

  if (is.null(cdmDatabaseSchema)) {
    if (any(
      cohortStartPadDays > 0,
      cohortEndPadDays > 0, !is.null(filterByAgeRange), !is.null(filterGenderConceptId)
    )) {
      if (any(
        cohortStartPadDays != 0,
        cohortEndPadDays != 0
      )) {
        stop(
          "cdmDatabaseSchema is NULL but cohort pad days is not 0.
            When padding, the output cohort may result in cohort days that
            are outside a persons observation period. This makes the resultant cohort is not valid.
            To avoid this - we require provide cdmDatabaseSchema with era Pad.
            The function will then ensure that cohort days are always in observation period."
        )
      }
      if (any(!is.null(filterByAgeRange), !is.null(filterGenderConceptId))) {
        stop(
          "cdmDatabaseSchema is NULL.
            To filter cohort by age range or by gender, OMOP person table in cdmDatabaseSchema is required."
        )
      }
    }
  }

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  if (oldCohortId != newCohortId) {
    if (!purgeConflicts) {
      cohortIdsInCohortTable <-
        getCohortIdsInCohortTable(
          connection = connection,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortTable = cohortTable,
          tempEmulationSchema = tempEmulationSchema
        )
      conflicitingCohortIdsInTargetCohortTable <-
        intersect(
          x = newCohortId,
          y = cohortIdsInCohortTable %>% unique()
        )

      if (length(conflicitingCohortIdsInTargetCohortTable) > 0) {
        stop(
          paste0(
            "The following cohortIds already exist in the target cohort table, causing conflicts :",
            paste0(newCohortId,
              collapse = ","
            )
          )
        )
      }
    }
  }

  tempTableName <- generateRandomString()
  tempTable1 <- paste0("#", tempTableName, "1")
  tempTable2 <- paste0("#", tempTableName, "2")

  copyCohortsToTempTable(
    connection = connection,
    oldToNewCohortId = dplyr::tibble(oldCohortId = oldCohortId, newCohortId = newCohortId),
    sourceCohortDatabaseSchema = cohortDatabaseSchema,
    sourceCohortTable = cohortTable,
    targetCohortTable = tempTable1
  )

  ## Cohort Censor Start Date -----
  if (!is.null(cohortStartCensorDate)) {
    sql <- "  DROP TABLE IF EXISTS @temp_table_2;
          	SELECT cohort_definition_id,
              	  subject_id,
              		CASE
              			WHEN DATEFROMPARTS(@year_cohort_censor_start,
              			                    @month_cohort_censor_start,
              			                    @day_cohort_censor_start) >= cohort_start_date
              				THEN DATEFROMPARTS(@year_cohort_censor_start,
              			                    @month_cohort_censor_start,
              			                    @day_cohort_censor_start)
              			ELSE cohort_start_date
              			END AS cohort_start_date,
              		cohort_end_date
          	INTO @temp_table_2
          	FROM @temp_table_1;

          	DROP TABLE IF EXISTS @temp_table_1;

          SELECT *
          INTO @temp_table_1
          FROM @temp_table_2
          WHERE cohort_start_date <= cohort_end_date;

          DROP TABLE IF EXISTS @temp_table_2;
  "

    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      profile = FALSE,
      progressBar = FALSE,
      reportOverallTime = FALSE,
      tempEmulationSchema = tempEmulationSchema,
      year_cohort_censor_start = clock::get_year(cohortStartCensorDate),
      month_cohort_censor_start = clock::get_month(cohortStartCensorDate),
      day_cohort_censor_start = clock::get_day(cohortStartCensorDate),
      temp_table_1 = tempTable1,
      temp_table_2 = tempTable2
    )
  }

  ## Cohort Censor End Date -----
  if (!is.null(cohortEndCensorDate)) {
    sql <- "  DROP TABLE IF EXISTS @temp_table_2;
          	SELECT cohort_definition_id,
          	        subject_id,
          	        cohort_start_date,
              		CASE
              			WHEN DATEFROMPARTS(@year_cohort_censor_end,
              			                    @month_cohort_censor_end,
              			                    @day_cohort_censor_end) <= cohort_end_date
              				THEN DATEFROMPARTS(@year_cohort_censor_end,
              			                    @month_cohort_censor_end,
              			                    @day_cohort_censor_end)
              			ELSE cohort_end_date
              			END AS cohort_end_date
          	INTO @temp_table_2
          	FROM @temp_table_1;

          	DROP TABLE IF EXISTS @temp_table_1;

          SELECT *
          INTO @temp_table_1
          FROM @temp_table_2
          WHERE cohort_start_date <= cohort_end_date;

          DROP TABLE IF EXISTS @temp_table_2;
  "

    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      profile = FALSE,
      progressBar = FALSE,
      reportOverallTime = FALSE,
      tempEmulationSchema = tempEmulationSchema,
      year_cohort_censor_end = clock::get_year(cohortEndCensorDate),
      month_cohort_censor_end = clock::get_month(cohortEndCensorDate),
      day_cohort_censor_end = clock::get_day(cohortEndCensorDate),
      temp_table_1 = tempTable1,
      temp_table_2 = tempTable2
    )
  }


  ## Cohort Start Filter Range -----
  if (!is.null(cohortStartFilterRange)) {
    sql <- "  DROP TABLE IF EXISTS @temp_table_2;
          	SELECT cohort_definition_id,
              	  subject_id,
              		cohort_start_date,
              		cohort_end_date
          	INTO @temp_table_2
          	FROM @temp_table_1
          	WHERE cohort_start_date >= DATEFROMPARTS( @year_cohort_censor_start_low,
                            			                    @month_cohort_censor_start_low,
                            			                    @day_cohort_censor_start_low) AND
          	      cohort_start_date <= DATEFROMPARTS( @year_cohort_censor_start_high,
                            			                    @month_cohort_censor_start_high,
                            			                    @day_cohort_censor_start_high);


          	DROP TABLE IF EXISTS @temp_table_1;

          SELECT *
          INTO @temp_table_1
          FROM @temp_table_2
          WHERE cohort_start_date <= cohort_end_date;

          DROP TABLE IF EXISTS @temp_table_2;
  "

    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      profile = FALSE,
      progressBar = FALSE,
      reportOverallTime = FALSE,
      tempEmulationSchema = tempEmulationSchema,
      year_cohort_censor_start_low = clock::get_year(min(cohortStartFilterRange)),
      month_cohort_censor_start_low = clock::get_month(min(cohortStartFilterRange)),
      day_cohort_censor_start_low = clock::get_day(min(cohortStartFilterRange)),
      year_cohort_censor_start_high = clock::get_year(max(cohortStartFilterRange)),
      month_cohort_censor_start_high = clock::get_month(max(cohortStartFilterRange)),
      day_cohort_censor_start_high = clock::get_day(max(cohortStartFilterRange)),
      temp_table_1 = tempTable1,
      temp_table_2 = tempTable2
    )
  }

  ## Cohort End Filter Range -----
  if (!is.null(cohortEndFilterRange)) {
    sql <- "  DROP TABLE IF EXISTS @temp_table_2;
          	SELECT cohort_definition_id,
              	  subject_id,
              	  cohort_start_date,
              	  cohort_end_date
          	INTO @temp_table_2
          	FROM @temp_table_1
          	WHERE cohort_end_date >= DATEFROMPARTS( @year_cohort_censor_end_low,
                            			                    @month_cohort_censor_end_low,
                            			                    @day_cohort_censor_end_low) AND
          	      cohort_end_date <= DATEFROMPARTS( @year_cohort_censor_end_high,
                            			                    @month_cohort_censor_end_high,
                            			                    @day_cohort_censor_end_high);

          	DROP TABLE IF EXISTS @temp_table_1;

          SELECT *
          INTO @temp_table_1
          FROM @temp_table_2
          WHERE cohort_start_date <= cohort_end_date;

          DROP TABLE IF EXISTS @temp_table_2;
  "

    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      profile = FALSE,
      progressBar = FALSE,
      reportOverallTime = FALSE,
      tempEmulationSchema = tempEmulationSchema,
      year_cohort_censor_end_low = clock::get_year(min(cohortEndFilterRange)),
      month_cohort_censor_end_low = clock::get_month(min(cohortEndFilterRange)),
      day_cohort_censor_end_low = clock::get_day(min(cohortEndFilterRange)),
      year_cohort_censor_end_high = clock::get_year(max(cohortEndFilterRange)),
      month_cohort_censor_end_high = clock::get_month(max(cohortEndFilterRange)),
      day_cohort_censor_end_high = clock::get_day(max(cohortEndFilterRange)),
      temp_table_1 = tempTable1,
      temp_table_2 = tempTable2
    )
  }

  ## filter- Gender Concept Id -----
  if (!is.null(filterGenderConceptId)) {
    sql <- "DROP TABLE IF EXISTS @temp_table_2;
          	SELECT cohort_definition_id,
              	  subject_id,
              	  cohort_start_date,
              	  cohort_end_date
          	INTO @temp_table_2
          	FROM @temp_table_1 t
          	INNER JOIN @cdm_database_schema.person p
          	ON t.subject_id = p.person_id
          	WHERE p.gender_concept_id IN (@gender_concept_id);

          	DROP TABLE IF EXISTS @temp_table_1;

          SELECT *
          INTO @temp_table_1
          FROM @temp_table_2;

          DROP TABLE IF EXISTS @temp_table_2;
  "

    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      profile = FALSE,
      progressBar = FALSE,
      reportOverallTime = FALSE,
      tempEmulationSchema = tempEmulationSchema,
      gender_concept_id = filterGenderConceptId,
      cdm_database_schema = cdmDatabaseSchema,
      temp_table_1 = tempTable1,
      temp_table_2 = tempTable2
    )
  }


  ## filter- Age Range -----
  if (!is.null(filterByAgeRange)) {
    sql <- "DROP TABLE IF EXISTS @temp_table_2;
          	SELECT cohort_definition_id,
              	  subject_id,
              	  cohort_start_date,
              	  cohort_end_date
          	INTO @temp_table_2
          	FROM @temp_table_1 t
          	INNER JOIN @cdm_database_schema.person p
          	ON t.subject_id = p.person_id
          	WHERE YEAR(t.cohort_start_date) - p.year_of_birth >= @age_lower
          	      AND YEAR(t.cohort_start_date) - p.year_of_birth <= @age_higher;

          	DROP TABLE IF EXISTS @temp_table_1;

          SELECT *
          INTO @temp_table_1
          FROM @temp_table_2;

          DROP TABLE IF EXISTS @temp_table_2;
  "

    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      profile = FALSE,
      progressBar = FALSE,
      reportOverallTime = FALSE,
      tempEmulationSchema = tempEmulationSchema,
      age_lower = min(filterByAgeRange),
      age_higher = max(filterByAgeRange),
      cdm_database_schema = cdmDatabaseSchema,
      temp_table_1 = tempTable1,
      temp_table_2 = tempTable2
    )
  }

  ## Cohort Pad -----
  if (any(!is.null(cohortStartPadDays), !is.null(cohortEndPadDays))) {
    sql <- "  DROP TABLE IF EXISTS @temp_table_2;

            with date_offset as
            (
            	SELECT cohort_definition_id,
                	  subject_id,
                	  {@cohort_start_pad_days != ''} ?
                	    {DATEADD(DAY, @cohort_start_pad_days, cohort_start_date)} :
                	    {cohort_start_date} cohort_start_date,
                     {@cohort_end_pad_days != ''} ?
                      {DATEADD(DAY, @cohort_end_pad_days, cohort_end_date)} :
                	        {cohort_end_date} cohort_end_date,
                	   observation_period_start_date,
                	   observation_period_end_date
            	FROM @temp_table_1 t
            	INNER JOIN @cdm_database_schema.observation_period op
            	ON t.subject_id = op.person_id
              WHERE op.observation_period_start_date <= t.cohort_start_date
                    AND op.observation_period_end_date >= t.cohort_start_date
            )
            , fix_outside as
            (
            SELECT cohort_definition_id,
                    subject_id,
                    CASE WHEN cohort_start_date < observation_period_start_date
                          THEN observation_period_start_date
                        ELSE cohort_start_date
                    END cohort_start_date,
                    CASE WHEN cohort_end_date > observation_period_end_date
                        THEN observation_period_end_date
                      ELSE cohort_end_date
                    END cohort_end_date,
                    observation_period_start_date,
                    observation_period_end_date
            from date_offset
            )
            , fix_inside as
            (
            SELECT cohort_definition_id,
                    subject_id,
                    CASE WHEN cohort_start_date > observation_period_end_date
                          THEN observation_period_end_date
                        ELSE cohort_start_date
                    END cohort_start_date,
                    CASE WHEN cohort_end_date < observation_period_start_date
                        THEN observation_period_start_date
                      ELSE cohort_end_date
                    END cohort_end_date
            from fix_outside
            )
            , fix_cohort_date as
            (
            SELECT cohort_definition_id,
                    subject_id,
                    CASE WHEN cohort_start_date > cohort_end_date
                          THEN cohort_end_date
                        ELSE cohort_end_date
                    END cohort_start_date,
                    cohort_end_date
            from fix_inside
            )
            SELECT DISTINCT
                    cohort_definition_id,
                   subject_id,
                   cohort_start_date,
                   cohort_end_date
          	INTO @temp_table_2
          	FROM fix_inside;

          	DROP TABLE IF EXISTS @temp_table_1;

          SELECT *
          INTO @temp_table_1
          FROM @temp_table_2;

          DROP TABLE IF EXISTS @temp_table_2;
  "

    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      profile = FALSE,
      progressBar = FALSE,
      reportOverallTime = FALSE,
      tempEmulationSchema = tempEmulationSchema,
      cohort_start_pad_days = cohortStartPadDays,
      cohort_end_pad_days = cohortEndPadDays,
      cdm_database_schema = cdmDatabaseSchema,
      temp_table_1 = tempTable1,
      temp_table_2 = tempTable2
    )

    eraFyCohorts(
      connection = connection,
      oldToNewCohortId = dplyr::tibble(
        oldCohortId = newCohortId,
        newCohortId = newCohortId
      ),
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortTable = tempTable1,
      cohortDatabaseSchema = NULL,
      purgeConflicts = TRUE
    )
  }

  ## First Occurrence -----
  if (firstOccurrence) {
    sql <- "  DROP TABLE IF EXISTS @temp_table_2;

            SELECT cohort_definition_id,
                   subject_id,
                   min(cohort_start_date) cohort_start_date,
                   min(cohort_end_date) cohort_end_date
          	INTO @temp_table_2
          	FROM @temp_table_1
          	GROUP BY cohort_definition_id, subject_id;

          	DROP TABLE IF EXISTS @temp_table_1;

          SELECT *
          INTO @temp_table_1
          FROM @temp_table_2;

          DROP TABLE IF EXISTS @temp_table_2;
  "

    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      profile = FALSE,
      progressBar = FALSE,
      reportOverallTime = FALSE,
      tempEmulationSchema = tempEmulationSchema,
      temp_table_1 = tempTable1,
      temp_table_2 = tempTable2
    )
  }


  ## Filter cohorts by minimum cohort period -----
  if (!is.null(filterByMinimumCohortPeriod)) {
    sql <- "  DROP TABLE IF EXISTS @temp_table_2;

            SELECT cohort_definition_id,
                   subject_id,
                   cohort_start_date,
                   cohort_end_date
          	INTO @temp_table_2
          	FROM @temp_table_1
          	WHERE DATEDIFF(day, cohort_start_date, cohort_end_date) >= @min_period;

          	DROP TABLE IF EXISTS @temp_table_1;

          SELECT *
          INTO @temp_table_1
          FROM @temp_table_2;

          DROP TABLE IF EXISTS @temp_table_2;
  "

    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      profile = FALSE,
      progressBar = FALSE,
      reportOverallTime = FALSE,
      tempEmulationSchema = tempEmulationSchema,
      temp_table_1 = tempTable1,
      temp_table_2 = tempTable2,
      min_period = filterByMinimumCohortPeriod
    )
  }


  ## Filter by minimum prior continuous observation period -----
  if (!is.null(filterByMinimumPriorObservationPeriod)) {
    sql <- "  DROP TABLE IF EXISTS @temp_table_2;

            SELECT cohort_definition_id,
                   subject_id,
                   cohort_start_date,
                   cohort_end_date
          	INTO @temp_table_2
          	FROM @temp_table_1 t
          	INNER JOIN @cdm_database_schema.observation_period op
          	ON t.subject_id = op.person_id
          	    AND observation_period_start_date <= cohort_start_date
		            AND observation_period_end_date >= cohort_start_date
          	WHERE DATEDIFF(DAY, observation_period_start_date, cohort_start_date) >= @min_period;

          	DROP TABLE IF EXISTS @temp_table_1;

          SELECT *
          INTO @temp_table_1
          FROM @temp_table_2;

          DROP TABLE IF EXISTS @temp_table_2;
  "

    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      profile = FALSE,
      progressBar = FALSE,
      reportOverallTime = FALSE,
      tempEmulationSchema = tempEmulationSchema,
      temp_table_1 = tempTable1,
      temp_table_2 = tempTable2,
      cdm_database_schema = cdmDatabaseSchema,
      min_period = filterByMinimumPriorObservationPeriod
    )
  }


  ## Filter by minimum post continuous observation period -----
  if (!is.null(filterByMinimumPostObservationPeriod)) {
    sql <- "  DROP TABLE IF EXISTS @temp_table_2;

            SELECT cohort_definition_id,
                   subject_id,
                   cohort_start_date,
                   cohort_end_date
          	INTO @temp_table_2
          	FROM @temp_table_1 t
          	INNER JOIN @cdm_database_schema.observation_period op
          	ON t.subject_id = op.person_id
          	    AND observation_period_start_date <= cohort_start_date
		            AND observation_period_end_date >= cohort_start_date
          	WHERE DATEDIFF(DAY, cohort_start_date, observation_period_end_date) >= @min_period;

          	DROP TABLE IF EXISTS @temp_table_1;

          SELECT *
          INTO @temp_table_1
          FROM @temp_table_2;

          DROP TABLE IF EXISTS @temp_table_2;
  "

    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      profile = FALSE,
      progressBar = FALSE,
      reportOverallTime = FALSE,
      tempEmulationSchema = tempEmulationSchema,
      temp_table_1 = tempTable1,
      temp_table_2 = tempTable2,
      cdm_database_schema = cdmDatabaseSchema,
      min_period = filterByMinimumPostObservationPeriod
    )
  }

  if (oldCohortId != newCohortId) {
    ParallelLogger::logTrace(
      paste0(
        "The following cohortIds will be deleted from your cohort table and \n",
        " replaced with ear fy'd version of those cohorts using the same original cohort id: ",
        paste0(newCohortId, collapse = ",")
      )
    )
  }
  deleteCohort(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = newCohortId
  )

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " INSERT INTO {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table}
            SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
            FROM @temp_table_1;
            UPDATE STATISTICS  {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table};",
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cohort_database_schema = cohortDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohort_table = cohortTable,
    temp_table_1 = tempTable1
  )

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " DROP TABLE IF EXISTS @temp_table_1;
            DROP TABLE IF EXISTS @temp_table_2;",
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    temp_table_1 = tempTable1,
    temp_table_2 = tempTable2
  )
}
