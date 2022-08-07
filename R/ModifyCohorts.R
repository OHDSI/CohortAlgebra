# Copyright 2022 Observational Health Data Sciences and Informatics
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
#' @param cdmDatabaseSchema   Schema name where your patient-level data in OMOP CDM format resides.
#'                            Note that for SQL Server, this should include both the database and
#'                            schema name, for example 'cdm_data.dbo'. cdmDataschema is required
#'                            when eraConstructorPad is > 0. eraConstructorPad is optional.
#'
#' @param cohortStartCensorDate   the minimum date for the cohort. All rows with cohort start date before this date will be censored to given date.
#'
#' @param cohortEndCensorDate     the maximum date for the cohort. All rows with cohort end date after this date will be censored to given date.
#'
#' @param cohortStartFilterRange  A range of dates representing minimum to maximum to filter the cohort by its cohort start date e.g c(as.Date('1999-01-01'), as.Date('1999-12-31'))
#'
#' @param cohortEndFilterRange    A range of dates representing minimum to maximum to filter the cohort by its cohort end date e.g c(as.Date('1999-01-01'), as.Date('1999-12-31'))
#'
#' @param cohortStartPadDays      An integer value to pad the cohort start date. Default is 0 - no padding. The final cohort will have no days outside the observation period dates.
#'
#' @param cohortEndPadDays        An integer value to pad the cohort end date. Default is 0 - no padding. The final cohort will have no days outside the observation period dates.
#'
#' @return
#' NULL
modifyCohort <- function(connectionDetails = NULL,
                         connection = NULL,
                         cohortDatabaseSchema = NULL,
                         cohortTable = "cohort",
                         oldCohortId,
                         newCohortId = oldCohortId,
                         cohortStartCensorDate = NULL,
                         cohortEndCensorDate = NULL,
                         cohortStartFilterRange = NULL,
                         cohortEndFilterRange = NULL,
                         cohortStartPadDays = NULL,
                         cohortEndPadDays = NULL,
                         cdmDatabaseSchema = NULL,
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
  
  checkmate::reportAssertions(collection = errorMessages)
  
  if (is.null(cdmDatabaseSchema)) {
    if (any(cohortStartPadDays > 0,
            cohortEndPadDays > 0)) {
      stop(
        "cdmDatabaseSchema is NULL but cohort pad days > 0. This may result in cohorts that
            that are outside a persons observation period - ie. the resultant cohort is not valid.
            To avoid this - please always provide cdmDatabaseSchema with era Pad.
            The function will then ensure that cohort days are always in observation period."
      )
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
        intersect(x = newCohortId,
                  y = cohortIdsInCohortTable %>% unique())
      
      if (length(conflicitingCohortIdsInTargetCohortTable) > 0) {
        stop(
          paste0(
            "The following cohortIds already exist in the target cohort table, causing conflicts :",
            paste0(newCohortId,
                   collapse = ",")
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
  
  ## Cohort Pad -----
  if (any(!is.null(cohortStartPadDays), !is.null(cohortEndPadDays))) {
    sql <- "  DROP TABLE IF EXISTS @temp_table_2;
          	SELECT cohort_definition_id,
              	  subject_id,
              	  {@cohort_start_pad_days != ''} ? {DATEADD(DAY, @cohort_start_pad_days, cohort_start_date)} ? {cohort_start_date},
              	  {@cohort_end_pad_days != ''} ? {DATEADD(DAY, @cohort_end_pad_days, cohort_end_date)} ? {cohort_end_date}
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
      cohort_start_pad_days = cohortStartPadDays,
      cohort_end_pad_days = cohortEndPadDays,
      temp_table_1 = tempTable1,
      temp_table_2 = tempTable2
    )
    
    eraFyCohorts(
      connection = connection,
      oldToNewCohortId = dplyr::tibble(oldCohortId = oldCohortId,
                                       newCohortId = newCohortId),
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortTable = tempTable1,
      cohortDatabaseSchema = NULL
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
  deleteCohortRecords(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = newCohortId
  )
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " INSERT INTO {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table}
            SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
            FROM @temp_table_1;",
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
