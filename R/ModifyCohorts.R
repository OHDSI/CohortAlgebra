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

#' Modify cohort(s)
#'
#' @description
#' Modify cohort(s) by censoring, padding, limiting cohorts periods.
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
#' @template OldToNewCohortId
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
modifyCohorts <- function(connectionDetails = NULL,
                          connection = NULL,
                          cohortDatabaseSchema = NULL,
                          cohortTable = "cohort",
                          oldToNewCohortId,
                          cohortStartCensorDate = NULL,
                          cohortEndCensorDate = NULL,
                          cohortStartFilterRange = NULL,
                          cohortEndFilterRange = NULL,
                          cohortStartPadDays = 0,
                          cohortEndPadDays = 0,
                          cdmDatabaseSchema = NULL,
                          tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                          purgeConflicts = FALSE) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertDataFrame(x = oldToNewCohortId,
                             min.rows = 1,
                             add = errorMessages)
  checkmate::assertNames(
    x = colnames(oldToNewCohortId),
    must.include = c("oldCohortId", "newCohortId"),
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = oldToNewCohortId$oldCohortId,
    min.len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = oldToNewCohortId$newCohortId,
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
  checkmate::assertArray(
    x = cohortStartFilterRange,
    any.missing = FALSE,
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
  checkmate::assertArray(
    x = cohortEndFilterRange,
    any.missing = TRUE,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertDate(
    x = cohortEndFilterRange,
    any.missing = TRUE,
    min.len = 1,
    max.len = 2,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = cohortStartPadDays,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = cohortEndPadDays,
    len = 1,
    null.ok = FALSE,
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
  
  cohortIdsInCohortTable <-
    getCohortIdsInCohortTable(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      tempEmulationSchema = tempEmulationSchema
    )
  
  conflicitingCohortIdsInTargetCohortTable <-
    intersect(x = oldToNewCohortId$newCohortId %>% unique(),
              y = cohortIdsInCohortTable %>% unique())
  
  performPurgeConflicts <- FALSE
  if (length(conflicitingCohortIdsInTargetCohortTable) > 0) {
    if (purgeConflicts) {
      performPurgeConflicts <- TRUE
    } else {
      stop(
        paste0(
          "The following cohortIds already exist in the target cohort table, causing conflicts :",
          paste0(conflicitingCohortIdsInTargetCohortTable, collapse = ",")
        )
      )
    }
  }
  
  tempTableName <- generateRandomString()
  tempTable1 <- paste0("#", tempTableName, "1")
  tempTable2 <- paste0("#", tempTableName, "2")
  
  copyCohortsToTempTable(
    connection = connection,
    oldToNewCohortId = oldToNewCohortId,
    sourceCohortDatabaseSchema = cohortDatabaseSchema,
    sourceCohortTable = cohortTable,
    targetCohortTable = tempTable1
  )
  
  sqlEraFy <- " DROP TABLE IF EXISTS @temp_table_2;

 ; "
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlEraFy,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    tempEmulationSchema = tempEmulationSchema,
    eraconstructorpad = eraconstructorpad,
    cdm_database_schema = cdmDatabaseSchema,
    temp_table_1 = tempTable1,
    temp_table_2 = tempTable2
  )
  
  cohortIdsToDeleteFromSource <- oldToNewCohortId %>%
    dplyr::filter(.data$oldCohortId == .data$newCohortId) %>%
    dplyr::pull(.data$oldCohortId)
  
  if (length(cohortIdsToDeleteFromSource) > 0) {
    ParallelLogger::logTrace(
      paste0(
        "The following cohortIds will be deleted from your cohort table and \n",
        " replaced with ear fy'd version of those cohorts using the same original cohort id: ",
        paste0(cohortIdsToDeleteFromSource, collapse = ",")
      )
    )
    deleteCohortRecords(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = cohortIdsToDeleteFromSource
    )
  }
  
  if (performPurgeConflicts) {
    ParallelLogger::logTrace(
      paste0(
        "The following conflicting cohortIds will be deleted from your cohort table \n",
        " as part resolving conflicts: ",
        paste0(conflicitingCohortIdsInTargetCohortTable, collapse = ",")
      )
    )
    deleteCohortRecords(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = conflicitingCohortIdsInTargetCohortTable
    )
  }
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " INSERT INTO {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table}
            SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
            FROM @temp_table_2;",
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cohort_database_schema = cohortDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohort_table = cohortTable,
    temp_table_2 = tempTable2
  )
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " DROP TABLE IF EXISTS @temp_table_1;
            DROP TABLE IF EXISTS @temp_table_2;
            DROP TABLE IF EXISTS #old_to_new_cohort_id;",
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    temp_table_1 = tempTable1,
    temp_table_2 = tempTable2
  )
}
