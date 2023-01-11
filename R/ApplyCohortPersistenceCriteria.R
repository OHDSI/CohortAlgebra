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

#' Apply persistence criteria.
#'
#' @description
#' Apply cohort persistence criteria. Only one persistence criteria may be used at a time. The three options are a) persist till
#' end of observation period, b) persist for a certain number of fixed days after cohort_start_date, c) persist for
#' a certain number of fixed days after cohort_end_days. In all cases, the given cohort (oldCohortId) is treated as
#' an event and the criteria is applied to get new event dates. Event dates are converted to cohort dates by cohort
#' era fy routine in final step.
#'
#' Offset: The event end date is derived from adding a number of days to the event's start or end date. If an offset is added to the
#' event's start date, all cohort episodes will have the same fixed duration (limited by duration of continuos observation).
#' If an offset is added to the event's end date, persons in the cohort may have varying cohort duration
#' times due to the varying event duration. This event persistence assures that the cohort end date will be no greater than
#' the selected index event date, plus the days offset.
#'
#'
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
#' @param tillEndOfObservationPeriod The cohort will persist till end of the overlapping observation period. An era logic
#'                                                 will be applied.
#'
#' @param offsetCohortStartDate
#'
#' @param offsetCohortEndDate        Apply a fixed persistence criteria relative to cohort end date. A new cohort
#'                                       end date will be created by adding persistence days to cohort_end_date with a
#'                                       value that is minimum of the cohort_end_date + offsetCohortEndDate
#'                                       or observation_period_end_date of the overlapping observation period. An era logic
#'                                       will be applied.
#'                                       
#' @template CdmDatabaseSchema
#'
#' @return
#' NULL
#'
#'
#' @examples
#' \dontrun{
#' CohortAlgebra::applyCohortPersistenceCriteria(
#'   connection = connection,
#'   sourceCohortTable = 'cohort',
#'   targetCohortTable = 'cohort',
#'   oldCohortId = 3,
#'   newCohortId = 2,
#'   tillEndOfObservationPeriod = TRUE,
#'   purgeConflicts = TRUE
#' )
#' }
#'
#' @export
applyCohortPersistenceCriteria <- function(connectionDetails = NULL,
                                           connection = NULL,
                                           sourceCohortDatabaseSchema = NULL,
                                           sourceCohortTable,
                                           targetCohortDatabaseSchema = NULL,
                                           targetCohortTable,
                                           cdmDatabaseSchema,
                                           oldCohortId,
                                           newCohortId,
                                           tillEndOfObservationPeriod = FALSE,
                                           offsetCohortStartDate = NULL,
                                           offsetCohortEndDate = NULL,
                                           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                           purgeConflicts = FALSE) {
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
    x = cdmDatabaseSchema,
    min.chars = 1,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = sourceCohortDatabaseSchema,
    min.chars = 1,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = sourceCohortTable,
    min.chars = 1,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = targetCohortDatabaseSchema,
    min.chars = 1,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = targetCohortTable,
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
  checkmate::assertLogical(
    x = tillEndOfObservationPeriod,
    any.missing = FALSE,
    min.len = 1,
    add = errorMessages
  )
  checkmate::assertDouble(
    x = offsetCohortStartDate,
    any.missing = FALSE,
    lower = 0,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertDouble(
    x = offsetCohortEndDate,
    any.missing = FALSE,
    lower = 0,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  
  checkmate::reportAssertions(collection = errorMessages)
  
  if (sum(
    tillEndOfObservationPeriod,!is.null(offsetCohortStartDate),!is.null(offsetCohortEndDate)
  ) > 1) {
    stop("Multiple persistence criteria specified.")
  }
  
  if (sum(
    tillEndOfObservationPeriod,!is.null(offsetCohortStartDate),!is.null(offsetCohortEndDate)
  ) == 0) {
    stop("No persistence criteria specified.")
  }
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  if (!purgeConflicts) {
    cohortIdsInCohortTable <-
      getCohortIdsInCohortTable(
        connection = connection,
        cohortDatabaseSchema = targetCohortDatabaseSchema,
        cohortTable = targetCohortTable,
        tempEmulationSchema = tempEmulationSchema
      )
    conflicitingCohortIdsInTargetCohortTable <-
      intersect(x = newCohortId,
                y = cohortIdsInCohortTable %>% unique())
    
    if (length(conflicitingCohortIdsInTargetCohortTable) > 0) {
      stop("Target cohort id already in use in target cohort table")
    }
  }
  
  if (tillEndOfObservationPeriod) {
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "PersistEndOfContinuousObservationPeriod.sql",
      packageName = utils::packageName(),
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      cdm_database_schema = cdmDatabaseSchema,
      source_cohort_database_schema = sourceCohortDatabaseSchema,
      source_cohort_table = sourceCohortTable,
      target_cohort_database_schema = targetCohortDatabaseSchema,
      target_cohort_table = targetCohortTable,
      old_cohort_id = oldCohortId,
      new_cohort_id = newCohortId
    )
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql,
      profile = FALSE,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  
  if (!is.null(offsetCohortStartDate)) {
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "CohortStartDayPersistence.sql",
      packageName = utils::packageName(),
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      cdm_database_schema = cdmDatabaseSchema,
      source_cohort_database_schema = sourceCohortDatabaseSchema,
      source_cohort_table = sourceCohortTable,
      target_cohort_database_schema = targetCohortDatabaseSchema,
      target_cohort_table = targetCohortTable,
      old_cohort_id = oldCohortId,
      new_cohort_id = newCohortId,
      offset_cohort_start_date = offsetCohortStartDate
    )
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql,
      profile = FALSE,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  
  if (!is.null(offsetCohortEndDate)) {
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "CohortEndDayPersistence.sql",
      packageName = utils::packageName(),
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      cdm_database_schema = cdmDatabaseSchema,
      source_cohort_database_schema = sourceCohortDatabaseSchema,
      source_cohort_table = sourceCohortTable,
      target_cohort_database_schema = targetCohortDatabaseSchema,
      target_cohort_table = targetCohortTable,
      old_cohort_id = oldCohortId,
      new_cohort_id = newCohortId,
      offset_cohort_end_date = offsetCohortEndDate
    )
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql,
      profile = FALSE,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
}
