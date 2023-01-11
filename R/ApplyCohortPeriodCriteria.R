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

#' Apply cohort period criteria.
#'
#' @description
#' Apply cohort period criteria, allows to limit cohort records by any combination of pre, during or post cohort periods. Pre and post
#' are continous observation period#'
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
#' @param filterByMinimumCohortPeriod Do you want to filter cohort records by minimum cohort period, i.e. cohort period is calculated
#'                                    as DATEDIFF(cohort_start_date, cohort_start_date). if cohort_start_date = cohort_end_date then days = 0
#'
#' @param filterByMinimumPriorObservationPeriod  Do you want to filter cohort records by minimum Prior continuous Observation period
#'
#' @param filterByMinimumPostObservationPeriod  Do you want to filter cohort records by minimum Post continous Observation period
#'
#'
#' @return
#' NULL
#'
#'
#' @examples
#' \dontrun{
#' CohortAlgebra::applyCohortPeriodCriteria(
#'   connection = connection,
#'   sourceCohortTable = "cohort",
#'   targetCohortTable = "cohort",
#'   oldCohortId = 3,
#'   newCohortId = 2,
#'   filterByMinimumCohortPeriod = 34,
#'   purgeConflicts = TRUE
#' )
#' }
#'
#' @export
applyCohortPeriodCriteria <- function(connectionDetails = NULL,
                                      connection = NULL,
                                      sourceCohortDatabaseSchema = NULL,
                                      sourceCohortTable,
                                      targetCohortDatabaseSchema = NULL,
                                      targetCohortTable,
                                      cdmDatabaseSchema,
                                      oldCohortId,
                                      newCohortId,
                                      filterByMinimumCohortPeriod = NULL,
                                      filterByMinimumPriorObservationPeriod = NULL,
                                      filterByMinimumPostObservationPeriod = NULL,
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
    !is.null(filterByMinimumPriorObservationPeriod),
    !is.null(filterByMinimumPriorObservationPeriod),
    !is.null(filterByMinimumPostObservationPeriod)
  ) > 1) {
    stop("Multiple period criteria specified.")
  }
  
  if (sum(
    tillEndOfObservationPeriod,
    !is.null(filterByMinimumPriorObservationPeriod),
    !is.null(filterByMinimumPostObservationPeriod)
  ) == 0) {
    stop("No period criteria specified.")
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
