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

#' Censor cohort date
#'
#' @description
#' Censor cohort date by right, left, both censoring. All cohorts will be truncated to the given date.
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
#' @param cohortStartDateLeftCensor   the minimum date for the cohort start.
#'
#' @param cohortEndDateRightCensor     the maximum date for the cohort end.
#'
#' @return
#' NULL
#'
#'
#' @examples
#' \dontrun{
#' CohortAlgebra::censorCohortDates(
#'   connection = connection,
#'   sourceCohortTable = "cohort",
#'   targetCohortTable = "cohort",
#'   oldCohortId = 3,
#'   newCohortId = 2,
#'   cohortStartDateLeftCensor = as.Date("2010-01-09"),
#'   purgeConflicts = TRUE
#' )
#' }
#'
#' @export
censorCohortDates <- function(connectionDetails = NULL,
                              connection = NULL,
                              sourceCohortDatabaseSchema = NULL,
                              sourceCohortTable,
                              targetCohortDatabaseSchema = NULL,
                              targetCohortTable,
                              oldCohortId,
                              newCohortId,
                              cohortStartDateLeftCensor = NULL,
                              cohortEndDateRightCensor = NULL,
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
  checkmate::assertDate(
    x = cohortStartDateLeftCensor,
    any.missing = FALSE,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertDate(
    x = cohortEndDateRightCensor,
    any.missing = FALSE,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )

  checkmate::reportAssertions(collection = errorMessages)

  if (all(
    is.null(cohortStartDateLeftCensor),
    is.null(cohortEndDateRightCensor)
  )) {
    stop("Censort information not provided.")
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
      intersect(
        x = newCohortId,
        y = cohortIdsInCohortTable %>% unique()
      )

    if (length(conflicitingCohortIdsInTargetCohortTable) > 0) {
      stop("Target cohort id already in use in target cohort table")
    }
  }

  cohort_start_left_censort_start_year <- NULL
  cohort_start_left_censort_start_month <- NULL
  cohort_start_left_censort_start_day <- NULL

  cohort_end_right_censort_start_year <- NULL
  cohort_end_right_censort_start_month <- NULL
  cohort_end_right_censort_start_day <- NULL

  if (!is.null(cohortStartDateLeftCensor)) {
    cohort_start_left_censort_start_year <- clock::get_year(cohortStartDateLeftCensor)
    cohort_start_left_censort_start_month <- clock::get_month(cohortStartDateLeftCensor)
    cohort_start_left_censort_start_day <- clock::get_day(cohortStartDateLeftCensor)
  }

  if (!is.null(cohortEndDateRightCensor)) {
    cohort_end_right_censort_start_year <- clock::get_year(cohortEndDateRightCensor)
    cohort_end_right_censort_start_month <- clock::get_month(cohortEndDateRightCensor)
    cohort_end_right_censort_start_day <- clock::get_day(cohortEndDateRightCensor)
  }

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "CohortCensorDates.sql",
    packageName = utils::packageName(),
    dbms = connection@dbms,
    old_cohort_id = oldCohortId,
    new_cohort_id = newCohortId,
    source_cohort_database_schema = sourceCohortDatabaseSchema,
    source_cohort_table = sourceCohortTable,
    target_cohort_database_schema = targetCohortDatabaseSchema,
    target_cohort_table = targetCohortTable,
    cohort_start_left_censort_start_year = cohort_start_left_censort_start_year,
    cohort_start_left_censort_start_month = cohort_start_left_censort_start_month,
    cohort_start_left_censort_start_day = cohort_start_left_censort_start_day,
    cohort_end_right_censort_start_year = cohort_end_right_censort_start_year,
    cohort_end_right_censort_start_month = cohort_end_right_censort_start_month,
    cohort_end_right_censort_start_day = cohort_end_right_censort_start_day
  )

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
}
