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


#' Nearest interval cohort(s)
#'
#' @description
#' Find the nearest interval cohort among a set of interval Cohort
#' for the first occurrence of a target cohort.
#'
#' `r lifecycle::badge("experimental")`
#'
#' @template ConnectionDetails
#'
#' @template Connection
#'
#' @template sourceCohortTable
#'
#' @template sourceCohortDatabaseSchema
#'
#' @template targetCohortTable
#'
#' @template targetCohortDatabaseSchema
#'
#' @param targetCohortId The cohort id to apply the interval logic on
#'
#' @param intervalCohortIds The cohort ids of the cohort that the target cohort has to be temporally proximal/nearest to
#'
#' @template PurgeConflicts
#'
#' @template NewCohortId
#'
#' @template TempEmulationSchema
#'
#' @return
#' NULL
#'
#' @examples
#' \dontrun{
#' nearestInterval(
#'   connectionDetails = Eunomia::getEunomiaConnectionDetails(),
#'   sourceCohortDatabaseSchema = "main",
#'   sourceCohortTable = "cohort",
#'   targetCohortId = 1,
#'   newCohortId = 9,
#'   intervalCohortIds = c(2, 3, 9),
#'   purgeConflicts = TRUE
#' )
#' }
#' @export
nearestInterval <- function(connectionDetails = NULL,
                            connection = NULL,
                            sourceCohortDatabaseSchema = NULL,
                            sourceCohortTable,
                            targetCohortDatabaseSchema = NULL,
                            targetCohortTable,
                            targetCohortId,
                            intervalCohortIds,
                            newCohortId,
                            purgeConflicts = FALSE,
                            tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertIntegerish(
    x = intervalCohortIds,
    min.len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = targetCohortId,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = newCohortId,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = sourceCohortDatabaseSchema,
    min.chars = 1,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = sourceCohortTable,
    min.chars = 1,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = targetCohortDatabaseSchema,
    min.chars = 1,
    len = 1,
    null.ok = TRUE,
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
  checkmate::reportAssertions(collection = errorMessages)
  
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
                y = cohortIdsInCohortTable |> unique())
    if (length(conflicitingCohortIdsInTargetCohortTable) > 0) {
      stop("Target cohort id already in use in target cohort table")
    }
  }
  
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "NearestIntervalCohorts.sql",
    packageName = utils::packageName(),
    dbms = connection@dbms,
    target_cohort_id = targetCohortId,
    interval_cohort_ids = intervalCohortIds,
    tempEmulationSchema = tempEmulationSchema,
    new_cohort_id = newCohortId,
    source_cohort_database_schema = sourceCohortDatabaseSchema,
    source_cohort_table = sourceCohortTable,
    target_cohort_database_schema = targetCohortDatabaseSchema,
    target_cohort_table = targetCohortTable
  )
  
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = TRUE,
    profile = FALSE
  )
}
