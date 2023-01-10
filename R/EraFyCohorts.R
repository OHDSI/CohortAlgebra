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

#' Era-fy cohort(s)
#'
#' @description
#' Given a table with cohort_definition_id, subject_id, cohort_start_date,
#' cohort_end_date execute era logic. This will delete and replace the
#' original rows with the cohort_definition_id(s). edit privileges
#' to the cohort table is required.
#'
#' `r lifecycle::badge("stable")`
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
#' @template OldCohortIds
#'
#' @template NewCohortId
#'
#' @template PurgeConflicts
#'
#' @template TempEmulationSchema
#'
#' @param eraconstructorpad   Optional value to pad cohort era construction logic. Default = 0. i.e. no padding.
#'
#' @template CdmDatabaseSchema
#'
#' @return
#' NULL
#'
eraFyCohorts <- function(connectionDetails = NULL,
                         connection = NULL,
                         sourceCohortDatabaseSchema = NULL,
                         sourceCohortTable = "cohort",
                         targetCohortDatabaseSchema = NULL,
                         targetCohortTable,
                         oldCohortIds,
                         newCohortId,
                         eraconstructorpad = 0,
                         cdmDatabaseSchema = NULL,
                         purgeConflicts = FALSE,
                         tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertIntegerish(
    x = newCohortId,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = oldCohortIds,
    min.len = 1,
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
  checkmate::assertCharacter(
    x = cdmDatabaseSchema,
    min.chars = 1,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertLogical(
    x = purgeConflicts,
    any.missing = FALSE,
    min.len = 1,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = eraconstructorpad,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::reportAssertions(collection = errorMessages)

  if (is.null(cdmDatabaseSchema)) {
    if (eraconstructorpad > 0) {
      stop(
        "eraconstructorpad > 0 when cdmDatabaseSchema is NULL is not allowed. cdm table observation_period needed."
      )
    }
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

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "EraFyCohorts.sql",
    packageName = utils::packageName(),
    dbms = connection@dbms,
    tempEmulationSchema = tempEmulationSchema,
    era_constructor_pad = eraconstructorpad,
    cdm_database_schema = cdmDatabaseSchema,
    source_cohort_database_schema = sourceCohortDatabaseSchema,
    source_cohort_table = sourceCohortTable,
    target_cohort_database_schema = targetCohortDatabaseSchema,
    target_cohort_table = targetCohortTable,
    old_cohort_ids = oldCohortIds,
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
