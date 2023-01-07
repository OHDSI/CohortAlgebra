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



#' Base cohort, cohort definition set.
#'
#' @description
#' Base cohort, cohort definition set.
#'
#' @export
#'
getBaseCohortDefinitionSet <- function() {
  dplyr::tibble(
    cohortId = c(0, -1, -2, -3),
    cohortName = c(
      "Observation Period",
      "Visits all",
      "Visits Inpatient",
      "Visits Emergency Room"
    )
  )
}



#' Generate Base Cohorts
#'
#' @description
#' Generates a set of cohorts that are commonly used in cohort algebra functions. Four cohorts will be generated
#' with the cohort_definition_id of 0, -1, -2, -3 for Observation Period, Visits all, Visits Inpatient, Visits Emergency Room.
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
#' @template CdmDatabaseSchema
#'
#' @template TempEmulationSchema
#'
#' @param incremental                 Create only cohorts that haven't been created before?
#'
#' @param incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are
#'                                    kept of which definition has been executed.
#'
#' @template PurgeConflicts
#'
#' @return
#' NULL
#'
#' @examples
#' \dontrun{
#' CohortAlgebra::generateBaseCohorts(
#'   connection = connection,
#'   cohortDatabaseSchema = cohortDatabaseSchema,
#'   cdmDatabaseSchema = cdmDatabaseSchema,
#'   cohortTable = tableName,
#'   incremental = TRUE,
#'   incrementalFolder = incrementalFolder,
#'   purgeConflicts = TRUE
#' )
#' }
#'
#' @export
#'
generateBaseCohorts <- function(connectionDetails = NULL,
                                connection = NULL,
                                cohortDatabaseSchema,
                                cdmDatabaseSchema,
                                cohortTable = "CohortsBase",
                                incremental,
                                incrementalFolder = NULL,
                                purgeConflicts,
                                tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  errorMessages <- checkmate::makeAssertCollection()
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
  checkmate::assertLogical(
    x = incremental,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::reportAssertions(collection = errorMessages)

  cohortDefinitionSet <- dplyr::tibble(
    cohortId = 0,
    cohortName = "Base Cohort"
  )

  if (incremental) {
    if (is.null(incrementalFolder)) {
      stop("Must specify incrementalFolder when incremental = TRUE")
    }
    if (!file.exists(incrementalFolder)) {
      dir.create(incrementalFolder, recursive = TRUE)
    }
  }

  taskRequired <- TRUE
  if (incremental) {
    cohortDefinitionSet$checksum <-
      CohortGenerator::computeChecksum(
        paste0(
          cohortDefinitionSet$cohortName,
          cohortDefinitionSet$cohortId,
          cohortTable
        )
      )
    recordKeepingFile <-
      file.path(incrementalFolder, "GeneratedCohorts.csv")

    if (file.exists(recordKeepingFile)) {
      taskRequired <-
        CohortGenerator::isTaskRequired(
          cohortId = 0,
          checksum = cohortDefinitionSet$checksum,
          recordKeepingFile = recordKeepingFile
        )
    }
  }

  if (!taskRequired) {
    ParallelLogger::logTrace(
      "Skipping Base Cohort generation as it has already been generated according to incremental log"
    )
    return(NULL)
  }

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  baseCohortTableNames <-
    CohortGenerator::getCohortTableNames(cohortTable = cohortTable)

  CohortGenerator::createCohortTables(
    connection = connection,
    cohortTableNames = baseCohortTableNames,
    cohortDatabaseSchema = cohortDatabaseSchema,
    incremental = incremental
  )
  CohortGenerator::dropCohortStatsTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = baseCohortTableNames
  )

  cohortIdsInCohortTable <-
    getCohortIdsInCohortTable(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      tempEmulationSchema = tempEmulationSchema
    )

  conflicitingCohortIdsInTargetCohortTable <-
    intersect(
      x = cohortDefinitionSet$cohortId %>% unique(),
      y = cohortIdsInCohortTable %>% unique()
    )

  if (length(conflicitingCohortIdsInTargetCohortTable) > 0) {
    if (!purgeConflicts) {
      stop(
        paste0(
          "The following cohortIds already exist in the target cohort table, causing conflicts :",
          paste0(conflicitingCohortIdsInTargetCohortTable, collapse = ",")
        )
      )
    }
  }
  ParallelLogger::logTrace(" Generating base cohorts.")
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "BaseCohorts.sql",
    packageName = utils::packageName(),
    dbms = connection@dbms,
    tempEmulationSchema = tempEmulationSchema,
    cohort_database_schema = cohortDatabaseSchema,
    cdm_database_schema = cdmDatabaseSchema,
    cohort_table = baseCohortTableNames$cohortTable
  )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    profile = FALSE,
    progressBar = TRUE,
    reportOverallTime = TRUE
  )

  ParallelLogger::logTrace(" Era fy base cohorts.")

  eraFyCohorts(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = baseCohortTableNames$cohortTable,
    oldToNewCohortId = dplyr::tibble(
      oldCohortId = cohortDefinitionSet$cohortId,
      newCohortId = cohortDefinitionSet$cohortId
    ),
    eraconstructorpad = 0,
    cdmDatabaseSchema = cdmDatabaseSchema,
    purgeConflicts = TRUE
  )

  CohortGenerator::recordTasksDone(
    cohortId = 0,
    checksum = cohortDefinitionSet %>% dplyr::select("checksum") %>% dplyr::pull("checksum") %>% as.character(),
    recordKeepingFile = recordKeepingFile,
    incremental = incremental
  )
}
