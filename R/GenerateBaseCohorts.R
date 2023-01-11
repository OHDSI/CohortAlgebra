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
  specifications <- dplyr::tibble(
    cohortId = c(-1, -2, -3, -4, -5, -6, -7, -8, -9, -10),
    cohortName = c(
      "01 Observation Period",
      "02 Visits all",
      "03 Visits Inpatient or Inpatient Emergency",
      "04 Visits Emergency Room",
      "05 Visits with 365 days post continuous observation days",
      "06 Visits Inpatient or Inpatient Emergency with 365 days post continuous observation days",
      "07 Visits Emergency Room with 365 days post continuous observation days",
      "08 Visits with 365 days prior continuous observation days",
      "09 Visits Inpatient or Inpatient Emergency with 365 days prior continuous observation days",
      "10 Visits Emergency Room with 365 days prior continuous observation days"
    )
  )

  cohorts <- c()
  for (i in (1:nrow(specifications))) {
    cohorts[[i]] <- specifications[i, ] %>%
      dplyr::mutate(sqlFileName = paste0(paste(
        "BaseCohort", formatC(
          abs(specifications[i, ]$cohortId),
          width =
            2,
          flag = "0"
        ),
        sep = ""
      ), ".sql"))


    sqlFileName <- cohorts[[i]]$sqlFileName
    pathToSql <-
      system.file(file.path("sql", "sql_server", sqlFileName),
        package = utils::packageName()
      )
    cohorts[[i]]$sql <- SqlRender::readSql(sourceFile = pathToSql)
  }

  cohortDefinitionSet <- dplyr::bind_rows(cohorts)
  cohortDefinitionSet$checksum <-
    CohortGenerator::computeChecksum(
      paste0(
        cohortDefinitionSet$cohortName,
        cohortDefinitionSet$cohortId,
        cohortDefinitionSet$sql
      )
    )

  return(cohortDefinitionSet %>%
    dplyr::tibble())
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
#'   incrementalFolder = incrementalFolder
#' )
#' }
#'
#' @export
#'
generateBaseCohorts <- function(connectionDetails = NULL,
                                cohortDatabaseSchema,
                                cdmDatabaseSchema,
                                cohortTable = "cohorts_base",
                                incremental,
                                incrementalFolder = NULL,
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
    x = incremental,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::reportAssertions(collection = errorMessages)

  cohortDefinitionSet <- getBaseCohortDefinitionSet()

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

  baseCohortTableNames <-
    CohortGenerator::getCohortTableNames(cohortTable = cohortTable)

  CohortGenerator::createCohortTables(
    connectionDetails = connectionDetails,
    cohortTableNames = baseCohortTableNames,
    cohortDatabaseSchema = cohortDatabaseSchema,
    incremental = incremental
  )

  CohortGenerator::generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = baseCohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = incremental,
    incrementalFolder = incrementalFolder,
    tempEmulationSchema = tempEmulationSchema
  )

  CohortGenerator::dropCohortStatsTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = baseCohortTableNames
  )

  ParallelLogger::logTrace(" Era fy base cohorts.")
  for (i in (1:nrow(cohortDefinitionSet))) {
    ParallelLogger::logInfo(paste0("  Working on ", cohortDefinitionSet[i, ]$cohortName))
    eraFyCohorts(
      connectionDetails = connectionDetails,
      sourceCohortDatabaseSchema = cohortDatabaseSchema,
      sourceCohortTable = baseCohortTableNames$cohortTable,
      targetCohortDatabaseSchema = cohortDatabaseSchema,
      targetCohortTable = baseCohortTableNames$cohortTable,
      oldCohortIds = cohortDefinitionSet[i, ]$cohortId,
      newCohortId = cohortDefinitionSet[i, ]$cohortId,
      eraconstructorpad = 0,
      cdmDatabaseSchema = cdmDatabaseSchema,
      purgeConflicts = TRUE
    )
  }
}
