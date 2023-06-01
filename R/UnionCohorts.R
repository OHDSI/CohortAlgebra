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

#' Union cohort(s)
#'
#' @description
#' Given a specified array of cohortIds in a cohort table, perform
#' cohort union operator to create new cohorts.
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
#' @template OldToNewCohortId
#'
#' @template PurgeConflicts
#'
#' @template IsTempTable
#'
#' @template TempEmulationSchema
#'
#' @return
#' NULL
#'
#' @examples
#' \dontrun{
#' unionCohorts(
#'   connectionDetails = Eunomia::getEunomiaConnectionDetails(),
#'   sourceDatabaseSchema = "main",
#'   sourceCohortTable = "cohort",
#'   oldToNewCohortId = dplyr::tibble(oldCohortId = c(1, 2), newCohortId = 4),
#'   purgeConflicts = TRUE
#' )
#' }
#'
#' @export
unionCohorts <- function(connectionDetails = NULL,
                         connection = NULL,
                         sourceCohortDatabaseSchema = NULL,
                         sourceCohortTable,
                         targetCohortDatabaseSchema = NULL,
                         targetCohortTable,
                         oldToNewCohortId,
                         isTempTable = FALSE,
                         tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                         purgeConflicts = FALSE) {
  if (isTempTable) {
    if (!all(
      is.null(targetCohortDatabaseSchema),
      tableNameIsCompatibleWithTempTableName(tableName = targetCohortTable),
      !is.null(connection)
    )) {
      stop("Cannot output temp table - check input specifications")
    }
  }
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  newCohortIds <- oldToNewCohortId$newCohortId %>% unique()
  
  tempTables <- c()
  
  for (i in (1:length(newCohortIds))) {
    tempTableName <- paste0("#", generateRandomString())
    tempTables <- c(tempTables, tempTableName)
    
    eraFyCohorts(
      connection = connection,
      sourceCohortDatabaseSchema = sourceCohortDatabaseSchema,
      sourceCohortTable = sourceCohortTable,
      targetCohortDatabaseSchema = NULL,
      targetCohortTable = tempTableName,
      oldCohortIds = oldToNewCohortId %>%
        dplyr::filter(.data$newCohortId == newCohortIds[[i]]) %>%
        dplyr::pull("oldCohortId") %>%
        unique(),
      newCohortId = newCohortIds[[i]],
      eraconstructorpad = 0,
      cdmDatabaseSchema = NULL,
      isTempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      purgeConflicts = FALSE
    )
  }
  
  sqlNest <- c()
  for (i in (1:length(tempTables))) {
    sqlNest[[i]] <- paste0(
      "SELECT cohort_definition_id,
                                    subject_id,
                                    cohort_start_date,
                                    cohort_end_date
                              FROM ",
      
      tempTables[[i]],
      " "
    )
  }
  
  if (isTempTable) {
    sql <- paste0(
      "SELECT cohort_definition_id,
                        subject_id,
                        cohort_start_date,
                        cohort_end_date
                INTO @temp_table_name
                FROM (",
      paste0(paste0(sqlNest, collapse = " union all "), ";"),
      ") f;"
    )
    
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      progressBar = FALSE,
      reportOverallTime = FALSE,
      tempEmulationSchema = tempEmulationSchema,
      temp_table_name = targetCohortTable
    )
  } else {
    sql <- paste0(
      "  INSERT INTO
              {@target_cohort_database_schema != ''} ? {
                @target_cohort_database_schema.@target_cohort_table
              } : {@target_cohort_table}
              SELECT cohort_definition_id,
                    subject_id,
                    cohort_start_date,
                    cohort_end_date
              FROM (",
      paste0(paste0(sqlNest, collapse = " union all ")),
      ") f;"
    )
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      progressBar = FALSE,
      reportOverallTime = FALSE,
      tempEmulationSchema = tempEmulationSchema,
      target_cohort_database_schema = targetCohortDatabaseSchema,
      target_cohort_table = targetCohortTable
    )
  }
}
