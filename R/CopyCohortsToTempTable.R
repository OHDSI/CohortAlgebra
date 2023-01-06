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

#' Get cohort ids in table
#'
#' @description
#' Get cohort ids in table. This function is not exported.
#'
#' `r lifecycle::badge("stable")`
#'
#' @template Connection
#'
#' @template OldToNewCohortId
#'
#' @template TempEmulationSchema
#'
#' @param sourceCohortDatabaseSchema The database schema of the source cohort table.
#'
#' @param sourceCohortTable         The name of the source cohort table.
#'
#' @param targetCohortTable         A temp table to copy the cohorts from the source table.
#'
#' @return
#' NULL
#'
copyCohortsToTempTable <- function(connection = NULL,
                                   oldToNewCohortId,
                                   sourceCohortDatabaseSchema = NULL,
                                   sourceCohortTable,
                                   targetCohortTable = "#cohort_rows",
                                   tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = "#old_to_new_cohort_id",
    createTable = TRUE,
    dropTableIfExists = TRUE,
    tempTable = TRUE,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    bulkLoad = (Sys.getenv("bulkLoad") == TRUE),
    camelCaseToSnakeCase = TRUE,
    data = oldToNewCohortId
  )

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "CopyCohorts.sql",
    packageName = utils::packageName(),
    dbms = connection@dbms,
    source_database_schema = sourceCohortDatabaseSchema,
    source_cohort_table = sourceCohortTable,
    target_cohort_table = targetCohortTable,
    tempEmulationSchema = tempEmulationSchema
  )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
}
