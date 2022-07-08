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

#' Get cohort ids in table
#'
#' @description
#' Get cohort ids in table. This function is not exported.
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
copyCohortsToTempTable <- function(connectionDetails = NULL,
                                   connection = NULL,
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

  sqlCopyCohort <- "
                  DROP TABLE IF EXISTS @target_cohort_table;
                  SELECT target.new_cohort_id cohort_definition_id,
                          source.subject_id,
                          source.cohort_start_date,
                          source.cohort_end_date
                  INTO @target_cohort_table
                  FROM {@source_database_schema != ''} ? {@source_database_schema.@source_cohort_table} : {@source_cohort_table} source
                  INNER JOIN #old_to_new_cohort_id target
                  ON source.cohort_definition_id = target.old_cohort_id
                  ;"

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlCopyCohort,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    source_database_schema = sourceCohortDatabaseSchema,
    source_cohort_table = sourceCohortTable,
    target_cohort_table = targetCohortTable,
    tempEmulationSchema = tempEmulationSchema
  )
}
