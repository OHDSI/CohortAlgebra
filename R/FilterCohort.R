# Copyright 2024 Observational Health Data Sciences and Informatics
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

#' Filter existing cohort to create new cohort
#'
#' This function filters an existing cohort to create a new cohort.
#'
#' @param connectionDetails (optional) Details for establishing a database connection if `connection` is NULL.
#' @param connection (optional) An existing database connection object.
#' @param tempEmulationSchema (optional) The schema used for emulating temporary tables;
#'        defaults to the value set in global options with `getOption("sqlRenderTempEmulationSchema")`.
#' @param cohortDatabaseSchema The name of the schema where the cohort table exists.
#' @param cohortTableName The name of the cohort table.
#' @param minCohortDays (default = 0)
#' @param maxCohortDays (default = 9999)
#' @param oldCohortId cohort id of the cohort to filter.
#' @param newCohortId cohort id of the filtered cohort that will be created.
#' @export
filterCohort <- function(connectionDetails = NULL,
                         connection = NULL,
                         tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                         cohortDatabaseSchema = NULL,
                         cohortTableName,
                         oldCohortId,
                         newCohortId,
                         minCohortDays = 0,
                         maxCohortDays = 9999) {
  useCohortDatabaseSchema <- FALSE
  if (!is.null(cohortDatabaseSchema)) {
    useCohortDatabaseSchema <- TRUE
  }
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  if (oldCohortId == newCohortId) {
    stop("oldCohortId and newCohortId are the same.")
  }
  
  sql <-
    "DELETE FROM {@use_cohort_database_schema} ? {@cohort_database_schema.@cohort_table_name} : {@cohort_table_name}
      WHERE cohort_definition_id = @new_cohort_id;

    INSERT INTO {@use_cohort_database_schema} ? {@cohort_database_schema.@cohort_table_name} : {@cohort_table_name}
    SELECT @new_cohort_id cohort_definition_id,
          subject_id,
          cohort_start_date,
          cohort_end_date
    FROM {@use_cohort_database_schema} ? {@cohort_database_schema.@cohort_table_name} : {@cohort_table_name}
    WHERE cohort_definition_id  = @old_cohort_id
      AND DATEDIFF(DAY, cohort_start_date, cohort_end_date) >= @min_cohort_days
      AND DATEDIFF(DAY, cohort_start_date, cohort_end_date) <= @max_cohort_days;"
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    tempEmulationSchema = tempEmulationSchema,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table_name = cohortTableName,
    use_cohort_database_schema = useCohortDatabaseSchema,
    old_cohort_id = oldCohortId,
    new_cohort_id = newCohortId,
    min_cohort_days = minCohortDays,
    max_cohort_days = maxCohortDays
  )
}