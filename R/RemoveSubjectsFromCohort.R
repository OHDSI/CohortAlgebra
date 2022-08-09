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

#' Remove subjects from cohort(s).
#'
#' @description
#' Remove subjects from a given array of cohort(s) who are present in any of
#' another array of cohort(s).
#'
#' @template ConnectionDetails
#'
#' @template Connection
#'
#' @template OldToNewCohortId
#'
#' @param cohortsWithSubjectsToRemove An array of one or more cohorts with subjects to remove from given cohorts.
#'
#' @template CohortDatabaseSchema
#'
#' @template PurgeConflicts
#'
#' @template TempEmulationSchema
#'
#' @return
#' NULL
#'
#'
#' @examples
#' removeSubjectsFromCohorts(
#'   connection = connection,
#'   cohortDatabaseSchema = cohortDatabaseSchema,
#'   oldToNewCohortId = dplyr::tibble(oldCohortId = 1, newCohortId = 6),
#'   cohortsWithSubjectsToRemove = c(3),
#'   purgeConflicts = FALSE,
#'   cohortTable = tableName
#' )
#'
#' @export
removeSubjectsFromCohorts <- function(connectionDetails = NULL,
                                      connection = NULL,
                                      cohortDatabaseSchema,
                                      oldToNewCohortId,
                                      cohortsWithSubjectsToRemove,
                                      cohortTable = "cohort",
                                      purgeConflicts = FALSE,
                                      tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertDataFrame(
    x = oldToNewCohortId,
    min.rows = 1,
    add = errorMessages
  )
  checkmate::assertNames(
    x = colnames(oldToNewCohortId),
    must.include = c("oldCohortId", "newCohortId"),
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = oldToNewCohortId$oldCohortId,
    min.len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = oldToNewCohortId$newCohortId,
    min.len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = cohortsWithSubjectsToRemove,
    min.len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = cohortDatabaseSchema,
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
  checkmate::reportAssertions(collection = errorMessages)

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  if (nrow(oldToNewCohortId) > 0) {
    if (!purgeConflicts) {
      cohortIdsInCohortTable <-
        getCohortIdsInCohortTable(
          connection = connection,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortTable = cohortTable,
          tempEmulationSchema = tempEmulationSchema
        )
      conflicitingCohortIdsInTargetCohortTable <-
        intersect(
          x = oldToNewCohortId$newCohortId %>% unique(),
          y = cohortIdsInCohortTable %>% unique()
        )

      if (length(conflicitingCohortIdsInTargetCohortTable) > 0) {
        stop(
          paste0(
            "The following cohortIds already exist in the target cohort table, causing conflicts :",
            paste0(newCohortId,
              collapse = ","
            )
          )
        )
      }
    }
  }

  tempTableName <- generateRandomString()
  tempTable1 <- paste0("#", tempTableName, "1")

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " SELECT c.*
            INTO @temp_table_1
            FROM {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table} c
            LEFT JOIN
                (
                    SELECT DISTINCT SUBJECT_ID
                    FROM {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table} s
                    WHERE cohort_definition_id IN (@remove_cohort_ids)
                ) r
            ON c.subject_id = r.subject_id
            WHERE c.cohort_definition_id IN (@given_cohort_ids)
                  AND r.subject_id IS NULL;",
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cohort_database_schema = cohortDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohort_table = cohortTable,
    given_cohort_ids = oldToNewCohortId$oldCohortId %>% unique(),
    remove_cohort_ids = cohortsWithSubjectsToRemove,
    temp_table_1 = tempTable1
  )

  eraFyCohorts(
    connection = connection,
    cohortDatabaseSchema = NULL,
    cohortTable = tempTable1,
    oldToNewCohortId = oldToNewCohortId,
    eraconstructorpad = 0,
    cdmDatabaseSchema = NULL,
    tempEmulationSchema = tempEmulationSchema,
    purgeConflicts = TRUE
  )

  if (purgeConflicts) {
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = " DELETE FROM {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table}
            WHERE cohort_definition_id IN (@given_cohort_ids);",
      profile = FALSE,
      progressBar = FALSE,
      reportOverallTime = FALSE,
      cohort_database_schema = cohortDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohort_table = cohortTable,
      given_cohort_ids = oldToNewCohortId$oldCohortId %>% unique()
    )
  }
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " INSERT INTO {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table}
            SELECT *
            FROM @temp_table_1;

            DROP TABLE IF EXISTS @temp_table_1;",
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cohort_database_schema = cohortDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohort_table = cohortTable,
    temp_table_1 = tempTable1
  )
}
