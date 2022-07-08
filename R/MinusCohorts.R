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

#' Minus cohort(s)
#'
#' @description
#' Given two cohorts, substract (minus) the dates from the first cohort, the
#' dates the subject also had on the second cohort.
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @param firstCohortId The cohort id of the cohort from which to substract.
#'
#' @param secondCohortId The cohort id of the cohort that is used to substract.
#'
#' @template NewCohortId
#'
#' @template PurgeConflicts
#'
#' @template TempEmulationSchema
#'
#' @return
#' NULL
#'
#' @export
substractCohorts <- function(connectionDetails = NULL,
                             connection = NULL,
                             cohortDatabaseSchema,
                             cohortTable = "cohort",
                             firstCohortId,
                             secondCohortId,
                             newCohortId,
                             purgeConflicts = FALSE,
                             tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertIntegerish(
    x = firstCohortId,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = secondCohortId,
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
  checkmate::assertLogical(
    x = purgeConflicts,
    any.missing = FALSE,
    min.len = 1,
    add = errorMessages
  )
  checkmate::reportAssertions(collection = errorMessages)

  if (firstCohortId == secondCohortId) {
    warning(
      "During minus operation, both first and second cohorts have the same cohort id. The result will be a NULL cohort."
    )
  }

  start <- Sys.time()

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  cohortIdsInCohortTable <-
    getCohortIdsInCohortTable(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      tempEmulationSchema = tempEmulationSchema
    )

  conflicitingCohortIdsInTargetCohortTable <-
    intersect(
      x = newCohortId %>% unique(),
      y = cohortIdsInCohortTable %>% unique()
    )

  performPurgeConflicts <- FALSE
  if (length(conflicitingCohortIdsInTargetCohortTable) > 0) {
    if (purgeConflicts) {
      performPurgeConflicts <- TRUE
    } else {
      stop(
        paste0(
          "The following cohortIds already exist in the target cohort table, causing conflicts :",
          paste0(conflicitingCohortIdsInTargetCohortTable, collapse = ",")
        )
      )
    }
  }

  tempTableName <- generateRandomString()
  tempTable1 <- paste0("#", tempTableName, "1")
  tempTable2 <- paste0("#", tempTableName, "2")

  copyCohortsToTempTable(
    connection = connection,
    oldToNewCohortId = dplyr::tibble(oldCohortId = c(firstCohortId, secondCohortId)) %>%
      dplyr::mutate(newCohortId = .data$oldCohortId) %>%
      dplyr::distinct(),
    sourceCohortDatabaseSchema = cohortDatabaseSchema,
    sourceCohortTable = cohortTable,
    targetCohortTable = tempTable1
  )

  intersectCohorts(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = tempTable1,
    cohortIds = cohortIds,
    newCohortId = -999,
    purgeConflicts = FALSE,
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")
  )

  minusSql <- "DROP TABLE IF EXISTS @temp_table_2;

                  WITH cohort_dates
                    AS (
                    	SELECT DISTINCT subject_id,
                    		cohort_date
                    	FROM (
                    		SELECT subject_id,
                    			cohort_start_date cohort_date
                    		FROM @temp_table_1
                    		WHERE cohort_definition_id IN (@first_cohort_id, -999)

                    		UNION

                    		SELECT subject_id,
                    			cohort_end_date cohort_date
                    		FROM @temp_table_1
                    		WHERE cohort_definition_id IN (@first_cohort_id, -999)
                    		) all_dates
                    	),
                    time_periods
                    AS (
                    	SELECT subject_id,
                    		cohort_date,
                    		LEAD(cohort_date, 1) OVER (
                    			PARTITION BY subject_id ORDER BY cohort_date ASC
                    			) next_cohort_date
                    	FROM cohort_dates
                    	GROUP BY subject_id,
                    		cohort_date
                    	)
                    SELECT @new_cohort_id cohort_definition_id,
                      subject_id,
                    	cohort_date cohort_start_date,
                    	next_cohort_date cohort_end_date
                    INSERT INTO @temp_table_2
                    FROM time_periods
                    GROUP BY subject_id,
                    	cohort_date,
                    	next_cohort_date
                    HAVING COUNT(*) = 1;"

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = minusSql,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    new_cohort_id = newCohortId,
    temp_table_1 = tempTable1,
    temp_table_2 = tempTable2,
    tempEmulationSchema = tempEmulationSchema
  )

  if (performPurgeConflicts) {
    ParallelLogger::logInfo(
      paste0(
        "The following conflicting cohortIds will be deleted from your cohort table \n",
        " as part resolving conflicts: ",
        paste0(conflicitingCohortIdsInTargetCohortTable, collapse = ",")
      )
    )
    deleteCohortRecords(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = conflicitingCohortIdsInTargetCohortTable
    )
  }
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " INSERT INTO {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table}
            SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
            FROM @temp_table_2;",
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cohort_database_schema = cohortDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohort_table = cohortTable,
    temp_table_2 = tempTable2
  )

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " DROP TABLE IF EXISTS @temp_table_1;
            DROP TABLE IF EXISTS @temp_table_2;",
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    temp_table_1 = tempTable1,
    temp_table_2 = tempTable2
  )
}
