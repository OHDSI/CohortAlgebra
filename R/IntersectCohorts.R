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


#' Intersect cohort(s)
#'
#' @description
#' Find the common cohort period for persons present in all the cohorts. Note: if
#' subject is not found in any of the cohorts, then they will not
#' be in the final cohort.
#'
#' @template ConnectionDetails
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @template CohortDatabaseSchema
#'
#' @template CohortIds
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
#' intersectCohorts(
#'  connectionDetails = Eunomia::getEunomiaConnectionDetails(),
#'  cohortDatabaseSchema = "study1234",
#'  cohortTable = "cohort",
#'  cohortIds = c(1, 2, 3),
#'  newCohortId = 9,
#'  purgeConflicts = TRUE
#' )
#' @export
intersectCohorts <- function(connectionDetails = NULL,
                             connection = NULL,
                             cohortDatabaseSchema = NULL,
                             cohortTable = "cohort",
                             cohortIds,
                             newCohortId,
                             purgeConflicts = FALSE,
                             tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertIntegerish(
    x = cohortIds,
    min.len = 1,
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
    oldToNewCohortId = dplyr::tibble(oldCohortId = cohortIds) %>%
      dplyr::mutate(newCohortId = .data$oldCohortId) %>%
      dplyr::distinct(),
    sourceCohortDatabaseSchema = cohortDatabaseSchema,
    sourceCohortTable = cohortTable,
    targetCohortTable = tempTable1
  )

  eraFyCohorts(
    connection = connection,
    oldToNewCohortId = dplyr::tibble(oldCohortId = cohortIds) %>%
      dplyr::mutate(newCohortId = .data$oldCohortId) %>%
      dplyr::distinct(),
    cohortTable = tempTable1,
    purgeConflicts = TRUE
  )

  numberOfCohorts <- length(cohortIds %>% unique())

  intersectSql <- "DROP TABLE IF EXISTS @temp_table_2;

                  WITH cohort_dates
                  AS (
                  	SELECT subject_id,
                  		cohort_date,
                  		-- LEAD will ignore values that are same (e.g. if cohort_start_date = cohort_end_date)
                  		ROW_NUMBER() OVER(PARTITION BY subject_id
                  		                  ORDER BY cohort_date ASC) cohort_date_seq
                  	FROM (
                  		SELECT subject_id,
                  			cohort_start_date cohort_date
                  		FROM @temp_table_1

                  		UNION ALL -- we need all dates, even if duplicates

                  		SELECT subject_id,
                  			cohort_end_date cohort_date
                  		FROM @temp_table_1
                  		) all_dates
                  	),
                  candidate_periods
                  AS (
                  	SELECT
                  		subject_id,
                  		cohort_date candidate_start_date,
                  		cohort_date_seq,
                  		LEAD(cohort_date, 1) OVER (
                  			PARTITION BY subject_id ORDER BY cohort_date, cohort_date_seq ASC
                  			) candidate_end_date
                  	FROM cohort_dates
                  	GROUP BY subject_id,
                  		cohort_date,
                  		cohort_date_seq
                  	),
                  candidate_cohort_date
                  AS (
                  	SELECT DISTINCT cohort.*,
                  		candidate_start_date,
                  		candidate_end_date
                  	FROM @temp_table_1 cohort
                  	INNER JOIN candidate_periods candidate ON cohort.subject_id = candidate.subject_id
                  		AND candidate_start_date >= cohort_start_date
                  		AND candidate_end_date <= cohort_end_date
                  	)
                  SELECT @new_cohort_id cohort_definition_id,
                  	subject_id,
                  	candidate_start_date cohort_start_date,
                  	candidate_end_date cohort_end_date
                  INTO @temp_table_2
                  FROM candidate_cohort_date
                  GROUP BY subject_id,
                  	candidate_start_date,
                  	candidate_end_date
                  HAVING COUNT(*) = @number_of_cohorts;"

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = intersectSql,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    number_of_cohorts = numberOfCohorts,
    new_cohort_id = newCohortId,
    tempEmulationSchema = tempEmulationSchema,
    temp_table_1 = tempTable1,
    temp_table_2 = tempTable2
  )

  suppressMessages(
    eraFyCohorts(
      connection = connection,
      oldToNewCohortId = dplyr::tibble(oldCohortId = newCohortId) %>%
        dplyr::mutate(newCohortId = .data$oldCohortId) %>%
        dplyr::distinct(),
      cohortTable = tempTable2,
      purgeConflicts = TRUE
    )
  )

  if (performPurgeConflicts) {
    ParallelLogger::logTrace(
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
