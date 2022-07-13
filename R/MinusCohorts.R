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
#' @template ConnectionDetails
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @template CohortDatabaseSchema
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
minusCohorts <- function(connectionDetails = NULL,
                         connection = NULL,
                         cohortDatabaseSchema = NULL,
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
  tempTable3 <- paste0("#", tempTableName, "3")

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
    cohortTable = tempTable1,
    cohortIds = c(firstCohortId, secondCohortId),
    newCohortId = -999,
    purgeConflicts = FALSE,
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")
  )

  minusSql <- "DROP TABLE IF EXISTS @temp_table_2;

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
                  		WHERE cohort_definition_id IN (@first_cohort_id, -999)

                  		UNION ALL -- we need all dates, even if duplicates

                  		SELECT subject_id,
                  			cohort_end_date cohort_date
                  		FROM @temp_table_1
                  		WHERE cohort_definition_id IN (@first_cohort_id, -999)
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
                  SELECT
                  	subject_id,
                  	candidate_start_date,
                  	candidate_end_date
                  INTO @temp_table_2
                  FROM candidate_cohort_date
                  GROUP BY subject_id,
                  	candidate_start_date,
                  	candidate_end_date
                  HAVING COUNT(*) = 1;"

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = minusSql,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    first_cohort_id = firstCohortId,
    temp_table_1 = tempTable1,
    temp_table_2 = tempTable2,
    tempEmulationSchema = tempEmulationSchema
  )

  # date corrections
  dateCorrectionSql <- "
          DROP TABLE IF EXISTS @temp_table_3;
          WITH intersect_cohort
          AS (
          	SELECT subject_id,
          		cohort_start_date,
          		cohort_end_date
          	FROM @temp_table_1
          	WHERE cohort_definition_id IN (- 999)
          	)
          SELECT @new_cohort_id cohort_definition_id,
          	mc.subject_id,
          	CASE
          		WHEN cs.cohort_end_date IS NULL
          			THEN mc.candidate_start_date
          		ELSE DATEADD(DAY, 1, mc.candidate_start_date)
          		END AS cohort_start_date,
          	CASE
          		WHEN ce.cohort_end_date IS NULL
          			THEN mc.candidate_end_date
          		ELSE DATEADD(DAY, - 1, mc.candidate_end_date)
          		END AS cohort_end_date
          INTO @temp_table_3
          FROM @temp_table_2 mc
          LEFT JOIN intersect_cohort cs ON mc.subject_id = cs.subject_id
          	AND mc.candidate_start_date = cs.cohort_end_date
          LEFT JOIN intersect_cohort ce ON mc.subject_id = ce.subject_id
          	AND mc.candidate_end_date = ce.cohort_start_date;"

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = dateCorrectionSql,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    temp_table_1 = tempTable1,
    temp_table_2 = tempTable2,
    temp_table_3 = tempTable3,
    new_cohort_id = newCohortId,
    tempEmulationSchema = tempEmulationSchema
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
            FROM @temp_table_3;",
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cohort_database_schema = cohortDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohort_table = cohortTable,
    temp_table_3 = tempTable3
  )

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " DROP TABLE IF EXISTS @temp_table_1;
            DROP TABLE IF EXISTS @temp_table_2;
            DROP TABLE IF EXISTS @temp_table_3;",
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    temp_table_1 = tempTable1,
    temp_table_2 = tempTable2,
    temp_table_3 = tempTable3
  )
}
