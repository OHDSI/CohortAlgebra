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

#' Era-fy cohort(s)
#'
#' @description
#' Given a table with cohort_definition_id, subject_id, cohort_start_date,
#' cohort_end_date execute era logic. This will delete and replace the
#' original rows with the cohort_definition_id(s). edit privileges
#' to the cohort table is required.
#'
#' @template ConnectionDetails
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @template OldToNewCohortId
#'
#' @template PurgeConflicts
#'
#' @template TempEmulationSchema
#'
#' @param eraconstructorpad   Optional value to pad cohort era construction logic. Default = 0. i.e. no padding.
#'
#' @return
#' NULL
#'
#' @export
eraFyCohort <- function(connectionDetails = NULL,
                        connection = NULL,
                        cohortDatabaseSchema,
                        cohortTable = "cohort",
                        oldToNewCohortId,
                        eraconstructorpad = 0,
                        tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                        purgeConflicts = FALSE) {
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
      x = oldToNewCohortId$newCohortId %>% unique(),
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
    oldToNewCohortId = oldToNewCohortId,
    sourceCohortDatabaseSchema = cohortDatabaseSchema,
    sourceCohortTable = cohortTable,
    targetCohortTable = tempTable1
  )

  sqlEraFy <- " DROP TABLE IF EXISTS @temp_table_2;
                with cteEndDates (cohort_definition_id, subject_id, cohort_end_date) AS -- the magic
                (
                	SELECT
                	  cohort_definition_id
                		, subject_id
                		, DATEADD(day,-1 * @eraconstructorpad, event_date)  as cohort_end_date
                	FROM
                	(
                		SELECT
                		  cohort_definition_id
                			, subject_id
                			, event_date
                			, event_type
                			, MAX(start_ordinal) OVER (PARTITION BY cohort_definition_id, subject_id
                			                            ORDER BY event_date, event_type, start_ordinal ROWS UNBOUNDED PRECEDING) AS start_ordinal
                			, ROW_NUMBER() OVER (PARTITION BY cohort_definition_id, subject_id
                			                      ORDER BY event_date, event_type, start_ordinal) AS overall_ord
                		FROM
                		(
                			SELECT
                			  cohort_definition_id
                				, subject_id
                				, cohort_start_date AS event_date
                				, -1 AS event_type
                				, ROW_NUMBER() OVER (PARTITION BY cohort_definition_id, subject_id ORDER BY cohort_start_date) AS start_ordinal
                			FROM @temp_table_1

                			UNION ALL


                			SELECT
                			  cohort_definition_id
                				, subject_id
                				, DATEADD(day,@eraconstructorpad,cohort_end_date) as cohort_end_date
                				, 1 AS event_type
                				, NULL
                			FROM @temp_table_1
                		) RAWDATA
                	) e
                	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
                ),
                cteEnds (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date) AS
                (
                	SELECT
                	  c. cohort_definition_id
                		, c.subject_id
                		, c.cohort_start_date
                		, MIN(e.cohort_end_date) AS cohort_end_date
                	FROM @temp_table_1 c
                	JOIN cteEndDates e ON c.cohort_definition_id = e.cohort_definition_id AND
                	                      c.subject_id = e.subject_id AND
                	                      e.cohort_end_date >= c.cohort_start_date
                	GROUP BY c.cohort_definition_id, c.subject_id, c.cohort_start_date
                )
                select cohort_definition_id, subject_id, min(cohort_start_date) as cohort_start_date, cohort_end_date
                into @temp_table_2
                from cteEnds
                group by cohort_definition_id, subject_id, cohort_end_date
                ;
  "

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlEraFy,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    tempEmulationSchema = tempEmulationSchema,
    eraconstructorpad = eraconstructorpad,
    temp_table_1 = tempTable1,
    temp_table_2 = tempTable2
  )

  cohortIdsToDeleteFromSource <- oldToNewCohortId %>%
    dplyr::filter(.data$oldCohortId == .data$newCohortId) %>%
    dplyr::pull(.data$oldCohortId)

  if (length(cohortIdsToDeleteFromSource) > 0) {
    ParallelLogger::logInfo(
      paste0(
        "The following cohortIds will be deleted from your cohort table and \n",
        " replaced with ear fy'd version of those cohorts using the same original cohort id: ",
        paste0(cohortIdsToDeleteFromSource, collapse = ",")
      )
    )
    deleteCohortRecords(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = cohortIdsToDeleteFromSource
    )
  }

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
            DROP TABLE IF EXISTS @temp_table_2;
            DROP TABLE IF EXISTS #old_to_new_cohort_id;",
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    temp_table_1 = tempTable1,
    temp_table_2 = tempTable2
  )
}
