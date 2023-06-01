#' # Copyright 2023 Observational Health Data Sciences and Informatics
#' #
#' # This file is part of CohortAlgebra
#' #
#' # Licensed under the Apache License, Version 2.0 (the "License");
#' # you may not use this file except in compliance with the License.
#' # You may obtain a copy of the License at
#' #
#' #     http://www.apache.org/licenses/LICENSE-2.0
#' #
#' # Unless required by applicable law or agreed to in writing, software
#' # distributed under the License is distributed on an "AS IS" BASIS,
#' # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#' # See the License for the specific language governing permissions and
#' # limitations under the License.
#'
#' #' Keep records in cohort that overlap with another cohort
#' #'
#' #' @description
#' #' Keep records in cohort that overlap with another cohort. Given a Cohort A, check if the records of
#' #' subjects in cohort A overlaps with records for the same subject in cohort B. If there is overlap
#' #' then only keep those records in Cohort A. All non overlapping records in Cohort A will be removed.
#' #' Overlap is defined as b.cohort_end_date >= a.cohort_start_date AND b.cohort_start_date <= a.cohort_end_date.
#' #' The overlap logic maybe offset by using a startDayOffSet (applied on cohort A's cohort_start_date)
#' #' and endDayOffSet (applied on Cohort A's cohort_end_date). If while applying offset, the window becomes
#' #' such that (a.cohort_start_date + startDayOffSet) > (a.cohort_end_date + endDayOffset) that record is ignored
#' #' and thus deleted.
#' #'
#' #' By default we are looking for atleast one day of overlap. We can change this to look for any number of overlap
#' #' days e.g. 2 days of overlap in the window. The overlap days are calculated as the total number of days
#' #' between maximum of cohort_start_date's of both cohorts, and minimum of cohort_end_date's of both cohorts, using
#' #' offset when used.
#' #'
#' #' Overlap formula is (min(a.cohort_end_date, b.cohort_end_date) - max(a.cohort_start_date, b.cohort_start_date)) + 1.
#' #' Note the use of +1, i.e. the lowest number of days of overlap is 1 day.
#' #'
#' #' `r lifecycle::badge("experimental")`
#' #'
#' #' @template ConnectionDetails
#' #'
#' #' @template Connection
#' #'
#' #' @template CohortTable
#' #'
#' #' @template CohortDatabaseSchema
#' #'
#' #' @param firstCohortId The cohort id of the cohort whose records will be retained after the operation.
#' #'
#' #' @param secondCohortId The cohort id of the cohort that will be used to check for the presence of overlap.
#' #'
#' #' @param minimumOverlapDays (Default = 1) The minimum number of days of overlap.
#' #'
#' #' @param offsetCohortStartDate (Default = 0) If you want to offset cohort start date, please provide a integer number.
#' #'
#' #' @param offsetCohortEndDate (Default = 0) If you want to offset cohort start date, please provide a integer number.
#' #'
#' #' @param restrictSecondCohortStartBeforeFirstCohortStart  (Default = FALSE) If TRUE, then the secondCohort's cohort_start_date
#' #'                                                          should be < firstCohort's cohort_start_date.
#' #'
#' #' @param restrictSecondCohortStartAfterFirstCohortStart  (Default = FALSE) If TRUE, then the secondCohort's cohort_start_date
#' #'                                                          should be > firstCohort's cohort_start_date.
#' #'
#' #' @template NewCohortId
#' #'
#' #' @template PurgeConflicts
#' #'
#' #' @template TempEmulationSchema
#' #'
#' #' @return
#' #' NULL
#' #'
#' #'
#' #' @examples
#' #' \dontrun{
#' #' keepCohortOverlaps(
#' #'   connectionDetails = Eunomia::getEunomiaConnectionDetails(),
#' #'   cohortDatabaseSchema = "main",
#' #'   cohortTable = "cohort",
#' #'   firstCohortId = 1,
#' #'   secondCohortId = 2,
#' #'   newCohortId = 9,
#' #'   purgeConflicts = TRUE
#' #' )
#' #' }
#' #'
#' #' @export
#' keepCohortOverlaps <- function(connectionDetails = NULL,
#'                                connection = NULL,
#'                                cohortDatabaseSchema = NULL,
#'                                cohortTable = "cohort",
#'                                firstCohortId,
#'                                secondCohortId,
#'                                newCohortId,
#'                                offsetCohortStartDate = 0,
#'                                offsetCohortEndDate = 0,
#'                                restrictSecondCohortStartBeforeFirstCohortStart = FALSE,
#'                                restrictSecondCohortStartAfterFirstCohortStart = FALSE,
#'                                minimumOverlapDays = 1,
#'                                purgeConflicts = FALSE,
#'                                tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
#'   errorMessages <- checkmate::makeAssertCollection()
#'   checkmate::assertIntegerish(
#'     x = firstCohortId,
#'     len = 1,
#'     null.ok = FALSE,
#'     add = errorMessages
#'   )
#'   checkmate::assertIntegerish(
#'     x = secondCohortId,
#'     len = 1,
#'     null.ok = FALSE,
#'     add = errorMessages
#'   )
#'   checkmate::assertIntegerish(
#'     x = newCohortId,
#'     len = 1,
#'     null.ok = FALSE,
#'     add = errorMessages
#'   )
#'   checkmate::assertCharacter(
#'     x = cohortDatabaseSchema,
#'     min.chars = 1,
#'     len = 1,
#'     null.ok = TRUE,
#'     add = errorMessages
#'   )
#'   checkmate::assertCharacter(
#'     x = cohortTable,
#'     min.chars = 1,
#'     len = 1,
#'     null.ok = FALSE,
#'     add = errorMessages
#'   )
#'   checkmate::assertLogical(
#'     x = purgeConflicts,
#'     any.missing = FALSE,
#'     min.len = 1,
#'     add = errorMessages
#'   )
#'   checkmate::assertLogical(
#'     x = restrictSecondCohortStartBeforeFirstCohortStart,
#'     any.missing = FALSE,
#'     min.len = 1,
#'     add = errorMessages
#'   )
#'   checkmate::assertLogical(
#'     x = restrictSecondCohortStartAfterFirstCohortStart,
#'     any.missing = FALSE,
#'     min.len = 1,
#'     add = errorMessages
#'   )
#'   checkmate::assertDouble(
#'     x = minimumOverlapDays,
#'     len = 1,
#'     null.ok = FALSE,
#'     add = errorMessages
#'   )
#'   checkmate::reportAssertions(collection = errorMessages)
#'
#'   if (firstCohortId == secondCohortId) {
#'     warning(
#'       "During overlap operation, both first and second cohorts have the same cohort id. The result may be a NULL cohort."
#'     )
#'   }
#'
#'   if (is.null(connection)) {
#'     connection <- DatabaseConnector::connect(connectionDetails)
#'     on.exit(DatabaseConnector::disconnect(connection))
#'   }
#'
#'   cohortIdsInCohortTable <-
#'     getCohortIdsInCohortTable(
#'       connection = connection,
#'       cohortDatabaseSchema = cohortDatabaseSchema,
#'       cohortTable = cohortTable,
#'       tempEmulationSchema = tempEmulationSchema
#'     )
#'
#'   conflicitingCohortIdsInTargetCohortTable <-
#'     intersect(
#'       x = newCohortId |> unique(),
#'       y = cohortIdsInCohortTable |> unique()
#'     )
#'
#'   if (length(conflicitingCohortIdsInTargetCohortTable) > 0) {
#'     if (!purgeConflicts) {
#'       stop(
#'         paste0(
#'           "The following cohortIds already exist in the target cohort table, causing conflicts :",
#'           paste0(conflicitingCohortIdsInTargetCohortTable, collapse = ",")
#'         )
#'       )
#'     }
#'   }
#'
#'   tempTableName <- generateRandomString()
#'   tempTable1 <- paste0("#", tempTableName, "1")
#'
#'   sql <- SqlRender::loadRenderTranslateSql(
#'     sqlFilename = "KeepErasWithOverlap.sql",
#'     packageName = utils::packageName(),
#'     dbms = connection@dbms,
#'     first_cohort_id = firstCohortId,
#'     temp_table_1 = tempTable1,
#'     first_cohort_id = firstCohortId,
#'     second_cohort_id = secondCohortId,
#'     new_cohort_id = newCohortId,
#'     tempEmulationSchema = tempEmulationSchema,
#'     min_days_overlap = minimumOverlapDays,
#'     first_offset = offsetCohortStartDate,
#'     second_offset = offsetCohortEndDate,
#'     cohort_database_schema = cohortDatabaseSchema,
#'     cohort_table = cohortTable,
#'     second_cohort_start_before_first_cohort_start = restrictSecondCohortStartBeforeFirstCohortStart,
#'     second_cohort_start_after_first_cohort_start = restrictSecondCohortStartAfterFirstCohortStart
#'   )
#'   ParallelLogger::logInfo("Looking for overlaps.")
#'   DatabaseConnector::executeSql(
#'     connection = connection,
#'     sql = sql,
#'     profile = FALSE,
#'     progressBar = TRUE,
#'     reportOverallTime = TRUE
#'   )
#'
#'   ParallelLogger::logInfo("Saving overlaps.")
#'   DatabaseConnector::renderTranslateExecuteSql(
#'     connection = connection,
#'     sql = " DELETE FROM {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table}
#'             WHERE cohort_definition_id IN (SELECT DISTINCT cohort_definition_id FROM @temp_table_1);
#'
#'             INSERT INTO {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table}
#'             SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
#'             FROM @temp_table_1;
#'             UPDATE STATISTICS  {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table};",
#'     profile = FALSE,
#'     progressBar = FALSE,
#'     reportOverallTime = FALSE,
#'     cohort_database_schema = cohortDatabaseSchema,
#'     tempEmulationSchema = tempEmulationSchema,
#'     cohort_table = cohortTable,
#'     temp_table_1 = tempTable1
#'   )
#'
#'   DatabaseConnector::renderTranslateExecuteSql(
#'     connection = connection,
#'     sql = " DROP TABLE IF EXISTS @temp_table_1;",
#'     profile = FALSE,
#'     progressBar = FALSE,
#'     reportOverallTime = FALSE,
#'     temp_table_1 = tempTable1
#'   )
#' }
