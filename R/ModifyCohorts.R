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
#' #' Modify cohort
#' #'
#' #' @description
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
#' #' @template OldCohortId
#' #'
#' #' @template NewCohortId
#' #'
#' #' @template PurgeConflicts
#' #'
#' #' @template TempEmulationSchema
#' #'
#' #' @template CdmDatabaseSchema
#' #'
#' #' @param filterGenderConceptId   Provide an array of integers corresponding to conceptId to look for in the gender_concept_id
#' #'                                field of the person table.
#' #'
#' #' @param filterByAgeRange        Provide an array of two values, where second value is >= first value to filter the persons age on cohort_start_date.
#' #'                                Age is calculated as YEAR(cohort_start_date) - person.year_of_birth
#' #'
#' #' @param firstOccurrence         Do you want to restrict the cohort to the first occurrence per person?
#' #'
#' #' @return
#' #' NULL
#' #'
#' #'
#' #' @examples
#' #' \dontrun{
#' #' CohortAlgebra::modifyCohort(
#' #'   connection = connection,
#' #'   cohortDatabaseSchema = cohortDatabaseSchema,
#' #'   cohortTable = tableName,
#' #'   oldCohortId = 3,
#' #'   newCohortId = 2,
#' #'   filterGenderConceptId = c(8201),
#' #'   purgeConflicts = TRUE
#' #' )
#' #' }
#' #'
#' #' @export
#' modifyCohort <- function(connectionDetails = NULL,
#'                          connection = NULL,
#'                          cohortDatabaseSchema = NULL,
#'                          cdmDatabaseSchema = NULL,
#'                          cohortTable = "cohort",
#'                          oldCohortId,
#'                          newCohortId = oldCohortId,
#'                          filterGenderConceptId = NULL,
#'                          filterByAgeRange = NULL,
#'                          firstOccurrence = FALSE,
#'                          tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
#'                          purgeConflicts = TRUE) {
#'   errorMessages <- checkmate::makeAssertCollection()
#'   checkmate::assertIntegerish(
#'     x = oldCohortId,
#'     min.len = 1,
#'     null.ok = FALSE,
#'     add = errorMessages
#'   )
#'   checkmate::assertIntegerish(
#'     x = newCohortId,
#'     min.len = 1,
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
#'     x = cdmDatabaseSchema,
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
#'   checkmate::assertIntegerish(
#'     x = filterGenderConceptId,
#'     min.len = 1,
#'     null.ok = TRUE,
#'     add = errorMessages
#'   )
#'   checkmate::assertIntegerish(
#'     x = filterByAgeRange,
#'     min.len = 2,
#'     null.ok = TRUE,
#'     add = errorMessages
#'   )
#'   if (!is.null(filterByAgeRange)) {
#'     checkmate::assert_true(x = filterByAgeRange[1] <= filterByAgeRange[2])
#'   }
#'   
#'   checkmate::reportAssertions(collection = errorMessages)
#'   
#'   if (is.null(connection)) {
#'     connection <- DatabaseConnector::connect(connectionDetails)
#'     on.exit(DatabaseConnector::disconnect(connection))
#'   }
#'   
#'   if (oldCohortId != newCohortId) {
#'     if (!purgeConflicts) {
#'       cohortIdsInCohortTable <-
#'         getCohortIdsInCohortTable(
#'           connection = connection,
#'           cohortDatabaseSchema = cohortDatabaseSchema,
#'           cohortTable = cohortTable,
#'           tempEmulationSchema = tempEmulationSchema
#'         )
#'       conflicitingCohortIdsInTargetCohortTable <-
#'         intersect(x = newCohortId,
#'                   y = cohortIdsInCohortTable %>% unique())
#'       
#'       if (length(conflicitingCohortIdsInTargetCohortTable) > 0) {
#'         stop(
#'           paste0(
#'             "The following cohortIds already exist in the target cohort table, causing conflicts :",
#'             paste0(newCohortId,
#'                    collapse = ",")
#'           )
#'         )
#'       }
#'     }
#'   }
#'   
#'   tempTableName <- generateRandomString()
#'   tempTable1 <- paste0("#", tempTableName, "1")
#'   tempTable2 <- paste0("#", tempTableName, "2")
#'   
#'   copyCohortsToTempTable(
#'     connection = connection,
#'     oldToNewCohortId = dplyr::tibble(oldCohortId = oldCohortId, newCohortId = newCohortId),
#'     sourceCohortDatabaseSchema = cohortDatabaseSchema,
#'     sourceCohortTable = cohortTable,
#'     targetCohortTable = tempTable1
#'   )
#'   
#'   ## filter- Gender Concept Id -----
#'   if (!is.null(filterGenderConceptId)) {
#'     sql <- "DROP TABLE IF EXISTS @temp_table_2;
#'           	SELECT cohort_definition_id,
#'               	  subject_id,
#'               	  cohort_start_date,
#'               	  cohort_end_date
#'           	INTO @temp_table_2
#'           	FROM @temp_table_1 t
#'           	INNER JOIN @cdm_database_schema.person p
#'           	ON t.subject_id = p.person_id
#'           	WHERE p.gender_concept_id IN (@gender_concept_id);
#' 
#'           	DROP TABLE IF EXISTS @temp_table_1;
#' 
#'           SELECT *
#'           INTO @temp_table_1
#'           FROM @temp_table_2;
#' 
#'           DROP TABLE IF EXISTS @temp_table_2;
#'   "
#'     
#'     DatabaseConnector::renderTranslateExecuteSql(
#'       connection = connection,
#'       sql = sql,
#'       profile = FALSE,
#'       progressBar = FALSE,
#'       reportOverallTime = FALSE,
#'       tempEmulationSchema = tempEmulationSchema,
#'       gender_concept_id = filterGenderConceptId,
#'       cdm_database_schema = cdmDatabaseSchema,
#'       temp_table_1 = tempTable1,
#'       temp_table_2 = tempTable2
#'     )
#'   }
#'   
#'   
#'   ## filter- Age Range -----
#'   if (!is.null(filterByAgeRange)) {
#'     sql <- "DROP TABLE IF EXISTS @temp_table_2;
#'           	SELECT cohort_definition_id,
#'               	  subject_id,
#'               	  cohort_start_date,
#'               	  cohort_end_date
#'           	INTO @temp_table_2
#'           	FROM @temp_table_1 t
#'           	INNER JOIN @cdm_database_schema.person p
#'           	ON t.subject_id = p.person_id
#'           	WHERE YEAR(t.cohort_start_date) - p.year_of_birth >= @age_lower
#'           	      AND YEAR(t.cohort_start_date) - p.year_of_birth <= @age_higher;
#' 
#'           	DROP TABLE IF EXISTS @temp_table_1;
#' 
#'           SELECT *
#'           INTO @temp_table_1
#'           FROM @temp_table_2;
#' 
#'           DROP TABLE IF EXISTS @temp_table_2;
#'   "
#'     
#'     DatabaseConnector::renderTranslateExecuteSql(
#'       connection = connection,
#'       sql = sql,
#'       profile = FALSE,
#'       progressBar = FALSE,
#'       reportOverallTime = FALSE,
#'       tempEmulationSchema = tempEmulationSchema,
#'       age_lower = min(filterByAgeRange),
#'       age_higher = max(filterByAgeRange),
#'       cdm_database_schema = cdmDatabaseSchema,
#'       temp_table_1 = tempTable1,
#'       temp_table_2 = tempTable2
#'     )
#'   }
#'   
#'   ## First Occurrence -----
#'   if (firstOccurrence) {
#'     sql <- "  DROP TABLE IF EXISTS @temp_table_2;
#' 
#'             SELECT cohort_definition_id,
#'                    subject_id,
#'                    min(cohort_start_date) cohort_start_date,
#'                    min(cohort_end_date) cohort_end_date
#'           	INTO @temp_table_2
#'           	FROM @temp_table_1
#'           	GROUP BY cohort_definition_id, subject_id;
#' 
#'           	DROP TABLE IF EXISTS @temp_table_1;
#' 
#'           SELECT *
#'           INTO @temp_table_1
#'           FROM @temp_table_2;
#' 
#'           DROP TABLE IF EXISTS @temp_table_2;
#'   "
#'     
#'     DatabaseConnector::renderTranslateExecuteSql(
#'       connection = connection,
#'       sql = sql,
#'       profile = FALSE,
#'       progressBar = FALSE,
#'       reportOverallTime = FALSE,
#'       tempEmulationSchema = tempEmulationSchema,
#'       temp_table_1 = tempTable1,
#'       temp_table_2 = tempTable2
#'     )
#'   }
#'   
#'   if (oldCohortId != newCohortId) {
#'     ParallelLogger::logTrace(
#'       paste0(
#'         "The following cohortIds will be deleted from your cohort table and \n",
#'         " replaced with ear fy'd version of those cohorts using the same original cohort id: ",
#'         paste0(newCohortId, collapse = ",")
#'       )
#'     )
#'   }
#'   deleteCohort(
#'     connection = connection,
#'     cohortDatabaseSchema = cohortDatabaseSchema,
#'     cohortTable = cohortTable,
#'     cohortIds = newCohortId
#'   )
#'   
#'   DatabaseConnector::renderTranslateExecuteSql(
#'     connection = connection,
#'     sql = " INSERT INTO {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table}
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
#'     sql = " DROP TABLE IF EXISTS @temp_table_1;
#'             DROP TABLE IF EXISTS @temp_table_2;",
#'     profile = FALSE,
#'     progressBar = FALSE,
#'     reportOverallTime = FALSE,
#'     temp_table_1 = tempTable1,
#'     temp_table_2 = tempTable2
#'   )
#' }
