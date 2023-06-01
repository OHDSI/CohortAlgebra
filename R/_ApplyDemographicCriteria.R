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
#' #' Apply Demographic cohort
#' #'
#' #' @description
#' #'
#' #' `r lifecycle::badge("experimental")`
#' #'
#' #' @template ConnectionDetails
#' #'
#' #' @template Connection
#' #'
#' #' @template SourceCohortDatabaseSchema
#' #'
#' #' @template SourceCohortTable
#' #'
#' #' @template TargetCohortDatabaseSchema
#' #'
#' #' @template TargetCohortTable
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
#' #' @return
#' #' NULL
#' #'
#' #'
#' #' @examples
#' #' \dontrun{
#' #' CohortAlgebra::applyDemographicCriteria(
#' #'   connection = connection,
#' #'   sourceCohortTable = tableName,
#' #'   oldCohortId = 3,
#' #'   newCohortId = 2,
#' #'   filterGenderConceptId = c(8201),
#' #'   purgeConflicts = TRUE
#' #' )
#' #' }
#' #'
#' #' @export
#' applyDemographicCriteria <- function(connectionDetails = NULL,
#'                                      connection = NULL,
#'                                      sourceCohortDatabaseSchema = NULL,
#'                                      sourceCohortTable,
#'                                      targetCohortDatabaseSchema = NULL,
#'                                      targetCohortTable,
#'                                      cdmDatabaseSchema,
#'                                      oldCohortId,
#'                                      newCohortId,
#'                                      filterGenderConceptId = NULL,
#'                                      filterByAgeRange = NULL,
#'                                      tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
#'                                      purgeConflicts = TRUE) {
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
#'     x = cdmDatabaseSchema,
#'     min.chars = 1,
#'     len = 1,
#'     null.ok = FALSE,
#'     add = errorMessages
#'   )
#'   checkmate::assertCharacter(
#'     x = sourceCohortDatabaseSchema,
#'     min.chars = 1,
#'     len = 1,
#'     null.ok = FALSE,
#'     add = errorMessages
#'   )
#'   checkmate::assertCharacter(
#'     x = sourceCohortTable,
#'     min.chars = 1,
#'     len = 1,
#'     null.ok = TRUE,
#'     add = errorMessages
#'   )
#'   checkmate::assertCharacter(
#'     x = targetCohortDatabaseSchema,
#'     min.chars = 1,
#'     len = 1,
#'     null.ok = FALSE,
#'     add = errorMessages
#'   )
#'   checkmate::assertCharacter(
#'     x = targetCohortTable,
#'     min.chars = 1,
#'     len = 1,
#'     null.ok = FALSE,
#'     add = errorMessages
#'   )
#' 
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
#'   if (all(
#'     is.null(filterByAgeRange),
#'     is.null(filterGenderConceptId)
#'   )) {
#'     stop("No criteria specified")
#'   }
#' 
#'   if (is.null(connection)) {
#'     connection <- DatabaseConnector::connect(connectionDetails)
#'     on.exit(DatabaseConnector::disconnect(connection))
#'   }
#' 
#'   if (!purgeConflicts) {
#'     cohortIdsInCohortTable <-
#'       getCohortIdsInCohortTable(
#'         connection = connection,
#'         cohortDatabaseSchema = targetCohortDatabaseSchema,
#'         cohortTable = targetCohortTable,
#'         tempEmulationSchema = tempEmulationSchema
#'       )
#'     conflicitingCohortIdsInTargetCohortTable <-
#'       intersect(
#'         x = newCohortId,
#'         y = cohortIdsInCohortTable |> unique()
#'       )
#' 
#'     if (length(conflicitingCohortIdsInTargetCohortTable) > 0) {
#'       stop("Target cohort id already in use in target cohort table")
#'     }
#'   }
#' 
#'   if (all(
#'     paste0(sourceCohortDatabaseSchema, sourceCohortTable) ==
#'       paste0(targetCohortDatabaseSchema, targetCohortTable),
#'     oldCohortId == newCohortId
#'   )) {
#'     tempTableName <- generateRandomString()
#'     tempTable1 <- paste0("#", tempTableName, "1")
#' 
#'     DatabaseConnector::renderTranslateExecuteSql(
#'       connection = connection,
#'       sql = "
#'       DROP TABLE IF EXISTS @target_cohort_table;
#'       CREATE TABLE @target_cohort_table (
#'                     	cohort_definition_id BIGINT,
#'                     	subject_id BIGINT,
#'                     	cohort_start_date DATE,
#'                     	cohort_end_date DATE
#'   );",
#'       target_cohort_table = tempTable1,
#'       progressBar = FALSE,
#'       reportOverallTime = FALSE
#'     )
#'     copyCohortsToTempTable(
#'       connection = connection,
#'       sourceCohortDatabaseSchema = sourceCohortDatabaseSchema,
#'       sourceCohortTable = sourceCohortTable,
#'       tempEmulationSchema = tempEmulationSchema,
#'       targetCohortTable = tempTable1,
#'       oldToNewCohortId = dplyr::tibble(
#'         oldCohortId = oldCohortId,
#'         newCohortId = newCohortId
#'       )
#'     )
#'     sourceCohortDatabaseSchema <- NULL
#'     sourceCohortTable <- tempTable1
#'   }
#' 
#' 
#'   sql <- SqlRender::loadRenderTranslateSql(
#'     sqlFilename = "DemographicCritera.sql",
#'     packageName = utils::packageName(),
#'     dbms = connection@dbms,
#'     tempEmulationSchema = tempEmulationSchema,
#'     cdm_database_schema = cdmDatabaseSchema,
#'     source_cohort_database_schema = sourceCohortDatabaseSchema,
#'     source_cohort_table = sourceCohortTable,
#'     target_cohort_database_schema = targetCohortDatabaseSchema,
#'     target_cohort_table = targetCohortTable,
#'     old_cohort_id = oldCohortId,
#'     new_cohort_id = newCohortId,
#'     gender_concept_id = filterGenderConceptId,
#'     age_lower = filterByAgeRange[[1]],
#'     age_higher = filterByAgeRange[[2]]
#'   )
#'   DatabaseConnector::executeSql(
#'     connection = connection,
#'     sql = sql,
#'     profile = FALSE,
#'     progressBar = FALSE,
#'     reportOverallTime = FALSE
#'   )
#' }
