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

#' Create cohort summary
#'
#' This function calculates the number of subjects, records, and total cohort days
#' for each provided `cohortDefinitionId`. It connects to a specified database,
#' and queries a cohort table to gather the necessary information.
#'
#' @param connectionDetails (optional) Details for establishing a database connection if `connection` is NULL.
#' @param connection (optional) An existing database connection object.
#' @param tempEmulationSchema (optional) The schema used for emulating temporary tables;
#'        defaults to the value set in global options with `getOption("sqlRenderTempEmulationSchema")`.
#' @param cohortDatabaseSchema The name of the schema where the cohort table exists.
#' @param cdmDatabaseSchema The name of the schema with OMOP CDM person level tables.
#' @param cohortTable The name of the cohort table.
#' @param cohortIds (optional) Vector of IDs corresponding to cohort definitions.
#' @param cohortTableIsTemp is Cohort table temp.
#'
#' @return A tibble with columns for cohort_definition_id, number of distinct subjects, number of events, and total days.
#' @export
getCohortTimes <- function(connectionDetails = NULL,
                           connection = NULL,
                           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                           cohortDatabaseSchema = NULL,
                           cdmDatabaseSchema,
                           cohortTable,
                           cohortIds = NULL,
                           cohortTableIsTemp = FALSE) {
  errorMessages <- checkmate::makeAssertCollection()

  checkmate::assertIntegerish(
    x = cohortIds,
    null.ok = TRUE,
    min.len = 1,
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

  covariateSettings <-
    FeatureExtraction::createCovariateSettings(
      useDemographicsPriorObservationTime = TRUE,
      useDemographicsPostObservationTime = TRUE,
      useDemographicsTimeInCohort = TRUE
    )

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  covariateData <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    oracleTempSchema = tempEmulationSchema,
    cohortIds = cohortIds,
    covariateSettings = covariateSettings,
    aggregated = TRUE,
    rowIdField = "subject_id",
    cohortTableIsTemp = cohortTableIsTemp
  )

  analysisRef <-
    covariateData$analysisRef |> dplyr::collect()
  covariateRef <-
    covariateData$covariateRef |> dplyr::collect()
  covariatesContinuous <-
    covariateData$covariatesContinuous |> dplyr::collect()

  report <- covariatesContinuous |>
    dplyr::inner_join(covariateRef,
      by = "covariateId"
    ) |>
    dplyr::inner_join(
      analysisRef |>
        dplyr::select(-"startDay", -"endDay", -"domainId"),
      by = "analysisId"
    ) |>
    dplyr::select(
      -"covariateId",
      -"analysisId", -"analysisName",
      -"isBinary", -"missingMeansZero",
      -"conceptId"
    ) |>
    dplyr::relocate(
      .data$cohortDefinitionId,
      "covariateName"
    )

  return(report)
}
