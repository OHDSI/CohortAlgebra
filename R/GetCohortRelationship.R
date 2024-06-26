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

#' Generate Cohort Relationship Data
#'
#' @description
#' Given a data frame in R with `cohortDefinitionId`, `subjectId`, `cohortStartDate`,
#' and `cohortEndDate`, this function processes the data to identify the relationship
#' between first cohort start of the target cohort, and cohort starts of the feature
#' cohorts. The function can optionally select the closest
#' period in the feature cohort relative to the target cohort's start date.
#'
#' @param cohortTable A data frame with cohort data conforming to `cohortDefinitionId`, `subjectId`, `cohortStartDate`, `cohortEndDate`.
#' @param targetCohortId Integer value representing the cohortDefinitionId of the target cohort.
#' @param featureCohortIds Integer value representing the cohortDefinitionId of the feature cohort.
#' @param closestPeriod Boolean indicating whether to select the closest period in the feature cohort relative to the target cohort's start date. Default is FALSE.
#'
#' @return A data frame with columns `subjectId`, `startDay`, and `endDay`.
#' @export
getCohortRelationship <-
  function(cohortTable,
           targetCohortId,
           featureCohortIds,
           closestPeriod = FALSE) {
    # Check that cohortTable is a dataframe with the required columns
    checkmate::assert_data_frame(cohortTable, any.missing = FALSE)
    checkmate::assert_names(
      colnames(cohortTable),
      must.include = c(
        "cohortDefinitionId",
        "subjectId",
        "cohortStartDate",
        "cohortEndDate"
      )
    )

    # Check that targetCohortId and featureCohortIds are integers
    checkmate::assert_int(targetCohortId)
    checkmate::assertIntegerish(featureCohortIds)

    # Create targetCohort tibble
    targetCohort <- cohortTable |>
      dplyr::filter(.data$cohortDefinitionId == targetCohortId) |>
      dplyr::group_by(.data$subjectId) |>
      dplyr::summarize(
        minStartDate = min(.data$cohortStartDate, na.rm = TRUE),
        .groups = "drop"
      ) |>
      dplyr::mutate(day0 = .data$minStartDate)

    # Create a dataframe joining targetCohort to featureCohort
    featureCohort <- cohortTable |>
      dplyr::filter(.data$cohortDefinitionId %in% c(featureCohortIds)) |>
      dplyr::mutate(cohortDefinitionId = 0) |>
      eraFyCohortDataFrame() |>
      dplyr::left_join(targetCohort, by = "subjectId") |>
      dplyr::mutate(
        startDay = as.numeric(.data$cohortStartDate - .data$day0),
        endDay = as.numeric(.data$cohortEndDate - .data$day0)
      )

    if (closestPeriod) {
      featureCohortOverlap <- featureCohort |>
        dplyr::filter(.data$cohortStartDate <= .data$day0 &
          .data$cohortEndDate > .data$day0) |>
        dplyr::mutate(days = 0)

      featureCohortFirstBefore <- featureCohort |>
        dplyr::filter(.data$cohortEndDate < .data$day0) |>
        dplyr::mutate(days = as.integer(.data$day0 - .data$cohortEndDate))

      featureCohortFirstAfter <- featureCohort |>
        dplyr::filter(.data$cohortStartDate > .data$day0) |>
        dplyr::mutate(days = as.integer(.data$cohortStartDate - .data$day0))

      featureCohort <- dplyr::bind_rows(
        featureCohortOverlap,
        featureCohortFirstBefore,
        featureCohortFirstAfter
      ) |>
        dplyr::group_by(.data$subjectId) |>
        dplyr::arrange(.data$subjectId, .data$days) |>
        dplyr::slice(1) |>
        dplyr::ungroup() |>
        dplyr::select(.data$subjectId, .data$startDay, .data$endDay)
    } else {
      featureCohort <- featureCohort |>
        dplyr::select(.data$subjectId, .data$startDay, .data$endDay)
    }

    return(featureCohort)
  }
