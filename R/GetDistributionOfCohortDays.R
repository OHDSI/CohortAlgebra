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
#
#' Calculate cohort days summary statistics
#'
#' This function retrieves the full cohort tables into R and performs several summary statistics on the days duration of cohort memberships.
#'
#' @param cohort A data frame that includes the columns cohortDefinitionId, subjectId, cohortStartDate, and cohortEndDate.
#' @param cohortDefinitionId (optional) The cohort id to filter the data; if NULL, all rows will be used.
#' @return A data frame of summary statistics for each cohort definition id.
#' @export
getDistributionOfCohortDays <-
  function(cohort, cohortDefinitionId = NULL) {
    # Validate input data structure
    stopifnot(
      "cohortDefinitionId" %in% names(cohort),
      "subjectId" %in% names(cohort),
      "cohortStartDate" %in% names(cohort),
      "cohortEndDate" %in% names(cohort),
      all(cohort$cohortEndDate >= cohort$cohortStartDate)
    )

    # Convert dates to Date class
    cohort$cohortStartDate <-
      lubridate::as_date(cohort$cohortStartDate)
    cohort$cohortEndDate <- lubridate::as_date(cohort$cohortEndDate)

    # Filter data if cohortDefinitionId is specified
    if (!is.null(cohortDefinitionId)) {
      cohort <-
        dplyr::filter(cohort, cohortDefinitionId == !!cohortDefinitionId)
    }

    # Calculate the duration in days
    cohort <-
      dplyr::mutate(cohort,
        days = as.numeric(cohort$cohortEndDate - cohort$cohortStartDate)
      )

    # Calculate summary statistics
    stats <- dplyr::group_by(cohort, cohortDefinitionId) |>
      dplyr::summarise(
        mean_days = mean(.data$days, na.rm = TRUE),
        percentile_1 = stats::quantile(.data$days, probs = 0.01, na.rm = TRUE),
        percentile_5 = stats::quantile(.data$days, probs = 0.05, na.rm = TRUE),
        percentile_10 = stats::quantile(.data$days, probs = 0.10, na.rm = TRUE),
        percentile_15 = stats::quantile(.data$days, probs = 0.15, na.rm = TRUE),
        percentile_20 = stats::quantile(.data$days, probs = 0.20, na.rm = TRUE),
        percentile_25 = stats::quantile(.data$days, probs = 0.25, na.rm = TRUE),
        percentile_50 = stats::quantile(.data$days, probs = 0.50, na.rm = TRUE),
        percentile_75 = stats::quantile(.data$days, probs = 0.75, na.rm = TRUE),
        percentile_80 = stats::quantile(.data$days, probs = 0.80, na.rm = TRUE),
        percentile_90 = stats::quantile(.data$days, probs = 0.90, na.rm = TRUE),
        percentile_95 = stats::quantile(.data$days, probs = 0.95, na.rm = TRUE),
        percentile_99 = stats::quantile(.data$days, probs = 0.99, na.rm = TRUE),
        .groups = "drop"
      )

    return(stats)
  }
