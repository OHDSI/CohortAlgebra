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

#' Era-fy cohort(s)
#'
#' @description
#' Given a data frame in R with cohortDefinitionId, subjectId, cohortStartDate,
#' cohortEndDate execute era logic. Returns a cohort.
#'
#' `r lifecycle::badge("stable")`
#'
#' @param eraconstructorpad   Optional value to pad cohort era construction logic. Default = 0. i.e. no padding.
#'
#' @param cohort A data frame with cohort data conforming to cohortDefinitionId, subjectId, cohortStartDate, cohortEndDate
#' @return cohort table
#'
#' @export
#'
eraFyCohortDataFrame <- function(cohort, eraconstructorpad = 0) {
  # Define the function logic
  cohort <- cohort |>
    dplyr::mutate(cohortEndDate = .data$cohortEndDate + lubridate::days(eraconstructorpad)) |>
    dplyr::group_by(.data$cohortDefinitionId, .data$subjectId) |>
    dplyr::arrange(
      .data$cohortDefinitionId,
      .data$subjectId,
      .data$cohortStartDate,
      .data$cohortEndDate
    ) |>
    dplyr::mutate(
      prev_end_date = dplyr::lag(
        .data$cohortEndDate,
        default = dplyr::first(.data$cohortEndDate) - lubridate::days(1)
      ),
      is_start = dplyr::if_else(.data$prev_end_date >= .data$cohortStartDate, 0, 1),
      group_idx = cumsum(.data$is_start)
    ) |>
    dplyr::group_by(.data$cohortDefinitionId, .data$subjectId, .data$group_idx) |>
    dplyr::summarise(
      cohortStartDate = min(.data$cohortStartDate),
      cohortEndDate = max(.data$cohortEndDate) - lubridate::days(eraconstructorpad),
      .groups = "drop"
    ) |>
    dplyr::select(
      .data$cohortDefinitionId,
      .data$subjectId,
      .data$cohortStartDate,
      .data$cohortEndDate
    ) |>
    dplyr::arrange(
      .data$cohortDefinitionId,
      .data$subjectId,
      .data$cohortStartDate,
      .data$cohortEndDate
    )

  # Return the modified cohort data frame
  return(cohort)
}
