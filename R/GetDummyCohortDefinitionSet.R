# Copyright 2023 Observational Health Data Sciences and Informatics
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

#' Get dummy cohort definition set
#'
#' @description
#' Get dummy cohort definition set
#'
#' `r lifecycle::badge("stable")`
#'
#' @return
#' NULL
#'
#' @examples
#' \dontrun{
#' CohortAlgebra::getDummyCohortDefinitionSet(cohortId = 0)
#' }
#'
#' @export
#'
#'
getDummyCohortDefinitionSet <- function(cohortId = 0,
                                        cohortName = "Do not use") {
  pathToCsv <- system.file("dummyCohortDefinitionSet.csv", package = utils::packageName())
  readr::read_csv(file = pathToCsv, col_types = readr::cols(), na = "") %>% 
    dplyr::mutate(cohortId = cohortId,
                  cohortName = cohortName) %>% 
    dplyr::select(cohortId,
                  cohortName,
                  json,
                  sql)
}
