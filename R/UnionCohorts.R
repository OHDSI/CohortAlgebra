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

#' Union cohort(s)
#'
#' @description
#' Given a specified array of cohortIds in a cohort table, perform
#' cohort union operator to create new cohorts.
#' 
#' `r lifecycle::badge("stable")`
#'
#' @template ConnectionDetails
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @template CohortDatabaseSchema
#'
#' @template OldToNewCohortId
#'
#' @template PurgeConflicts
#'
#' @template TempEmulationSchema
#'
#' @return
#' NULL
#'
#' @examples
#' \dontrun{
#' unionCohorts(
#'   connectionDetails = Eunomia::getEunomiaConnectionDetails(),
#'   cohortDatabaseSchema = "main",
#'   cohortTable = "cohort",
#'   oldToNewCohortId = dplyr::tibble(
#'     oldCohortId = c(1, 2, 3),
#'     newCohortId = c(9, 9, 9)
#'   ),
#'   purgeConflicts = TRUE
#' )
#' }
#'
#' @export
unionCohorts <- function(connectionDetails = NULL,
                         connection = NULL,
                         cohortDatabaseSchema = NULL,
                         cohortTable = "cohort",
                         oldToNewCohortId,
                         tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                         purgeConflicts = FALSE) {
  eraFyCohorts(
    connectionDetails = connectionDetails,
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    oldToNewCohortId = oldToNewCohortId,
    eraconstructorpad = 0,
    cdmDatabaseSchema = NULL,
    tempEmulationSchema = tempEmulationSchema,
    purgeConflicts = purgeConflicts
  )
}
