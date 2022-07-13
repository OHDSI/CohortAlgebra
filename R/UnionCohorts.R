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
#' Create a new cohort from two or more cohorts that contains all person-days that are in any of the input cohorts.
#'
#' @template ConnectionDetails
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @template CohortDatabaseSchema
#'
#' @param CohortIds IDs of cohorts to be unioned together
#'
#' @param NewCohortId ID of new cohort that will include all person-days in any of the {`CohortIds`} cohorts.
#'
#' @template PurgeConflicts
#'
#' @template TempEmulationSchema
#'
#' @return
#' NULL
#'
#' @export
unionCohorts <- function(connectionDetails = NULL,
                         connection = NULL,
                         cohortDatabaseSchema = NULL,
                         cohortTable = "cohort",
                         cohortIds,
                         newCohortId,
                         purgeConflicts = FALSE,
                         tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertIntegerish(x = cohortIds, min.len = 1, null.ok = FALSE, add = errorMessages)
  checkmate::assertIntegerish(x = newCohortId, len = 1, null.ok = FALSE, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  
  oldToNewCohortId <- dplyr::tibble(oldCohortId = cohortIds, newCohortId = newCohortId)
  
  eraFyCohorts(connectionDetails = connectionDetails,
               connection = connection,
               cohortDatabaseSchema = cohortDatabaseSchema,
               cohortTable = cohortTable,
               oldToNewCohortId = oldToNewCohortId,
               eraconstructorpad = 0,
               cdmDatabaseSchema = NULL,
               tempEmulationSchema = tempEmulationSchema,
               purgeConflicts = purgeConflicts)
  
}
