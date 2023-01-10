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
#' @template sourceCohortTable
#'
#' @template sourceCohortDatabaseSchema
#'
#' @template targetCohortTable
#'
#' @template targetCohortDatabaseSchema
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
#'   sourceDatabaseSchema = "main",
#'   sourceCohortTable = "cohort",
#'   oldToNewCohortId = dplyr::tibble(oldCohortId = c(1, 2), newCohortId = 4),
#'   purgeConflicts = TRUE
#' )
#' }
#'
#' @export
unionCohorts <- function(connectionDetails = NULL,
                         connection = NULL,
                         sourceCohortDatabaseSchema = NULL,
                         sourceCohortTable,
                         targetCohortDatabaseSchema = NULL,
                         targetCohortTable,
                         oldToNewCohortId,
                         tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                         purgeConflicts = FALSE) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertDataFrame(
    x = oldToNewCohortId,
    min.rows = 1,
    add = errorMessages
  )
  checkmate::assertNames(
    x = colnames(oldToNewCohortId),
    must.include = c("oldCohortId", "newCohortId"),
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = oldToNewCohortId$oldCohortId,
    min.len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = oldToNewCohortId$newCohortId,
    min.len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = sourceCohortDatabaseSchema,
    min.chars = 1,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = targetCohortDatabaseSchema,
    min.chars = 1,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = sourceCohortTable,
    min.chars = 1,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = targetCohortTable,
    min.chars = 1,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertLogical(
    x = purgeConflicts,
    any.missing = FALSE,
    min.len = 1,
    add = errorMessages
  )
  checkmate::reportAssertions(collection = errorMessages)
  newCohortIds <- oldToNewCohortId$newCohortId %>% unique()

  for (i in (1:length(newCohortIds))) {
    eraFyCohorts(
      connectionDetails = connectionDetails,
      connection = connection,
      sourceCohortDatabaseSchema = sourceCohortDatabaseSchema,
      sourceCohortTable = sourceCohortTable,
      targetCohortDatabaseSchema = targetCohortDatabaseSchema,
      targetCohortTable = targetCohortTable,
      oldCohortIds = oldToNewCohortId %>%
        dplyr::filter(.data$newCohortId == newCohortIds[[i]]) %>%
        dplyr::pull("oldCohortId") %>%
        unique(),
      newCohortId = newCohortIds[[i]],
      eraconstructorpad = 0,
      cdmDatabaseSchema = NULL,
      tempEmulationSchema = tempEmulationSchema,
      purgeConflicts = purgeConflicts
    )
  }
}
