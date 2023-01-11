#'
#' @param oldToNewCohortId    A data.frame object with two columns. oldCohortId and newCohortId. Both should be integers.
#'                            The oldCohortId are the cohorts that are the input cohorts that need to be transformed.
#'                            The newCohortId are the cohortIds of the corresponding output after transformation.
#'                            If the oldCohortId = newCohortId then the data corresponding to oldCohortId 
#'                            will be replaced by the data from the newCohortId.
