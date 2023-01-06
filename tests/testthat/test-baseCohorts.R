testthat::test_that("Testing base cohorts", {
  cohortDefinitionSet <- getBaseCohortDefinitionSet()
  testthat::expect_true(object = ("data.frame" %in% class(cohortDefinitionSet)))
  
  eunomiaConnectionDetails <- Eunomia::getEunomiaConnectionDetails()
  
  incrementalFolder <- file.path(tempfile())
  
  generateBaseCohorts(
    connectionDetails = eunomiaConnectionDetails,
    cohortDatabaseSchema = "main",
    cdmDatabaseSchema = "main",
    incremental = FALSE,
    purgeConflicts = TRUE
  )
  
  testthat::expect_error(
    generateBaseCohorts(
      connectionDetails = eunomiaConnectionDetails,
      cohortDatabaseSchema = "main",
      cdmDatabaseSchema = "main",
      incremental = TRUE,
      incrementalFolder = incrementalFolder,
      purgeConflicts = FALSE
    )
  )
  
  generateBaseCohorts(
    connectionDetails = eunomiaConnectionDetails,
    cohortDatabaseSchema = "main",
    cdmDatabaseSchema = "main",
    incremental = TRUE,
    incrementalFolder = incrementalFolder,
    purgeConflicts = TRUE
  )
  
})
