testthat::test_that("Testing base cohorts", {
  cohortDefinitionSet <- getBaseCohortDefinitionSet()
  testthat::expect_true(object = ("data.frame" %in% class(cohortDefinitionSet)))

  eunomiaConnectionDetails <- Eunomia::getEunomiaConnectionDetails()

  incrementalFolder <- file.path(tempfile())

  # generate but not incremental, no log file
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
      purgeConflicts = FALSE
    )
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

  # should create by purging old files
  generateBaseCohorts(
    connectionDetails = eunomiaConnectionDetails,
    cohortDatabaseSchema = "main",
    cdmDatabaseSchema = "main",
    incremental = TRUE,
    incrementalFolder = incrementalFolder,
    purgeConflicts = TRUE
  )

  # should skip because already generated
  generateBaseCohorts(
    connectionDetails = eunomiaConnectionDetails,
    cohortDatabaseSchema = "main",
    cdmDatabaseSchema = "main",
    incremental = TRUE,
    incrementalFolder = incrementalFolder,
    purgeConflicts = TRUE
  )
})
