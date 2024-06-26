testthat::test_that("Testing cohort distribution days", {
  testthat::skip_if(condition = skipCdmTests)

  tempCohortTableName <- paste0("#", cohortTableName, "_4")
  cohort <- dplyr::tibble(
    cohortDefinitionId = c(1),
    subjectId = c(1),
    cohortStartDate = c(as.Date("1999-01-01")),
    cohortEndDate = c(as.Date("1999-01-31"))
  )

  # upload table
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = cohortDatabaseSchema,
    tableName = tempCohortTableName,
    data = cohort,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = FALSE,
    camelCaseToSnakeCase = TRUE,
    progressBar = FALSE
  )

  output <- CohortAlgebra::getCohortTimes(
    connectionDetails = connectionDetails,
    cohortTable = tempCohortTableName,
    tempEmulationSchema = tempEmulationSchema,
    connection = connection,
    cohortDatabaseSchema = NULL,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortIds = 1,
    cohortTableIsTemp = TRUE
  )

  testthat::expect_true(object = "data.frame" %in% class(output))

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "DROP TABLE IF EXISTS @cohort_table;",
    cohort_table = cohortTableName,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
})
