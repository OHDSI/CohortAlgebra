testthat::test_that("Testing cohort filter", {
  testthat::skip_if(condition = skipCdmTests)
  
  tempCohortTableName <- paste0("#", cohortTableName, "_5")
  cohort <- dplyr::tibble(
    cohortDefinitionId = c(1, 2),
    subjectId = c(1, 1),
    cohortStartDate = c(as.Date("1999-01-01"), as.Date("1999-01-01")),
    cohortEndDate = c(as.Date("1999-12-31"), as.Date("1999-01-31"))
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
  
  CohortAlgebra::filterCohort(
    connectionDetails = connectionDetails,
    cohortTable = tempCohortTableName,
    tempEmulationSchema = tempEmulationSchema,
    connection = connection,
    cohortDatabaseSchema = NULL,
    newCohortId = 2,
    oldCohortId = 1
  )
  
  output <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,sql = "SELECT * FROM @cohort_table WHERE cohort_definition_id = 2;",
    snakeCaseToCamelCase = TRUE,
    cohort_table = tempCohortTableName
  )
  
  testthat::expect_true(object = "data.frame" %in% class(output))
  testthat::expect_true(object = nrow(output) == 1)
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "DROP TABLE IF EXISTS @cohort_table;",
    cohort_table = cohortTableName,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
})
