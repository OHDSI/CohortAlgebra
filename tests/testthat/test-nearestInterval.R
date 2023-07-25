testthat::test_that("Testing nearest interval", {
  # generate unique name for a cohort table
  sysTime <- as.numeric(Sys.time()) * 100000
  tableName <- paste0("cr", sysTime)
  tempTableName <- paste0("#", tableName, "_1")
  
  # make up date for a cohort table
  cohort <- dplyr::tibble(
    cohortDefinitionId = c(1, 1, 2, 2, 3),
    subjectId = c(1, 1, 1, 1, 1),
    cohortStartDate = c(
      as.Date("2022-01-01"),
      as.Date("2022-05-10"),
      as.Date("2022-01-11"),
      as.Date("2022-05-10"),
      as.Date("2022-01-11")
    ),
    cohortEndDate = c(
      as.Date("2022-03-01"),
      as.Date("2022-05-10"),
      as.Date("2022-01-11"),
      as.Date("2022-12-30"),
      as.Date("2022-01-11")
    )
  )
  
  # upload table
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = cohortDatabaseSchema,
    tableName = tableName,
    data = cohort,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = FALSE,
    camelCaseToSnakeCase = TRUE,
    progressBar = FALSE
  )
  # disconnecting - as this is a test for a non temp cohort table
  DatabaseConnector::disconnect(connection)
  # should not throw error
  nearestInterval(
    connectionDetails = connectionDetails,
    sourceCohortDatabaseSchema = cohortDatabaseSchema,
    sourceCohortTable = tableName,
    targetCohortDatabaseSchema = cohortDatabaseSchema,
    targetCohortTable = tableName,
    targetCohortId = 1,
    intervalCohortIds = c(2, 3),
    newCohortId = 0,
    tempEmulationSchema = tempEmulationSchema,
    purgeConflicts = FALSE
  )
  
  # extract the generated output and compare to expected
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)
  dataPost <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @cohort_database_schema.@table_name
        where cohort_definition_id = 0
        order by cohort_definition_id, subject_id, cohort_start_date;"
      ),
      cohort_database_schema = cohortDatabaseSchema,
      table_name = tableName,
      snakeCaseToCamelCase = TRUE
    ) |>
    dplyr::tibble()
  
  testthat::expect_equal(object = nrow(dataPost),
                         expected = 1) # should have 1 rows
  
  # create the expected output data frame object to compare
  cohortExpected <- dplyr::tibble(
    cohortDefinitionId = c(0),
    subjectId = c(1),
    cohortStartDate = c(as.Date("2022-01-11")),
    cohortEndDate = c(as.Date("2022-01-11"))
  )
  
  testthat::expect_true(object = all(dataPost == cohortExpected))
  
  testthat::expect_error(
    nearestInterval(
      connection = connection,
      sourceCohortDatabaseSchema = cohortDatabaseSchema,
      sourceCohortTable = tableName,
      targetCohortDatabaseSchema = cohortDatabaseSchema,
      targetCohortTable = tableName,
      targetCohortId = 1,
      intervalCohortIds = c(2, 3),
      newCohortId = 0,
      tempEmulationSchema = tempEmulationSchema,
      purgeConflicts = FALSE
    )
  )
  
  nearestInterval(
    connectionDetails = connectionDetails,
    sourceCohortDatabaseSchema = cohortDatabaseSchema,
    sourceCohortTable = tableName,
    targetCohortDatabaseSchema = cohortDatabaseSchema,
    targetCohortTable = tableName,
    targetCohortId = 1,
    intervalCohortIds = c(2, 3),
    newCohortId = 5,
    tempEmulationSchema = tempEmulationSchema,
    purgeConflicts = FALSE
  )
  
  DatabaseConnector::disconnect(connection = connection)
})
