testthat::test_that("Testing censor cohort dates", {
  # generate unique name for a cohort table
  sysTime <- as.numeric(Sys.time()) * 100000
  tableName <- paste0("cr", sysTime)
  tempTableName <- paste0("#", tableName, "_1")
  
  # make up date for a cohort table
  cohort <- dplyr::tibble(
    cohortDefinitionId = c(1, 1, 1),
    subjectId = c(1, 1, 2),
    cohortStartDate = c(
      as.Date("1999-01-01"),
      as.Date("1999-02-15"),
      as.Date("1999-03-10")
    ),
    cohortEndDate = c(
      as.Date("1999-01-31"),
      as.Date("1999-03-30"),
      as.Date("1999-01-01")
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
  
  testthat::expect_error(
    censorCohortDates(
      connectionDetails = connectionDetails,
      sourceCohortDatabaseSchema = cohortDatabaseSchema,
      sourceCohortTable = tableName,
      targetCohortDatabaseSchema = cohortDatabaseSchema,
      targetCohortTable = tableName,
      oldCohortId = 1,
      newCohortId = 10,
      purgeConflicts = FALSE
    )
  )
  
  censorCohortDates(
    connectionDetails = connectionDetails,
    sourceCohortDatabaseSchema = cohortDatabaseSchema,
    sourceCohortTable = tableName,
    targetCohortDatabaseSchema = cohortDatabaseSchema,
    targetCohortTable = tableName,
    oldCohortId = 1,
    newCohortId = 10,
    cohortStartDateLeftCensor = as.Date("1999-01-15"),
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
        where cohort_definition_id = 10
        order by cohort_definition_id, subject_id, cohort_start_date;"
      ),
      cohort_database_schema = cohortDatabaseSchema,
      table_name = tableName,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble()
  
  testthat::expect_equal(object = nrow(dataPost),
                         expected = 2)
  expected <- dplyr::tibble(
    cohortDefinitionId = 10,
    subjectId = c(1,1),
    cohortStartDate = c(as.Date("1999-01-15"),as.Date("1999-02-15")),
    cohortEndDate = c(as.Date("1999-01-31"),as.Date("1999-03-30"))
  )
  testthat::expect_equal(object = dataPost,
                         expected = expected)
  
  censorCohortDates(
    connectionDetails = connectionDetails,
    sourceCohortDatabaseSchema = cohortDatabaseSchema,
    sourceCohortTable = tableName,
    targetCohortDatabaseSchema = cohortDatabaseSchema,
    targetCohortTable = tableName,
    oldCohortId = 1,
    newCohortId = 20,
    cohortStartDateLeftCensor = as.Date("1999-01-15"),
    cohortEndDateRightCensor = as.Date("1999-01-30"),
    purgeConflicts = FALSE
  )
  
  dataPost <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @cohort_database_schema.@table_name
        where cohort_definition_id = 20
        order by cohort_definition_id, subject_id, cohort_start_date;"
      ),
      cohort_database_schema = cohortDatabaseSchema,
      table_name = tableName,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble()
  
  testthat::expect_equal(object = nrow(dataPost),
                         expected = 1)
  expected <- dplyr::tibble(
    cohortDefinitionId = 20,
    subjectId = c(1),
    cohortStartDate = c(as.Date("1999-01-15")),
    cohortEndDate = c(as.Date("1999-01-30"))
  )
  testthat::expect_equal(object = dataPost,
                         expected = expected)
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
    sql = "DROP TABLE IF EXISTS @cohort_database_schema.@table_temp;",
    table_temp = tableName,
    cohort_database_schema = cohortDatabaseSchema,
    cdm_database_schema = cohortDatabaseSchema,
    progressBar = FALSE, 
    reportOverallTime = FALSE
  )
})
