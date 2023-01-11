testthat::test_that("Testing filter cohort by calendar date", {
  # generate unique name for a cohort table
  sysTime <- as.numeric(Sys.time()) * 100000
  tableName <- paste0("cr", sysTime)
  tempTableName <- paste0("#", tableName, "_1")

  # make up date for a cohort table
  cohort <- dplyr::tibble(
    cohortDefinitionId = c(1, 1, 1),
    subjectId = c(1, 1, 1),
    cohortStartDate = c(
      as.Date("1999-01-01"),
      as.Date("2000-01-01"),
      as.Date("2001-01-01")
    ),
    cohortEndDate = c(
      as.Date("1999-12-01"),
      as.Date("2000-12-01"),
      as.Date("2001-12-01")
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
    limitCohortOccurrence(
      connectionDetails = connectionDetails,
      sourceCohortDatabaseSchema = cohortDatabaseSchema,
      sourceCohortTable = tableName,
      targetCohortDatabaseSchema = cohortDatabaseSchema,
      targetCohortTable = tableName,
      oldCohortId = 1,
      newCohortId = 10,
      firstOccurrence = TRUE,
      lastOccurrence = TRUE,
      purgeConflicts = TRUE
    )
  )

  limitCohortOccurrence(
    connectionDetails = connectionDetails,
    sourceCohortDatabaseSchema = cohortDatabaseSchema,
    sourceCohortTable = tableName,
    targetCohortDatabaseSchema = cohortDatabaseSchema,
    targetCohortTable = tableName,
    oldCohortId = 1,
    newCohortId = 10,
    firstOccurrence = TRUE,
    purgeConflicts = TRUE
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

  testthat::expect_equal(
    object = nrow(dataPost),
    expected = 1
  )
  expected <- dplyr::tibble(
    cohortDefinitionId = 10,
    subjectId = 1,
    cohortStartDate = as.Date("1999-01-01"),
    cohortEndDate = as.Date("1999-12-01")
  )
  testthat::expect_equal(
    object = dataPost,
    expected = expected
  )

  testthat::expect_error(
    limitCohortOccurrence(
      connectionDetails = connectionDetails,
      sourceCohortDatabaseSchema = cohortDatabaseSchema,
      sourceCohortTable = tableName,
      targetCohortDatabaseSchema = cohortDatabaseSchema,
      targetCohortTable = tableName,
      oldCohortId = 1,
      newCohortId = 10,
      firstOccurrence = TRUE,
      purgeConflicts = FALSE
    )
  )

  limitCohortOccurrence(
    connectionDetails = connectionDetails,
    sourceCohortDatabaseSchema = cohortDatabaseSchema,
    sourceCohortTable = tableName,
    targetCohortDatabaseSchema = cohortDatabaseSchema,
    targetCohortTable = tableName,
    oldCohortId = 1,
    newCohortId = 11,
    lastOccurrence = TRUE,
    purgeConflicts = TRUE
  )
  # extract the generated output and compare to expected
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)
  dataPost <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @cohort_database_schema.@table_name
        where cohort_definition_id = 11
        order by cohort_definition_id, subject_id, cohort_start_date;"
      ),
      cohort_database_schema = cohortDatabaseSchema,
      table_name = tableName,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble()

  testthat::expect_equal(
    object = nrow(dataPost),
    expected = 1
  )
  expected <- dplyr::tibble(
    cohortDefinitionId = 11,
    subjectId = 1,
    cohortStartDate = as.Date("2001-01-01"),
    cohortEndDate = as.Date("2001-12-01")
  )
  testthat::expect_equal(
    object = dataPost,
    expected = expected
  )

  DatabaseConnector::disconnect(connection)

  DatabaseConnector::renderTranslateExecuteSql(
    connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
    sql = "DROP TABLE IF EXISTS @cohort_database_schema.@table_temp;
           DROP TABLE IF EXISTS @cdm_database_schema.observation_period;",
    table_temp = tableName,
    cohort_database_schema = cohortDatabaseSchema,
    cdm_database_schema = cohortDatabaseSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
})
