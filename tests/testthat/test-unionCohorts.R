testthat::test_that("Testing cohort union", {
  # generate unique name for a cohort table
  sysTime <- as.numeric(Sys.time()) * 100000
  tableName <- paste0("cr", sysTime)
  tempTableName <- paste0("#", tableName, "_1")

  # make up date for a cohort table
  cohort <- dplyr::tibble(
    cohortDefinitionId = c(1, 2, 2),
    subjectId = c(1, 1, 1),
    cohortStartDate = c(
      as.Date("2022-01-01"),
      as.Date("2022-02-10"),
      as.Date("2022-08-15")
    ),
    cohortEndDate = c(
      as.Date("2022-03-01"),
      as.Date("2022-05-10"),
      as.Date("2022-12-30")
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
  unionCohorts(
    connectionDetails = connectionDetails,
    sourceCohortDatabaseSchema = cohortDatabaseSchema,
    sourceCohortTable = tableName,
    oldToNewCohortId = dplyr::tibble(
      oldCohortId = c(1, 2, 2),
      newCohortId = c(3, 3, 3)
    ),
    targetCohortDatabaseSchema = cohortDatabaseSchema,
    targetCohortTable = tableName,
    tempEmulationSchema = tempEmulationSchema,
    purgeConflicts = FALSE
  )

  # extract the generated output and compare to expected
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)
  dataPostUnion <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @cohort_database_schema.@table_name
        where cohort_definition_id = 3
        order by cohort_definition_id, subject_id, cohort_start_date;"
      ),
      cohort_database_schema = cohortDatabaseSchema,
      table_name = tableName,
      snakeCaseToCamelCase = TRUE
    ) |>
    dplyr::tibble()

  testthat::expect_equal(
    object = nrow(dataPostUnion),
    expected = 2
  ) # union logic should collapse to 2 rows

  # create the expected output data frame object to compare
  cohortExpected <- dplyr::tibble(
    cohortDefinitionId = c(3, 3),
    subjectId = c(1, 1),
    cohortStartDate = c(as.Date("2022-01-01"), as.Date("2022-08-15")),
    cohortEndDate = c(as.Date("2022-05-10"), as.Date("2022-12-30"))
  )

  testthat::expect_true(object = all(dataPostUnion == cohortExpected))

  testthat::expect_error(
    unionCohorts(
      connectionDetails = connectionDetails,
      sourceCohortDatabaseSchema = cohortDatabaseSchema,
      sourceCohortTable = tableName,
      oldToNewCohortId = dplyr::tibble(
        oldCohortId = c(1, 2, 2),
        newCohortId = c(3, 3, 3)
      ),
      targetCohortDatabaseSchema = cohortDatabaseSchema,
      targetCohortTable = tableName,
      tempEmulationSchema = tempEmulationSchema,
      isTempTable = TRUE
    )
  )

  # unionCohorts(
  #   connection = connection,
  #   sourceCohortDatabaseSchema = cohortDatabaseSchema,
  #   sourceCohortTable = tableName,
  #   oldToNewCohortId = dplyr::tibble(
  #     oldCohortId = c(1, 2, 2),
  #     newCohortId = c(3, 3, 3)
  #   ),
  #   tempEmulationSchema = tempEmulationSchema,
  #   targetCohortDatabaseSchema = NULL,
  #   targetCohortTable = paste0("#", tableName, "2"),
  #   isTempTable = TRUE
  # )

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "DROP TABLE IF EXISTS @cohort_database_schema.@table_temp;
           DROP TABLE IF EXISTS @cdm_database_schema.observation_period;",
    table_temp = tableName,
    cohort_database_schema = cohortDatabaseSchema,
    cdm_database_schema = cohortDatabaseSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )

  DatabaseConnector::disconnect(connection = connection)
})
