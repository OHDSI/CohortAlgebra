testthat::test_that("Testing cohort era fy", {
  # generate unique name for a cohort table
  sysTime <- as.numeric(Sys.time()) * 100000
  tableName <- paste0("cr", sysTime)
  tempTableName <- paste0("#", tableName, "_1")

  # make up date for a cohort table
  # this cohort table will have two subjects * two cohorts, within the same cohort
  cohort <- dplyr::tibble(
    cohortDefinitionId = c(1, 1, 1),
    subjectId = c(1, 1, 1),
    cohortStartDate = c(
      as.Date("1999-01-01"),
      as.Date("1999-01-20"),
      as.Date("1999-03-10")
    ),
    cohortEndDate = c(
      as.Date("1999-01-31"),
      as.Date("1999-02-28"),
      as.Date("1999-03-31")
    )
  )
  cohort <- dplyr::bind_rows(
    cohort,
    cohort %>% dplyr::mutate(subjectId = 2)
  )
  cohort <- dplyr::bind_rows(
    cohort,
    cohort %>% dplyr::mutate(cohortDefinitionId = 2)
  ) %>%
    dplyr::arrange(
      .data$subjectId,
      .data$cohortStartDate,
      .data$cohortEndDate
    )

  observationPeriod <- dplyr::tibble(
    personId = c(1, 1, 2),
    observation_period_start_date = c(
      as.Date("1999-01-01"),
      as.Date("1999-03-06"),
      as.Date("1998-01-01")
    ),
    observation_period_end_date = c(
      as.Date("1999-03-04"),
      as.Date("1999-04-30"),
      as.Date("2000-12-31")
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
  eraFyCohorts(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = tableName,
    oldToNewCohortId = dplyr::tibble(oldCohortId = 1, newCohortId = 9),
    eraconstructorpad = 0,
    purgeConflicts = FALSE
  )

  # extract the generated output and compare to expected
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)
  dataPostEraFy <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @cohort_database_schema.@table_name
        where cohort_definition_id = 9
        order by cohort_definition_id, subject_id, cohort_start_date;"
      ),
      cohort_database_schema = cohortDatabaseSchema,
      table_name = tableName,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble()

  testthat::expect_equal(
    object = nrow(dataPostEraFy),
    expected = 4
  ) # era fy logic should collapse to 4 rows

  # create the expected output data frame object to compare
  cohortExpected <- dplyr::tibble(
    cohortDefinitionId = c(9, 9),
    subjectId = c(1, 1),
    cohortStartDate = c(as.Date("1999-01-01"), as.Date("1999-03-10")),
    cohortEndDate = c(as.Date("1999-02-28"), as.Date("1999-03-31"))
  )
  cohortExpected <- dplyr::bind_rows(
    cohortExpected,
    cohortExpected %>%
      dplyr::mutate(subjectId = 2)
  ) %>%
    dplyr::distinct() %>%
    dplyr::arrange(
      .data$cohortDefinitionId,
      .data$subjectId,
      .data$cohortStartDate
    )

  testthat::expect_true(object = all(dataPostEraFy == cohortExpected))

  # this should throw error as there is already a cohort with cohort_definition_id = 9
  testthat::expect_error(
    eraFyCohorts(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = tableName,
      oldToNewCohortId = dplyr::tibble(oldCohortId = 1, newCohortId = 9),
      eraconstructorpad = 0,
      purgeConflicts = FALSE
    )
  )

  testthat::expect_message(
    object =
      eraFyCohorts(
        connectionDetails = connectionDetails,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = tableName,
        oldToNewCohortId = dplyr::tibble(oldCohortId = 1, newCohortId = 9),
        eraconstructorpad = 0,
        purgeConflicts = TRUE
      )
  )

  # check era padding -on temporary table
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = paste0(
      "
      DROP TABLE IF EXISTS @temp_table_name;
      SELECT *
      INTO @temp_table_name
      FROM @cohort_database_schema.@table_name
      WHERE cohort_definition_id IN (1,2);"
    ),
    cohort_database_schema = cohortDatabaseSchema,
    table_name = tableName,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    temp_table_name = tempTableName
  )

  testthat::expect_error(
    object = # throw error because cdmDatabaseSchema is not provide
      eraFyCohorts(
        connection = connection,
        cohortTable = tempTableName,
        oldToNewCohortId = dplyr::tibble(oldCohortId = 1, newCohortId = 10),
        eraconstructorpad = 30,
        purgeConflicts = FALSE
      )
  )

  # this should NOT throw error as we will purge conflicts.
  # it should return a message
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = cohortDatabaseSchema,
    tableName = "observation_period",
    data = observationPeriod,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = FALSE,
    camelCaseToSnakeCase = TRUE,
    progressBar = FALSE
  )

  eraFyCohorts(
    connection = connection,
    cohortTable = tempTableName,
    oldToNewCohortId = dplyr::tibble(oldCohortId = 1, newCohortId = 10),
    eraconstructorpad = 30,
    purgeConflicts = FALSE,
    cdmDatabaseSchema = cohortDatabaseSchema
  )

  dataPostEraFyWithEraPad <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @table_name
        where cohort_definition_id = 10
        order by cohort_definition_id, subject_id, cohort_start_date;"
      ),
      table_name = tempTableName,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble()

  cohortExpectedEraPad <- dplyr::tibble(
    cohortDefinitionId = c(10, 10),
    subjectId = c(1, 2),
    cohortStartDate = c(as.Date("1999-01-01"), as.Date("1999-01-01")),
    cohortEndDate = c(as.Date("1999-03-04"), as.Date("1999-03-31"))
  ) %>%
    dplyr::arrange(
      .data$cohortDefinitionId,
      .data$subjectId,
      .data$cohortStartDate
    )

  # this should throw error as there is already a cohort with cohort_definition_id = 10
  testthat::expect_error(
    eraFyCohorts(
      connectionDetails = connectionDetails,
      cohortTable = tempTableName,
      oldToNewCohortId = dplyr::tibble(oldCohortId = 1, newCohortId = 10),
      eraconstructorpad = 30,
      purgeConflicts = FALSE
    )
  )

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = paste0(
      "
      DROP TABLE IF EXISTS @temp_table_name;
      DROP TABLE IF EXISTS @cohort_database_schema.@table_name;"
    ),
    cohort_database_schema = cohortDatabaseSchema,
    table_name = tableName,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    temp_table_name = tempTableName
  )

  DatabaseConnector::disconnect(connection)
  testthat::expect_true(object = all.equal(target = dataPostEraFyWithEraPad, current = cohortExpectedEraPad))

  DatabaseConnector::renderTranslateExecuteSql(
    connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
    sql = "DROP TABLE IF EXISTS @cohort_database_schema.@table_temp;
           DROP TABLE IF EXISTS @cdm_database_schema.observation_period;",
    table_temp = tableName,
    cohort_database_schema = cohortDatabaseSchema,
    cdm_database_schema = cohortDatabaseSchema
  )
})
