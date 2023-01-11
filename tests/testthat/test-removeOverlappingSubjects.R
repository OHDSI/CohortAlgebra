testthat::test_that("Testing Remove Subjects from cohorts", {
  # generate unique name for a cohort table
  sysTime <- as.numeric(Sys.time()) * 100000
  tableName <- paste0("cr", sysTime)
  tempTableName <- paste0("#", tableName, "_1")

  # make up date for a cohort table
  cohort <- dplyr::tibble(
    cohortDefinitionId = c(1, 1, 3, 5),
    subjectId = c(1, 2, 2, 2),
    cohortStartDate = c(
      as.Date("1999-01-01"),
      as.Date("2010-01-01"),
      as.Date("1999-01-15"),
      as.Date("1999-01-01")
    ),
    cohortEndDate = c(
      as.Date("1999-01-31"),
      as.Date("2010-01-05"),
      as.Date("1999-01-25"),
      as.Date("1999-01-31")
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

  removeOverlappingSubjects(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    offsetCohortStartDate = -99999,
    offsetCohortEndDate = 99999,
    tempEmulationSchema = tempEmulationSchema,
    cohortId = 1,
    newCohortId = 6,
    cohortsWithSubjectsToRemove = c(3),
    purgeConflicts = FALSE,
    cohortTable = tableName
  )

  cohortExpected <- dplyr::tibble(
    cohortDefinitionId = c(6),
    subjectId = c(1),
    cohortStartDate = as.Date("1999-01-01"),
    cohortEndDate = as.Date("1999-01-31")
  )

  cohortObserved <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @cohort_database_schema.@table_name
        WHERE cohort_definition_id = 6
        order by cohort_definition_id, subject_id, cohort_start_date;"
      ),
      cohort_database_schema = cohortDatabaseSchema,
      table_name = tableName,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble()

  testthat::expect_equal(
    object = cohortObserved %>%
      nrow(),
    expected = 1
  )
  testthat::expect_true(object = all.equal(target = cohortExpected, current = cohortObserved))

  ####################

  removeOverlappingSubjects(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortId = 1,
    newCohortId = 11,
    cohortsWithSubjectsToRemove = c(5),
    purgeConflicts = FALSE,
    offsetCohortStartDate = 0,
    offsetCohortEndDate = 0,
    cohortTable = tableName
  )

  cohortExpected <- cohort %>%
    dplyr::filter(cohortDefinitionId == 1)

  cohortObserved <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @cohort_database_schema.@table_name
        WHERE cohort_definition_id = 11
        order by cohort_definition_id, subject_id, cohort_start_date;"
      ),
      cohort_database_schema = cohortDatabaseSchema,
      table_name = tableName,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble()

  testthat::expect_equal(
    object = cohortObserved %>%
      nrow(),
    expected = 2
  )
  testthat::expect_true(object = all.equal(
    target = cohortExpected,
    current = cohortObserved %>%
      dplyr::mutate(cohortDefinitionId = 1)
  ))


  ####################


  testthat::expect_error(
    removeOverlappingSubjects(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortId = 1,
      newCohortId = 1,
      cohortsWithSubjectsToRemove = c(3),
      purgeConflicts = FALSE,
      cohortTable = tableName
    )
  )

  removeOverlappingSubjects(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortId = 1,
    newCohortId = 1,
    cohortsWithSubjectsToRemove = c(3),
    purgeConflicts = TRUE,
    cohortTable = tableName
  )

  cohortExpected <- dplyr::tibble(
    cohortDefinitionId = c(1),
    subjectId = c(1),
    cohortStartDate = as.Date("1999-01-01"),
    cohortEndDate = as.Date("1999-01-31")
  )

  cohortObserved <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @cohort_database_schema.@table_name
        WHERE cohort_definition_id = 1
        order by cohort_definition_id, subject_id, cohort_start_date;"
      ),
      cohort_database_schema = cohortDatabaseSchema,
      table_name = tableName,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble()

  testthat::expect_equal(
    object = cohortObserved %>%
      nrow(),
    expected = 1
  )
  testthat::expect_true(object = all.equal(target = cohortExpected, current = cohortObserved))

  DatabaseConnector::disconnect(connection = connection)


  #######################################
  removeOverlappingSubjects(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortId = 5,
    newCohortId = 7,
    cohortsWithSubjectsToRemove = c(3),
    purgeConflicts = TRUE,
    cohortTable = tableName
  )

  cohortExpected <- cohort %>%
    dplyr::slice(0)

  cohortObserved <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
      sql = "SELECT * FROM @cohort_database_schema.@table_name
             WHERE cohort_definition_id = 7
             order by cohort_definition_id, subject_id, cohort_start_date;",
      cohort_database_schema = cohortDatabaseSchema,
      table_name = tableName,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble()

  testthat::expect_equal(
    object = cohortObserved %>%
      nrow(),
    expected = 0
  )
  testthat::expect_true(object = all.equal(target = cohortExpected, current = cohortObserved))

  DatabaseConnector::renderTranslateExecuteSql(
    connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
    sql = "DROP TABLE IF EXISTS @cohort_database_schema.@table_temp;",
    table_temp = tableName,
    cohort_database_schema = cohortDatabaseSchema
  )
})
