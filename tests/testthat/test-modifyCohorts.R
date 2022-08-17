testthat::test_that("Testing Modify cohorts", {
  # generate unique name for a cohort table
  sysTime <- as.numeric(Sys.time()) * 100000
  tableName <- paste0("cr", sysTime)
  tempTableName <- paste0("#", tableName, "_1")

  # make up date for a cohort table
  # this cohort table will have two subjects * two cohorts, within the same cohort
  cohort <- dplyr::tibble(
    cohortDefinitionId = c(1, 3, 3, 5, 5),
    subjectId = c(1, 3, 3, 1, 3),
    cohortStartDate = c(
      as.Date("1999-01-01"),
      as.Date("2010-01-01"),
      as.Date("1999-01-15"),
      as.Date("1999-01-01"),
      as.Date("1999-01-01")
    ),
    cohortEndDate = c(
      as.Date("1999-01-31"),
      as.Date("2010-01-05"),
      as.Date("1999-01-25"),
      as.Date("1999-01-31"),
      as.Date("1999-01-31")
    )
  )

  observationPeriod <- dplyr::tibble(
    personId = c(1),
    observation_period_start_date = as.Date("1998-12-30"),
    observation_period_end_date = as.Date("2020-12-31")
  )

  person <- dplyr::tibble(
    personId = c(1, 3, 5),
    gender_concept_id = c(8507, 8532, 8532),
    year_of_birth = c(1990, 1930, 1930)
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
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = cohortDatabaseSchema,
    tableName = "person",
    data = person,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = FALSE,
    camelCaseToSnakeCase = TRUE,
    progressBar = FALSE
  )

  CohortAlgebra::modifyCohort(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = tableName,
    oldCohortId = 1,
    newCohortId = 2,
    cohortStartCensorDate = as.Date("1999-01-05"),
    cohortEndCensorDate = as.Date("1999-01-25")
  )

  cohortExpected <- dplyr::tibble(
    cohortDefinitionId = c(2),
    subjectId = c(1),
    cohortStartDate = c(as.Date("1999-01-05")),
    cohortEndDate = c(as.Date("1999-01-25"))
  )

  cohortObserved <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @cohort_database_schema.@table_name
        WHERE cohort_definition_id = 2
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


  # test for range date start ----
  # should error because purgeConflicts if FALSE
  testthat::expect_error(
    CohortAlgebra::modifyCohort(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = tableName,
      oldCohortId = 3,
      newCohortId = 5,
      cohortStartFilterRange = c(as.Date("1998-01-01"), as.Date("1999-12-31")),
      purgeConflicts = FALSE
    )
  )

  CohortAlgebra::modifyCohort(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = tableName,
    oldCohortId = 3,
    newCohortId = 2,
    cohortStartFilterRange = c(as.Date("1998-01-01"), as.Date("1999-12-31")),
    purgeConflicts = TRUE
  )

  cohortExpected <- dplyr::tibble(
    cohortDefinitionId = c(2),
    subjectId = c(3),
    cohortStartDate = c(as.Date("1999-01-15")),
    cohortEndDate = c(as.Date("1999-01-25"))
  )

  cohortObserved <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @cohort_database_schema.@table_name
        WHERE cohort_definition_id = 2
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



  # test for range date end ----
  CohortAlgebra::modifyCohort(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = tableName,
    oldCohortId = 3,
    newCohortId = 2,
    cohortEndFilterRange = c(as.Date("2010-01-01"), as.Date("2010-01-09")),
    purgeConflicts = TRUE
  )

  cohortExpected <- dplyr::tibble(
    cohortDefinitionId = c(2),
    subjectId = c(3),
    cohortStartDate = as.Date("2010-01-01"),
    cohortEndDate = as.Date("2010-01-05")
  )

  cohortObserved <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @cohort_database_schema.@table_name
        WHERE cohort_definition_id = 2
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

  # test era pad ----
  testthat::expect_error(
    CohortAlgebra::modifyCohort(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cdmDatabaseSchema = NULL,
      cohortTable = tableName,
      oldCohortId = 1,
      newCohortId = 2,
      purgeConflicts = TRUE,
      cohortStartPadDays = -10,
      cohortEndPadDays = 5
    )
  )

  ## test 1 era pad ----
  CohortAlgebra::modifyCohort(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cohortDatabaseSchema,
    cohortTable = tableName,
    oldCohortId = 1,
    newCohortId = 2,
    purgeConflicts = TRUE,
    cohortStartPadDays = -10,
    cohortEndPadDays = 5
  )

  cohortExpected <- dplyr::tibble(
    cohortDefinitionId = c(2),
    subjectId = c(1),
    cohortStartDate = c(as.Date("1998-12-30")),
    cohortEndDate = as.Date("1999-02-05")
  )

  cohortObserved <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @cohort_database_schema.@table_name
        WHERE cohort_definition_id = 2
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

  ## test 2 era pad ----
  CohortAlgebra::modifyCohort(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cohortDatabaseSchema,
    cohortTable = tableName,
    oldCohortId = 1,
    newCohortId = 2,
    purgeConflicts = TRUE,
    cohortStartPadDays = -1,
    cohortEndPadDays = 500000
  )

  cohortExpected <- dplyr::tibble(
    cohortDefinitionId = c(2),
    subjectId = c(1),
    cohortStartDate = c(as.Date("1998-12-31")),
    cohortEndDate = as.Date("2020-12-31")
  )

  cohortObserved <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @cohort_database_schema.@table_name
        WHERE cohort_definition_id = 2
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



  # test filter by gender ----
  CohortAlgebra::modifyCohort(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cohortDatabaseSchema,
    cohortTable = tableName,
    oldCohortId = 5,
    newCohortId = 6,
    purgeConflicts = FALSE,
    filterGenderConceptId = 8507
  )

  cohortExpected <- dplyr::tibble(
    cohortDefinitionId = c(6),
    subjectId = c(1),
    cohortStartDate = c(as.Date("1999-01-01")),
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



  # test filter by age range ----
  CohortAlgebra::modifyCohort(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cohortDatabaseSchema,
    cohortTable = tableName,
    oldCohortId = 5,
    newCohortId = 7,
    purgeConflicts = FALSE,
    filterByAgeRange = c(5, 20)
  )

  cohortExpected <- dplyr::tibble(
    cohortDefinitionId = c(7),
    subjectId = c(1),
    cohortStartDate = c(as.Date("1999-01-01")),
    cohortEndDate = as.Date("1999-01-31")
  )

  cohortObserved <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @cohort_database_schema.@table_name
        WHERE cohort_definition_id = 7
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
  
  
  
  # test first occurrence ----
  CohortAlgebra::modifyCohort(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cohortDatabaseSchema,
    cohortTable = tableName,
    oldCohortId = 3,
    newCohortId = 8,
    firstOccurrence = TRUE,
    purgeConflicts = TRUE
  )
  
  cohortExpected <- dplyr::tibble(
    cohortDefinitionId = c(8),
    subjectId = c(3),
    cohortStartDate = c(
      as.Date("1999-01-15")
    ),
    cohortEndDate = c(
      as.Date("1999-01-25")
    )
  )
  
  cohortObserved <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @cohort_database_schema.@table_name
        WHERE cohort_definition_id = 8
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

  # test with new connection
  CohortAlgebra::modifyCohort(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = tableName,
    oldCohortId = 1
  )

  DatabaseConnector::renderTranslateExecuteSql(
    connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
    sql = "DROP TABLE IF EXISTS @cohort_database_schema.@table_temp;
           DROP TABLE IF EXISTS @cdm_database_schema.observation_period;",
    table_temp = tableName,
    cohort_database_schema = cohortDatabaseSchema,
    cdm_database_schema = cohortDatabaseSchema
  )
})
