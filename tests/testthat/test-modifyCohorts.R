testthat::test_that("Testing Modify cohorts", {
  # generate unique name for a cohort table
  sysTime <- as.numeric(Sys.time()) * 100000
  tableName <- paste0("cr", sysTime)
  tempTableName <- paste0("#", tableName, "_1")
  
  # make up date for a cohort table
  # this cohort table will have two subjects * two cohorts, within the same cohort
  cohort <- dplyr::tibble(
    cohortDefinitionId = c(1, 3, 3),
    subjectId = c(1, 3, 3),
    cohortStartDate = c(
      as.Date("1999-01-01"),
      as.Date("2010-01-01"),
      as.Date("1999-01-15")
    ),
    cohortEndDate = c(
      as.Date("1999-01-31"),
      as.Date("2010-01-05"),
      as.Date("1999-01-25")
    )
  )
  
  observationPeriod <- dplyr::tibble(
    personId = c(3),
    observation_period_start_date = as.Date("1979-01-01"),
    observation_period_end_date = as.Date("2020-12-31")
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
  
  CohortAlgebra:::modifyCohort(
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
  
  testthat::expect_equal(object = cohortObserved %>%
                           nrow(),
                         expected = 1)
  testthat::expect_true(object = all.equal(target = cohortExpected, current = cohortObserved))
  
  
  # test for range date start ----
  CohortAlgebra:::modifyCohort(
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
  
  testthat::expect_equal(object = cohortObserved %>%
                           nrow(),
                         expected = 1)
  testthat::expect_true(object = all.equal(target = cohortExpected, current = cohortObserved))
  
  
  
  # test for range date end ----
  CohortAlgebra:::modifyCohort(
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
  
  testthat::expect_equal(object = cohortObserved %>%
                           nrow(),
                         expected = 1)
  testthat::expect_true(object = all.equal(target = cohortExpected, current = cohortObserved))


# 
#   debug(CohortAlgebra:::modifyCohort)
#   # test era pad ----
#   testthat::expect_error(
#     CohortAlgebra:::modifyCohort(
#       connection = connection,
#       cohortDatabaseSchema = cohortDatabaseSchema,
#       cdmDatabaseSchema = NULL,
#       cohortTable = tableName,
#       oldCohortId = 1,
#       newCohortId = 2,
#       purgeConflicts = TRUE,
#       cohortStartPadDays = -10,
#       cohortEndPadDays = 5
#     )
#   )
# 
#   CohortAlgebra:::modifyCohort(
#     connection = connection,
#     cohortDatabaseSchema = cohortDatabaseSchema,
#     cdmDatabaseSchema = cdmDatabaseSchema,
#     cohortTable = tableName,
#     oldCohortId = 1,
#     newCohortId = 2,
#     purgeConflicts = TRUE,
#     cohortStartPadDays = -10,
#     cohortEndPadDays = 5
#   )
# 
# 
#   cohortExpected <- dplyr::tibble(
#     cohortDefinitionId = c(2),
#     subjectId = c(1),
#     cohortStartDate = c(as.Date("1998-12-21")),
#     cohortEndDate = as.Date("1999-02-05")
#   )
# 
#   cohortObserved <-
#     DatabaseConnector::renderTranslateQuerySql(
#       connection = connection,
#       sql = paste0(
#         "SELECT * FROM @cohort_database_schema.@table_name
#         WHERE cohort_definition_id = 2
#         order by cohort_definition_id, subject_id, cohort_start_date;"
#       ),
#       cohort_database_schema = cohortDatabaseSchema,
#       table_name = tableName,
#       snakeCaseToCamelCase = TRUE
#     ) %>%
#     dplyr::tibble()
# 
#   testthat::expect_equal(
#     object = cohortObserved %>%
#       nrow(),
#     expected = 1
#   )
#   testthat::expect_true(object = all.equal(target = cohortExpected, current = cohortObserved))

  
  
})
