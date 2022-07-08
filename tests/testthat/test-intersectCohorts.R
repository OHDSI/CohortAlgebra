testthat::test_that("Testing cohort intersect", {
  # generate unique name for a cohort table
  sysTime <- as.numeric(Sys.time()) * 100000
  tableName <- paste0("cr", sysTime)
  tempTableName <- paste0("#", tableName, "_1")
  
  # make up date for a cohort table
  # this cohort table will have two subjects * three cohorts, with subject 2 not present in cohort id 3
  cohort <- dplyr::tibble(
    cohortDefinitionId = c(1, 2, 3, 1, 2),
    subjectId = c(1, 1, 1, 2, 2),
    cohortStartDate = c(
      as.Date("1999-01-01"),
      as.Date("1999-01-20"),
      as.Date("1999-01-15"),
      as.Date("1999-01-20"),
      as.Date("1999-01-15")
    ),
    cohortEndDate = c(
      as.Date("1999-01-31"),
      as.Date("1999-02-28"),
      as.Date("1999-01-31"),
      as.Date("1999-02-28"),
      as.Date("1999-01-31")
    )
  ) %>%
    dplyr::arrange(.data$subjectId,
                   .data$cohortDefinitionId,
                   .data$cohortStartDate)
  
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
  CohortAlgebra::intersectCohorts(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = tableName,
    cohortIds = c(1, 2, 3),
    newCohortId = 9,
    purgeConflicts = FALSE
  )
  
  # extract the generated output and compare to expected
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)
  dataPostIntersect <-
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
  
  testthat::expect_equal(object = nrow(dataPostIntersect),
                         expected = 1) # era fy logic should collapse to 1 row
  
  # create the expected output data frame object to compare
  cohortExpected <- dplyr::tibble(
    cohortDefinitionId = c(9),
    subjectId = c(1),
    cohortStartDate = c(as.Date("1999-01-20")),
    cohortEndDate = c(as.Date("1999-01-31"))
  )
  
  testthat::expect_true(object = all(dataPostIntersect == cohortExpected))
  
  # this should throw error as there is already a cohort with cohort_definition_id = 9
  testthat::expect_error(
    CohortAlgebra::intersectCohorts(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = tableName,
      cohortIds = c(1, 2, 3),
      newCohortId = 9,
      purgeConflicts = FALSE
    )
  )
  
  # this should NOT throw error as we will purge conflicts.
  # it should return a message
  testthat::expect_message(
    object =
      CohortAlgebra::intersectCohorts(
        connectionDetails = connectionDetails,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = tableName,
        cohortIds = c(1, 2, 3),
        newCohortId = 9,
        purgeConflicts = TRUE
      )
  )
  
  # check on temporary table
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = paste0(
      "
      DROP TABLE IF EXISTS @temp_table_name;
      SELECT *
      INTO @temp_table_name
      FROM @cohort_database_schema.@table_name
      WHERE cohort_definition_id IN (1,2, 3);"
    ),
    cohort_database_schema = cohortDatabaseSchema,
    table_name = tableName,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    temp_table_name = tempTableName
  )
  
  CohortAlgebra::intersectCohorts(
    connection = connection,
    cohortTable = tempTableName,
    cohortIds = c(1, 2, 3),
    newCohortId = 9,
    purgeConflicts = FALSE
  )
  
  dataPostIntersectTemp <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @table_name
        where cohort_definition_id = 9
        order by cohort_definition_id, subject_id, cohort_start_date;"
      ),
      table_name = tempTableName,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble()
  
  # this should throw error as there is already a cohort with cohort_definition_id = 9
  testthat::expect_error(
    CohortAlgebra::intersectCohorts(
      connection = connection,
      cohortTable = tempTableName,
      cohortIds = c(1, 2, 3),
      newCohortId = 9,
      purgeConflicts = FALSE
    )
  )
  
  testthat::expect_message(
    CohortAlgebra::intersectCohorts(
      connection = connection,
      cohortTable = tempTableName,
      cohortIds = c(1, 2, 3),
      newCohortId = 9,
      purgeConflicts = TRUE
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
  testthat::expect_true(object = all(dataPostIntersectTemp == cohortExpected))
  
})