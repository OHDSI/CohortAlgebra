test_that("Testing cohort era fy", {
  cohort <- dplyr::tibble(
    cohortDefinitionId = c(1, 1),
    subjectId = c(1, 1),
    cohortStartDate = c(as.Date("1999-01-15"), as.Date("1999-01-20")),
    cohortEndDate = c(as.Date("1999-01-31"), as.Date("1999-02-15"))
  )

  sysTime <- as.numeric(Sys.time()) * 100000
  tableName <- paste0("cr", sysTime)

  DatabaseConnector::insertTable(
    connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
    databaseSchema = cohortDatabaseSchema,
    tableName = tableName,
    data = cohort,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = FALSE,
    camelCaseToSnakeCase = TRUE,
    progressBar = FALSE
  )

  # should not throw error
  eraFyCohort(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = NULL,
    cohortTable = tableName,
    oldToNewCohortId = dplyr::tibble(oldCohortId = 1, newCohortId = 2),
    eraconstructorpad = 0,
    purgeConflicts = TRUE
  )

  dataPostEraFy <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
      sql = paste0(
        "SELECT * FROM main.@table_name where cohort_definition_id = 2;"
      ),
      table_name = tableName,
      snakeCaseToCamelCase = TRUE
    )

  testthat::expect_equal(
    object = nrow(dataPostEraFy),
    expected = 1
  ) # era fy logic should collapse to 1 row
})
