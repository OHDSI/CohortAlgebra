testthat::test_that("Testing cohort union", {
  # generate unique name for a cohort table
  sysTime <- as.numeric(Sys.time()) * 100000
  tableName <- paste0("cr", sysTime)
  tableName1 <-
    CohortGenerator::getCohortTableNames(cohortTable = paste0(tableName, 1))
  tableName2 <-
    CohortGenerator::getCohortTableNames(cohortTable = paste0(tableName, 2))
  
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
  ) %>%
    dplyr::arrange(cohortDefinitionId,
                   subjectId,
                   cohortStartDate,
                   cohortEndDate)
  
  # upload table
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)
  
  CohortGenerator::createCohortTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = tableName1
  )
  CohortGenerator::dropCohortStatsTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = tableName1
  )
  CohortGenerator::createCohortTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = tableName2
  )
  CohortGenerator::dropCohortStatsTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = tableName2
  )
  
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = cohortDatabaseSchema,
    tableName = tableName1$cohortTable,
    data = cohort,
    dropTableIfExists = FALSE,
    createTable = FALSE,
    tempTable = FALSE,
    camelCaseToSnakeCase = TRUE,
    progressBar = FALSE
  )
  
  copyCohorts(
    connection = connection,
    oldToNewCohortId = dplyr::tibble(oldCohortId = c(1, 2),
                                     newCohortId = c(1, 2)),
    sourceCohortDatabaseSchema = cohortDatabaseSchema,
    targetCohortDatabaseSchema = cohortDatabaseSchema,
    sourceCohortTable = tableName1$cohortTable,
    targetCohortTable = tableName2$cohortTable,
    purgeConflicts = FALSE,
    tempEmulationSchema = tempEmulationSchema
  )
  
  tempTable2Data <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT * FROM @cohort_database_schema.@cohort
            ORDER BY cohort_definition_id, subject_id, cohort_start_date, cohort_end_date;",
    cohort_database_schema = cohortDatabaseSchema,
    cohort = tableName2$cohortTable,
    snakeCaseToCamelCase = TRUE,
    tempEmulationSchema = tempEmulationSchema
  ) %>%
    dplyr::tibble()
  
  testthat::expect_equal(object = nrow(tempTable2Data),
                         expected = nrow(cohort))
  
  testthat::expect_identical(object = tempTable2Data,
                             expected = cohort)
  
  testthat::expect_error(
    copyCohorts(
      connection = connection,
      oldToNewCohortId = dplyr::tibble(oldCohortId = c(1, 2),
                                       newCohortId = c(1, 2)),
      sourceCohortDatabaseSchema = cohortDatabaseSchema,
      targetCohortDatabaseSchema = cohortDatabaseSchema,
      sourceCohortTable = tableName1$cohortTable,
      targetCohortTable = tableName2$cohortTable,
      purgeConflicts = FALSE,
      tempEmulationSchema = tempEmulationSchema
    )
  )
  
  copyCohorts(
    connection = connection,
    oldToNewCohortId = dplyr::tibble(oldCohortId = c(1, 2),
                                     newCohortId = c(4, 5)),
    sourceCohortDatabaseSchema = cohortDatabaseSchema,
    targetCohortDatabaseSchema = cohortDatabaseSchema,
    sourceCohortTable = tableName1$cohortTable,
    targetCohortTable = tableName2$cohortTable,
    purgeConflicts = FALSE,
    tempEmulationSchema = tempEmulationSchema
  )
  
  copyCohorts(
    connection = connection,
    oldToNewCohortId = dplyr::tibble(oldCohortId = c(1, 2),
                                     newCohortId = c(4, 5)),
    sourceCohortDatabaseSchema = cohortDatabaseSchema,
    targetCohortDatabaseSchema = cohortDatabaseSchema,
    sourceCohortTable = tableName1$cohortTable,
    targetCohortTable = tableName2$cohortTable,
    purgeConflicts = TRUE,
    tempEmulationSchema = tempEmulationSchema
  )
  
  DatabaseConnector::disconnect(connection = connection)
  
  copyCohorts(
    connectionDetails = connectionDetails,
    oldToNewCohortId = dplyr::tibble(oldCohortId = c(1, 2),
                                     newCohortId = c(6, 7)),
    sourceCohortDatabaseSchema = cohortDatabaseSchema,
    targetCohortDatabaseSchema = cohortDatabaseSchema,
    sourceCohortTable = tableName1$cohortTable,
    targetCohortTable = tableName2$cohortTable,
    purgeConflicts = FALSE,
    tempEmulationSchema = tempEmulationSchema
  )
  
})
