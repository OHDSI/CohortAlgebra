# testthat::test_that("Testing keep cohort overlaps", {
#   # generate unique name for a cohort table
#   sysTime <- as.numeric(Sys.time()) * 100000
#   tableName <- paste0("cr", sysTime)
#   tempTableName <- paste0("#", tableName, "_1")
# 
#   # make up date for a cohort table
#   cohort <- dplyr::tibble(
#     cohortDefinitionId = c(1, 1, 2, 3),
#     subjectId = c(1, 1, 1, 1),
#     cohortStartDate = c(
#       as.Date("1999-01-01"),
#       as.Date("2000-01-01"),
#       as.Date("1998-01-01"),
#       as.Date("1998-11-08")
#     ),
#     cohortEndDate = c(
#       as.Date("1999-01-31"),
#       as.Date("2000-01-31"),
#       as.Date("1998-01-31"),
#       as.Date("1999-01-20")
#     )
#   ) |>
#     dplyr::arrange(
#       .data$subjectId,
#       .data$cohortDefinitionId,
#       .data$cohortStartDate
#     )
# 
#   # upload table
#   connection <-
#     DatabaseConnector::connect(connectionDetails = connectionDetails)
#   DatabaseConnector::insertTable(
#     connection = connection,
#     databaseSchema = cohortDatabaseSchema,
#     tableName = tableName,
#     data = cohort,
#     dropTableIfExists = TRUE,
#     createTable = TRUE,
#     tempTable = FALSE,
#     camelCaseToSnakeCase = TRUE,
#     progressBar = FALSE
#   )
# 
#   # should not throw error
#   CohortAlgebra::keepCohortOverlaps(
#     connection = connection,
#     cohortDatabaseSchema = cohortDatabaseSchema,
#     cohortTable = tableName,
#     firstCohortId = 1,
#     secondCohortId = 3,
#     newCohortId = 9,
#     purgeConflicts = FALSE,
#     minimumOverlapDays = 1,
#     offsetCohortStartDate = 0,
#     offsetCohortEndDate = 0,
#     tempEmulationSchema = tempEmulationSchema
#   )
# 
#   cohortObservered <-
#     DatabaseConnector::renderTranslateQuerySql(
#       connection = connection,
#       sql = paste0(
#         "SELECT * FROM @cohort_database_schema.@table_name
#         where cohort_definition_id = 9
#         order by cohort_definition_id, subject_id, cohort_start_date;"
#       ),
#       cohort_database_schema = cohortDatabaseSchema,
#       table_name = tableName,
#       snakeCaseToCamelCase = TRUE
#     ) |>
#     dplyr::tibble()
# 
#   testthat::expect_equal(
#     object = nrow(cohortObservered),
#     expected = 1
#   )
# 
#   # create the expected output data frame object to compare
#   cohortExpected <- dplyr::tibble(
#     cohortDefinitionId = c(9),
#     subjectId = c(1),
#     cohortStartDate = c(as.Date("1999-01-01")),
#     cohortEndDate = c(as.Date("1999-01-31"))
#   )
# 
#   testthat::expect_true(object = all(cohortObservered == cohortExpected))
# 
#   # this should throw error as there is already a cohort with cohort_definition_id = 9
#   testthat::expect_error(
#     CohortAlgebra::keepCohortOverlaps(
#       connection = connection,
#       cohortDatabaseSchema = cohortDatabaseSchema,
#       cohortTable = tableName,
#       firstCohortId = 1,
#       secondCohortId = 3,
#       newCohortId = 9,
#       purgeConflicts = FALSE,
#       minimumOverlapDays = 1,
#       offsetCohortStartDate = 0,
#       offsetCohortEndDate = 0,
#       tempEmulationSchema = tempEmulationSchema
#     )
#   )
# 
#   # repeat after purging conflicts
#   CohortAlgebra::keepCohortOverlaps(
#     connection = connection,
#     cohortDatabaseSchema = cohortDatabaseSchema,
#     cohortTable = tableName,
#     firstCohortId = 1,
#     secondCohortId = 3,
#     newCohortId = 9,
#     purgeConflicts = TRUE,
#     minimumOverlapDays = 1,
#     offsetCohortStartDate = 0,
#     offsetCohortEndDate = 0,
#     tempEmulationSchema = tempEmulationSchema
#   )
# 
#   cohortObservered <-
#     DatabaseConnector::renderTranslateQuerySql(
#       connection = connection,
#       sql = paste0(
#         "SELECT * FROM @cohort_database_schema.@table_name
#         where cohort_definition_id = 9
#         order by cohort_definition_id, subject_id, cohort_start_date;"
#       ),
#       cohort_database_schema = cohortDatabaseSchema,
#       table_name = tableName,
#       snakeCaseToCamelCase = TRUE
#     ) |>
#     dplyr::tibble()
# 
#   testthat::expect_equal(
#     object = nrow(cohortObservered),
#     expected = 1
#   )
# 
#   # create the expected output data frame object to compare
#   cohortExpected <- dplyr::tibble(
#     cohortDefinitionId = c(9),
#     subjectId = c(1),
#     cohortStartDate = c(as.Date("1999-01-01")),
#     cohortEndDate = c(as.Date("1999-01-31"))
#   )
# 
#   testthat::expect_true(object = all(cohortObservered == cohortExpected))
# 
# 
#   # should throw warning - check for NULL result
#   testthat::expect_warning(
#     CohortAlgebra::keepCohortOverlaps(
#       connection = connection,
#       cohortDatabaseSchema = cohortDatabaseSchema,
#       cohortTable = tableName,
#       firstCohortId = 1,
#       secondCohortId = 1,
#       newCohortId = 10,
#       purgeConflicts = TRUE,
#       minimumOverlapDays = 1,
#       offsetCohortStartDate = 0,
#       offsetCohortEndDate = 0,
#       tempEmulationSchema = tempEmulationSchema
#     )
#   )
# 
#   # with large offset
#   CohortAlgebra::keepCohortOverlaps(
#     connection = connection,
#     cohortDatabaseSchema = cohortDatabaseSchema,
#     cohortTable = tableName,
#     firstCohortId = 1,
#     secondCohortId = 2,
#     newCohortId = 11,
#     purgeConflicts = TRUE,
#     minimumOverlapDays = 1,
#     offsetCohortStartDate = -600,
#     offsetCohortEndDate = 0,
#     tempEmulationSchema = tempEmulationSchema
#   )
# 
#   cohortObservered <-
#     DatabaseConnector::renderTranslateQuerySql(
#       connection = connection,
#       sql = paste0(
#         "SELECT * FROM @cohort_database_schema.@table_name
#         where cohort_definition_id = 11
#         order by cohort_definition_id, subject_id, cohort_start_date;"
#       ),
#       cohort_database_schema = cohortDatabaseSchema,
#       table_name = tableName,
#       snakeCaseToCamelCase = TRUE
#     ) |>
#     dplyr::tibble()
# 
#   testthat::expect_equal(
#     object = nrow(cohortObservered),
#     expected = 1
#   )
# 
#   # create the expected output data frame object to compare
#   cohortExpected <- dplyr::tibble(
#     cohortDefinitionId = c(11),
#     subjectId = c(1),
#     cohortStartDate = c(as.Date("1999-01-01")),
#     cohortEndDate = c(as.Date("1999-01-31"))
#   )
# 
#   testthat::expect_true(object = all(cohortObservered == cohortExpected))
# 
# 
# 
#   # with no offset
#   CohortAlgebra::keepCohortOverlaps(
#     connection = connection,
#     cohortDatabaseSchema = cohortDatabaseSchema,
#     cohortTable = tableName,
#     firstCohortId = 1,
#     secondCohortId = 2,
#     newCohortId = 12,
#     purgeConflicts = TRUE,
#     minimumOverlapDays = 1,
#     offsetCohortStartDate = 0,
#     offsetCohortEndDate = 0,
#     tempEmulationSchema = tempEmulationSchema
#   )
# 
#   cohortObservered <-
#     DatabaseConnector::renderTranslateQuerySql(
#       connection = connection,
#       sql = paste0(
#         "SELECT * FROM @cohort_database_schema.@table_name
#         where cohort_definition_id = 12
#         order by cohort_definition_id, subject_id, cohort_start_date;"
#       ),
#       cohort_database_schema = cohortDatabaseSchema,
#       table_name = tableName,
#       snakeCaseToCamelCase = TRUE
#     ) |>
#     dplyr::tibble()
# 
#   testthat::expect_equal(
#     object = nrow(cohortObservered),
#     expected = 0
#   )
# 
#   # create the expected output data frame object to compare
#   cohortExpected <- cohortExpected |>
#     dplyr::slice(0)
# 
#   testthat::expect_true(object = all(cohortObservered == cohortExpected))
# 
#   DatabaseConnector::disconnect(connection)
# 
#   # check if establishes connection
#   # also check if count of days workds - 20 vs 21
#   CohortAlgebra::keepCohortOverlaps(
#     connectionDetails = connectionDetails,
#     cohortDatabaseSchema = cohortDatabaseSchema,
#     cohortTable = tableName,
#     firstCohortId = 1,
#     secondCohortId = 3,
#     newCohortId = 16,
#     purgeConflicts = FALSE,
#     minimumOverlapDays = 20,
#     # the cohort has 20 days, 21 days should give 0 records
#     offsetCohortStartDate = 0,
#     offsetCohortEndDate = 0,
#     tempEmulationSchema = tempEmulationSchema
#   )
# 
#   cohortObservered <-
#     DatabaseConnector::renderTranslateQuerySql(
#       connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
#       sql = paste0(
#         "SELECT * FROM @cohort_database_schema.@table_name
#         where cohort_definition_id = 16
#         order by cohort_definition_id, subject_id, cohort_start_date;"
#       ),
#       cohort_database_schema = cohortDatabaseSchema,
#       table_name = tableName,
#       snakeCaseToCamelCase = TRUE
#     ) |>
#     dplyr::tibble()
# 
#   testthat::expect_equal(
#     object = nrow(cohortObservered),
#     expected = 1
#   )
# 
#   # create the expected output data frame object to compare
#   cohortExpected <- cohort |>
#     dplyr::filter(
#       subjectId == 1,
#       cohortDefinitionId == 1
#     ) |>
#     dplyr::filter(cohortStartDate == as.Date("1999-01-01")) |>
#     dplyr::mutate(cohortDefinitionId = 16)
# 
#   testthat::expect_true(object = all(cohortObservered == cohortExpected))
# 
# 
#   # check if establishes connection
#   # also check if using more than occurrence works
#   CohortAlgebra::keepCohortOverlaps(
#     connectionDetails = connectionDetails,
#     cohortDatabaseSchema = cohortDatabaseSchema,
#     cohortTable = tableName,
#     firstCohortId = 1,
#     secondCohortId = 3,
#     newCohortId = 17,
#     purgeConflicts = FALSE,
#     minimumOverlapDays = 21,
#     # the cohort has 20 days, 21 days should give 0 records
#     offsetCohortStartDate = 0,
#     offsetCohortEndDate = 0,
#     tempEmulationSchema = tempEmulationSchema
#   )
# 
#   cohortObservered <-
#     DatabaseConnector::renderTranslateQuerySql(
#       connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
#       sql = paste0(
#         "SELECT * FROM @cohort_database_schema.@table_name
#         where cohort_definition_id = 17
#         order by cohort_definition_id, subject_id, cohort_start_date;"
#       ),
#       cohort_database_schema = cohortDatabaseSchema,
#       table_name = tableName,
#       snakeCaseToCamelCase = TRUE
#     ) |>
#     dplyr::tibble()
# 
#   testthat::expect_equal(
#     object = nrow(cohortObservered),
#     expected = 0
#   )
# 
#   # create the expected output data frame object to compare
#   cohortExpected <- cohortExpected |>
#     dplyr::slice(0)
# 
#   testthat::expect_true(object = all(cohortObservered == cohortExpected))
# })
