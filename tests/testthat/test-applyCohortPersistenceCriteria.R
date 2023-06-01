# testthat::test_that("Testing cohort persistence", {
#   # generate unique name for a cohort table
#   sysTime <- as.numeric(Sys.time()) * 100000
#   tableName <- paste0("cr", sysTime)
#   tempTableName <- paste0("#", tableName, "_1")
# 
#   # make up date for a cohort table
#   cohort <- dplyr::tibble(
#     cohortDefinitionId = c(1, 1, 1),
#     subjectId = c(1, 1, 2),
#     cohortStartDate = c(
#       as.Date("1999-01-01"),
#       as.Date("1999-02-15"),
#       as.Date("1999-03-10")
#     ),
#     cohortEndDate = c(
#       as.Date("1999-01-31"),
#       as.Date("1999-03-30"),
#       as.Date("1999-01-01")
#     )
#   )
# 
#   observationPeriod <- dplyr::tibble(
#     personId = c(1, 2),
#     observation_period_start_date = c(
#       as.Date("1999-01-01"),
#       as.Date("2000-01-01")
#     ),
#     observation_period_end_date = c(
#       as.Date("1999-03-04"),
#       as.Date("2010-01-01")
#     )
#   )
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
#   DatabaseConnector::insertTable(
#     connection = connection,
#     databaseSchema = cohortDatabaseSchema,
#     tableName = "observation_period",
#     data = observationPeriod,
#     dropTableIfExists = TRUE,
#     createTable = TRUE,
#     tempTable = FALSE,
#     camelCaseToSnakeCase = TRUE,
#     progressBar = FALSE
#   )
#   # disconnecting - as this is a test for a non temp cohort table
#   DatabaseConnector::disconnect(connection)
# 
#   applyCohortPersistenceCriteria(
#     connectionDetails = connectionDetails,
#     sourceCohortDatabaseSchema = cohortDatabaseSchema,
#     sourceCohortTable = tableName,
#     targetCohortDatabaseSchema = cohortDatabaseSchema,
#     targetCohortTable = tableName,
#     cdmDatabaseSchema = cohortDatabaseSchema,
#     oldCohortId = 1,
#     newCohortId = 10,
#     tillEndOfObservationPeriod = TRUE,
#     purgeConflicts = FALSE
#   )
# 
#   # extract the generated output and compare to expected
#   connection <-
#     DatabaseConnector::connect(connectionDetails = connectionDetails)
#   dataPost <-
#     DatabaseConnector::renderTranslateQuerySql(
#       connection = connection,
#       sql = paste0(
#         "SELECT * FROM @cohort_database_schema.@table_name
#         where cohort_definition_id = 10
#         order by cohort_definition_id, subject_id, cohort_start_date;"
#       ),
#       cohort_database_schema = cohortDatabaseSchema,
#       table_name = tableName,
#       snakeCaseToCamelCase = TRUE
#     ) %>%
#     dplyr::tibble()
# 
#   testthat::expect_equal(
#     object = nrow(dataPost),
#     expected = 1
#   )
#   expected <- dplyr::tibble(
#     cohortDefinitionId = 10,
#     subjectId = 1,
#     cohortStartDate = as.Date("1999-01-01"),
#     cohortEndDate = as.Date("1999-03-04")
#   )
#   testthat::expect_equal(
#     object = dataPost,
#     expected = expected
#   )
# 
#   # repeat for temp table
#   applyCohortPersistenceCriteria(
#     connection = connection,
#     sourceCohortDatabaseSchema = cohortDatabaseSchema,
#     sourceCohortTable = tableName,
#     targetCohortDatabaseSchema = cohortDatabaseSchema,
#     targetCohortTable = tableName,
#     cdmDatabaseSchema = cohortDatabaseSchema,
#     oldCohortId = 10,
#     newCohortId = 10,
#     tillEndOfObservationPeriod = TRUE,
#     purgeConflicts = TRUE
#   )
#   dataPost <-
#     DatabaseConnector::renderTranslateQuerySql(
#       connection = connection,
#       sql = paste0(
#         "SELECT * FROM @cohort_database_schema.@table_name
#         where cohort_definition_id = 10
#         order by cohort_definition_id, subject_id, cohort_start_date;"
#       ),
#       cohort_database_schema = cohortDatabaseSchema,
#       table_name = tableName,
#       snakeCaseToCamelCase = TRUE
#     ) %>%
#     dplyr::tibble()
# 
#   testthat::expect_equal(
#     object = nrow(dataPost),
#     expected = 1
#   )
#   expected <- dplyr::tibble(
#     cohortDefinitionId = 10,
#     subjectId = 1,
#     cohortStartDate = as.Date("1999-01-01"),
#     cohortEndDate = as.Date("1999-03-04")
#   )
#   testthat::expect_equal(
#     object = dataPost,
#     expected = expected
#   )
# 
#   testthat::expect_error(
#     applyCohortPersistenceCriteria(
#       connection = connection,
#       sourceCohortDatabaseSchema = cohortDatabaseSchema,
#       sourceCohortTable = tableName,
#       targetCohortDatabaseSchema = cohortDatabaseSchema,
#       targetCohortTable = tableName,
#       cdmDatabaseSchema = cohortDatabaseSchema,
#       oldCohortId = 1,
#       newCohortId = 10,
#       tillEndOfObservationPeriod = TRUE,
#       purgeConflicts = FALSE
#     )
#   )
# 
#   DatabaseConnector::disconnect(connection)
#   testthat::expect_error(
#     applyCohortPersistenceCriteria(
#       connectionDetails = connectionDetails,
#       sourceCohortDatabaseSchema = cohortDatabaseSchema,
#       sourceCohortTable = tableName,
#       targetCohortDatabaseSchema = cohortDatabaseSchema,
#       targetCohortTable = tableName,
#       cdmDatabaseSchema = cohortDatabaseSchema,
#       oldCohortId = 1,
#       newCohortId = 10,
#       purgeConflicts = FALSE
#     )
#   )
# 
#   testthat::expect_error(
#     applyCohortPersistenceCriteria(
#       connectionDetails = connectionDetails,
#       sourceCohortDatabaseSchema = cohortDatabaseSchema,
#       sourceCohortTable = tableName,
#       targetCohortDatabaseSchema = cohortDatabaseSchema,
#       targetCohortTable = tableName,
#       cdmDatabaseSchema = cohortDatabaseSchema,
#       oldCohortId = 1,
#       newCohortId = 10,
#       tillEndOfObservationPeriod = TRUE,
#       offsetCohortStartDate = 100,
#       purgeConflicts = FALSE
#     )
#   )
# 
# 
#   connection <-
#     DatabaseConnector::connect(connectionDetails = connectionDetails)
#   applyCohortPersistenceCriteria(
#     connectionDetails = connectionDetails,
#     sourceCohortDatabaseSchema = cohortDatabaseSchema,
#     sourceCohortTable = tableName,
#     targetCohortDatabaseSchema = cohortDatabaseSchema,
#     targetCohortTable = tableName,
#     cdmDatabaseSchema = cohortDatabaseSchema,
#     oldCohortId = 1,
#     newCohortId = 30,
#     offsetCohortStartDate = 2000,
#     purgeConflicts = TRUE
#   )
#   dataPost <-
#     DatabaseConnector::renderTranslateQuerySql(
#       connection = connection,
#       sql = paste0(
#         "SELECT * FROM @cohort_database_schema.@table_name
#         where cohort_definition_id = 30
#         order by cohort_definition_id, subject_id, cohort_start_date;"
#       ),
#       cohort_database_schema = cohortDatabaseSchema,
#       table_name = tableName,
#       snakeCaseToCamelCase = TRUE
#     ) %>%
#     dplyr::tibble()
# 
#   testthat::expect_equal(
#     object = nrow(dataPost),
#     expected = 1
#   )
#   expected <- dplyr::tibble(
#     cohortDefinitionId = 30,
#     subjectId = 1,
#     cohortStartDate = as.Date("1999-01-01"),
#     cohortEndDate = as.Date("1999-03-04")
#   )
#   testthat::expect_equal(
#     object = dataPost,
#     expected = expected
#   )
# 
# 
# 
#   applyCohortPersistenceCriteria(
#     connectionDetails = connectionDetails,
#     sourceCohortDatabaseSchema = cohortDatabaseSchema,
#     sourceCohortTable = tableName,
#     targetCohortDatabaseSchema = cohortDatabaseSchema,
#     targetCohortTable = tableName,
#     cdmDatabaseSchema = cohortDatabaseSchema,
#     oldCohortId = 1,
#     newCohortId = 31,
#     offsetCohortEndDate = 2000,
#     purgeConflicts = FALSE
#   )
# 
# 
#   dataPost <-
#     DatabaseConnector::renderTranslateQuerySql(
#       connection = connection,
#       sql = paste0(
#         "SELECT * FROM @cohort_database_schema.@table_name
#         where cohort_definition_id = 31
#         order by cohort_definition_id, subject_id, cohort_start_date;"
#       ),
#       cohort_database_schema = cohortDatabaseSchema,
#       table_name = tableName,
#       snakeCaseToCamelCase = TRUE
#     ) %>%
#     dplyr::tibble()
# 
#   testthat::expect_equal(
#     object = nrow(dataPost),
#     expected = 1
#   )
#   expected <- dplyr::tibble(
#     cohortDefinitionId = 31,
#     subjectId = 1,
#     cohortStartDate = as.Date("1999-01-01"),
#     cohortEndDate = as.Date("1999-03-04")
#   )
#   testthat::expect_equal(
#     object = dataPost,
#     expected = expected
#   )
# 
# 
#   applyCohortPersistenceCriteria(
#     connection = connection,
#     sourceCohortDatabaseSchema = cohortDatabaseSchema,
#     sourceCohortTable = tableName,
#     targetCohortDatabaseSchema = cohortDatabaseSchema,
#     targetCohortTable = tableName,
#     cdmDatabaseSchema = cohortDatabaseSchema,
#     oldCohortId = 1,
#     newCohortId = 33,
#     offsetCohortEndDate = 2,
#     purgeConflicts = FALSE
#   )
# 
#   dataPost <-
#     DatabaseConnector::renderTranslateQuerySql(
#       connection = connection,
#       sql = paste0(
#         "SELECT * FROM @cohort_database_schema.@table_name
#         where cohort_definition_id = 33
#         order by cohort_definition_id, subject_id, cohort_start_date;"
#       ),
#       cohort_database_schema = cohortDatabaseSchema,
#       table_name = tableName,
#       snakeCaseToCamelCase = TRUE
#     ) %>%
#     dplyr::tibble()
# 
#   testthat::expect_equal(
#     object = nrow(dataPost),
#     expected = 2
#   )
# 
#   DatabaseConnector::renderTranslateExecuteSql(
#     connection = connection,
#     sql = paste0(
#       "
#       DROP TABLE IF EXISTS @temp_table_name;
#       DROP TABLE IF EXISTS @cohort_database_schema.@table_name;"
#     ),
#     cohort_database_schema = cohortDatabaseSchema,
#     table_name = tableName,
#     profile = FALSE,
#     progressBar = FALSE,
#     reportOverallTime = FALSE,
#     temp_table_name = tempTableName
#   )
# 
#   DatabaseConnector::disconnect(connection)
# 
#   DatabaseConnector::renderTranslateExecuteSql(
#     connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
#     sql = "DROP TABLE IF EXISTS @cohort_database_schema.@table_temp;
#            DROP TABLE IF EXISTS @cdm_database_schema.observation_period;",
#     table_temp = tableName,
#     cohort_database_schema = cohortDatabaseSchema,
#     cdm_database_schema = cohortDatabaseSchema,
#     progressBar = FALSE,
#     reportOverallTime = FALSE
#   )
# })
