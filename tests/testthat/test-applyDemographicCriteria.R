# testthat::test_that("Testing filter cohort by calendar date", {
#   # generate unique name for a cohort table
#   sysTime <- as.numeric(Sys.time()) * 100000
#   tableName <- paste0("cr", sysTime)
#   tempTableName <- paste0("#", tableName, "_1")
#
#   cohort <- dplyr::tibble(
#     cohortDefinitionId = c(1, 1),
#     subjectId = c(1, 3),
#     cohortStartDate = c(
#       as.Date("1999-01-01"),
#       as.Date("2010-01-01")
#     ),
#     cohortEndDate = c(
#       as.Date("1999-01-31"),
#       as.Date("2010-01-05")
#     )
#   )
#
#   observationPeriod <- dplyr::tibble(
#     personId = c(1),
#     observation_period_start_date = as.Date("1998-12-30"),
#     observation_period_end_date = as.Date("2020-12-31")
#   )
#
#   person <- dplyr::tibble(
#     personId = c(1, 3),
#     gender_concept_id = c(8507, 8532),
#     year_of_birth = c(1995, 1900)
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
#
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
#   DatabaseConnector::insertTable(
#     connection = connection,
#     databaseSchema = cohortDatabaseSchema,
#     tableName = "person",
#     data = person,
#     dropTableIfExists = TRUE,
#     createTable = TRUE,
#     tempTable = FALSE,
#     camelCaseToSnakeCase = TRUE,
#     progressBar = FALSE
#   )
#   # disconnecting - as this is a test for a non temp cohort table
#   DatabaseConnector::disconnect(connection)
#
#   testthat::expect_error(
#     applyDemographicCriteria(
#       connectionDetails = connectionDetails,
#       cdmDatabaseSchema = cohortDatabaseSchema,
#       sourceCohortDatabaseSchema = cohortDatabaseSchema,
#       sourceCohortTable = tableName,
#       targetCohortDatabaseSchema = cohortDatabaseSchema,
#       targetCohortTable = tableName,
#       oldCohortId = 1,
#       newCohortId = 10,
#       purgeConflicts = FALSE
#     )
#   )
#
#   applyDemographicCriteria(
#     connectionDetails = connectionDetails,
#     cdmDatabaseSchema = cohortDatabaseSchema,
#     sourceCohortDatabaseSchema = cohortDatabaseSchema,
#     sourceCohortTable = tableName,
#     targetCohortDatabaseSchema = cohortDatabaseSchema,
#     targetCohortTable = tableName,
#     oldCohortId = 1,
#     newCohortId = 10,
#     filterGenderConceptId = c(8507),
#     purgeConflicts = FALSE
#   )
#
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
#     ) |>
#     dplyr::tibble()
#
#   testthat::expect_error(
#     applyDemographicCriteria(
#       connectionDetails = connectionDetails,
#       cdmDatabaseSchema = cohortDatabaseSchema,
#       sourceCohortDatabaseSchema = cohortDatabaseSchema,
#       sourceCohortTable = tableName,
#       targetCohortDatabaseSchema = cohortDatabaseSchema,
#       targetCohortTable = tableName,
#       oldCohortId = 1,
#       newCohortId = 10,
#       filterGenderConceptId = c(8507),
#       purgeConflicts = FALSE
#     )
#   )
#
#   applyDemographicCriteria(
#     connectionDetails = connectionDetails,
#     cdmDatabaseSchema = cohortDatabaseSchema,
#     sourceCohortDatabaseSchema = cohortDatabaseSchema,
#     sourceCohortTable = tableName,
#     targetCohortDatabaseSchema = cohortDatabaseSchema,
#     targetCohortTable = tableName,
#     oldCohortId = 1,
#     newCohortId = 12,
#     filterByAgeRange = c(0, 30),
#     purgeConflicts = FALSE
#   )
#
#   dataPost <-
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
#   DatabaseConnector::renderTranslateExecuteSql(
#     connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
#     sql = "DROP TABLE IF EXISTS @cohort_database_schema.@table_temp;
#            DROP TABLE IF EXISTS @cdm_database_schema.person;
#            DROP TABLE IF EXISTS @cdm_database_schema.observation_period;",
#     table_temp = tableName,
#     cohort_database_schema = cohortDatabaseSchema,
#     cdm_database_schema = cohortDatabaseSchema,
#     progressBar = FALSE,
#     reportOverallTime = FALSE
#   )
# })
