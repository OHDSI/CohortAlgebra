testthat::test_that("Get Cohort Relationship", {
  cohortTable <- dplyr::tibble(
    cohortDefinitionId = c(1, 1, 2, 2, 2, 2),
    subjectId = c(1, 2, 1, 2, 1, 2),
    cohortStartDate = as.Date(
      c(
        "2023-01-01",
        "2023-01-05",
        "2023-01-10",
        "2023-01-12",
        "2023-02-01",
        "2023-03-01"
      )
    ),
    cohortEndDate = as.Date(
      c(
        "2023-01-20",
        "2023-01-25",
        "2023-01-30",
        "2023-01-20",
        "2023-02-10",
        "2023-03-10"
      )
    )
  )

  # Expected output when closestPeriod = TRUE
  expectedClosest <- dplyr::tibble(
    subjectId = c(1, 2) |> as.double(),
    startDay = c(9, 7) |> as.double(),
    endDay = c(29, 15) |> as.double()
  )

  # Expected output when closestPeriod = FALSE
  expectedFull <- dplyr::tibble(
    subjectId = c(1, 1, 2, 2) |> as.double(),
    startDay = c(9, 31, 7, 55) |> as.double(),
    endDay = c(29, 40, 15, 64) |> as.double()
  )

  # Test when closestPeriod = TRUE
  expectedClosest <-
    getCohortRelationship(
      cohortTable = cohortTable,
      targetCohortId = 1,
      featureCohortIds = 2,
      closestPeriod = FALSE
    )
  testthat::expect_equal(expectedClosest, expectedClosest)

  # Test when closestPeriod = FALSE
  observedFull <-
    getCohortRelationship(
      cohortTable,
      targetCohortId = 1,
      featureCohortIds = 2,
      closestPeriod = FALSE
    )
  testthat::expect_equal(observedFull, expectedFull)
})
