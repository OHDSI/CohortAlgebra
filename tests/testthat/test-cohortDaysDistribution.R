testthat::test_that("Testing cohort days distribution", {
  # Sample data setup
  cohort <- data.frame(
    cohortDefinitionId = c(1, 1, 2, 2, 2),
    subjectId = c(101, 102, 103, 104, 105),
    cohortStartDate = as.Date(
      c(
        "2020-01-01",
        "2020-01-05",
        "2020-01-10",
        "2020-01-15",
        "2020-01-20"
      )
    ),
    cohortEndDate = as.Date(
      c(
        "2020-01-11",
        "2020-01-15",
        "2020-01-20",
        "2020-01-25",
        "2020-01-30"
      )
    )
  )

  # Tests
  expected <- dplyr::tibble(
    cohortDefinitionId = 1,
    mean_days = mean(c(10, 10)),
    percentile_1 = quantile(c(10, 10), probs = 0.01),
    percentile_50 = quantile(c(10, 10), probs = 0.50),
    percentile_99 = quantile(c(10, 10), probs = 0.99)
  )

  result <- getDistributionOfCohortDays(cohort, 1) |>
    dplyr::tibble() |>
    dplyr::select(colnames(expected))

  testthat::expect_equal(result, expected)

  result <- getDistributionOfCohortDays(cohort)
  testthat::expect_equal(nrow(result), 2) # Expect two groups: id 1 and id 2
  testthat::expect_true(all(result$mean_days == c(10, 10))) # All means should be the same in this case


  incorrect_cohort <- cohort
  names(incorrect_cohort) <-
    c("wrongId", "subjectId", "wrongStart", "wrongEnd")
  testthat::expect_error(
    getDistributionOfCohortDays(incorrect_cohort),
    "cohortDefinitionId"
  )


  boundary_cohort <- data.frame(
    cohortDefinitionId = c(3),
    subjectId = c(106),
    cohortStartDate = as.Date("2020-01-01"),
    cohortEndDate = as.Date("2020-12-31")
  )
  result <- getDistributionOfCohortDays(boundary_cohort)
  expected_days <- as.numeric(as.Date("2020-12-31") - as.Date("2020-01-01"))
  testthat::expect_equal(result$mean_days, expected_days)
})
