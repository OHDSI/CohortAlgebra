testthat::test_that("Testing cohort era fy", {
  testthat::skip_if(condition = skipCdmTests)

  # make up date for a cohort table
  # this cohort table will have two subjects * two cohorts, within the same cohort
  cohort <- dplyr::tibble(
    cohortDefinitionId = c(1, 1, 1),
    subjectId = c(1, 1, 1),
    cohortStartDate = c(
      as.Date("1999-01-01"),
      as.Date("1999-01-20"),
      as.Date("1999-03-10")
    ),
    cohortEndDate = c(
      as.Date("1999-01-31"),
      as.Date("1999-02-28"),
      as.Date("1999-03-31")
    )
  )
  cohort <- dplyr::bind_rows(
    cohort,
    cohort |> dplyr::mutate(subjectId = 2)
  )
  cohort <- dplyr::bind_rows(
    cohort,
    cohort |> dplyr::mutate(cohortDefinitionId = 2)
  ) |>
    dplyr::arrange(
      .data$subjectId,
      .data$cohortStartDate,
      .data$cohortEndDate
    )


  # should not throw error
  dataPostEraFy <-
    CohortAlgebra:::eraFyCohortDataFrame(
      cohort = cohort,
      eraconstructorpad = 0
    ) |>
    dplyr::filter(cohortDefinitionId == 1) |>
    dplyr::mutate(cohortDefinitionId = 9) |>
    dplyr::arrange(
      cohortDefinitionId,
      subjectId,
      cohortStartDate
    )

  testthat::expect_equal(
    object = nrow(dataPostEraFy),
    expected = 4
  ) # era fy logic should collapse to 4 rows

  # create the expected output data frame object to compare
  cohortExpected <- dplyr::tibble(
    cohortDefinitionId = c(9, 9),
    subjectId = c(1, 1),
    cohortStartDate = c(as.Date("1999-01-01"), as.Date("1999-03-10")),
    cohortEndDate = c(as.Date("1999-02-28"), as.Date("1999-03-31"))
  )
  cohortExpected <- dplyr::bind_rows(
    cohortExpected,
    cohortExpected |>
      dplyr::mutate(subjectId = 2)
  ) |>
    dplyr::distinct() |>
    dplyr::arrange(
      .data$cohortDefinitionId,
      .data$subjectId,
      .data$cohortStartDate
    )

  testthat::expect_true(object = all(dataPostEraFy == cohortExpected))
})
