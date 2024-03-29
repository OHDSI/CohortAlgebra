% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/EraFyCohorts.R
\name{eraFyCohorts}
\alias{eraFyCohorts}
\title{Era-fy cohort(s)}
\usage{
eraFyCohorts(
  connectionDetails = NULL,
  connection = NULL,
  sourceCohortDatabaseSchema = NULL,
  sourceCohortTable = "cohort",
  targetCohortDatabaseSchema = NULL,
  targetCohortTable,
  oldCohortIds,
  newCohortId,
  eraconstructorpad = 0,
  cdmDatabaseSchema = NULL,
  purgeConflicts = FALSE,
  isTempTable = FALSE,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")
)
}
\arguments{
\item{connectionDetails}{An object of type \code{connectionDetails} as created using the
\code{\link[DatabaseConnector]{createConnectionDetails}} function in the
DatabaseConnector package. Can be left NULL if \code{connection} is
provided.}

\item{connection}{An object of type \code{connection} as created using the
\code{\link[DatabaseConnector]{connect}} function in the
DatabaseConnector package. Can be left NULL if \code{connectionDetails}
is provided, in which case a new connection will be opened at the start
of the function, and closed when the function finishes.}

\item{sourceCohortDatabaseSchema}{Schema name where your source cohort tables reside. Note that for SQL Server,
this should include both the database and schema name, for example
'scratch.dbo'.}

\item{sourceCohortTable}{The name of the source cohort table.}

\item{targetCohortDatabaseSchema}{Schema name where your target cohort tables reside. Note that for SQL Server,
this should include both the database and schema name, for example
'scratch.dbo'.}

\item{targetCohortTable}{The name of the target cohort table.}

\item{oldCohortIds}{An array of 1 or more integer id representing the cohort id of the cohort
on which the function will be applied.}

\item{newCohortId}{The cohort id of the output cohort.}

\item{eraconstructorpad}{Optional value to pad cohort era construction logic. Default = 0. i.e. no padding.}

\item{cdmDatabaseSchema}{Schema name where your patient-level data in OMOP CDM format resides.
Note that for SQL Server, this should include both the database and
schema name, for example 'cdm_data.dbo'.}

\item{purgeConflicts}{If there are conflicts in the target cohort table i.e. the target cohort table
already has records with newCohortId, do you want to purge and replace them
with transformed. By default - it will not be replaced, and an error message is thrown.}

\item{isTempTable}{Is the output a temp table. If yes, a new temp table is created. This will required
an active connection. Any old temp table is dropped and replaced.}

\item{tempEmulationSchema}{Some database platforms like Oracle and Impala do not truly support
temp tables. To emulate temp tables, provide a schema with write
privileges where temp tables can be created.}
}
\value{
Nothing is returned
}
\description{
Given a table with cohort_definition_id, subject_id, cohort_start_date,
cohort_end_date execute era logic. This will delete and replace the
original rows with the cohort_definition_id(s). edit privileges
to the cohort table is required.

\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#stable}{\figure{lifecycle-stable.svg}{options: alt='[Stable]'}}}{\strong{[Stable]}}
}
