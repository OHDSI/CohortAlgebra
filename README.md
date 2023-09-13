CohortAlgebra
================

[![Build Status](https://github.com/OHDSI/CohortAlgebra/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/CohortAlgebra/actions?query=workflow%3AR-CMD-check)
[![codecov.io](https://codecov.io/github/OHDSI/CohortAlgebra/coverage.svg?branch=main)](https://app.codecov.io/github/OHDSI/CohortAlgebra?branch=main)
[![CRAN_Status_Badge](http://www.r-pkg.org/badges/version/CohortAlgebra)](https://cran.r-project.org/package=CohortAlgebra)
[![CRAN_Status_Badge](http://cranlogs.r-pkg.org/badges/CohortAlgebra)](https://cran.r-project.org/package=CohortAlgebra)

Introduction
============

CohortAlgebra is a R package that allows you to create new cohorts from previously instantiated cohorts in a cohort table. New cohorts may be created by performing the union, intersect or minus of one or more cohorts. 

Cohort Union: Takes two or more cohorts and creates a new cohort that has all the dates a subject was present in any of the given cohorts.
Cohort Intersect: Takes two or more cohorts and creates a new cohort that only has the dates a subject was present in all the given cohorts. If a person was not present in any of the given cohort, then that subject is not in the final cohort.
Cohort Minus: Take only two cohorts (different from Cohort union and intersect). Creates a new cohort that has the dates a subject was present in the first cohort and not the second cohort. Note: the sequence of cohorts is important for Cohort Minus. If person is not in the second cohort, then all the dates the person was present in the first cohort is part of the final cohort.

Other capabilities include modify cohorts which allows to censor or filter an instantiated cohort, or pad an instantiated cohort additional dates. You can also remove persons from one set of cohorts, any subject present in another set of cohorts.

Features
========
- Create new cohorts from existing cohorts.

Technology
============
CohortAlgebra is an R package.

System Requirements
============
Requires R (version 3.6.0 or higher). 

Installation
=============
1. See the instructions [here](https://ohdsi.github.io/Hades/rSetup.html) for configuring your R environment, including RTools and Java.

2. In R, use the following commands to download and install CohortAlgebra:

  ```r
  install.packages("remotes")
  remotes::install_github("ohdsi/CohortAlgebra")
  ```

User Documentation
==================
Documentation can be found on the [package website](https://ohdsi.github.io/CohortAlgebra).

PDF versions of the documentation are also available:
* Package manual: [CohortAlgebra.pdf](https://raw.githubusercontent.com/OHDSI/CohortAlgebra/main/extras/CohortAlgebra.pdf)

Support
=======
* Developer questions/comments/feedback: <a href="http://forums.ohdsi.org/c/developers">OHDSI Forum</a>
* We use the <a href="https://github.com/OHDSI/CohortAlgebra/issues">GitHub issue tracker</a> for all bugs/issues/enhancements

Contributing
============
Read [here](https://ohdsi.github.io/Hades/contribute.html) how you can contribute to this package.

License
=======
CohortAlgebra is licensed under Apache License 2.0

Development
===========
CohortAlgebra is being developed in R Studio.

### Development status

CohortAlgebra is under development.