CohortAlgebra 0.3.0
======================

Changes:
- New function modifyCohorts. Allows modifying previously instantiated cohort by censoring by date, filtering by date range or padding days.

Bug fix:

CohortAlgebra 0.2.1
======================

Changes:
- Code reorganization to follow HADES convention. Long SQL moved to inst/sql_server/sql folder per HADES convention.

Bug fix:
- Fixed missing purgeConflicts in unionCohorts function.

CohortAlgebra 0.2.0
======================

Changes:
- Cohort union function
- Cohort eraFy function is now a hidden function. 
- Updated vignette

CohortAlgebra 0.1.3
======================

Bug fix:
- Add DISTINCT in SQL to support minus of 1 day cohorts

CohortAlgebra 0.1.2
======================

Bug fix:
- Add DISTINCT in SQL to support intersects of 1 day cohorts
- update to PowerPoint of vignettes

CohortAlgebra 0.1.1
======================

Changes:
- Fixes wrong tag
- Remove DO NOT USE.

CohortAlgebra 0.1.0
======================

Changes:
- In eraFyCohort function - if eraConstructionPad > 0 then specifying cdmDatabaseSchema is required. This will allow the eraFy'd cohort period to be checked against the observation_period table, and for the function to ensure that the cohort period corresponds to the persons observation_period. 
- Improved tests for deleting records in cohort table.
- Updated vignette "HowToUseCohortAlgebraRPackage"


CohortAlgebra 0.0.1
======================

This is a unreleased package. 
