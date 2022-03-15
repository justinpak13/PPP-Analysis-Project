# data-enineering-zoomcamp-project
Analysis of PPP loan data for the final project of the Data Engineering Zoomcamp

In response to the economic devastation of the COVID-19 pandemic, the United States Congress passed the Coronavirus Aid, Relief, and Economic Security (CARES) act. 

"The Paycheck Protection Program established by the CARES Act, is implemented by the Small Business Administration with support from the Department of the Treasury.  This program provides small businesses with funds to pay up to 8 weeks of payroll costs including benefits. Funds can also be used to pay interest on mortgages, rent, and utilities.

The Paycheck Protection Program prioritizes millions of Americans employed by small businesses by authorizing up to $659 billion toward job retention and certain other expenses." -> [Department of the Treasury](https://home.treasury.gov/policy-issues/coronavirus/assistance-for-small-businesses/paycheck-protection-program)

<b>Data set</b> : https://data.sba.gov/dataset/ppp-foia

<b>Question</b>:
* General facts of the program
* What do the distributions of the funds look like compared to the makeup of [small businesses](https://cdn.advocacy.sba.gov/wp-content/uploads/2019/04/23142719/2019-Small-Business-Profiles-US.pdf)
 
# Process

## Creating a pipeline for processing this dataset & putting it into a data lake 

I will be using GCP cloud storage as the data lake and Airflow in order to get the data into GCP. 

