# dataengineering_CEU_2023
SQL Workbench

Project: Loans Distributed by the Social Development Bank of Saudi Arabia in 2022

The Social Development Bank is one of the key tools of the government of the Kingdom of Saudi Arabia. 
The bank issues interest-free loans for citizens to drive the development and welfare of its citizens.
Unlike in other countries, the scope of these loans is broad and largely promotes diverse local values.

Target groups:
* SMEs, employers, freelancers and emerging trades,
* Citizens with limited incomes to overcome financial difficulties,
* MSME sector,
* Supporting important life events such as marriage and family,
* Promoting savings among individuals and institutions.
The Bank has 26 branches in different regions.

To limit the number of entries, only the business and project loans were included in the dataset on the github.

The goal of the analytical layer is to assess the distribution of these risk-free loans among the population.
One data entry represents one successful application.
The individual debtors have been anonymized. Thus, we do not know whether anyone succeeded in claiming financing multiple times.

The fact analysed is the total sum of a loan and the average amount awarded to an individual.
The final output is a datamart to display the average, minimum, and maximum loan values in various cities on a map.

Data source: 
Loans of KSA development banks: 
https://od.data.gov.sa/Data/en/dataset/social-development-bank-loan-for-2022
Location and population: 
https://simplemaps.com/data/sa-cities
Cost of living in cities: 
https://od.data.gov.sa/Data/en/dataset/average-prices-of-goods-and-services-in-sixteen-cities
https://od.data.gov.sa/Data/en/dataset/average-prices-of-goods-and-services-in-sixteen-cities--2-
Weather: 
https://en.climate-data.org/asia/saudi-arabia/asir-region-1999/


* The code works - make sure to check your SQL user permissions to update and access local infiles.
