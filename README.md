# nsi-weather-api-agregator  


## requirement  

National Situation Centre  
Data Engineer Technical Assessment  

### Prerequisites

You will need the following:  

- A GitHub Account
- Create a repository to upload the finished code to.
- A local development environment to complete Scenario 1.

### Scenario 1  

Estimated time: 2 hours

We have a requirement to ingest weather data from an open API. This data is in json format, published hourly and does not need a key to access it.

Write the python code required to pull the API data, aggregate the various fields to a daily total and output as a parquet file. This code may be structured however you think best, the example below is a guideline only, but your code must have sufficient error handling as well as unit tests to ensure the code is robust and accurate. Once the python code is complete, write the terraform required to deploy the code to AWS Lambda.

The API URL is: `https://api.open-meteo.com/v1/forecast?latitude=51.5085&longitude=-0.1257&hourly=temperature_2m,rain,showers,visibility&past_days=31 `

## Author
- [@keverall](https://www.github.com/keverall)