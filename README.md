# Loader Mongo Data

This is a personal project, whose purpose is load some datas to give support to the [TB_Challenge](https://github.com/egilsoncabral/tb_challenge) project.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

* JDK 8
* [Maven](https://maven.apache.org/)

## Build/Usage

	1. Clone project

		$ https://github.com/egilsoncabral/loadermongo
		
	2. Build the project

	    - Enter in the project folder and execute:	
            
            - Take the dataset file in (https://data.gov.ie/dataset/dublin-bus-gps-sample-data-from-dublin-city-council-insight-project), download one extract, and from that extract, use 1 example CSV as input. Put it in the same folder of the project.
            
            - Execute:
                
                $ mvn springboot:run -Dspring-boot.run.arguments=--input.file.name={$filename},--host.address={host}
        
                P.S: $filename example: bus.csv, for the host use "locahost" for docker CE or "192.168.99.100" for docker toolbox    
    
#### Used Technologies

* Java 8
* Lombok for data models
* SpringBoot 
* MongoDB

## Versioning

For versioning, i used the gitHub platform. For more details, (https://github.com/)

## Authors

* **Egilson Cabral** - [TBChallenge](https://github.com/egilsoncabral/tb_challenge)
