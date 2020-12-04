# england-crime
will analyse UK's crime data that can be found under the link:
https://data.police.uk/data/

The download setup should be as follows:
    
    * Select all police forces
    * Add "Include crime data" 
    * Add "Include outcomes data"

# Project structure

    * ETL project
    * WebServer project
    * Scripts to start a MongoDB instance
    
### Setup
1) Add "data" folder in the ETL project with the necessary raw files extracted from the link (verify the DB on: http://localhost:8081/)
2) Start MongoDB for testing:
    `sudo docker-compose -f stack.yml up`
3) Create a special user with the necessary rights:
```
use spark
db.createUser(
  {
    user: "spark_user",
    pwd: "xyz123",
    roles: [ { role: "readWrite", db: "spark" } ],
    mechanisms:[  "SCRAM-SHA-1" ]
  }
)
```


   
### ETL project
#### It's composed out of

* Mian application that will start the Spark job
* resources folder with custom properties
* pipeline package with the important transformations
* sparksessionmanager package with helpers to extend and manage Spark's capabilities
* "data" folder with the downloaded files
    
#### Existing work
To start the ETL process just run the `Main.scala` file.
The ETL process will start a local instance that is going to process the data folder, transform the necessary data 
and store it in the running MongoDB instance

Once the ETL completes there will be 3 Collections in the Database `spark`:
    
* felonies - with all the processed data
* district_stats - with statistics on the different districts
* crime_type_stats - with statistics on different crime types
    
#### Future improvements
*   Containerize the solution
*   Expose Spark as a distributed solution on a managed cluster
*   Extend the solution with different persistence capabilities

### Web project
#### It's composed out of
*   Application.scala - main starting point
*   resource folder   - properties of program
*   endpoints         - classes that describe the routes
*   models            - the data entities that help bring data from the database
*   modules           - setup for dependency injection
*   mongodb           - MongoDB files and setup

#### Existing work
Once the server starts it will have active 4 routes on the local machine
*   Felony data:           http://localhost:8080/api/felony/{pageNumber}
*   Crime type statistics: http://localhost:8080/api/crimetype/{pageNumber}
*   District statistics:   http://localhost:8080/api/district/{pageNumber}
*   Health status:         http://localhost:8080/api/health-check

#### Future improvements
*   Containerize the solution
*   Add a load-balancer
*   Deploy it in Kubernetes for scaling
*   Add new KPIs
*   If the model requires it, add pagination information