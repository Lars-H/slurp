![](http://people.aifb.kit.edu/zg2916/slurp/logo_color_sm.png)

## About

Triple Pattern Fragments (TPFs) allow for querying large RDF graphs with high availability
 by offering triple pattern-based access to the graphs. 
 This limited expressivity of TPFs leads to higher client-side querying cost 
 and the challenge of devising efficient query plans when evaluating SPARQL queries. 
 Different heuristics and cost-model based query planning approaches have been proposed to 
 obtain such efficient query plans. However, we require means to visualize, modify, 
 and execute alternative query plans, to better understand the differences between 
 existing planning approaches and their potential shortcomings. 
 
 To this end, we propose [SLURP](https://km.aifb.kit.edu/sites/slurp/), an interactive SPARQL query planner that assists RDF data consumers to visualize, modify, and compare the performance of different query execution plans over TPFs.


## Demo

Check out the SLURP Demo [here](https://km.aifb.kit.edu/sites/slurp/).

## Installation

SLURP is a containerized application that can be deployed using docker and docker-compose.


The docker container are executed by typing the comand 

```bash
sudo docker-compose build
sudo docker-compose up
```

Once all docker containers have been build and started, SLURP is available at:
```
http://localhost:3000/
```


## Components

### Frontend

The frontend is realized as a React.js web application. 

### Backend

The backend is provided by Flask server that exposes an API which allows to interact with 
the Query Engine, the Celery Task Queue, and the MongoDB database.

### Task Queue

Implements a simple worker using Celery. 
A worker can execute a query plan provided to the API of the backend.

### Crop Engine

Source code for the query engine.
The code is based on [CROP](https://github.com/Lars-H/crop) and [nLDE](https://github.com/maribelacosta/nlde) and has been adjusted for the purpose of this demonstration.

## License

This project is licensed under the MIT License.