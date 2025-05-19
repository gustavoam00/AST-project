
# Overview

This tool performs SQL query fuzzing using both a coverage-guided fuzzing pipeline and a probability-guided random query generator. It supports bug detection and metrics analysis through Docker and Python scripts.

This project was developed as part of the *Automatic Software Testing (AST)* course at ETH Zurich.

---

## Features

- Coverage-guided Fuzzing Pipeline and Probability-guided SQLite Query Generators
- Bug discovery and metrics collection

---

# SQLite3 Fuzzing Tool

## Setup

1. **Install Docker Desktop**  
   Ensure [Docker Desktop](https://www.docker.com/products/docker-desktop) is installed and running on your machine.

2. **Build the Docker Image**  
   Navigate to the directory containing the `Dockerfile` and run:
   ```bash
   docker build -t sqlite3-fuzzing .

---

## Running the Fuzzer and Testing

After the docker image is built, run and use the appropriate command to generate and test SQLite queries. Ensure you're in the project root folder to persist output correctly:

1. **macOS/Linux:**
    ```bash
    docker run -it -v "$(pwd)":/app sqlite3-fuzzing test-db <type> <cycles> <number_of_queries>

2. **Windows (Command Prompt):**
    ```bash
    docker run -it -v "%cd%":/app sqlite3-fuzzing test-db <type> <cycles> <number_of_sql>

3. **Windows (PowerShell):**
    ```bash
    docker run -it -v "${PWD}:/app" sqlite3-fuzzing test-db <type> <cycles> <number_of_sql>
    ```

    If you get an `invalid reference format` error, try removing the quotes:

    ```bash
    docker run -it -v ${PWD}:/app sqlite3-fuzzing test-db <type> <cycles> <number_of_sql>
    ```

- ```<type>```: specifies the mode:
    - ```PIPELINE```: Coverage-guided Fuzzing Pipeline
    - ```RANDOM```: Probability-guided Random Query Generator
    - ```TEST```: Bugs testing mode (does not use ```cycle``` or ```number_of_sql```)

- ```<cycles>``` (required for ```PIPELINE``` and ```RANDOM```): Number of iterations for generating queries.

- ```<number_of_sql>``` (required for ```PIPELINE``` and ```RANDOM```): Number of SQL files to generate.

*Note:* Make sure you're running the Docker command from the project root folder, the same folder that contains the Dockerfile. This ensures that Docker correctly mounts the volume and that output files are saved persistently inside the ```/app``` folder in the container.

---

## Testing

To test for bugs, place the ```.sql``` files to test in the ```data/test/queries``` folder. Run the tool in ```TEST``` mode. Results will be logged in the ```data/test/bugs/``` folder. 

The test returns three ```.sql``` files:
- A cleaned SQL file that removes different non-bug related outputs such as time, date or ```EXPLAIN``` queries
- The SQL file tested on SQLite version 3.26.0 with each of its output commented in
- The SQL file tested on SQLite version 3.39.4 with each of its output commented in

Additionally, a log of the query and the potential bug output in both versions is returned as a ```.txt``` file.

---

## Output

- Generated queries are saved in ```data/test/queries/``` 
- Metrics and logs are saved in ```data/test/stats/```
- Detected bugs are saved in ```data/test/bugs/```




