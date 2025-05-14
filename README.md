
# SQLite3 Fuzzing Tool

This tool performs SQL query fuzzing using both a hybrid pipeline and a probability-based random generator. It supports bug detection and metrics analysis via Docker and Python scripts.

## Features

- Coverage-guided Fuzzing Pipeline and Probability-guided SQLite Query Generators
- Bug discovery and metrics collection

---

## Setup

1. **Install Docker Desktop**  
   Ensure [Docker Desktop](https://www.docker.com/products/docker-desktop) is installed and running on your machine.

2. **Build the Docker Image**  
   Navigate to the directory containing the `Dockerfile` and run:
   ```bash
   docker build -t sqlite3-fuzzing .

---

## Running the Fuzzer and Testing

After the docker image is built, run and use the appropriate command to generate SQLite queries:

1. **macOS/Linux:**
    ```bash
    docker run -it -v "$(pwd)":/app sqlite3-fuzzing test-db <type> <cycles> <number_of_queries>

2. **Windows (Command Prompt):**
    ```bash
    docker run -it -v "%cd%":/app sqlite3-fuzzing test-db <type> <cycles> <number_of_sql>

3. **Windows (PowerShell):**
    ```bash
    docker run -it -v "${PWD}":/app sqlite3-fuzzing test-db <type> <cycles> <number_of_sql>

- ```<type>```: specified the mode:
    - ```PIPELINE```: Coverage-guided Fuzzing Pipeline
    - ```RANDOM```: Probability-guided Random Query Generator
    - ```TEST```: Bugs testing mode (does not use ```cycle``` or ```number_of_sql```)

- ```<cycles>``` (required for ```PIPELINE``` and ```RANDOM```): Number of iterations for generating queries.

- ```<number_of_sql>``` (required for ```PIPELINE``` and ```RANDOM```): Number of SQL files to generate

---

## Output and Testing

- Generated queries are saved in ```data/test/queries/``` 
- Metrics and logs are saved in ```data/test/stats/```
- Detected bugs are saved in ```data/test/bugs/```

To run bug tests, place the ```.sql``` files you want to test in ```data/test/queries``` folder. The tool will execute them and log them in the ```data/test/bugs/``` folder.


