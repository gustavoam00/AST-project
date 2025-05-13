
# SQLite3 Fuzzing Tool

This tool performs SQL query fuzzing using both a hybrid pipeline and a probability-based random generator. It supports bug detection and metrics analysis via Docker and Python scripts.

## Features

- Hybrid and random SQL query generation
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

Use the appropriate command for your operating system:
1. **macOS/Linux:**
    ```bash
    docker run -it -v "$(pwd)":/usr/bin/test-db sqlite3-fuzzing

2. **Windows (Command Prompt):**
    ```bash
    docker run -it -v "%cd%":/usr/bin/test-db sqlite3-fuzzing

3. **Windows (PowerShell):**
    ```bash
    docker run -it -v "${PWD}":/usr/bin/test-db sqlite3-fuzzing

---


## Running the Fuzzer and Testing

1. Inside the docker, run the following commands to generate SQLite queries: 
```bash
# Hybrid fuzzing pipeline
python fuzzing.py PIPELINE <cycles> <number_of_queries>

# Probability-based random generator
python fuzzing.py RANDOM <cycles> <number_of_queries>
```
The first number is the number of cycles (how many times should it cycle through the pipeline or random generator). The second number is how many queries ```.sql``` it should generate. The queries are saved in folder ```test/``` and metrics and other information are saved in ```test/fuzz_results/``` as ```.txt``` files.

2. Detect bugs by running:
```bash
python test.py
``` 
Bugs are saved in ```test/bugs/```. Make sure to put ```.sql``` queries into the ```test/`` folder to test for bugs.

3. To analyze queries and collect metrics, run:
```bash
python test.py DATA
```
Make sure to put ```.txt``` of metrics data in ```test/fuzz_results/```

3.26.0 -> 3.39.4
https://www3.sqlite.org/changes.html
