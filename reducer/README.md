# Overview

This tool performs SQLite3 query reduction. 

This project was developed as part of the *Automatic Software Testing (AST)* course at ETH Zurich.

---

## Features

- A reducer pipeline with a variety of reduction techniques

---

# Reducer Tool

## Setup

1. **Install Docker Desktop**  
   Ensure [Docker Desktop](https://www.docker.com/products/docker-desktop) is installed and running on your machine.

2. **Build the Docker Image**  
   Navigate to the directory containing the `Dockerfile` and run:
   ```bash
   docker build -t sqlite3-fuzzing .

3. After the docker image is built, run and use the appropriate command
    ```bash
    docker run -it sqlite3-fuzzing
    ```
    or for persistent results:

    - **macOS/Linux:**
        ```bash
        docker run -it -v "$(pwd)":/app sqlite3-fuzzing

    - **Windows (Command Prompt):**
        ```bash
        docker run -it -v "%cd%":/app sqlite3-fuzzing

    - **Windows (PowerShell):**
        ```bash
        docker run -it -v "${PWD}:/app" sqlite3-fuzzing
        ```
        If you get an `invalid reference format` error, try removing the quotes:
        ```bash
        docker run -it -v ${PWD}:/app sqlite3-fuzzing test-db
        ```

---

## Running the Reducer

After the docker image is built: 

1. Write a test-script.sh with bug ```<oracle>``` (DIFF, CRASH(3.26.0), CRASH(3.39.4))
   ```bash
   #!/bin/bash

   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
   DEFAULT_PATH="$SCRIPT_DIR/query.sql"
   FILE_PATH="${TEST_CASE_LOCATION:-$DEFAULT_PATH}"

   python3 src/test_bug.py --query "$FILE_PATH" --oracle <oracle>
   ```

2. Run and use the appropriate command to reduce a bug-triggering SQLite query:
    ```
    python3 main.py --query <path_to_bug_query:file> --test <path_to_test_script>
    ```
    or go to ```/usr/bin``` and run
    ```
    ./reducer --query <path_to_bug_query_file> --test <path_to_test_script>
    ```

    Example:
    ```
    ./reducer --query /app/queries/query1/original_test.sql --test /app/queries/query1/test-script.sh
    ```

---

## Reduction Results

The final reduction results will be in same folder as the ```test-script.sh``` script. 

*Note:* By setting ```TEST_CASE_LOCATION```, the reduction results can be output into another folder, for example with ```export TEST_CASE_LOCATION=/app/query.sql```.


