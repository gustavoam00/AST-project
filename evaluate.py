import docker
import logging
from tqdm import tqdm

# Configuration
DOCKER_IMAGE = "sqlite3-test"
SQLITE_VERSION = "sqlite3-3.39.4" 
INPUT_FILE = "data/sampled_queries.txt"

def run_query(sql_query, sqlite_version):
    """
    Executes an SQL query using the specified SQLite version inside a Docker container.
    """
    client = docker.from_env()
    try:
        result = client.containers.run(
            DOCKER_IMAGE,
            command=f'/bin/bash -c "echo \\"{sql_query}\\" | /usr/bin/{sqlite_version}"',
            remove=True
        )
        return "  " + result.decode().strip().replace("\n", "\n  ")
    except Exception as e:
        logging.error(f"{sqlite_version}: {e}")
        return str(e)

def validity_evaluation():
    total_queries = 0
    error_count = 0
    log_file = "data/evaluation_log.txt"

    with open(INPUT_FILE, "r", encoding="utf-8") as f, open(log_file, "w", encoding="utf-8") as log:
        queries = f.readlines()

        for i, query in enumerate(tqdm(queries, desc="Running queries"), start=1):
            query = query.strip()
            if not query:
                continue

            total_queries += 1
            result = run_query(query, SQLITE_VERSION)

            if "Error" in result or "ERROR" in result:
                error_count += 1
                log.write(f"\nError in query #{i}:\n")
                log.write(f"  Query: {query}\n")
                log.write(f"  Error: {result.strip()}\n")
            else:
                continue

        
        log.write("\n--- Summary ---\n")
        log.write(f"Total queries executed: {total_queries}\n")
        log.write(f"Total errors found: {error_count}\n")

if __name__ == "__main__":
    validity_evaluation()