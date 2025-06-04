1.  ```bash
    docker build -t sqlite3-fuzzing .
2.  ```bash
    docker run -it sqlite3-fuzzing
    ```
    or for persistent results:
    ```
    docker run -it -v "%cd%":/app sqlite3-fuzzing
    docker run -it -v ${PWD}:/app sqlite3-fuzzing
    ```

Run with:
```
python3 main.py --query <path_to_query_folder> --test <path_to_test_script>
```
or
```
cd /usr/bin
./reducer --query <path_to_query_folder> --test <path_to_test_script>
```

Example:
```
./reducer --query /app/queries/query1/original_test.sql --test /app/queries/query1/test-script.sh
```
add functions to call into main.py


