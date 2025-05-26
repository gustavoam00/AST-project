1.  ```bash
    docker build -t sqlite3-fuzzing .
2.  ```bash
    docker run -it -v "%cd%":/app sqlite3-fuzzing
    docker run -it -v ${PWD}:/app sqlite3-fuzzing
    ```

Run with:
```
python3 main.py
./reducer --query <path_to_query_folder> --test <test_script>
```
example:
```
./reducer --query queries/query1 --test ./test_bug.sh
```
add functions to call into main.py


reuse 
clean up
group by
context-dependent tracking
vertically and horizontally

