docker build -t sqlite3-fuzzing .
docker run -it -v "%cd%":/app sqlite3-fuzzing
docker run -it -v ${PWD}:/app sqlite3-fuzzing

python3 main.py
add functions to call into main.py


reuse 
clean up
group by
context-dependent tracking
vertically and horizontally

