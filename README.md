
## Setup
1. install and run docker desktop
2. go to folder with Dockerfile and run:
```docker build -t sqlite3-fuzzing .```
3. Run one of these commands depending on operating system:
```
docker run -it -v "$(pwd)":/usr/bin/test-db sqlite3-fuzzing
docker run -it -v "%cd%":/usr/bin/test-db sqlite3-fuzzing
docker run -it -v "${PWD}":/usr/bin/test-db sqlite3-fuzzing
``` 

## Run Fuzzer and Testing
1. Run ```PIPELINE``` to generate random queries with the hybrid fuzzing pipeline and ```RANDOM``` to generate queries with the probability-based random query generator. 
```
python fuzzing.py PIPELINE 3 1
python fuzzing.py RANDOM 3 10
```
The first number is the number of cycles (how many times should it cycle through the pipeline or random generator). The second number is how many queries ```.sql``` it should generate. The queries are saved in folder ```test/``` and metrics and other information are saved in ```test/fuzz_results/``` as ```.txt``` files.
2. To find bugs, run:
```
python test.py
``` 
Bugs are saved in ```test/bugs/```. Make sure to put ```.sql``` queries into the ```test/`` folder to test for bugs.
3. To collect metrics for each query, run:
```
python test.py DATA
```
Make sure to put ```.txt``` of metrics data in ```test/fuzz_results/```

3.26.0 -> 3.39.4
https://www3.sqlite.org/changes.html
