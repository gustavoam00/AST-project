

1. install and run docker desktop
2. go to folder with Dockerfile and run ```docker build -t sqlite3-fuzzing .```
3. run ```docker run -it -v "$(pwd)":/usr/bin/test-db sqlite3-fuzzing``` 
docker run -it -v "%cd%":/usr/bin/test-db sqlite3-fuzzing 
docker run -it -v "${PWD}":/usr/bin/test-db sqlite3-fuzzing
4. run ```python fuzzing.py```
python optimization.py
python opt.py

docker pull theosotr/sqlite3-test
docker run -it -v /path/to/AST-project:/mnt/project theosotr/sqlite3-test bash
sudo apt update && sudo apt install -y python3 && sudo apt install -y python3-pip && pip install tqdm
python3 fuzzing.py "RANDOM" 2

3.26.0 -> 3.39.4
https://www3.sqlite.org/changes.html
