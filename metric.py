import re
from collections import Counter
from generator import randomQueryGen
from config import prob

def metric(query: str) -> Counter:
    words = re.sub(r'[^a-zA-Z ]', ' ', query).split()
    upper = [w for w in words if w.isupper()]
    return Counter(upper)

if __name__ == "__main__":
    query = randomQueryGen(prob, debug=False, cycle=1000)
    print(metric(query))