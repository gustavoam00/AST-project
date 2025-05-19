import os
import sys
import time
import glob
import shutil
import multiprocessing
import src.generator as gen

def worker(prob, stop_event, worker_id, output_dir):
    text_path = os.path.join(output_dir, f"worker_{worker_id}.txt")
    count_path = os.path.join(output_dir, f"worker_{worker_id}.count")
    count = 0

    with open(text_path, "w", encoding="utf-8") as f:
        while not stop_event.is_set():
            query, _ = gen.randomQueryGen(prob, cycle=3)
            f.write(" ".join(query) + "\n")
            count += 1

    with open(count_path, "w") as c:
        c.write(str(count))

def parallelized_query_gen(param_prob, duration_seconds, num_workers, output_dir="temp_output"):
    os.makedirs(output_dir, exist_ok=True)
    stop_event = multiprocessing.Event()

    workers = [
        multiprocessing.Process(target=worker, args=(param_prob, stop_event, i, output_dir))
        for i in range(num_workers)
    ]

    for w in workers:
        w.start()

    time.sleep(duration_seconds)
    stop_event.set()

    for w in workers:
        w.join()


    with open("all_queries.txt", "w", encoding="utf-8") as outfile:
        for file in sorted(glob.glob(os.path.join(output_dir, "worker_*.txt"))):
            with open(file, "r", encoding="utf-8") as infile:
                shutil.copyfileobj(infile, outfile)

    total = 0
    worker_counts = []
    for i in range(num_workers):
        count_file = os.path.join(output_dir, f"worker_{i}.count")
        with open(count_file, "r") as f:
            count = int(f.read().strip())
            total += count
            worker_counts.append((i, count))

    queries_per_minute = total / (duration_seconds / 60)

    with open("perrformance_log.txt", "w") as log:
        log.write(f"Query generation log\n")
        log.write(f"--------------------\n")
        log.write(f"Duration (seconds): {duration_seconds:.2f}\n")
        log.write(f"Number of workers: {num_workers}\n")
        log.write(f"Total query groups: {total}\n")
        log.write(f"Throughput: {queries_per_minute:.2f} groups/minute\n\n")
        log.write("Worker breakdown:\n")
        for worker_id, count in worker_counts:
            log.write(f"  Worker {worker_id}: {count} query groups\n")
    
    for file in glob.glob(os.path.join(output_dir, "worker_*.txt")):
        os.remove(file)
    for file in glob.glob(os.path.join(output_dir, "worker_*.count")):
        os.remove(file)
    os.rmdir(output_dir)

def estimate_average_size(param_prob=None, num_samples=10000):
    total_bytes = 0
    start = time.time()

    for _ in range(num_samples):
        result = gen.randomQueryGen(param_prob)
        result_bytes = sys.getsizeof(result.encode('utf-8'))
        total_bytes += result_bytes

    avg_size = total_bytes / num_samples
    stop = time.time()
    print(f"Avg {avg_size:.2f} bytes in {stop - start:.2f} seconds, for {num_samples} samples")

if __name__ == '__main__':
    # estimate_average_size(param_prob=None)
    parallelized_query_gen(param_prob=None, duration_seconds=10, num_workers=1)
