import cma
import random
from tqdm import tqdm
from fuzzing import run_pipeline, FUZZING_PIPELINE
from config import PROB_TABLE, SEED, TEST_FOLDER

random.seed(SEED)

PROB_KEYS = list(PROB_TABLE)

def dict_to_vector(prob_dict):
    return [prob_dict[key] for key in PROB_KEYS]

def vector_to_dict(prob_vector):
    return {key: float(prob_vector[i]) for i, key in enumerate(PROB_KEYS)}

def fuzz_optimize(fuzz_pipeline, prob: dict, popsize: int = 5, num_iterations: int = 5):
    """
    CMA-ES evolution to tweak the probability values 
    """
    init_vector = dict_to_vector(prob)
    es = cma.CMAEvolutionStrategy(init_vector, 0.3, {'bounds': [0.0, 1.0], 'popsize': popsize})

    best_cov = -1
    best_query = ""
    best_params = None
    cov = 0
    c = (0, 0, 0, 0)

    for i in tqdm(range(num_iterations), desc="CMA-ES Fuzzer Optimization: "):
        solutions = es.ask()
        rewards = []

        for sol in solutions:
            prob_dict = vector_to_dict(sol)
            
            cov, c, query, tables, nodes = run_pipeline(0, [], [], [], fuzz_pipeline(prob_dict), repeat=3, save=False)
            rewards.append(-cov)  # CMA-ES minimizes

            if cov > best_cov:
                best_cov = cov
                best_query = query
                best_params = sol

                with open(TEST_FOLDER + f"optimization_{i}_{cov}.txt", "w") as f:
                    f.write("=== BEST RESULT ===\n")
                    f.write(f"Best Coverage: {best_cov}, {c}\n")
                    f.write(f"Best Query: {best_query}\n")
                    f.write(f"Best Probs: {vector_to_dict(best_params)}\n")

        es.tell(solutions, rewards)
        es.disp()

    with open(TEST_FOLDER + "optimization.txt", "w") as f:
        f.write("\n=== BEST RESULT ===")
        f.write(f"Best Coverage: {best_cov}, {c}")
        f.write(f"Best Query: {best_query}")
        f.write(f"Best Probs: {vector_to_dict(best_params)}")

if __name__ == "__main__":
    fuzz_optimize(FUZZING_PIPELINE, PROB_TABLE)