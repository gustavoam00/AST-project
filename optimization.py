import cma
import random
from fuzzing import run_pipeline, FUZZING_PIPELINE
from config import PROB_TABLE, SEED, TEST_FOLDER

random.seed(SEED)

PROB_KEYS = list(PROB_TABLE)

def dict_to_vector(prob_dict):
    return [prob_dict[key] for key in PROB_KEYS]

def vector_to_dict(prob_vector):
    return {key: float(prob_vector[i]) for i, key in enumerate(PROB_KEYS)}

def fuzz_optimize(fuzz_pipeline, prob: dict, popsize: int = 4, num_iterations: int = 6):
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

    for i in range(num_iterations):
        solutions = es.ask()
        rewards = []

        for j, sol in enumerate(solutions):
            prob_dict = vector_to_dict(sol)

            print(f"CMA-ES: {i+1} it {j+1} pop")
            
            cov, c, query, tables, nodes = run_pipeline(0, [], [], [], fuzz_pipeline(prob_dict), desc=f"CMA-ES_{i}_{j}")
            rewards.append(-cov) 

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
    prob = {k: 0.5 for k in PROB_TABLE}
    fuzz_optimize(FUZZING_PIPELINE, prob)