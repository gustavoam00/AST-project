import cma
import random, argparse
from fuzzing import random_query
from config import PROB_TABLE, SEED, TEST_FOLDER

random.seed(SEED)

PROB_KEYS = list(PROB_TABLE)

def dict_to_vector(prob_dict):
    return [prob_dict[key] for key in PROB_KEYS]

def vector_to_dict(prob_vector):
    return {key: float(prob_vector[i]) for i, key in enumerate(PROB_KEYS)}

def fuzz_optimize(prob: dict, popsize: int = 4, num_iterations: int = 6):
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

            print(f"CMA-ES: {i+1:<2}/{num_iterations:<2} it {j+1:<2}/{popsize:<2} pop, Coverage: {cov:<5.2f}, Best: {best_cov:<5.2f}")
            
            cov, c, query, tables = random_query(repeat=10, save=False, param_prob = prob_dict)
            rewards.append(-cov) 

            if cov > best_cov:
                best_cov = cov
                best_query = query
                best_params = sol
                c = c

        es.tell(solutions, rewards)
        #es.disp()

    with open(TEST_FOLDER + f"CMA-ES_{popsize}_{num_iterations}.txt", "w") as f:
        f.write("\n=== BEST RESULT ===")
        f.write(f"Best Coverage: {best_cov}, {c}\n")
        f.write(f"Best Query: {best_query}\n")
        f.write(f"Best Probs: {vector_to_dict(best_params)}")

def main():
    parser = argparse.ArgumentParser(description="CMA-ES Fuzzing Optimizer")
    parser.add_argument("popsize", help="CMA-ES population size")
    parser.add_argument("iter", help="CMA-ES iterations")

    args = parser.parse_args()

    prob = {k: 0.5 for k in PROB_TABLE}
    fuzz_optimize(prob, popsize=int(args.popsize), num_iterations=int(args.iter))

if __name__ == "__main__":
    main()