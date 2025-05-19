import argparse
import os

def generate_dir():
    '''
    Ensure required folders exist.
    '''
    dirs = [
        "data/test/queries",
        "data/test/stats",
        "data/test/bugs",
        "data/test/errors",
        "data/db"
    ]

    for dir in dirs:
        os.makedirs(dir, exist_ok=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("type", help="Select type: FUZZ, TEST", nargs="?", default="TEST")

    args, remaining_args = parser.parse_known_args()

    generate_dir()

    if args.type == "TEST":
        from src import test
        test.main(remaining_args)
    else: 
        from src import fuzzing
        fuzzing.main(args, remaining_args)


if __name__ == "__main__":
    main()





