import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("type", help="Select type: FUZZ, TEST", nargs="?", default="TEST")

    args, remaining_args = parser.parse_known_args()

    if args.type == "FUZZ": 
        from src import fuzzing
        fuzzing.main(remaining_args)
    elif args.type == "TEST":
        from src import test
        test.main(remaining_args)


if __name__ == "__main__":
    main()





