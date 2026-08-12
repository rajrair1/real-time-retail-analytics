import argparse
import time
from src.producer import main

parser = argparse.ArgumentParser()
parser.add_argument("--events", type=int, default=10000)
parser.add_argument("--rate", type=int, default=500)
args = parser.parse_args()
started = time.perf_counter()
main(args.events, args.rate)
elapsed = time.perf_counter() - started
print(f"published={args.events} elapsed_seconds={elapsed:.2f} observed_rate={args.events/elapsed:.2f}")
