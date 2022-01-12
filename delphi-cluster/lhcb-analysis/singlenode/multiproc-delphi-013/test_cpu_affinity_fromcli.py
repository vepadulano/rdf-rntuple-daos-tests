import multiprocessing
import subprocess
import time

def proc(procid):
    pid = multiprocessing.current_process().pid
    print(f"Printing from procesis {pid}, with process id {procid}")

    run = subprocess.run(f"taskset -pc {pid}", shell=True, check=True, capture_output=True)
    print(f"{run.stdout}")
    time.sleep(5)

if __name__ == "__main__":
    procids = [i for i in range(10)]
    with multiprocessing.Pool(processes=len(procids)) as pool:
        pool.map(proc, procids)
