import multiprocessing
import subprocess
import time

def proc(cpu):
    pid = multiprocessing.current_process().pid
    print(f"Printing from procesis {pid}, assigning to CPU {cpu}")

    run = subprocess.run(f"taskset -pc {cpu} {pid}", shell=True, check=True, capture_output=True)
    print(f"{run.stdout}")
    time.sleep(20)

if __name__ == "__main__":
    cpus = [0,1,2,32,33,34]
    with multiprocessing.Pool(processes=len(cpus)) as pool:
        pool.map(proc, cpus)
