import argparse
import os
import subprocess

import ROOT

import multiprocessing

parser = argparse.ArgumentParser()
parser.add_argument("--nchunks", help="Number of chunks to split the original RNTuple in", type=int)
parser.add_argument("--storagetype", help="Type of storage chosen for the analysis")
parser.add_argument("--daospool", help="UUID of the DAOS pool used for the tests, if needed")
args = parser.parse_args()


def n_even_chunks(iterable, n_chunks):
    last = 0
    itlenght = len(iterable)
    for i in range(1, n_chunks + 1):
        cur = int(round(i * (itlenght / n_chunks)))
        yield iterable[last:cur]
        last = cur


def create_ntuples():

    # This is the total number of entries in the dataset of the LHCB analysis.
    # It is 100x the original dataset size.
    # dataset_entries = 855611800
    # It is 200x the original dataset size.
    dataset_entries = 1711223600

    # Create the python lists of ranges and paths
    ntupleranges = [(r.start, r.stop) for r in n_even_chunks(range(dataset_entries), args.nchunks)]
    if args.storagetype == "daos":
        uuids = [
            subprocess.run("uuidgen", shell=True, check=True, capture_output=True).stdout.decode().rstrip("\n")
            for _ in range(args.nchunks)]
        ntupleuris = [f"\"daos://{args.daospool}:1/{container}\"" for container in uuids]
    elif args.storagetype == "filesystem":
        ntupleuris = [f"\"{os.path.join(os.getcwd(), f'lhcb_ntuple_{i}.root')}\"" for i in range(args.nchunks)]

    # Convert the Python ranges to C++ ranges
    cpp_pairs = [f"{{{begin},{end}}}" for (begin, end) in ntupleranges]
    ntupleranges_cpp = f"""
    std::vector<std::pair<std::uint64_t, std::uint64_t>> ntupleranges{{{",".join(cpp_pairs)}}};
    """.rstrip().lstrip()

    # Convert the Python paths to C++ paths
    ntupleuris_cpp = f"""
    std::vector<std::string> ntupleuris{{{",".join(ntupleuris)}}};
    """.rstrip().lstrip()

    # Open the convert program template and write into it the C++ vectors
    with open("gen_lhcb_template.cpp", "r") as f:
        template = f.read()

    template_mod = template.replace("/*REPLACEBYNTUPLEURIS*/", ntupleuris_cpp)\
                           .replace("/*REPLACEBYNTUPLEURANGES*/", ntupleranges_cpp)

    out_cpp_filename = f"gen_lhcb_{args.storagetype}_{args.nchunks}chunks.cpp"
    out_cpp_executable = out_cpp_filename.replace(".cpp", ".o")
    with open(out_cpp_filename, "w") as f:
        f.write(template_mod)

    # Compile and run the C++ convert program
    subprocess.run(f"g++ -O3 -o {out_cpp_executable} {out_cpp_filename} `root-config --cflags --glibs` -lROOTNTuple",
                   shell=True, check=True)
    subprocess.run(f"./{out_cpp_executable}", shell=True, check=True)

    return [uri.replace("\"", "") for uri in ntupleuris]



if __name__ == "__main__":
    ntuplename = "DecayTree"
    ntupleuris = create_ntuples()

    if(args.nchunks <= 8): 
        # Run on a single NUMA domain
        cpulist = "8-15,24-31"
        numadomain = 1
        cliuris = " ".join(ntupleuris)
        subprocess.run(f"taskset -c {cpulist} python multiproc_run_fromuris.py {numadomain} {cliuris}", shell=True, check=True)
    else:
        # Run on both NUMA domains, assigning each Python process to its own domain
        cpulists = ["8-15,24-31", "0-7,16-23"]
        numadomains = [1, 0]
        cliuris = [" ".join(ntupleuris[:8]), " ".join(ntupleuris[8:])]
        commands = [f"taskset -c {cpulist} python multiproc_run_fromuris.py {numadomain} {uris}"
                    for cpulist, numadomain, uris in zip(cpulists, numadomains, cliuris)]
        subprocs = [subprocess.Popen(command, shell=True) for command in commands]
        for subproc in subprocs:
            subproc.wait()

