import argparse
import os
import subprocess

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
    # It is 20x the original dataset size.
    dataset_entries = 171122360

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

def compile_lhcb():
    subprocess.run(f"g++ -O3 -o lhcb.o lhcb.cpp `root-config --cflags --glibs` -lROOTNTuple", shell=True, check=True)


if __name__ == "__main__":

    ntupleuris = create_ntuples()

    compile_lhcb()

    bashcommand = "./lhcb.o " + " & ./lhcb.o ".join(ntupleuris) + " &"
    command_out = subprocess.run(bashcommand, shell=True, check=True, capture_output=True)
    #throughputs = [out.split("|")[-1] for out in command_out.stdout.decode().split("\n")[:-1]]
    #print(throughputs)
    with open("cppmultiproc.out", "a+") as f:
        f.write(command_out.stdout.decode())

    if args.storagetype == "filesystem":
        for filename in ntupleuris:
            os.remove(filename)
    elif args.storagetype == "daos":
        for daosuri in ntupleuris:
            subprocess.run(f"daos cont destroy --pool=vpadulan-distrdf --cont={daosuri.split('/')[-1]}", shell=True, check=True)

