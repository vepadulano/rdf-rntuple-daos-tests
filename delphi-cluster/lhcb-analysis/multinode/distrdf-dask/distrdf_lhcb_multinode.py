import argparse
import os
import subprocess

from dask.distributed import Client

import ROOT

from DistRDF import initialize
from DistRDF.Backends import Dask


parser = argparse.ArgumentParser()
parser.add_argument("--storagetype", help="Type of storage chosen for the analysis")
parser.add_argument("--daospool", help="UUID of the DAOS pool used for the tests, if needed")
parser.add_argument("--nodes", help="Number of nodes", type=int)
parser.add_argument("--corespernode", help="Number of cores per node", type=int)
args = parser.parse_args()

NCHUNKS=args.nodes * args.corespernode

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
    # dataset_entries = 1711223600
    # It is 800x the original dataset size.
    dataset_entries = 6844894400

    # Create the python lists of ranges and paths
    ntupleranges = [(r.start, r.stop) for r in n_even_chunks(range(dataset_entries), NCHUNKS)]
    if args.storagetype == "daos":
        uuids = [
            subprocess.run("uuidgen", shell=True, check=True, capture_output=True).stdout.decode().rstrip("\n")
            for _ in range(NCHUNKS)]
        ntupleuris = [f"\"daos://{args.daospool}:1/{container}\"" for container in uuids]
    elif args.storagetype == "filesystem":
        ntupleuris = [f"\"{os.path.join(os.getcwd(), f'lhcb_ntuple_{i}.root')}\"" for i in range(NCHUNKS)]

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

    out_cpp_filename = f"gen_lhcb_{args.storagetype}_{NCHUNKS}chunks.cpp"
    out_cpp_executable = out_cpp_filename.replace(".cpp", ".o")
    with open(out_cpp_filename, "w") as f:
        f.write(template_mod)

    # Compile and run the C++ convert program
    subprocess.run(f"g++ -O3 -o {out_cpp_executable} {out_cpp_filename} `root-config --cflags --glibs` -lROOTNTuple",
                   shell=True, check=True)
    subprocess.run(f"./{out_cpp_executable}", shell=True, check=True)

    return [uri.replace("\"", "") for uri in ntupleuris]


def myinit():
    ROOT.gInterpreter.Declare("""
        #ifndef LHCBANALYSIS
        #define LHCBANALYSIS
        constexpr double kKaonMassMeV = 493.677;

        static double GetP2(double px, double py, double pz)
        {
        return px*px + py*py + pz*pz;
        }

        static double GetKE(double px, double py, double pz)
        {
        double p2 = GetP2(px, py, pz);
        return sqrt(p2 + kKaonMassMeV*kKaonMassMeV);
        }
        #endif
    """)


def analysis(df, logfile="distrdf_lhcb.csv"):

    df_muon_cut = df.Filter("!H1_isMuon")\
                    .Filter("!H2_isMuon")\
                    .Filter("!H3_isMuon")

    df_k_cut = df_muon_cut.Filter("H1_ProbK > 0.5")\
                          .Filter("H2_ProbK > 0.5")\
                          .Filter("H3_ProbK > 0.5")

    df_pi_cut = df_k_cut.Filter("H1_ProbPi < 0.5")\
                        .Filter("H2_ProbPi < 0.5")\
                        .Filter("H3_ProbPi < 0.5")

    df_mass = df_pi_cut.Define("B_PX", "H1_PX + H2_PX + H3_PX")\
                       .Define("B_PY", "H1_PY + H2_PY + H3_PY")\
                       .Define("B_PZ", "H1_PZ + H2_PZ + H3_PZ")\
                       .Define("B_P2", "GetP2(B_PX, B_PY, B_PZ)")\
                       .Define("K1_E", "GetKE(H1_PX, H1_PY, H1_PZ)")\
                       .Define("K2_E", "GetKE(H2_PX, H2_PY, H2_PZ)")\
                       .Define("K3_E", "GetKE(H3_PX, H3_PY, H3_PZ)")\
                       .Define("B_E", "K1_E + K2_E + K3_E")\
                       .Define("B_m", "sqrt(B_E*B_E - B_P2)")

    hMass = df_mass.Histo1D(("B_mass", "", 500, 5050, 5500), "B_m")
    watch = ROOT.TStopwatch()
    entries = hMass.GetEntries()
    elapsed = watch.RealTime()
    print(f"lhcb event loop: {round(elapsed, 3)} s")
    with open(logfile, "a+") as f:
        f.write(f"{round(elapsed, 3)}\n")


if __name__ == "__main__":

    ntuplename = "DecayTree"
    ntupleuris = create_ntuples()

    client = Client("tcp://hl-d104:8786")
    initialize(myinit)
    df = Dask.MakeNTupleDataFrame(ntuplename, ntupleuris, daskclient=client)

    outcsv = f"distrdf_lhcb_800x_{args.storagetype}_{NCHUNKS}chunks_{args.nodes}nodes_{args.corespernode}corespernode.csv"
    outcsv = os.path.join(os.getcwd(), outcsv)
    for i in range(40):
        print(f"starting lhcb analysis {i}")
        analysis(df, outcsv)

    # Restart the connection to the cluster. This avoids that Dask processes
    # hang on to resources that prevent the DAOS agent from properly deleting
    # the containers, leading to DER_BUSY(-1012): 'Device or resource busy' errors
    client.restart()

    for daosuri in ntupleuris:
        subprocess.run(f"daos cont destroy --pool=vpadulan-distrdf --cont={daosuri.split('/')[-1]}", shell=True, check=True)
