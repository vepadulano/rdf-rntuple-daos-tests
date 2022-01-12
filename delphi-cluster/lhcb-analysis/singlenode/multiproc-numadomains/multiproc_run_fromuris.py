import argparse
import os
import subprocess

import ROOT

import multiprocessing

parser = argparse.ArgumentParser()
parser.add_argument("numadomain", help="Assigned NUMA socket.")
parser.add_argument("ntupleuris", nargs="*", help="Paths to the RNTuple instances used by the analysis.")
args = parser.parse_args()


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

def _analysis_impl(df):
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

    return elapsed


def analysis(ntuplename, ntuplepath, procid):
    pid = multiprocessing.current_process().pid
    r = subprocess.run(f"taskset -cp {pid}", shell=True, check=True, capture_output=True)
    print(f"Process {procid} on cpu list {args.numadomain}: {r.stdout.decode()}")
    myinit()
    metrics = ROOT.Experimental.MetricsWrapper()
    df = ROOT.Experimental.MakeNTupleDataFrame(ntuplename, ntuplepath,
            ROOT.Experimental.RNTupleReadOptions(), metrics.GetPtr())

    elapsed = _analysis_impl(df)

    walltime = metrics.GetMetrics().GetCounter("RPageSourceDaos.timeWallRead").GetValueAsInt()
    readthroughput = metrics.GetMetrics().GetCounter("RPageSourceDaos.bwRead").GetValueAsInt()
    readpayload = metrics.GetMetrics().GetCounter("RPageSourceDaos.szReadPayload").GetValueAsInt()
    
    with open(f"multiproc_lhcb_process{procid}_numa{args.numadomain}.txt", "a+") as f:
        f.write(f"{round(elapsed, 3)},{walltime},{readthroughput},{readpayload}\n")


if __name__ == "__main__":
    ntuplename = "DecayTree"
    nchunks = len(args.ntupleuris)
    outcsv = f"multiproc_lhcb_200x_{nchunks}cores_numa{args.numadomain}.csv"

    with multiprocessing.Pool(processes=nchunks) as pool:
        for _ in range(30):
            watch = ROOT.TStopwatch()
            pool.starmap(analysis, [(ntuplename, ntuplepath, procid) for procid, ntuplepath in enumerate(args.ntupleuris)])
            elapsed = watch.RealTime()
            print(f"lhcb event loop: {round(elapsed, 3)} s")
            with open(outcsv, "a+") as f:
                f.write(f"{round(elapsed, 3)}\n")

    for daosuri in args.ntupleuris:
        subprocess.run(f"daos cont destroy --pool=vpadulan-distrdf --cont={daosuri.split('/')[-1]}", shell=True, check=True)

