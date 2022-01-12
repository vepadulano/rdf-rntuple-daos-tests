import ROOT

from DistRDF import initialize
from DistRDF.Backends import Spark

import pyspark

def myinit():
    ROOT.gInterpreter.Declare(
    """
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
    """
    )

def analysis(df):

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
   print(f"Entries in the histogram: {entries}")
   print(f"lhcb event loop: {round(elapsed, 3)}")


if __name__ == "__main__":
    treename = "DecayTree"
    filenames = [
        "daos://fe874479-18d3-450f-a15c-5f52e2e4fc20:1/e4a91548-3979-4eff-9626-4f820484a7fa"
    ]
    sconf = pyspark.SparkConf().setAll([("spark.master",f"local[{len(filenames)}]")])
    sc = pyspark.SparkContext(conf=sconf)
    initialize(myinit)
    df = Spark.MakeNTupleDataFrame(treename, filenames, sparkcontext=sc)
   # filenames = ["/mnt/cern/tmpfs/B2HHH~none.root"]
   # filenames = ["daos://3a26098d-8f18-4e9b-b496-83cf4d75bbcd:1/54013869-22fa-4d63-872b-731e50383ac8"]
   # df = ROOT.RDataFrame(treename, filenames)
    for i in range(1):
        analysis(df)
