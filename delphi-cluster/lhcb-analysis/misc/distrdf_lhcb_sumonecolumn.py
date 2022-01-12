import ROOT

from DistRDF import initialize
from DistRDF.Backends import Spark

#ntuplename = "Events"
#ntuplefiles = ["ntpl004_dimuon_1_5GB_4MBpage_50MBcluster.root"]

# Dimuon dataset split in three (10 M events each)
#ntuplefiles = [
#"daos://3a26098d-8f18-4e9b-b496-83cf4d75bbcd:1/54013869-22fa-4d63-872b-731e50383ac8",
#"daos://3a26098d-8f18-4e9b-b496-83cf4d75bbcd:1/fd9c89b1-d4c2-416f-b7f0-79148772e2e0",
#"daos://3a26098d-8f18-4e9b-b496-83cf4d75bbcd:1/af23ff69-12d6-4def-a0f9-306d9d32b487",
#]

# lhcb dataset
ntuplename = "DecayTree"
ntuplefiles = "daos://fe874479-18d3-450f-a15c-5f52e2e4fc20:1/e4a91548-3979-4eff-9626-4f820484a7fa"

def analysis(df):

    s = df.Sum("H1_Charge")
    watch = ROOT.TStopwatch()
    sumvalue = s.GetValue()
    elapsed = watch.RealTime()
    print(f"Sum of column: {sumvalue}")
    print(f"lhcb event loop: {round(elapsed, 3)}")

if __name__ == "__main__":
    df = Spark.MakeNTupleDataFrame(ntuplename, ntuplefiles)
    analysis(df)




