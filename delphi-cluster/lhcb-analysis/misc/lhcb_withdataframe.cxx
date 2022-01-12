/**
 * Copyright CERN; jblomer@cern.ch
 */

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include <ROOT/RDataFrame.hxx>
#include <ROOT/RNTuple.hxx>
#include <ROOT/RNTupleDS.hxx>
#include <ROOT/RNTupleOptions.hxx>
#include <ROOT/RNTupleMetrics.hxx>

#include <Compression.h>
#include <TApplication.h>
#include <TBranch.h>
#include <TCanvas.h>
#include <TClassTable.h>
#include <TFile.h>
#include <TH1D.h>
#include <TROOT.h>
#include <TStyle.h>
#include <TSystem.h>
#include <TTree.h>
#include <TTreeReader.h>
#include <TTreePerfStats.h>


bool g_perf_stats = false;
bool g_show = false;

//static ROOT::Experimental::RNTupleReadOptions GetRNTupleOptions() {
//   using RNTupleReadOptions = ROOT::Experimental::RNTupleReadOptions;
//
//   RNTupleReadOptions options;
//   options.SetClusterCache(RNTupleReadOptions::kOn);
//   std::cout << "{Using async cluster pool}" << std::endl;
//   return options;
//}

constexpr double kKaonMassMeV = 493.677;


static void Show(TH1D *h) {
   new TApplication("", nullptr, nullptr);

   gStyle->SetTextFont(42);
   auto c = new TCanvas("c", "", 800, 700);
   h->GetXaxis()->SetTitle("m_{KKK} [MeV/c^{2}]");
   h->DrawCopy();
   c->Modified();

   std::cout << "press ENTER to exit..." << std::endl;
   auto future = std::async(std::launch::async, getchar);
   while (true) {
      gSystem->ProcessEvents();
      if (future.wait_for(std::chrono::seconds(0)) == std::future_status::ready)
         break;
   }
}


static double GetP2(double px, double py, double pz)
{
   return px*px + py*py + pz*pz;
}

static double GetKE(double px, double py, double pz)
{
   double p2 = GetP2(px, py, pz);
   return sqrt(p2 + kKaonMassMeV*kKaonMassMeV);
}



static void Dataframe(ROOT::RDataFrame &frame)
{
   auto ts_init = std::chrono::steady_clock::now();
   std::chrono::steady_clock::time_point ts_first;

   auto fn_muon_cut_and_stopwatch = [&](unsigned int slot, ULong64_t entry, int is_muon) {
      if (entry == 0) {
         std::cout << "starting timer" << std::endl;
         ts_first = std::chrono::steady_clock::now();
      }
      return !is_muon;
   };
   auto fn_muon_cut = [](int is_muon) { return !is_muon; };
   auto fn_k_cut = [](double prob_k) { return prob_k > 0.5; };
   auto fn_pi_cut = [](double prob_pi) { return prob_pi < 0.5; };
   auto fn_sum = [](double p1, double p2, double p3) { return p1 + p2 + p3; };
   auto fn_mass = [](double B_E, double B_P2) { double r = sqrt(B_E*B_E - B_P2); return r; };

   auto df_muon_cut = frame.Filter(fn_muon_cut_and_stopwatch, {"rdfslot_", "rdfentry_", "H1_isMuon"})
                           .Filter(fn_muon_cut, {"H2_isMuon"})
                           .Filter(fn_muon_cut, {"H3_isMuon"});
   auto df_k_cut = df_muon_cut.Filter(fn_k_cut, {"H1_ProbK"})
                              .Filter(fn_k_cut, {"H2_ProbK"})
                              .Filter(fn_k_cut, {"H3_ProbK"});
   auto df_pi_cut = df_k_cut.Filter(fn_pi_cut, {"H1_ProbPi"})
                            .Filter(fn_pi_cut, {"H2_ProbPi"})
                            .Filter(fn_pi_cut, {"H3_ProbPi"});
   auto df_mass = df_pi_cut.Define("B_PX", fn_sum, {"H1_PX", "H2_PX", "H3_PX"})
                           .Define("B_PY", fn_sum, {"H1_PY", "H2_PY", "H3_PY"})
                           .Define("B_PZ", fn_sum, {"H1_PZ", "H2_PZ", "H3_PZ"})
                           .Define("B_P2", GetP2, {"B_PX", "B_PY", "B_PZ"})
                           .Define("K1_E", GetKE, {"H1_PX", "H1_PY", "H1_PZ"})
                           .Define("K2_E", GetKE, {"H2_PX", "H2_PY", "H2_PZ"})
                           .Define("K3_E", GetKE, {"H3_PX", "H3_PY", "H3_PZ"})
                           .Define("B_E", fn_sum, {"K1_E", "K2_E", "K3_E"})
                           .Define("B_m", fn_mass, {"B_E", "B_P2"});
   auto hMass = df_mass.Histo1D<double>({"B_mass", "", 500, 5050, 5500}, "B_m");

   *hMass;
   auto ts_end = std::chrono::steady_clock::now();
   auto runtime_init = std::chrono::duration_cast<std::chrono::microseconds>(ts_first - ts_init).count();
   auto runtime_analyze = std::chrono::duration_cast<std::chrono::microseconds>(ts_end - ts_first).count();
   std::cout << "Runtime-Initialization: " << runtime_init << "us" << std::endl;
   std::cout << "Runtime-Analysis: " << runtime_analyze << "us" << std::endl;

   if (g_show)
      Show(hMass.GetPtr());
}


static void NTupleDirect(const std::string &path)
{
   using RNTupleReader = ROOT::Experimental::RNTupleReader;
   using RNTupleModel = ROOT::Experimental::RNTupleModel;

   // Trigger download if needed.
   //delete OpenOrDownload(path);

   auto ts_init = std::chrono::steady_clock::now();

   auto model = RNTupleModel::Create();
   //auto options = GetRNTupleOptions();
   auto ntuple = RNTupleReader::Open(std::move(model), "DecayTree", path);
   if (g_perf_stats)
      ntuple->EnableMetrics();

   auto viewH1IsMuon = ntuple->GetView<int>("H1_isMuon");
   auto viewH2IsMuon = ntuple->GetView<int>("H2_isMuon");
   auto viewH3IsMuon = ntuple->GetView<int>("H3_isMuon");

   auto viewH1PX = ntuple->GetView<double>("H1_PX");
   auto viewH1PY = ntuple->GetView<double>("H1_PY");
   auto viewH1PZ = ntuple->GetView<double>("H1_PZ");
   auto viewH1ProbK = ntuple->GetView<double>("H1_ProbK");
   auto viewH1ProbPi = ntuple->GetView<double>("H1_ProbPi");

   auto viewH2PX = ntuple->GetView<double>("H2_PX");
   auto viewH2PY = ntuple->GetView<double>("H2_PY");
   auto viewH2PZ = ntuple->GetView<double>("H2_PZ");
   auto viewH2ProbK = ntuple->GetView<double>("H2_ProbK");
   auto viewH2ProbPi = ntuple->GetView<double>("H2_ProbPi");

   auto viewH3PX = ntuple->GetView<double>("H3_PX");
   auto viewH3PY = ntuple->GetView<double>("H3_PY");
   auto viewH3PZ = ntuple->GetView<double>("H3_PZ");
   auto viewH3ProbK = ntuple->GetView<double>("H3_ProbK");
   auto viewH3ProbPi = ntuple->GetView<double>("H3_ProbPi");

   auto hMass = new TH1D("B_mass", "", 500, 5050, 5500);

   unsigned nevents = 0;
   std::chrono::steady_clock::time_point ts_first = std::chrono::steady_clock::now();
   for (auto i : ntuple->GetEntryRange()) {
      nevents++;
      if ((nevents % 100000) == 0) {
         printf("processed %u k events\n", nevents / 1000);
         //printf("dummy is %lf\n", dummy); abort();
      }

      if (viewH1IsMuon(i) || viewH2IsMuon(i) || viewH3IsMuon(i)) {
         continue;
      }

      constexpr double prob_k_cut = 0.5;
      if (viewH1ProbK(i) < prob_k_cut) continue;
      if (viewH2ProbK(i) < prob_k_cut) continue;
      if (viewH3ProbK(i) < prob_k_cut) continue;

      constexpr double prob_pi_cut = 0.5;
      if (viewH1ProbPi(i) > prob_pi_cut) continue;
      if (viewH2ProbPi(i) > prob_pi_cut) continue;
      if (viewH3ProbPi(i) > prob_pi_cut) continue;

      double b_px = viewH1PX(i) + viewH2PX(i) + viewH3PX(i);
      double b_py = viewH1PY(i) + viewH2PY(i) + viewH3PY(i);
      double b_pz = viewH1PZ(i) + viewH2PZ(i) + viewH3PZ(i);
      double b_p2 = GetP2(b_px, b_py, b_pz);
      double k1_E = GetKE(viewH1PX(i), viewH1PY(i), viewH1PZ(i));
      double k2_E = GetKE(viewH2PX(i), viewH2PY(i), viewH2PZ(i));
      double k3_E = GetKE(viewH3PX(i), viewH3PY(i), viewH3PZ(i));
      double b_E = k1_E + k2_E + k3_E;
      double b_mass = sqrt(b_E*b_E - b_p2);
      hMass->Fill(b_mass);
   }
   auto ts_end = std::chrono::steady_clock::now();
   auto runtime_init = std::chrono::duration_cast<std::chrono::microseconds>(ts_first - ts_init).count();
   auto runtime_analyze = std::chrono::duration_cast<std::chrono::microseconds>(ts_end - ts_first).count();

   std::cout << "Runtime-Initialization: " << runtime_init << "us" << std::endl;
   std::cout << "Runtime-Analysis: " << runtime_analyze << "us" << std::endl;

   if (g_perf_stats)
      ntuple->PrintInfo(ROOT::Experimental::ENTupleInfo::kMetrics);
   if (g_show)
      Show(hMass);

   delete hMass;
}


static void Usage(const char *progname) {
  printf("%s [-i input.root] [-r(df)] [-m(t)] [-p(erformance stats)] [-s(show)]\n", progname);
}


int main(int argc, char **argv) {
   auto ts_init = std::chrono::steady_clock::now();

   std::string input_path;
   std::string input_suffix;
   bool use_rdf = false;
   int c;
   while ((c = getopt(argc, argv, "hvi:rpsm")) != -1) {
      switch (c) {
      case 'h':
      case 'v':
         Usage(argv[0]);
         return 0;
      case 'i':
         input_path = optarg;
         break;
      case 'p':
         g_perf_stats = true;
         break;
      case 's':
         g_show = true;
         break;
      case 'm':
         ROOT::EnableImplicitMT();
         break;
      case 'r':
         use_rdf = true;
         break;
      default:
         fprintf(stderr, "Unknown option: -%c\n", c);
         Usage(argv[0]);
         return 1;
      }
   }
   if (input_path.empty()) {
      Usage(argv[0]);
      return 1;
   }


//   auto suffix = GetSuffix(input_path);
   try {
      if (use_rdf) {
         //using RNTupleDS = ROOT::Experimental::RNTupleDS;
         //auto options = GetRNTupleOptions();
         //auto pageSource = ROOT::Experimental::Detail::RPageSource::Create("DecayTree", input_path, options);
         //ROOT::RDataFrame df(std::make_unique<RNTupleDS>(std::move(pageSource)))
	 for (int i = 0; i < 5; i++){
            ROOT::Experimental::Detail::RNTupleMetrics *metrics = nullptr;

            auto df = ROOT::Experimental::MakeNTupleDataFrame("DecayTree", input_path, ROOT::Experimental::RNTupleReadOptions(), &metrics);
            Dataframe(df);
            metrics->Print(std::cout);

	 }
      } else {
         NTupleDirect(input_path);
      }
   } catch (std::runtime_error &e) {
      std::cerr << "Caught exception: " << e.what() << std::endl;
   }

   auto ts_end = std::chrono::steady_clock::now();
   auto runtime_main = std::chrono::duration_cast<std::chrono::microseconds>(ts_end - ts_init).count();
   std::cout << "Runtime-Main: " << runtime_main << "us" << std::endl;

   return 0;
}
