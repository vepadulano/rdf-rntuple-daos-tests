

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <ROOT/RDataFrame.hxx>
#include <ROOT/RNTuple.hxx>
#include <ROOT/RNTupleDS.hxx>
#include <ROOT/RNTupleOptions.hxx>
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
#include <TTreePerfStats.h>
#include <TTreeReader.h>


constexpr double kKaonMassMeV = 493.677;

static double GetP2(double px, double py, double pz) {
  return px * px + py * py + pz * pz;
}

static double GetKE(double px, double py, double pz) {
  double p2 = GetP2(px, py, pz);
  return sqrt(p2 + kKaonMassMeV * kKaonMassMeV);
}

static void NTupleDirect(const std::string &path) {
  using RNTupleReader = ROOT::Experimental::RNTupleReader;
  using RNTupleModel = ROOT::Experimental::RNTupleModel;

  auto ts_init = std::chrono::steady_clock::now();

  auto model = RNTupleModel::Create();

  auto ntuple = RNTupleReader::Open(std::move(model), "DecayTree", path);
  ntuple->PrintInfo();
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

  std::chrono::steady_clock::time_point ts_first =
      std::chrono::steady_clock::now();
  for (auto i : ntuple->GetEntryRange()) {

    if (viewH1IsMuon(i) || viewH2IsMuon(i) || viewH3IsMuon(i)) {
      continue;
    }

    constexpr double prob_k_cut = 0.5;
    if (viewH1ProbK(i) < prob_k_cut)
      continue;
    if (viewH2ProbK(i) < prob_k_cut)
      continue;
    if (viewH3ProbK(i) < prob_k_cut)
      continue;

    constexpr double prob_pi_cut = 0.5;
    if (viewH1ProbPi(i) > prob_pi_cut)
      continue;
    if (viewH2ProbPi(i) > prob_pi_cut)
      continue;
    if (viewH3ProbPi(i) > prob_pi_cut)
      continue;

    double b_px = viewH1PX(i) + viewH2PX(i) + viewH3PX(i);
    double b_py = viewH1PY(i) + viewH2PY(i) + viewH3PY(i);
    double b_pz = viewH1PZ(i) + viewH2PZ(i) + viewH3PZ(i);
    double b_p2 = GetP2(b_px, b_py, b_pz);
    double k1_E = GetKE(viewH1PX(i), viewH1PY(i), viewH1PZ(i));
    double k2_E = GetKE(viewH2PX(i), viewH2PY(i), viewH2PZ(i));
    double k3_E = GetKE(viewH3PX(i), viewH3PY(i), viewH3PZ(i));
    double b_E = k1_E + k2_E + k3_E;
    double b_mass = sqrt(b_E * b_E - b_p2);
    hMass->Fill(b_mass);
  }
  auto ts_end = std::chrono::steady_clock::now();
  auto runtime_init =
      std::chrono::duration_cast<std::chrono::microseconds>(ts_first - ts_init)
          .count();
  auto runtime_analyze =
      std::chrono::duration_cast<std::chrono::microseconds>(ts_end - ts_first)
          .count();

  std::cout << "Runtime-Initialization: " << runtime_init << "us" << std::endl;
  std::cout << "Runtime-Analysis: " << runtime_analyze << "us" << std::endl;

  ntuple->PrintInfo(ROOT::Experimental::ENTupleInfo::kMetrics);

  delete hMass;
}

int main(int argc, char **argv) {
  for (int i = 0; i < 10; i++) {
    NTupleDirect(argv[1]);
  }
}
