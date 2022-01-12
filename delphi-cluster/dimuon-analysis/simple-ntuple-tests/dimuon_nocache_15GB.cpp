#include <ROOT/RDataFrame.hxx>
#include <ROOT/RNTuple.hxx>
#include <ROOT/RNTupleDS.hxx>
#include <ROOT/RNTupleOptions.hxx>
#include <ROOT/RVec.hxx>
 
#include <TCanvas.h>
#include <TH1D.h>
#include <TLatex.h>
#include <TStyle.h>
 
#include <cassert>
#include <cmath>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <utility>
 
// Import classes from experimental namespace for the time being
using RNTupleReader = ROOT::Experimental::RNTupleReader;
using RNTupleDS = ROOT::Experimental::RNTupleDS;
using RNTupleReadOptions = ROOT::Experimental::RNTupleReadOptions;
 
//constexpr auto kNTupleFileName{"http://root.cern.ch/files/ntpl004_dimuon_15GB.root"};
constexpr auto kNTupleFileName{"ntpl004_dimuon_15GB.root"};
//constexpr auto kNTupleFileName{"http://root.cern.ch/files/tutorials/ntpl004_dimuon_v0.root"};
//constexpr auto kNTupleFileName{"ntpl004_dimuon_v0.root"};

/// A wrapper for ROOT's InvariantMass function that takes std::vector instead of RVecs
template <typename T>
T InvariantMassStdVector(std::vector<T>& pt, std::vector<T>& eta, std::vector<T>& phi, std::vector<T>& mass)
{
   assert(pt.size() == 2 && eta.size() == 2 && phi.size() == 2 && mass.size() == 2);

   // We adopt the memory here, no copy
   ROOT::RVec<float> rvPt(pt);
   ROOT::RVec<float> rvEta(eta);
   ROOT::RVec<float> rvPhi(phi);
   ROOT::RVec<float> rvMass(mass);

   return InvariantMass(rvPt, rvEta, rvPhi, rvMass);
}
 
 
void ntpl004_dimuon_cache() {

   auto df = ROOT::Experimental::MakeNTupleDataFrame("Events", kNTupleFileName, RNTupleReadOptions{});


   // The tutorial is identical to df102_NanoAODDimuonAnalysis except the use of
   // InvariantMassStdVector instead of InvariantMass (to be fixed in a later version of RNTuple)

   // For simplicity, select only events with exactly two muons and require opposite charge
   auto df_2mu = df.Filter("nMuon == 2", "Events with exactly two muons");
   auto df_os = df_2mu.Filter("Muon_charge[0] != Muon_charge[1]", "Muons with opposite charge");

   // Compute invariant mass of the dimuon system
   auto df_mass = df_os.Define("Dimuon_mass", InvariantMassStdVector<float>, {"Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass"});
   auto df_size = df_os.Define("eta_size", "Muon_mass.size()");

   // Make histogram of dimuon mass spectrum
   auto h = df_mass.Histo1D({"Dimuon_mass", "Dimuon_mass", 30000, 0.25, 300}, "Dimuon_mass");

   // Request cut-flow report
   auto report = df_mass.Report();

   // Produce plot
   gStyle->SetOptStat(0); gStyle->SetTextFont(42);
   auto c = new TCanvas("c", "", 800, 700);
   c->SetLogx(); c->SetLogy();

   h->SetTitle("");
   h->GetXaxis()->SetTitle("m_{#mu#mu} (GeV)"); h->GetXaxis()->SetTitleSize(0.04);
   h->GetYaxis()->SetTitle("N_{Events}"); h->GetYaxis()->SetTitleSize(0.04);
   h->DrawCopy();

   TLatex label; label.SetNDC(true);
   label.DrawLatex(0.175, 0.740, "#eta");
   label.DrawLatex(0.205, 0.775, "#rho,#omega");
   label.DrawLatex(0.270, 0.740, "#phi");
   label.DrawLatex(0.400, 0.800, "J/#psi");
   label.DrawLatex(0.415, 0.670, "#psi'");
   label.DrawLatex(0.485, 0.700, "Y(1,2,3S)");
   label.DrawLatex(0.755, 0.680, "Z");
   label.SetTextSize(0.040); label.DrawLatex(0.100, 0.920, "#bf{CMS Open Data}");
   label.SetTextSize(0.030); label.DrawLatex(0.630, 0.920, "#sqrt{s} = 8 TeV, L_{int} = 11.6 fb^{-1}");

   // Print cut-flow report
   report->Print();
}

int main(){
    ntpl004_dimuon_cache();
}
