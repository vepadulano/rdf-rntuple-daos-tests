#include <ROOT/RNTuple.hxx>
#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleOptions.hxx>

#include <TH1F.h>
#include <TRandom.h>
#include <TH1F.h>
#include <TCanvas.h>

#include <cstdio>
#include <iostream>
#include <memory>
#include <vector>
#include <utility>
#include <string_view>

// Import classes from experimental namespace for the time being
using RNTupleModel = ROOT::Experimental::RNTupleModel;
using RNTupleReader = ROOT::Experimental::RNTupleReader;
using RNTupleWriter = ROOT::Experimental::RNTupleWriter;
using RNTupleWriteOptionsDaos = ROOT::Experimental::RNTupleWriteOptionsDaos;

constexpr static auto ntuplename{"myntuple"};
constexpr static auto filename{"daos://9dbdd80d-8721-4ec4-9a34-307383f64dc5:1/4e1d68e7-0a4a-454d-a5d3-e8f3e2538d6e"};

// Generate kNEvents with vectors in kNTupleFileName
void write_ntuple()
{

   RNTupleWriteOptionsDaos opts;
   opts.SetObjectClass("RP_XSF");
   opts.SetApproxUnzippedPageSize(4 * 1024 * 1024);
   auto model = RNTupleModel::Create();
   auto myintfield = model->MakeField<int>("myintfield");
   auto myintfieldsquared = model->MakeField<int>("myintfieldsquared");

   try {
      auto ntuple = RNTupleWriter::Recreate(std::move(model), ntuplename, filename);
      // Number of entries to generate
      constexpr int nentries = 100000;
      for (int i = 0; i < nentries; i++) {
         *myintfield = i;
         *myintfieldsquared = i*i;
         // Fill the ntuple field with the current value
         ntuple->Fill();
         // Create a cluster every 5 entries
         if (i % 1000 == 0 && i != 0) ntuple->CommitCluster();
      }

   } catch(...) {
      std::cerr << "Some problem while writing the DAOS RNTuple.\n";
   }


   // The ntuple unique pointer goes out of scope here.  On destruction, the ntuple flushes unwritten data to disk
   // and closes the attached ROOT file.
}

void printinfo()
{
   try {
      auto ntuple = RNTupleReader::Open(ntuplename, filename);
      ntuple->PrintInfo();
   } catch(...) {
      std::cerr << "Error while opening the RNTuple.\n";
   }

}


int main(){
   write_ntuple();
   printinfo();
}

