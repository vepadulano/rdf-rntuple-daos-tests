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
using RNTupleReadOptions = ROOT::Experimental::RNTupleReadOptions;

constexpr static auto ntuplename{"myntuple"};
constexpr static auto filename{"classicntuple.root"};
constexpr static auto cachepath{"mycache_tfile.root"};

// Generate kNEvents with vectors in kNTupleFileName
void write_ntuple()
{

   auto model = RNTupleModel::Create();
   auto myintfield = model->MakeField<int>("myintfield");
   auto myintfieldsquared = model->MakeField<int>("myintfieldsquared");

   try {
      auto ntuple = RNTupleWriter::Recreate(std::move(model), ntuplename, filename);
      // Number of entries to generate
      constexpr int nentries = 100;
      for (int i = 0; i < nentries; i++) {
         *myintfield = i;
         *myintfieldsquared = i*i;
         // Fill the ntuple field with the current value
         ntuple->Fill();
         // Create a cluster every 5 entries
         if (i % 5 == 0 && i != 0) ntuple->CommitCluster();
      }

   } catch(...) {
      std::cerr << "Some problem while writing the file RNTuple.\n";
   }


   // The ntuple unique pointer goes out of scope here.  On destruction, the ntuple flushes unwritten data to disk
   // and closes the attached ROOT file.
}

void read_ntuple(std::uint64_t nentries)
{
   std::cout << "\n\nReading the original RNTuple\n\n";
   try {

      RNTupleReadOptions opts{};
      opts.fCachePath = cachepath;
      auto model = RNTupleModel::Create();
      auto myintfield = model->MakeField<int>("myintfield");
      auto myintfieldsquared = model->MakeField<int>("myintfieldsquared");

      auto ntuple = RNTupleReader::Open(std::move(model), ntuplename, filename, opts);
      ntuple->PrintInfo();

      for (std::uint64_t entry{0}; entry <= nentries; entry++){
         ntuple->LoadEntry(entry);
         std::cout << "Read entry " << entry << " with value " << *myintfield << "\n";
         std::cout << "Read entry " << entry << " with value " << *myintfieldsquared << "\n";
      }


   } catch(...) {
      std::cerr << "Error while opening the RNTuple.\n";
   }

}

void read_cache(std::uint64_t nentries)
{
   std::cout << "\n\nReading the cached RNTuple\n\n";
   try {
      auto model = RNTupleModel::Create();
      auto myintfield = model->MakeField<int>("myintfield");
      auto myintfieldsquared = model->MakeField<int>("myintfieldsquared");

      auto ntuple = RNTupleReader::Open(std::move(model), ntuplename, cachepath);
      ntuple->PrintInfo();

      for (std::uint64_t entry{0}; entry <= nentries; entry++){
         ntuple->LoadEntry(entry);
         std::cout << "Read entry " << entry << " with value " << *myintfield << "\n";
         std::cout << "Read entry " << entry << " with value " << *myintfieldsquared << "\n";
      }


   } catch(...) {
      std::cerr << "Error while opening the RNTuple.\n";
   }

}

int main(){
   write_ntuple();
   read_ntuple(5);
   read_cache(5);
}

