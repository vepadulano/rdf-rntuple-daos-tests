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

// Import classes from experimental namespace for the time being
using RNTupleModel = ROOT::Experimental::RNTupleModel;
using RNTupleReader = ROOT::Experimental::RNTupleReader;
using RNTupleWriter = ROOT::Experimental::RNTupleWriter;
using RNTupleReadOptions = ROOT::Experimental::RNTupleReadOptions;

constexpr static auto ntuplename{"myntuple"};
constexpr static auto filename{"daos://9dbdd80d-8721-4ec4-9a34-307383f64dc5:1/4e1d68e7-0a4a-454d-a5d3-e8f3e2538d6e"};
constexpr static auto cachepath{"daos://9dbdd80d-8721-4ec4-9a34-307383f64dc5:1/15c65284-90b9-4baa-a538-51d4cff6ac19"};

void read_ntuple(std::uint64_t nentries)
{
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
      std::cerr << "Some problem when reading the RNTuple from DAOS\n";
   }
}

int main(){
   read_ntuple(50000);
}

