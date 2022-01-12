#include <ROOT/RNTuple.hxx>
#include <ROOT/RNTupleModel.hxx>

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

constexpr static auto ntuplename{"myntuple"};
constexpr static auto filename{"daos://210a3ac0-4589-4498-8822-2a3819f72159:1/15c65284-90b9-4baa-a538-51d4cff6ac19"};

void read_cache()
{
   try {

      auto model = RNTupleModel::Create();
      auto myintfield = model->MakeField<int>("myintfield");
      auto myintfieldsquared = model->MakeField<int>("myintfieldsquared");
      auto ntuple = RNTupleReader::Open(std::move(model), ntuplename, filename);
      ntuple->PrintInfo();

      for (auto entryid: *ntuple){
         ntuple->LoadEntry(entryid);
         std::cout << "Read entry " << entryid << " with value " << *myintfield << "\n";
         std::cout << "Read entry " << entryid << " with value " << *myintfieldsquared << "\n";
      }

   } catch(...) {
      std::cerr << "Some error while reading the cached NTuple.\n";
   }
   
}

int main(){
   read_cache();
}

