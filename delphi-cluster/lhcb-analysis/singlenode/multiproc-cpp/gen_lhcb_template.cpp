#include <ROOT/RField.hxx>
#include <ROOT/RNTuple.hxx>
#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleOptions.hxx>

#include <TBranch.h>
#include <TChain.h>
#include <TFile.h>
#include <TH1F.h>
#include <TLeaf.h>
#include <TROOT.h>
#include <TTree.h>

#include <cassert>
#include <cstdint>
#include <iostream>
#include <memory>
#include <stdlib.h>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

// Import classes from experimental namespace for the time being
using RNTupleModel = ROOT::Experimental::RNTupleModel;
using RFieldBase = ROOT::Experimental::Detail::RFieldBase;
using RNTupleWriter = ROOT::Experimental::RNTupleWriter;
using RNTupleWriteOptions = ROOT::Experimental::RNTupleWriteOptions;
using RNTupleWriteOptionsDaos = ROOT::Experimental::RNTupleWriteOptionsDaos;

void convert(const std::string &outputFile, std::uint64_t start,
             std::uint64_t end) {
  std::string inputFile{"/mnt/cern/tmpfs/B2HHH~none.root"};
  std::cout << "Converting " << inputFile << " --> " << outputFile << std::endl;

  std::unique_ptr<TFile> f(TFile::Open(inputFile.c_str()));
  assert(f && !f->IsZombie());

  // Get a unique pointer to an empty RNTuple model
  auto model = RNTupleModel::Create();

  // We create RNTuple fields based on the types found in the TTree
  // This simple approach only works for trees with simple branches and only one
  // leaf per branch
  TChain chain{"DecayTree"};
  for (int i = 0; i < 20; i++) {
    chain.Add(inputFile.c_str());
  }

  for (const auto &b : TRangeDynCast<TBranch>(*chain.GetListOfBranches())) {
    // The dynamic cast to TBranch should never fail for GetListOfBranches()
    assert(b);

    // We assume every branch has a single leaf
    TLeaf *l = static_cast<TLeaf *>(b->GetListOfLeaves()->First());

    // Create an ntuple field with the same name and type than the tree branch
    auto field = RFieldBase::Create(l->GetName(), l->GetTypeName()).Unwrap();
    std::cout << "Convert leaf " << l->GetName() << " [" << l->GetTypeName()
              << "]"
              << " --> "
              << "field " << field->GetName() << " [" << field->GetType() << "]"
              << std::endl;

    // Hand over ownership of the field to the ntuple model.  This will also
    // create a memory location attached to the model's default entry, that will
    // be used to place the data supposed to be written
    model->AddField(std::move(field));

    // We connect the model's default entry's memory location for the new field
    // to the branch, so that we can fill the ntuple with the data read from the
    // TTree
    void *fieldDataPtr =
        model->GetDefaultEntry()->GetValue(l->GetName()).GetRawPtr();
    chain.SetBranchAddress(b->GetName(), fieldDataPtr);
  }

  // The new ntuple takes ownership of the model
  RNTupleWriteOptionsDaos options;
  options.SetCompression(0);
  options.SetUseBufferedWrite(false);
  options.SetApproxUnzippedPageSize(4194304); // 4MB pages
  options.SetObjectClass("RP_XSF");
  auto ntuple = RNTupleWriter::Recreate(std::move(model), "DecayTree",
                                        outputFile, options);

  ntuple->EnableMetrics();
  std::uint64_t counter{0};
  for (decltype(start) i = start; i < end; ++i) {
    chain.GetEntry(i);
    ntuple->Fill();
    counter++;
    if (counter % 1000000 == 0)
      std::cout << "Wrote " << counter << " entries" << std::endl;
  }
  std::cout << "\nTotal entries written to the current RNTuple: " << counter << "\n\n";
  ntuple->GetMetrics().Print(std::cout);
}

int main() {

  /*REPLACEBYNTUPLEURIS*/

  /*REPLACEBYNTUPLEURANGES*/

  auto ntuples_size = ntupleuris.size();
  for (decltype(ntuples_size) i = 0; i < ntuples_size; i++) {
    convert(ntupleuris[i], ntupleranges[i].first, ntupleranges[i].second);
  }
}


