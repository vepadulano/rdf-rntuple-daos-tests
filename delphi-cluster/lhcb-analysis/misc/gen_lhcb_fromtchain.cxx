#include <ROOT/RField.hxx>
#include <ROOT/RNTuple.hxx>
#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleOptions.hxx>

#include <TBranch.h>
#include <TCanvas.h>
#include <TFile.h>
#include <TH1F.h>
#include <TLeaf.h>
#include <TTree.h>
#include <TROOT.h>
#include <TChain.h>

#include <cassert>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <stdlib.h>
#include <unistd.h>

#include "util.h"

// Import classes from experimental namespace for the time being
using RNTupleModel = ROOT::Experimental::RNTupleModel;
using RFieldBase = ROOT::Experimental::Detail::RFieldBase;
using RNTupleWriter = ROOT::Experimental::RNTupleWriter;
using RNTupleWriteOptions = ROOT::Experimental::RNTupleWriteOptions;
using RNTupleWriteOptionsDaos = ROOT::Experimental::RNTupleWriteOptionsDaos;

void Usage(char *progname) {
   std::cout << "Usage: " << progname << " -i <B2HHH.root> -o <ntuple-path> -c <compression> -m(t) "
                "[-b <bloat factor>]" << std::endl;
}


int main(int argc, char **argv) {
   std::string inputFile = "B2HHH.root";
   std::string outputPath = ".";
   int compressionSettings = 0;
   std::string compressionShorthand = "none";
   int bloatFactor = 1;

   int c;
   while ((c = getopt(argc, argv, "hvi:o:c:b:m")) != -1) {
      switch (c) {
      case 'h':
      case 'v':
         Usage(argv[0]);
         return 0;
      case 'i':
         inputFile = optarg;
         break;
      case 'o':
         outputPath = optarg;
         break;
      case 'm':
         ROOT::EnableImplicitMT();
         break;
      case 'b':
         bloatFactor = atoi(optarg);
         break;
      default:
         fprintf(stderr, "Unknown option: -%c\n", c);
         Usage(argv[0]);
         return 1;
      }
   }
   std::string bloatTag;
   if (bloatFactor > 1)
      bloatTag = std::string("X") + std::to_string(bloatFactor);
   std::string outputFile = outputPath;
   std::cout << "Converting " << inputFile << " --> " << outputFile << std::endl;

   std::unique_ptr<TFile> f(TFile::Open(inputFile.c_str()));
   assert(f && ! f->IsZombie());

   // Get a unique pointer to an empty RNTuple model
   auto model = RNTupleModel::Create();

   // We create RNTuple fields based on the types found in the TTree
   // This simple approach only works for trees with simple branches and only one leaf per branch
   //auto tree = f->Get<TTree>("DecayTree");
   TChain chain{"DecayTree"};
   for(int i = 0; i < 2; i++){
      chain.Add(inputFile.c_str());
   }

   for (auto b : TRangeDynCast<TBranch>(*chain.GetListOfBranches())) {
      // The dynamic cast to TBranch should never fail for GetListOfBranches()
      assert(b);

      // We assume every branch has a single leaf
      TLeaf *l = static_cast<TLeaf*>(b->GetListOfLeaves()->First());

      // Create an ntuple field with the same name and type than the tree branch
      auto field = RFieldBase::Create(l->GetName(), l->GetTypeName()).Unwrap();
      std::cout << "Convert leaf " << l->GetName() << " [" << l->GetTypeName() << "]"
                << " --> " << "field " << field->GetName() << " [" << field->GetType() << "]" << std::endl;

      // Hand over ownership of the field to the ntuple model.  This will also create a memory location attached
      // to the model's default entry, that will be used to place the data supposed to be written
      model->AddField(std::move(field));

      // We connect the model's default entry's memory location for the new field to the branch, so that we can
      // fill the ntuple with the data read from the TTree
      void *fieldDataPtr = model->GetDefaultEntry()->GetValue(l->GetName()).GetRawPtr();
      chain.SetBranchAddress(b->GetName(), fieldDataPtr);
   }

   // The new ntuple takes ownership of the model
   RNTupleWriteOptionsDaos options;
   options.SetCompression(compressionSettings);
   options.SetUseBufferedWrite(false);
   if (auto val = getenv("RNTUPLE_PAGE_SIZE")) {
	   auto ival = atoi(val);
	   std::cout << "NElementsPerPage=" << ival << " (from RNTUPLE_ELTS_PER_PAGE)" << std::endl;
	   options.SetApproxUnzippedPageSize(ival);
   }
   if (auto val = getenv("RNTUPLE_CLUSTER_SIZE")) {
	   auto ival = atoi(val);
	   std::cout << "NEntriesPerCluster=" << ival << " (from RNTUPLE_CLUSTER_SIZE)" << std::endl;
	   options.SetApproxZippedClusterSize(ival);
   }
   if (auto val = getenv("RNTUPLE_DAOS_OCLASS")) {
	   std::cout << "ObjectClass=" << val << " (from RNTUPLE_DAOS_OCLASS)" << std::endl;
	   options.SetObjectClass(val);
   }
   auto ntuple = RNTupleWriter::Recreate(std::move(model), "DecayTree", outputFile, options);

   ntuple->EnableMetrics();
   for (int b = 0; b < bloatFactor; ++b) {
      auto nEntries = chain.GetEntries();
      for (decltype(nEntries) i = 0; i <= nEntries; ++i) {
         chain.GetEntry(i);
         ntuple->Fill();

         if (i && i % 100000 == 0)
            std::cout << "Wrote " << i << " entries" << std::endl;
      }
   }
   ntuple->GetMetrics().Print(std::cout);
}
