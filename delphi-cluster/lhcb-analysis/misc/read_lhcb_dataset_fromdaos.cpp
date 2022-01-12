#include <cstdint> // std::int32_t
#include <iostream>
#include <string>
#include <fstream>
#include <iomanip> // std::set_precision

#include <TStopwatch.h>
#include <ROOT/RNTuple.hxx>
#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleOptions.hxx>

// Import classes from experimental namespace for the time being
using RNTupleModel = ROOT::Experimental::RNTupleModel;
using RNTupleReader = ROOT::Experimental::RNTupleReader;
using RNTupleReadOptions = ROOT::Experimental::RNTupleReadOptions;

void readlhcb(const std::string &name, const std::string &path) {
  
  auto model = RNTupleModel::Create();
  auto col1 = model->MakeField<double>("B_FlightDistance");
  auto col2 = model->MakeField<double>("B_VertexChi2");
  auto col3 = model->MakeField<double>("H1_PX");
  auto col4 = model->MakeField<double>("H1_PY");
  auto col5 = model->MakeField<double>("H1_PZ");
  auto col6 = model->MakeField<double>("H1_ProbK");
  auto col7 = model->MakeField<double>("H1_ProbPi");
  auto col8 = model->MakeField<std::int32_t>("H1_Charge");
  auto col9 = model->MakeField<std::int32_t>("H1_isMuon");
  auto col10 = model->MakeField<double>("H1_IpChi2");
  auto col11 = model->MakeField<double>("H2_PX");
  auto col12 = model->MakeField<double>("H2_PY");
  auto col13 = model->MakeField<double>("H2_PZ");
  auto col14 = model->MakeField<double>("H2_ProbK");
  auto col15 = model->MakeField<double>("H2_ProbPi");
  auto col16 = model->MakeField<std::int32_t>("H2_Charge");
  auto col17 = model->MakeField<std::int32_t>("H2_isMuon");
  auto col18 = model->MakeField<double>("H2_IpChi2");
  auto col19 = model->MakeField<double>("H3_PX");
  auto col20 = model->MakeField<double>("H3_PY");
  auto col21 = model->MakeField<double>("H3_PZ");
  auto col22 = model->MakeField<double>("H3_ProbK");
  auto col23 = model->MakeField<double>("H3_ProbPi");
  auto col24 = model->MakeField<std::int32_t>("H3_Charge");
  auto col25 = model->MakeField<std::int32_t>("H3_isMuon");
  auto col26 = model->MakeField<double>("H3_IpChi2");

  auto ntuple = RNTupleReader::Open(std::move(model), name, path);
  ntuple->EnableMetrics();

  std::cout << "RNTuple schema:\n";
  ntuple->PrintInfo();

  std::cout << "Reading the full RNTuple\n";

  TStopwatch t;
  std::string outcsv{"time_to_read_lhcb_dataset.csv"};
  for (auto entryid : *ntuple) {
    ntuple->LoadEntry(entryid);
  }
  double elapsed{t.RealTime()};
  std::cout << "Time to read the lhcb RNTuple: " << std::fixed
            << std::setprecision(2) << elapsed << " s\n";
  std::ofstream timecsv(outcsv.c_str(),
                        std::ofstream::out | std::ofstream::app);
  timecsv << std::fixed << std::setprecision(2) << elapsed << "\n";
  timecsv.close();

  ntuple->PrintInfo(ROOT::Experimental::ENTupleInfo::kMetrics);

}

int main() {
    readlhcb("DecayTree", "daos://fe874479-18d3-450f-a15c-5f52e2e4fc20:1/e4a91548-3979-4eff-9626-4f820484a7fa");
}
