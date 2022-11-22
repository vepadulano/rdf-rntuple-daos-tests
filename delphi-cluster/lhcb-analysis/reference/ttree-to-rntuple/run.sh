g++ -O3 -o gen_lhcb.o gen_lhcb.cpp `root-config --cflags --glibs` -lROOTNTuple
./gen_lhcb.o | tee gen_lhcb.out
