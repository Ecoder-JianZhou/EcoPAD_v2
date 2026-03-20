# TECO-SPRUCE_v3.1
1. build TECO-SPRUCE
   make sure the configs/env.nml has the correct settings about gfortran
   run "./scripts/build_teco.sh" to build TECO-SPRUCE.
   it will show "Build completed successfully!" in the terminal and create a new file of "teco-spruce.exe"
2. run simulation
   run "./scripts/run_alltreat_all_plots.sh", and it will run all experimental plots.
   you can find the results in the fold of "outputs".