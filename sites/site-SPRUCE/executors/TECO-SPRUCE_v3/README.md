This code is used to study the carbon budgets of 10 treatments (+0 , +2.25, +4.5 , +6.75, +9 ℃) in the SPRUCE project, which is the manuscript titled ***"The fate of peatland carbon interactively determined by elevated carbon dioxide and warming."*** 

_This code was wrotten in Fortran and is intended to run in a Linux Environment (also in WSL in Windows). When running it on a MacBook with an Apple M-series chip, slight differences may occur due to differences in floating-point handling._  

***  
- The fold of "TECO_inputs" contains the forcing data, parameters, and observations for each experimental plot
- The most code of TECO-SPRUCE model is in the folder of "TECO-SPRUCE_v3.1", including configs (settings for running model), scripts (build and run model), and src (sourses of the code)
***
# How to run simulation
- Modify the fortran environement path  
  
  _"TECO-SPRUCE_v3.1/configs/env.nml"_  
  ```nml
  ENV_BASE_PATH = "Your Fortran env path"  
- Build the code
  ```shell
  ./scripts/build_teco.sh 
- setting the case in _configs_  
  _For example, in the file of "conf_alltreat/teco_conf_P04.nml", it is the setting for the experimental plot with +4.5 and elevated CO2. You must modify the output dir as your own path._
   
- run simulation
  ``` shell
  ./scripts/run_opt_each_treat.sh  
***
Any question, you can email: j.zhou@cornell.edu (or ecoder-zj@outlook.com)