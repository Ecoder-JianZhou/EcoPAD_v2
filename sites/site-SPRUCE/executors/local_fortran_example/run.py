# this is for running TECO-SPRUCE and matrix-models
# local container provides the TECO-SPRUCE and 8 matrix models.
import argparse
import sys,os

# from importlib import reload
# reload(sys)
# sys.setdefaultencoding('utf8')

def readYml(ymlName):
    import yaml
    from yaml.loader import SafeLoader
    # Open the file and load the file
    with open(ymlName) as f:
        data = yaml.load(f, Loader=SafeLoader)
    return data

def getOutputs4EcoPad(resFromTECO, resFromMat, outPath):
    # automaticall forecast: TECO and matrix-model
    # gpp, npp, nee, er, ra, rh, ecoCstorage
    # id, year, doy, modname
    import pandas as pd
    pd.options.mode.chained_assignment = None
    df_teco = pd.read_csv(resFromTECO)
    df_teco.columns = ["seqday","year","doy","GPP_d","NEE_d","Reco_d","NPP_d","Ra_d","QC1","QC2","QC3","QC4","QC5","QC6","QC7","QC8","Rh_d"]
    # save the results of GPP from TECO_SPRUCE
    df_gpp  = pd.DataFrame(columns=["id","year","doy","TECO_SPRUCE"])
    df_gpp.loc[:,"id"]   = df_teco.loc[:,"seqday"]
    df_gpp.loc[:,"year"] = df_teco.loc[:,"year"]
    df_gpp.loc[:,"doy"]  = df_teco.loc[:,"doy"]
    df_gpp.loc[:,"TECO_SPRUCE"]  = df_teco.loc[:,"GPP_d"]
    df_gpp.to_csv(os.path.join(outPath, "gpp.csv"))
    # -------------------------------------------------
    ls_vars   = ["npp", "nee", "er", "ra", "rh", "cStorage"] # gpp just from TECO_SPRUCE
    dict_vars = {"npp": "NPP_d", "nee": "NEE_d", "er": "Reco_d", "ra":"Ra_d", "rh":"Rh_d"}
    colname   = ["id","year","doy","TECO_SPRUCE","TEM","DALEC","TECO","FBDC","CASA","CENTURY","CLM","ORCHIDEE"]
    lsModName = ["TEM","DALEC","TECO","FBDC","CASA","CENTURY","CLM","ORCHIDEE"]
    ls_out_files = []
    for idx, ivar in enumerate(ls_vars):
        df_all  = df_gpp[["id","year","doy"]] # pd.DataFrame(columns=colname)
        # df_all.insert(df_all.shape[1], "TECO_SPRUCE", df_teco[])
        if ivar == "cStorage": 
            df_all["TECO_SPRUCE"] = df_teco[["QC1","QC2","QC3","QC4","QC5","QC6","QC7","QC8"]].sum(axis=1).to_numpy()
        else:
            df_all["TECO_SPRUCE"] = df_teco.loc[:,dict_vars[ivar]].to_numpy()
        # --------------------------------------------------------
        for idxMod, iMod in enumerate(lsModName):
            df_mat  = pd.read_excel(resFromMat, sheet_name = iMod)  # gpp, npp, nee, er, ra, rh, cStorage
            df_all[iMod] = df_mat.loc[:,ivar]
        df_all.to_csv(outPath+"/"+ivar+".csv") 
        ls_out_files.append(outPath+"/"+ivar+".csv")
        # print(ivar+": ", outPath+"/"+ivar+".")
    return ls_out_files

# def TECO4input():


if __name__=="__main__":
    modname      = None
    file_params  = None
    file_forcing = None
    path_task    = None   # file for showing at website
    path_simu    = None

    try:
        parser = argparse.ArgumentParser(description="command-line")
        parser.add_argument("modname",      type=str, help='input the model name')
        parser.add_argument("file_params",  type=str, help='input the file path of parameters')
        parser.add_argument("file_forcing", type=str, help='input the file path of forcing data')
        parser.add_argument("path_task",    type=str, help='input the path of task')
        parser.add_argument("path_simu",    type=str, help='input the path of simulation (or output)')
        args         = parser.parse_args()
        modname      = args.modname
        file_params  = args.file_params
        file_forcing = args.file_forcing
        path_task    = args.path_task
        path_simu    = args.path_simu
        # --------------------------------------------------------------------------------
        # dictSettings = readYml(settingFile)
        if modname == "TECO_SPRUCE":
            import models.teco_spruce.run_TECO_SPRUCE as teco
            resFile = teco.run(file_params,file_forcing, path_task, path_simu)  # teco.run(dictSettings, resultDir) 
        elif modname == "matrix_models":
            import models.matrix_models.matrix_models_run as matModels # path_gpp, path_scal, resultDir
            path_gpp  = os.path.join(path_task, "input", "gpp")     # dictSettings["matrix_models"]["path_gpp"]
            path_scal = os.path.join(path_task, "input", "scalars") # dictSettings["matrix_models"]["scalars"]
            resFile = matModels.run(path_gpp, path_scal, path_simu)
        elif modname == "all":
            # both run TECO and matrix model for automaticall forecasting
            import models.teco_spruce.run_TECO_SPRUCE as teco
            import models.matrix_models.matrix_models_run as matModels # path_gpp, path_scal, resultDir

            simu_teco   = os.path.join(path_simu, "TECO_SPRUCE")
            simu_matrix = os.path.join(path_simu, "matrix_models")
            os.makedirs(simu_teco, exist_ok = True);   os.makedirs(simu_teco+"/input", exist_ok = True);   os.makedirs(simu_teco+"/output", exist_ok = True); 
            os.makedirs(simu_matrix, exist_ok = True); os.makedirs(simu_matrix+"/input", exist_ok = True); os.makedirs(simu_matrix+"/output", exist_ok = True); 

            resFile_teco = teco.run(file_params,file_forcing, simu_teco, simu_teco+"/output")
            # ------------------------------------------------------
            # put the data to the matrix 
            # ------------------------------------------------------
            import models.matrix_models.matrix_models_prepare   as matPrep 
            # 
            dictPaths = {}
            dictPaths["gpp"]  = simu_teco + "/output/Simu_dailyflux14001.csv"  #   gpp:  Simu_dailyflux14001.txt
            dictPaths["solT"] = simu_teco + "/output/Simu_soiltemp.txt"        #   solT: Simu_soiltemp.txt
            dictPaths["solW"] = simu_teco + "/output/Simu_dailywater001.txt"   #   solW: Simu_dailywater001.txt
            # dictPaths["clim"] = os.path.join(dictSettings["rootPath"],dictSettings["TECO_SPRUCE"]["forcingFile"])   #   clim: SPRUCE_forcing_plot17.txt
            # dictPaths["out"]  = os.path.join(dictSettings["rootPath"],dictSettings["matrix_models"]["path"])        #   out:  /output
            dictPaths["clim"] = file_forcing # dictSettings["TECO_SPRUCE"]["forcingFile"]   #   clim: SPRUCE_forcing_plot17.txt
            # dictPaths["out"]  = dictSettings["matrix_models"]["path"]        #   out:  /output
            dictPaths["out"]  = simu_matrix+"/input" # dictSettings["matrix_models"]["path"]
            print("here ...")
            # ------------------------------------------------------
            # path_gpp     = os.path.join(dictSettings["rootPath"],dictSettings["matrix_models"]["path_gpp"])
            # path_scal    = os.path.join(dictSettings["rootPath"],dictSettings["matrix_models"]["scalars"])
            path_gpp     = simu_matrix+"/input/gpp" # dictSettings["matrix_models"]["path_gpp"]
            path_scal    = simu_matrix+"/input/scalars" # dictSettings["matrix_models"]["scalars"]
            matPrep.run(dictPaths)
            import models.matrix_models.matrix_models_calScalar as calScal
            # calScal.run(os.path.join(dictSettings["rootPath"],dictSettings["matrix_models"]["path"]), os.path.join(dictSettings["rootPath"],dictSettings["matrix_models"]["scalars"]))
            # calScal.run(dictSettings["matrix_models"]["path"], dictSettings["matrix_models"]["scalars"])
            calScal.run(simu_matrix+"/input", path_scal)

            resFile_matrix = matModels.run(path_gpp, path_scal, simu_matrix)
            resFile = getOutputs4EcoPad(resFile_teco, resFile_matrix, path_simu)#os.path.join(path_task,"output"))

        else:
            print("No model name is included.")
            resFile = None
        # ----------------------------------------------------------------------------------

    except Exception as e:
        print(e)
    # return resFile