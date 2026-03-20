# This code modifies based on Hou et al.,2022, which is R-based code.
# It is used to run 8 matrix-based models: "TEM","DALEC","TECO","FBDC","CASA","CENTURY","CLM","ORCHIDEE".
# include: run spin-up, run simulation and save the traceability results.
#   input: path_gpp, path_scal, resultDir



import numpy as np
import pandas as pd
import copy, os
curPath = os.path.dirname(__file__)

class matrixModel:
    # common form of matrix-based model:
    #   dxPools = Bscalar*mat_B0*GPP - (mat_A*mat_K*scal_env + mat_Tr)*xPools
    def __init__(self, modname, pn, pd_pInfo, arr_initX, mat_B0, mat_A, mat_K, mat_Tr, depth, **kwargs):
        self.modname   = modname                # model name
        self.pn        = pn
        self.pd_pInfo  = pd_pInfo       
        self.arr_initX = arr_initX
        self.mat_B0    = mat_B0                 # allocation coefficients of GPP 
        self.mat_A     = mat_A    
        self.mat_K     = mat_K
        self.mat_Tr    = mat_Tr
        self.mat_B     = self.mat_B0/np.nansum(self.mat_B0)  # change the allocation coefficients based on NPP as input
        self.depth     = depth

    def runMatrix(self, gpp, scal_env, Bscalar, X_last): # each loop of matrix
        I4simu      = np.nansum(self.mat_B0) * gpp       # one number
        mat_scalEnv = np.diag(scal_env)                  # [pn,pn]
        mat_KS      = np.dot(self.mat_K, mat_scalEnv)    # [pn,pn]
        mat_AKST    = np.dot(self.mat_A, mat_KS) + self.mat_Tr      
        delta_x     = Bscalar*self.mat_B * I4simu - np.dot(mat_AKST,X_last.T)
        Xnew        = X_last + delta_x
        # -------------------------------------------------------------
        # Tch, chasing time # R: diag(matrixAK)[diag(matrixAK)<1e-15] = 1e-15
        diag_mat_AKST = np.diagonal(mat_AKST)
        diag_mat_AKST.flags.writeable = True
        diag_mat_AKST[diag_mat_AKST<1e-15] = 1e-15
        mat_AKST[np.diag_indices_from(mat_AKST)] = diag_mat_AKST
        Tch = np.linalg.inv(mat_AKST)       # Tch = solve(matrixAK)
        # Tn, residence time of individual pool in network
        Tn = np.dot(Tch,(Bscalar*self.mat_B).T)   # Tch%*%(Bscalar1[k,]*mat.B )
        Xc = Tn*I4simu                          # Xc, ecosystem storage capacity
        Xp = Xc - Xnew                          # Xp, ecosystem storage potential
        # calculate the net ecosystem exchange of each pool
        nec_pools = Xnew - X_last  # Jian: I think it can be calculated as the different of next pools and last pools
        # Hou et al., 2022 provides two ways to calculate the nec
        # nec_pools = mat_AKST*Xp                        # 1. based on the Carbon Potential: Rcode --> NEC[f,] = matrixAK%*%Xp[f,] 
        # nec_pools = self.mat_B*I4simu - mat_AKST*Xnew  # 2. based on the components of matrixes: Rcode --> NEC2[f,] =  mat.B*I4simu[f]-matrixAK%*%X[g,] 
        dictXsets = {}
        dictXsets["cStorage"]    = Xnew
        # dictXsets["chasingTime"] = Tch
        dictXsets["resTime"]     = Tn
        dictXsets["cCapacity"]   = Xc
        dictXsets["cPotential"]  = Xp
        dictXsets["nec"]         = nec_pools
        return  dictXsets

    def runSimu(self, arr_gpp, arr_scal_env, arr_Bscal, dictOrd, initXpools=None, mat_A4Sum = None):
        len_simu = len(arr_gpp)
        print("run simulation, and the length of simulation:", len_simu)
        # ------ results of carbon flux -------------
        nan_n = np.full(len_simu, np.nan)
        cflux = pd.DataFrame({"Date": np.arange(len_simu), "GPP": nan_n,"Ra":nan_n,"NEC_plant":nan_n,"NEC_litter":nan_n,"NEC_soil":nan_n,
                "Rlit":nan_n,"Rsol":nan_n,"Rfl":nan_n,"Rcwd":nan_n,"Rfs":nan_n,"Rss":nan_n,"Rps":nan_n,  # order 13
                "Ilit":nan_n,"Isol":nan_n,"Ifl":nan_n,"Icwd":nan_n,"Ifs":nan_n,"Iss":nan_n,"Ips":nan_n,
                "Ip":nan_n,"If":nan_n,"Iw":nan_n,"Ir":nan_n})
        # read the order of each part:
        dict_ords     = {"Rlit":dictOrd["litter"],"Rsol":dictOrd["soil"],"Rfl":dictOrd["fineLit"],\
                        "Rcwd":dictOrd["cwd"],"Rfs":dictOrd["fastSoil"],"Rss":dictOrd["slowSoil"],"Rps":dictOrd["passSoil"]}
        dict_ords_I   = {"Rlit":"Ilit","Rsol":"Isol","Rfl":"Ifl","Rcwd":"Icwd","Rfs":"Ifs","Rss":"Iss","Rps":"Ips"}
        dict_ords_IP  = {"Ip":dictOrd["plant"],"If":dictOrd["foliage"],"Iw":dictOrd["wood"],"Ir":dictOrd["root"]}
        dict_ords_nec = {"NEC_plant":dictOrd["plant"],"NEC_litter":dictOrd["litter"],"NEC_soil":dictOrd["soil"]}
        # ---------------------------------------------------------------------------------------------------------------
        if mat_A4Sum == None: mat_A4Sum = self.mat_A
        if initXpools.all() == None: 
            X_last = self.arr_initX    # if not give new init C pool, use the initialized values.
        else:
            X_last = initXpools
        # ------- results of carbon pools -----------
        X_out = np.full((len_simu, len(X_last)), np.nan)
        # ------ start simulation 
        for iLen in range(len_simu):
            dictXsets = self.runMatrix(arr_gpp[iLen], arr_scal_env[iLen,:], arr_Bscal[iLen,:], X_last)
            X_new = dictXsets["cStorage"]
            # calcualate C fluxes
            cflux.loc[iLen,"GPP"]  = arr_gpp[iLen]
            cflux.loc[iLen,"Ra"]   = arr_gpp[iLen]*(1-np.nansum(arr_Bscal[iLen,:]*self.mat_B0))
            # calculate C fluxes: Rate of pools and input of pools
            for key, lsOrd in dict_ords.items():
                if len(lsOrd) >=1:
                    if len(lsOrd) >1: 
                        sum_matA4Sum  = np.nansum(mat_A4Sum[:,lsOrd], axis=0)
                    else:
                        # print("mat_A4Sum:", lsOrd)
                        sum_matA4Sum  = np.nansum(mat_A4Sum[:,lsOrd])
                    temp_ASK      = sum_matA4Sum * arr_scal_env[iLen,lsOrd]*np.diagonal(self.mat_K)[lsOrd]
                    temp_ASKX     = temp_ASK*X_last[lsOrd]*self.depth[lsOrd]
                    cflux.loc[iLen, key] =  np.nansum(temp_ASKX)
                    # calculate the input 
                    temp_ASK4I = np.diagonal(mat_A4Sum)[lsOrd]*arr_scal_env[iLen,lsOrd]*np.diagonal(self.mat_K)[lsOrd]
                    cflux.loc[iLen, dict_ords_I[key]] = np.nansum(temp_ASK4I*X_last[lsOrd]*self.depth[lsOrd])
            # calculate C pools: inputs of plant pools
            for key, lsOrd in dict_ords_IP.items():
                if len(lsOrd) >=1:
                    cflux.loc[iLen, key] = np.nansum(arr_Bscal[iLen,lsOrd]*self.mat_B[lsOrd])*np.nansum(self.mat_B0) * arr_gpp[iLen]
            nec_pools = dictXsets["nec"]
            for key, lsOrd in dict_ords_nec.items():
                if len(lsOrd) >=1:
                    cflux.loc[iLen, key] = np.nansum(nec_pools[lsOrd])
            # update XPools
            X_out[iLen,:] = X_new 
            X_last = X_new
        #----------------------------------------------------------------------------------------------------------------
        X_out_new = X_out*self.depth
        return dictXsets, cflux, X_out, X_out_new

    def runSpinup(self, arr_gpp, arr_scal_env, arr_Bscal):
        # spinup using SASU
        scal_mean      = np.nanmean(arr_scal_env, axis = 0)        # mean of each column 
        mat_scal_mean  = np.diag(scal_mean)                                         # matrix of environmental scalars
        mat_sasu_KS    = np.dot(self.mat_K, mat_scal_mean)
        mat_sasu_AKST  = np.dot(self.mat_A, mat_sasu_KS) + self.mat_Tr
        # calculate input
        Bscal_mean     = np.nanmean(arr_Bscal, axis = 0)
        mat_sasu_BI    = Bscal_mean * self.mat_B * np.nanmean(np.nansum(self.mat_B0) * arr_gpp) # Bscal * mat_B * input
        # steady state of X pools based on SASU
        Xss_sasu = np.dot(np.linalg.inv(mat_sasu_AKST), mat_sasu_BI.T)
        # additional runs to reach steady status
        X_last = Xss_sasu
        for icyc in range(100):
            for iLen in range(len(arr_gpp)):
                dictXsets  = self.runMatrix(arr_gpp[iLen], arr_scal_env[iLen,:], arr_Bscal[iLen,:], X_last)
                X_new  = dictXsets["cStorage"]
                X_last = X_new
            # print(self.modname,X_new)
        return X_new        
        
    def calSaveTraceRes(self):
        print("calculate and save the traceabilty results ...")
        # 

# ===============================================================================================================
def pdMatrix2dict(pd_matrix, pn):
    # Pool_order, Pool3, Pool_major, Pool_name, Initial_X, B(gpp), A(pn), K(pn) Tr(pn)
    # pd_poolInfo, init_X, arr_B0, mat_A, mat_Tr
    dictMat = {}
    dictMat["pd_poolInfo"] = pd_matrix.iloc[:pn,:4].copy()
    dictMat["arr_initX"]   = pd_matrix.loc[:pn-1, "Initial_X"].to_numpy()
    dictMat["arr_B0"]      = pd_matrix.loc[:pn-1, "B"].to_numpy()
    dictMat["mat_A"]       = pd_matrix.iloc[:pn,6:6+pn].to_numpy()
    dictMat["mat_K"]       = pd_matrix.iloc[:pn,6+pn:6+pn*2].to_numpy()
    dictMat["mat_Tr"]      = pd_matrix.iloc[:pn,6+pn*2:6+pn*3].to_numpy()
    # --------------------------------------------------------------------------------
    # get the order of pools to sum up the results
    dictOrd = {}
    # OP: order of plant pools, OL: order of litter pools; OS: order of soil pools 
    dictOrd["plant"]   = list(pd_matrix[pd_matrix["Pool3"]      == "Plant"]["Pool_order"].to_numpy() -1  )   # from 0, not 1
    dictOrd["foliage"] = list(pd_matrix[pd_matrix["Pool_major"] == "Foliage"]["Pool_order"].to_numpy() -1  ) 
    dictOrd["wood"]    = list(pd_matrix[pd_matrix["Pool_major"] == "Wood"]["Pool_order"].to_numpy() -1      )
    dictOrd["root"]    = list(pd_matrix[pd_matrix["Pool_major"] == "Root"]["Pool_order"].to_numpy() -1 )   
    # order of pools for sum up
    dictOrd["litAndSoil"] = list(pd_matrix[(pd_matrix["Pool3"]=="Litter")|(pd_matrix["Pool3"]=="Soil")]["Pool_order"].to_numpy() -1)
    dictOrd["litter"]     = list(pd_matrix[(pd_matrix["Pool3"]=="Litter")]["Pool_order"].to_numpy() -1)
    dictOrd["soil"]       = list(pd_matrix[(pd_matrix["Pool3"]=="Soil")]["Pool_order"].to_numpy() -1 )                            
    dictOrd["fineLit"]    = list(pd_matrix[(pd_matrix["Pool_major"]=="Fine_Litter")]["Pool_order"].to_numpy() -1  )               
    dictOrd["cwd"]        = list(pd_matrix[(pd_matrix["Pool_major"]=="CWD")]["Pool_order"].to_numpy() -1 )                       
    dictOrd["fastSoil"]   = list(pd_matrix[(pd_matrix["Pool_major"]=="Fast_Soil")]["Pool_order"].to_numpy() -1   )              
    dictOrd["slowSoil"]   = list(pd_matrix[(pd_matrix["Pool_major"]=="Slow_Soil")]["Pool_order"].to_numpy()    -1    )           
    dictOrd["passSoil"]   = list(pd_matrix[(pd_matrix["Pool_major"]=="Passive_Soil")]["Pool_order"].to_numpy()   -1     )        
    # ordlst = [OL,OS,OFL,OCWD,OFS,OSS,OPS]
    # nlst   = [len(OL),len(OS),len(OFL),len(OCWD),len(OFS),len(OSS),len(OPS)]
    return dictMat, dictOrd

# # ===============================================================================================================
# def month2Daily():

    
# ===============================================================================================================
def run(path_gpp, path_scal, resultDir):
    Models     = ["TEM","DALEC","TECO","FBDC","CASA","CENTURY","CLM","ORCHIDEE"]
    Timestep   = ["month","day","day","day","month","month","day","day"]
    Pooln      = [2, 6, 8, 13, 14, 15, 71, 101]
    dictTime   = {"date_start": 2011, "date_end": 2014, "freq": "D"}
    # path_gpp   = gppPath #"output/matrix_models_output/gpp/"
    path_depth  = os.path.join(curPath,"depth/depth.xlsx")
    path_matrix = os.path.join(curPath,"matrix/matrix.xlsx")
    # file for ecopad results
    file4Ecopad = resultDir+"/output/results_ecopad.xlsx"
    pd.DataFrame().to_excel(file4Ecopad)  # save the excel file, default create the Sheet1
    writer = pd.ExcelWriter(file4Ecopad, mode="a", engine="openpyxl")
    wb = writer.book
    if "Sheet1" in wb.sheetnames: wb.remove(wb["Sheet1"])
    for imod, modname in enumerate(Models):
        pn = Pooln[imod]
        print(modname, "-pools-", pn)
        pd_matrix        = pd.read_excel(path_matrix,sheet_name = modname)  # Pool_order, Pool3, Pool_major, 
        dictMat, dictOrd = pdMatrix2dict(pd_matrix, pn)
        # read depth file:
        depth = pd.read_excel(path_depth, sheet_name = modname).to_numpy()[0,:pn]
        gpp   = pd.read_csv(os.path.join(path_gpp,"gpp_day.csv")).iloc[:,1:].to_numpy()
        gpp_m = pd.read_csv(os.path.join(path_gpp,"gpp_month.csv")).iloc[:,1:].to_numpy()
        df_dateTime = pd.read_csv(os.path.join(path_gpp,"gpp_day.csv")).loc[:,"Date"]
        df_dateTime_m = pd.read_csv(os.path.join(path_gpp,"gpp_month.csv")).loc[:,"Date"]
        len4sasu = 365
        if Timestep[imod] == "month": dictTime["freq"] = "M"; gpp =  gpp_m; len4sasu=12; df_dateTime=df_dateTime_m
        model    = matrixModel(modname, pn, dictMat["pd_poolInfo"], dictMat["arr_initX"], 
                   dictMat["arr_B0"], dictMat["mat_A"], dictMat["mat_K"], dictMat["mat_Tr"], depth)
        scal_env = pd.read_csv(os.path.join(path_scal,"scalar/")  + modname + ".csv").iloc[:,1:].to_numpy()
        Bscal    = pd.read_csv(os.path.join(path_scal,"Bscalar/") + modname + ".csv").iloc[:,1:].to_numpy()
        Xss      = model.runSpinup(gpp[:len4sasu], scal_env[:len4sasu], Bscal[:len4sasu])
        # print(Xss)
        mat_A4Sum = model.mat_A
        if modname == "ORCHIDEE": 
            for h in range(1,5):
                mat_A4Sum[:,h] = mat_A4Sum[:,h]*depth
        dictXsets, cflux, X_out, X_out_new = model.runSimu(gpp, scal_env, Bscal, dictOrd, initXpools=Xss, mat_A4Sum = None)
        # print(dictXsets)
        # print(dictMat["pd_poolInfo"]["Pool_name"])
        df_x_out = pd.DataFrame()
        print("here ..")
        df_x_out["Date"]=pd.to_datetime(df_dateTime)
        columns=list(dictMat["pd_poolInfo"]["Pool_name"].to_numpy())
        df_x_out.loc[:,columns] = X_out
        df_x_out.set_index("Date", inplace=True)
        df_x_out.to_excel(resultDir+"/output/out_"+modname+".xlsx")
        # pd.DataFrame(X_out,     columns=list(dictMat["pd_poolInfo"]["Pool_name"].to_numpy())).to_excel(resultDir+"/output/out_"+modname+".xlsx")
        df_x_out_new = pd.DataFrame()
        df_x_out_new["Date"] = pd.to_datetime(df_dateTime)
        columns=list(dictMat["pd_poolInfo"]["Pool_name"].values)
        df_x_out_new.loc[:,columns] = X_out_new
        df_x_out_new.set_index("Date", inplace=True)
        # df_x_out_new = pd.DataFrame(X_out_new, columns=list(dictMat["pd_poolInfo"]["Pool_name"].values))
        df_x_out_new.to_excel(resultDir+"/output/out_new_"+modname+".xlsx")
        pd.DataFrame(dictXsets).to_excel(resultDir+"/output/cpools_"+modname+".xlsx")
        df_cflux = cflux.copy()
        df_cflux.loc[:,"Date"] = pd.to_datetime(df_dateTime)
        df_cflux.set_index("Date", inplace=True)
        df_cflux.to_excel(resultDir+"/output/cflux_"+modname+".xlsx")
        # ----------------------------------------------------------------------------------------------------------------------------------------
        # get ouputs for EcoPad: npp, nee, er, ra, rh, cStorage
        # npp = GPP-Ra; Rh = Rlit+Rsol; nee = Ra+Rlit+Rsol-GPP; er=Ra+Rlit+Rsol; cStorage=
        df_res = pd.DataFrame()
        df_res["npp"] = cflux.loc[:,"GPP"].to_numpy() - cflux.loc[:,"Ra"].to_numpy()
        df_res["nee"] = cflux[["Ra","Rlit","Rsol"]].sum(axis=1).to_numpy() - cflux.loc[:,"GPP"].to_numpy()
        df_res["er"]  = cflux[["Ra","Rlit","Rsol"]].sum(axis=1)
        df_res["ra"]  = cflux["Ra"]
        df_res["rh"]  = cflux[["Rlit", "Rsol"]].sum(axis=1)
        if Timestep[imod] == "month": df_res = df_res/30 
        df_res["cStorage"] = pd.DataFrame(X_out_new, columns=list(dictMat["pd_poolInfo"]["Pool_name"].values)).sum(axis=1)
        if Timestep[imod] == "month":
            df_res["Date"] = pd.to_datetime(df_dateTime).dt.to_period('m')
        else:
            df_res["Date"] = pd.to_datetime(df_dateTime)
        df_res.set_index("Date",inplace=True)
        
        if Timestep[imod] == "month":
            df_res = df_res.resample('d').ffill().to_timestamp()
        if modname in wb.sheetnames: wb.remove(wb[modname])
        df_res.to_excel(writer, sheet_name=modname)
    writer.save()
    writer.close()
    return file4Ecopad
