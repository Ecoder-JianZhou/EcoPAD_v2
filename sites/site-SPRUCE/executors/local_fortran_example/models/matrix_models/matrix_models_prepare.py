
# 1. prepare the input data: climate (daily and monthly rain and tair); gpp; smoist; stemp
# 2. calculate the scalars: Bscalar; scalar; matix.
# 3. run matrix models
#   input: 
import pandas as pd
import os
import numpy as np
import datetime
import warnings
warnings.filterwarnings("ignore")

def judgeLeap(inYr):
    resLeap = False
    if (inYr%4)==0:
        if (inYr%100)==0:
            if (inYr%400)==0:
                resLeap = True
            else:
                resLeap = False
        else:
            resLeap=True
    else:
        resLeap=False
    return resLeap

def calDateTime(firstYr,endYr,nData):
    df_time = pd.date_range(str(firstYr)+"-01-01",str(endYr)+'-12-31')
    df_hour = pd.date_range(str(firstYr)+"-01-01",str(endYr+1)+'-01-01', freq="h").drop([str(endYr+1)+'-01-01'])
    len_time = len(df_time)
    if nData<len_time: # forcing data has no leap year.
        ls_leap = []
        for iyr in range(firstYr, endYr+1):
            if judgeLeap(iyr):
                ls_leap.append(str(iyr)+"-02-29")
                df_hour = df_hour.drop(df_hour[df_hour.date ==  datetime.date(iyr, 2, 29)])
        df_time = df_time.drop(ls_leap)
    return df_time, df_hour

# def prepareData(firstYr, endYr, outDirName):
def run(dictPaths):
    # dictPaths:
    #   gpp:  Simu_dailyflux14001.txt
    #   solT: Simu_soiltemp.txt
    #   solW: Simu_dailywater001.txt
    #   clim: SPRUCE_forcing_plot17.txt
    #   out:  /output
    # outPath = dictPaths["out"] # "output/process-based-TECO_output"
    # GPP --------------------------------------------------
    # os.makedirs(os.path.join(outDirName,"gpp"), exist_ok = True)
    print(dictPaths)
    outDirName = dictPaths["out"]
    os.makedirs(os.path.join(outDirName,"gpp"), exist_ok = True)
    os.makedirs(os.path.join(outDirName,"climate"), exist_ok = True)
    os.makedirs(os.path.join(outDirName,"climate","stemp"), exist_ok = True)
    os.makedirs(os.path.join(outDirName,"climate","smoist"), exist_ok = True)
    df_gpp  = pd.read_csv(dictPaths["gpp"])  # outPath+"/Simu_dailyflux14001.txt")
    firstYr = df_gpp.loc[0,"year"]
    endYr   = int(df_gpp.iloc[-1]["year"])
    
    df_gpp.columns = ["seqday","year","doy", "GPP_d", "NEE_d", "Reco_d", "NPP_d", "Ra_d", "QC1", "QC2", "QC3", "QC4", "QC5", "QC6", "QC7", "QC8", "Rh_d"]
    dat_gpp  = df_gpp[['GPP_d']]
    df_time, df_hour = calDateTime(firstYr, endYr, len(dat_gpp))
    dat_gpp.index = df_time
    dat_gpp.columns = ["GPP"]
    dat_gpp.to_csv(os.path.join(outDirName,"gpp")+"/gpp_day.csv",index_label="Date")
    dat_gpp_m = dat_gpp.resample('M').sum()
    dat_gpp_m.to_csv(os.path.join(outDirName,"gpp")+"/gpp_month.csv",index_label="Date")
    
    # stemp
    # os.makedirs(os.path.join(outDirName,"stemp"), exist_ok = True) # create output path of stemp
    df_stemp = pd.read_csv(dictPaths["solT"],header=None)   # outPath+"/Simu_soiltemp.txt",header=None)
    df_stemp.columns = ["id","Tem_soil_surface","Tem_soil_0_10cm","Tem_soil_10_20cm","Tem_soil_20_30cm","Tem_soil_30_40cm","Tem_soil_40_50cm","Tem_soil_50_60cm","Tem_soil_60_70cm","Tem_soil_70_80cm","Tem_soil_80_90cm","Tem_soil_90_100cm"]
    dat_stemp = df_stemp[["Tem_soil_surface","Tem_soil_0_10cm","Tem_soil_10_20cm","Tem_soil_20_30cm","Tem_soil_30_40cm","Tem_soil_40_50cm","Tem_soil_50_60cm","Tem_soil_60_70cm","Tem_soil_70_80cm","Tem_soil_80_90cm","Tem_soil_90_100cm"]]
    dat_stemp.index = df_time
    dat_stemp.to_csv(os.path.join(outDirName,"climate","stemp")+"/stemp_day.csv",index_label="Date")
    dat_stemp_m = dat_stemp.resample('M').mean()
    dat_stemp_m.to_csv(os.path.join(outDirName,"climate","stemp")+"/stemp_month.csv",index_label="Date")

    # smoist
    # os.makedirs(os.path.join(outDirName,"smoist"), exist_ok = True) # create output path of smoist
    df_smoist = pd.read_csv(dictPaths["solW"])    # outPath+"/Simu_dailywater001.txt")#,header=None)
    # df_smoist.columns = ["day","wcl1","wcl2","wcl3","wcl4","wcl5","wcl6","wcl7","wcl8","wcl9","wcl10","liq_water1","liq_water2","liq_water3","liq_water4","liq_water5"," liq_water6","liq_water7","liq_water8","liq_water9","liq_water10","ice1","ice2","ice3","ice4","ice5","ice6","ice7","ice8","ice9","ice10","zwt"]
    df_smoist.columns = ["day","Water_soil_0_10cm", "Water_soil_10_20cm","Water_soil_20_30cm","Water_soil_30_40cm","Water_soil_40_50cm","Water_soil_50_60cm","Water_soil_60_70cm","Water_soil_70_80cm","Water_soil_80_90cm","Water_soil_90_100cm","liq_water1","liq_water2","liq_water3","liq_water4","liq_water5"," liq_water6","liq_water7","liq_water8","liq_water9","liq_water10","ice1","ice2","ice3","ice4","ice5","ice6","ice7","ice8","ice9","ice10","zwt"]
    dat_smoist = df_smoist[["Water_soil_0_10cm", "Water_soil_10_20cm","Water_soil_20_30cm","Water_soil_30_40cm","Water_soil_40_50cm","Water_soil_50_60cm","Water_soil_60_70cm","Water_soil_70_80cm","Water_soil_80_90cm","Water_soil_90_100cm"]]
    dat_smoist.index = df_time
    dat_smoist.to_csv(os.path.join(outDirName,"climate","smoist")+"/smoist_day.csv",index_label="Date")
    dat_smoist_m = dat_smoist.resample('M').mean()
    dat_smoist_m.to_csv(os.path.join(outDirName,"climate","smoist")+"/smoist_month.csv",index_label="Date")
  
    # climate: Tair and rain
    # os.makedirs(os.path.join(outDirName,"climate"), exist_ok = True) # create output path of climate
    # cliPath = "input/process-based-TECO_input/outputs_co2"
    try:
        df_data = pd.read_csv(dictPaths["clim"])  # cliPath+"/SPRUCE_forcing_plot17.txt","\t")
        dat_rain = df_data[['Rain']]
    except:
        df_data = pd.read_csv(dictPaths["clim"],"\t")  # cliPath+"/SPRUCE_forcing_plot17.txt","\t")
        dat_rain = df_data[['Rain']]
    dat_rain.columns = ['Rainfall']
    dat_Tair = df_data[['Tair']]
    # ----------------------------------------
    if len(df_hour) > len(dat_rain):
        num = len(df_hour) - len(dat_rain)
        print(num)
        dat_rain = dat_rain.append(dat_rain[:num]).reset_index(drop=True)
        dat_Tair = dat_Tair.append(dat_Tair[:num]).reset_index(drop=True)
    dat_rain.index = df_hour
    dat_Tair.index = df_hour
    dat_rain_d = dat_rain.resample('D').sum()
    dat_Tair_d = dat_Tair.resample('D').mean()
    dat_Tair_d_max = dat_Tair.resample('D').max()
    dat_Tair_d_min = dat_Tair.resample('D').min()
    dat_Tair_d['Range'] = dat_Tair_d_max - dat_Tair_d_min
    # ----------------------------------------------------
    dat_rain_d.to_csv(os.path.join(outDirName,"climate")+"/rainfall_day.csv",index_label="Date")
    dat_Tair_d.to_csv(os.path.join(outDirName,"climate")+"/tair_day.csv",index_label="Date")
    # month
    dat_Tair_m = dat_Tair.resample('M').mean()
    dat_rain_m = dat_rain.resample('M').sum()
    dat_Tair_m_max = dat_Tair_d['Tair'].resample('M').max()
    dat_Tair_m_min = dat_Tair_d['Tair'].resample('M').min()
    dat_Tair_m['Range'] = dat_Tair_m_max - dat_Tair_m_min
    dat_rain_m.to_csv(os.path.join(outDirName,"climate")+"/rainfall_month.csv",index_label="Date")
    dat_Tair_m.to_csv(os.path.join(outDirName,"climate")+"/tair_month.csv",index_label="Date")

# # if __name__=="__main__":
# def run(firstYr, endYr, dictPaths):
#     # dictPaths:
#     #   gpp:  Simu_dailyflux14001.txt
#     #   solT: Simu_soiltemp.txt
#     #   solW: Simu_dailywater001.txt
#     #   clim: SPRUCE_forcing_plot17.txt
#     #   out:  /output
#     prepareData(firstYr,endYr,"output/matrix_models_output")


# if __name__=="__main__":
#     dictPaths = {'gpp': '/mnt/c/Users/jz964/Documents/Ubuntu_docs/ecopad_docs/3_devEcopad_zj/data/test/test_3/output/TECO_SPRUCE/output/Simu_dailyflux14001.csv', 
#     'solT': '/mnt/c/Users/jz964/Documents/Ubuntu_docs/ecopad_docs/3_devEcopad_zj/data/test/test_3/output/TECO_SPRUCE/output/Simu_soiltemp.txt', 
#     'solW': '/mnt/c/Users/jz964/Documents/Ubuntu_docs/ecopad_docs/3_devEcopad_zj/data/test/test_3/output/TECO_SPRUCE/output/Simu_dailywater001.txt', 
#     'clim': '/mnt/c/Users/jz964/Documents/Ubuntu_docs/ecopad_docs/3_devEcopad_zj/data/ecopad_test/sites_data/SPRUCE/forcing_data/SPRUCE_forcing.txt', 
#     'out': '/mnt/c/Users/jz964/Documents/Ubuntu_docs/ecopad_docs/3_devEcopad_zj/data/ecopad_test/sites_data/SPRUCE/matrix_data'}
#     run(dictPaths)