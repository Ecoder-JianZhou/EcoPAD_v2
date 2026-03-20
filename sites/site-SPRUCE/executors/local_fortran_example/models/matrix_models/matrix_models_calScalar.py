# calculate the environmental scalars

import pandas as pd
import numpy as np
import copy
import os

def readEnvData(filePath):
    envData  = {}
    print(filePath)
    envData["tair_d"]   = pd.read_csv(filePath+"/climate/tair_day.csv")
    envData["tair_m"]   = pd.read_csv(filePath+"/climate/tair_month.csv")
    envData["rain_d"]   = pd.read_csv(filePath+"/climate/rainfall_day.csv")
    envData["rain_m"]   = pd.read_csv(filePath+"/climate/rainfall_month.csv")
    envData["stemp_d"]  = pd.read_csv(filePath+"/climate/stemp/stemp_day.csv")
    envData["stemp_m"]  = pd.read_csv(filePath+"/climate/stemp/stemp_month.csv")
    envData["smoist_d"] = pd.read_csv(filePath+"/climate/smoist/smoist_day.csv")
    envData["smoist_m"] = pd.read_csv(filePath+"/climate/smoist/smoist_month.csv")
    return envData
    

def calScalars_TEM(tair_m, smoist_m): # monthly 
    # according to Raich, J. W., et al. (1991). Ecological Applications 1(4): 399-429.
    scal_temp_tem1 = np.exp(0.0693*tair_m["Tair"].to_numpy())
    # parameters for loam soil
    m1    = 0.14   # a parameter defining the skewness of the curve
    Msat  = 0.625  # a parmeter that determines the value of moist when the soil pore space is saturated with water
    Mopt  = 68.0   # the soil moisture content at which heterotrophic respiration is maximum
    moi   = np.nanmean(smoist_m.iloc[:,1:].to_numpy(), axis=1)   # unit: %
    moi   = (moi/np.nanmax(np.nanmax(moi)))*100
    B_tem = ((moi**m1-Mopt**m1)/(Mopt**m1-100**m1))**2
    scal_water_tem1=0.8*Msat**B_tem+0.2
    # ------------------------------------------------------
    scal_env_tem1 = scal_temp_tem1*scal_water_tem1

    scal_temp_tem = np.ones((len(tair_m),2))
    scal_water_tem = np.ones((len(smoist_m),2))
    scal_env_tem   = np.ones((len(smoist_m),2))

    scal_temp_tem[:,1]  = scal_temp_tem1
    scal_water_tem[:,1] = scal_water_tem1
    scal_env_tem[:,1]   = scal_env_tem1

    Bscalar_tem  = np.ones(scal_env_tem.shape) #scal_env_tem = 1 #?
    # return scal_temp, scal_water, scal_env, Bscalar
    return scal_temp_tem, scal_water_tem, scal_env_tem, Bscalar_tem

def calScalars_CENTURY(tair_d, tair_m, rain_m, stemp_m): # monthly
    # CENTURY ############################
    # according to Parton et al. (1987). Soil Science Society of America Journal, 51, 1173-1179.
    # and . Burke, et al. (2003). Evaluating and testing models of terrestrial biogeochemistry: the role of temperature in controlling decomposition. 225-253.
    # ---------------------------------------------------------------------------------------------------------------------------------------------------------
    # Temperature scalar
    tmax = 45
    topt = 35
    t    = (stemp_m.loc[:,"Tem_soil_surface"].to_numpy() + stemp_m.loc[:,"Tem_soil_0_10cm"].to_numpy())/2
    scal_temp_tem1 = (((tmax-t)/(tmax-topt))**0.2)*np.exp((0.2/2.63)*(1-((tmax-t)/(tmax-topt))**2.63))
    # ------------------------------------------------------------------------------------------------
    # water scalar
    # according to Linacre, E.T. (1977). Agricultural Meteorology, 18, 409-424.
    tair_np_d = tair_d.loc[:,"Tair"].to_numpy()
    tair_np_m = tair_m.loc[:,"Tair"].to_numpy()
    Lat   = 47.503 # latitude
    alt   = 422    # elevation
    Tm    = tair_np_d + 0.006*alt  # daily 
    Rann  = np.nanmax(np.nanmax(tair_np_m))-np.nanmin(tair_np_m)
    Tdiff = 0.0023*alt+0.37*tair_np_d+0.53*tair_d.loc[:,"Range"].to_numpy()+0.35*Rann-10.9
    Tdiff[Tdiff<4]=4
    pet_d=(700*Tm/(100-Lat)+15*Tdiff)/(80-tair_np_d)  # pet = pet.d # R: pet.d=data.frame(Date=rain.d$Date,pet=(700*Tm/(100-Lat)+15*Tdiff)/(80-tair.d$Tair))
    pet_d[pet_d<0.1] = 0.1                            # R: pet.d$pet[pet.d$pet<0.1]=0.1

    df_pet_d = pd.DataFrame(pd.to_datetime(copy.deepcopy(tair_d.loc[:,"Date"])))
    # df_pet_d = pd.DataFrame({"Date": pd.to_datetime(copy.deepcopy(tair_d.loc[:,"Date"])), "PET":pet_d})
    df_pet_d["PET"] = pet_d       # R: pet.d$Month=format(as.Date(pet.d$Date),format="%Y-%m")
    # df_pet_d["Date"] = pd.to_datetime(tair_d.loc[:,"Date"])
    df_pet_d = df_pet_d.set_index("Date")
    df_pet_m = df_pet_d.resample('M').sum()                                         # R: pet.m=aggregate(pet.d$pet,list(pet.d$Month),sum)
    df_pet_m.columns = ["PET"]       # R: colnames(pet.m)=c("Date","PET")
    # According to Fujita, et al. (2013). Soil Biology and Biochemistry, 58, 302-312.
    scal_water_tem1 = 1/(1+30*np.exp(-8.5*rain_m.loc[:,"Rainfall"].to_numpy()/df_pet_m.loc[:,"PET"].to_numpy()))  # water.scal.cen=1/(1+30*exp(-8.5*rain.m$Rainfall/pet.m$PET))
    scal_env_tem1   = scal_temp_tem1*scal_water_tem1
    # -------------------------------------------------------------------------------------------------------------
    len_m = len(tair_np_m)
    len_y = int(len_m/12)
    scal_temp_tem  = np.ones((len_m, 15))
    scal_water_tem = np.ones((len_m, 15))
    scal_env_tem   = np.ones((len_m, 15))
    fix_temp = np.array([0.3611,0.3611,0.3611,0.3611,0.3611,0.3611,0.3611,0.3611,0.6666,1,1,1]).reshape(12,1)
    scal_temp_tem[:,0]   = np.squeeze(np.tile(fix_temp,(len_y,1)))
    scal_temp_tem[:,5:]  = np.tile(scal_temp_tem1.reshape(len(scal_temp_tem1),1),10)
    scal_water_tem[:,5:] = np.tile(scal_water_tem1.reshape(len(scal_water_tem1),1),10)
    scal_env_tem[:,5:]   = np.tile(scal_env_tem1.reshape(len(scal_env_tem1),1),10)

    Bscalar_tem = np.ones((len_m, 15))
    return scal_temp_tem, scal_water_tem, scal_env_tem, Bscalar_tem, df_pet_m.loc[:,"PET"].to_numpy()

def calScalars_TECO(stemp_d, smoist_d):
    # TECO ###############  
    tmp_teco=np.nanmean(stemp_d.iloc[:,1:].to_numpy()[:,2:],  axis=1)  # mean of 10 layers of soils
    mst_teco=np.nanmean(smoist_d.iloc[:,1:].to_numpy()[:,1:], axis=1)  # mean of 10 layers of soils
    Q10=2.5 # unitless	sensitive of microbial decomposition to temperature
    
    # temperature scalar based on 10-day moving temperature
    t_movestep = 10  # moving step for calculating mean temperature, e.g., 10 means 10-day moving mean
    # R: t_movemean=rollmean(tmp.teco,t.movestep,fill = tmp.teco[t.movestep-1],align = "right")
    t_movemean = np.convolve(np.append(tmp_teco, np.ones(t_movestep-1)*tmp_teco[-1]), np.ones(t_movestep), 'valid')/t_movestep
    scal_temp_tem1 = 0.58*(Q10**((t_movemean-10)/10))
    
    # water scalar based on a critical soil water content
    scal_water_tem1=np.ones(len(mst_teco))
    scal_water_tem1[mst_teco<0.2]=5.0*mst_teco[mst_teco<0.2]
    scal_env_tem1=scal_temp_tem1*scal_water_tem1

    scal_temp_tem  = np.tile(scal_temp_tem1.reshape(len(scal_temp_tem1),   1), 8)
    scal_water_tem = np.tile(scal_water_tem1.reshape(len(scal_water_tem1), 1), 8)
    scal_env_tem   = np.tile(scal_env_tem1.reshape(len(scal_env_tem1),     1), 8)
    
    # B scalar
    Bscalar_tem=np.ones(scal_env_tem.shape)
    return scal_temp_tem, scal_water_tem, scal_env_tem, Bscalar_tem

def calScalars_CASA(tair_d, tair_m,stemp_m, smoist_m, rain_m):
    # CASA #######################
    tmp_casa = tair_m.loc[:,"Tair"].to_numpy()  # air temperature
    Q10_casa = 1.5
    scal_temp_tem1 = Q10_casa**((tmp_casa-30)/10)
    # wiltpt=0.2  # cm3 cm-3, a guess # Jian: some wrong because of the smoist simulated less than 0.2
    wiltpt = 0.002
    # names(smoist.m)
    soilm  = (np.nanmean(smoist_m.iloc[:,1:4].to_numpy(), axis=1)-wiltpt)*300    # available soil water content in the first 30 cm depth
    pet_m  = calScalars_CENTURY(tair_d, tair_m, rain_m, stemp_m)[4]
    smoist = (rain_m.loc[:,"Rainfall"].to_numpy()+soilm)/pet_m

    scal_water_tem1=np.zeros(len(smoist))
    scal_water_tem1[smoist<1]=smoist[smoist<1]
    scal_water_tem1[np.logical_and(smoist>=1,smoist<2)]=1
    scal_water_tem1[np.logical_and(smoist>=2,smoist<10)]=1-(smoist[np.logical_and(smoist>=2, smoist<10)]-2)*0.5/8
    scal_water_tem1[smoist>=10]=0.5
    scal_env_tem1 = scal_temp_tem1*scal_water_tem1

    scal_temp_tem  = np.ones((len(scal_temp_tem1),  14))
    scal_water_tem = np.ones((len(scal_water_tem1), 14))
    scal_env_tem   = np.ones((len(scal_env_tem1),   14))

    scal_temp_tem[:,4:]  = np.tile(scal_temp_tem1.reshape(len(scal_temp_tem1),   1), 10)
    scal_water_tem[:,4:] = np.tile(scal_water_tem1.reshape(len(scal_water_tem1), 1), 10)
    scal_env_tem[:,4:]   = np.tile(scal_env_tem1.reshape(len(scal_env_tem1),     1), 10)

    Bscalar_tem = np.ones(scal_env_tem.shape)
    return scal_temp_tem, scal_water_tem, scal_env_tem, Bscalar_tem


def calScalars_FBDC(tair_d):
    # FBDC ########################
    tmp_fbdc=tair_d.loc[:,"Tair"].to_numpy() # air temperature
    standard_t_fbdc=10 # oC	standard temperature for calculating Q10
    Q10_fbdc =np.array([1.83,1.83,2.14,1.83,1.83,2.4,2.4,2.4]) # unitless	sensitive of microbial decomposition to temperature of pools 6-13
    # scal.out.fbdc=data.frame(Date=dayseq365$Date,
    #                         scalar1=1,scalar2=1,scalar3=1,scalar4=1,scalar5=1,
    #                         scalar6=NA,scalar7=NA,scalar8=NA,scalar9=NA,scalar10=NA,
    #                         scalar11=NA,scalar12=NA,scalar13=NA )
    # for(i in 1:length(Q10_fbdc)) {
    # scal.out.fbdc[,6+i]=Q10.fbdc[i]^((tmp.fbdc-standard_t.fbdc)/10)
    # }
    scal_temp_tem = np.ones((len(tmp_fbdc), 13))
    scal_temp_tem[:,5:] = np.nan
    for i in range(len(Q10_fbdc)):
        scal_temp_tem[:,5+i] = Q10_fbdc[i]**((tmp_fbdc-standard_t_fbdc)/10)
    
    scal_water_tem = np.ones((len(tmp_fbdc), 13))
    scal_env_tem = scal_temp_tem*scal_water_tem
    # B scalar
    Bscalar_tem = np.ones((len(tmp_fbdc), 13))
    return scal_temp_tem, scal_water_tem, scal_env_tem, Bscalar_tem

def calScalars_DALEC2(stemp_d):
    # DALEC2 ###############################    
    pi = 3.1415927
    ml = 1.001 #Minimum labile life span to one year
    # pars_1 = 0.00458904  # litter to SOM conversion rate  # part of litter turnover
    pars_5 = 1.5  # leaf lifespan; unit: year
    pars_6 = 0.00059  # wood turnover rate, unit: gC g-1 day-1
    pars_7 = 0.001027299 # root turnover rate, unit: gC g-1 day-1
    pars_8 = 0.00191744 # litter turnover rate, unit: gC g-1 day-1
    pars_9 = 0.000033 # SOM turnover rate, unit: gC g-1 day-1
    pars_10 = 0.0663966  # parameter in exponential term of temperature
    pars_12 = 139.9738 # date of labile c release; unit: doy
    pars_14 = 23.76369 # labile C release duration period, unit: day
    pars_15 = 257.5559 # date of leaf fall; unit: doy
    pars_16 = 42.44377 # leaf fall duration period; unit: day
    
    datseq=np.linspace(1,len(stemp_d),len(stemp_d))
    mxc = np.array([0.000023599784710,0.000332730053021,0.000901865258885,-0.005437736864888,-0.020836027517787,0.126972018064287,-0.188459767342504])
    
    kbase=np.zeros(6)
    kbase[0]=1/pars_14 # base turnover rate of labile C, unit: gC g-1 day-1
    kbase[1]=1/(pars_5*365.25) # base turnover rate of leaf, unit: gC g-1 day-1
    kbase[2]=pars_7 # base turnover rate of root, unit gC g-1 day-1
    kbase[3]=pars_6 # base turnover rate of wood, unit gC g-1 day-1
    kbase[4]=pars_8 # base turnover rate of litter, unit gC g-1 day-1
    kbase[5]=pars_9 # base turnover rate of som, unit gC g-1 day-1
        
    # calculate k1 according to penology
    L_sf = ml
    LLog_sf = np.log(L_sf - 1)
    sf=365.25/pi
    
    fl=(np.log(1.001)-np.log(0.001))/2
    wl = pars_14*np.sqrt(2)/2
    osl = (mxc[0]*LLog_sf**6+mxc[1]*LLog_sf**5+mxc[2]*LLog_sf**4+mxc[3]*LLog_sf**3+mxc[4]*LLog_sf**2+mxc[5]*LLog_sf+mxc[6]) * wl
    F_16 = (2/(pi**0.5))*(fl/wl)*np.exp(-((np.sin((datseq-pars_12+osl)/sf)*sf/wl)**2)) 
    k1 = (1.0 - (1.0 - F_16)**1) / 1
    k1 = np.where(k1==0,0.00001,k1)  # turnover rate of labile c pool
    # hist(k1)
    
    # calculate k2 according to penology
    L_osf = pars_5
    LLog_osf = np.log(L_osf - 1)
    ff  = (np.log(pars_5)-np.log(pars_5-1))/2
    wf  = pars_16*np.sqrt(2)/2
    osf = (mxc[0]*LLog_osf**6+mxc[1]*LLog_osf**5+mxc[2]*LLog_osf**4+mxc[3]*LLog_osf**3+mxc[4]*LLog_osf**2+mxc[5]*LLog_osf+mxc[6])*wf
    F_9 = (2/(pi**0.5))*(ff/wf)*np.exp(-((np.sin((datseq-pars_15+osf)/sf)*sf/wf)**2)) 
    k2  = (1.0 - (1.0 - F_9)**1) / 1  # turnover rate of leaf c pool, i.e. leaf litter production
    
    # calculate k3
    k3 = (1.0 - (1.0 - pars_7)**1) / 1  # turnover rate of root c pool, i.e. root litter production
    
    # calculate k4
    k4 = (1.0 - (1.0 - pars_6)**1) / 1 # turnover rate of wood c pool, i.e. wood litter production
    
    # calculate k5
    F_2 = np.exp(pars_10 * 0.5 * (stemp_d.loc[:,"Tem_soil_surface"].to_numpy()+stemp_d.loc[:,"Tem_soil_0_10cm"].to_numpy())) 
    k5 = (1.0 - (1.0 - F_2 * pars_8)**1.0) / 1.0  # turnover rate of litter c pool
    
    # calculate k6
    k6 = (1.0 - (1.0 - F_2 * pars_9)**1) / 1 # turnover rate of SOM
    #------------------------------------------------------------------------------
    scal_env_tem = np.ones((len(stemp_d), 6))
    scal_env_tem[:,0] = k1/kbase[0]
    scal_env_tem[:,1] = k2/kbase[1]
    scal_env_tem[:,2] = k3/kbase[2]
    scal_env_tem[:,3] = k4/kbase[3]
    scal_env_tem[:,4] = k5/kbase[4]
    scal_env_tem[:,5] = k6/kbase[5]

    scal_water_tem = np.ones((len(stemp_d), 6))
    scal_temp_tem  = scal_env_tem 

    Bscalar_tem = np.ones((len(stemp_d), 6))
    return scal_temp_tem, scal_water_tem, scal_env_tem, Bscalar_tem

def calScalars_CLM(stemp_d, smoist_d):
    # CLM#######################
    # according to Oleson et al. 2013, Technical notes on CLM v4.5
    thick_clm  = np.array([0.018,0.028,0.045,0.075,0.124,0.204,0.336,0.554,0.913,1.506])*100  # thickness of each soil layer
    sdepth_clm = np.zeros(len(thick_clm))  # depth of each soil layer
    for i in range(len(thick_clm)):
        sdepth_clm[i]=np.nansum(thick_clm[:i])
    
    # calculate temperature scalar
    tsoil_clm = np.zeros((len(stemp_d),10))     # matrix(0,nrow = 2555,ncol = 10)
    tsoil_clm[:,:3] = np.tile(stemp_d.loc[:,"Tem_soil_0_10cm"].to_numpy().reshape(len(stemp_d), 1), 3)  # np.tile(scal_env_tem1.reshape(len(scal_env_tem1),     1), 10)
    tsoil_clm[:,3]  = stemp_d.loc[:,"Tem_soil_10_20cm"].to_numpy()
    tsoil_clm[:,4]  = stemp_d.loc[:,"Tem_soil_20_30cm"].to_numpy()
    tsoil_clm[:,5]  = stemp_d.loc[:,"Tem_soil_40_50cm"].to_numpy()
    tsoil_clm[:,6]  = stemp_d.loc[:,"Tem_soil_80_90cm"].to_numpy()
    tsoil_clm[:,7:] = np.tile(stemp_d.loc[:,"Tem_soil_90_100cm"].to_numpy().reshape(len(stemp_d), 1), 3)
    
    scal_temp_tem1 = np.ones((len(stemp_d), 10))     # matrix(1,nrow = 2555,ncol = 10)
    Q10_clm  = 1.5
    Tref_clm = 25 # oC
    for a in range(len(stemp_d)):
        for b in range(10):
            scal_temp_tem1[a,b]=Q10_clm**((tsoil_clm[a,b]-Tref_clm)/10)
    
    # calculate soil water scalar, do not consider frozen 
    Psand = 2  # sand proportion, unit: %
    Pclay = 2  # clay proportion, unit: %
    smoist_sat=0.6  # saturated soil moisture content
    f_om=0.9  # fraction of soil organic matter fraction
    Bom=2.7
    
    smoist_clm = np.zeros((len(smoist_d), 10))    # matrix(0,nrow = 2555,ncol = 10)
    smoist_clm[:,:3] = np.tile(smoist_d.loc[:,"Water_soil_0_10cm"].to_numpy().reshape(len(stemp_d), 1), 3)
    smoist_clm[:,3]  = smoist_d.loc[:,"Water_soil_10_20cm"].to_numpy()
    smoist_clm[:,4]  = smoist_d.loc[:,"Water_soil_20_30cm"].to_numpy()
    smoist_clm[:,5]  = smoist_d.loc[:,"Water_soil_40_50cm"].to_numpy()
    smoist_clm[:,6]  = smoist_d.loc[:,"Water_soil_80_90cm"].to_numpy()
    smoist_clm[:,7:] = np.tile(smoist_d.loc[:,"Water_soil_90_100cm"].to_numpy().reshape(len(stemp_d), 1), 3)
    
    pot_sat=-(9.8e-5)*np.exp((1.54-0.0095*Psand+0.0063*(100-Psand-Pclay))*np.log(10)) # unit: MPa
    pot_min=-10 # unit: MPa
    
    Bmin=2.91+0.159*Pclay/100
    Bi=(1-f_om)*Bmin+f_om*Bom
    scal_water_tem1 = np.zeros((len(smoist_d), 10)) # matrix(0,nrow = 2555,ncol = 10)
    
    for a in range(len(smoist_d)):
        for b in range(10):
            smoist_rel=max(min(smoist_clm[a,b]/smoist_sat,1),0.09) # Jian: from 0.01 to 0.09
            pot_i=pot_sat*smoist_rel**(-Bi)
            if pot_i<pot_min: 
                scal_water_tem1[a,b] = 0
            elif (pot_i>=pot_min and pot_i<=pot_sat):
                scal_water_tem1[a,b] = np.log(pot_min/pot_i)/np.log(pot_min/pot_sat)
            else:
                scal_water_tem1[a,b]=1

    scal_env_tem1 = scal_temp_tem1*scal_water_tem1     
    # Dateseq=data.frame(Date=dayseq365$Date,scalar_plant=1.0)
    
    # write out
    scal_temp_tem = np.ones((len(stemp_d), 71))
    scal_temp_tem[:,1:] = np.tile(scal_temp_tem1,7)

    scal_water_tem = np.ones((len(stemp_d), 71))
    scal_water_tem[:,1:] = np.tile(scal_water_tem1,7)

    scal_env_tem = np.ones((len(stemp_d), 71))
    scal_env_tem[:,1:] = np.tile(scal_env_tem1,7)
    
    Bscalar_tem = np.ones((len(stemp_d), 71))
    return scal_temp_tem, scal_water_tem, scal_env_tem, Bscalar_tem

def calScalars_ORCHIDEE(stemp_d, smoist_d):
    # ORCHIDEE##############################
    # frozon_respiration_func = 1
    sdepth_orc=np.array([0.000977,0.00391,0.00977,0.0215,0.0449,0.0918,0.185,0.3734,0.74878,1.4995])
    soil_Q10=0.69
    # calculate temperature scalar
    tsoil_orc= np.zeros((len(stemp_d), 32))  # matrix(0,nrow = 2555,ncol = 32)
    tsoil_orc[:,:6] = np.tile(stemp_d.loc[:,"Tem_soil_0_10cm"].to_numpy().reshape(len(stemp_d), 1), 6)
    tsoil_orc[:,6]  = stemp_d.loc[:,"Tem_soil_10_20cm"].to_numpy()
    tsoil_orc[:,7]  = stemp_d.loc[:,"Tem_soil_30_40cm"].to_numpy()
    tsoil_orc[:,8]  = stemp_d.loc[:,"Tem_soil_50_60cm"].to_numpy()
    tsoil_orc[:,9:] = np.tile(stemp_d.loc[:,"Tem_soil_90_100cm"].to_numpy().reshape(len(stemp_d), 1), 23)
    
    # scal.temp.orc=matrix(NA,nrow = 2555,ncol=100)
    scal_temp_tem1 = np.full((len(stemp_d), 100), np.nan)
    
    for a in range(len(stemp_d)):
        for b in range(100):
            if b<=3: # if scalar for litter decomposition
                tempin=tsoil_orc[a,0]
            else:
                layern=(b-3)%32
                if(layern==0): layern=32
                tempin=tsoil_orc[a,layern-1]
            # if b value
            if(tempin>0):
                scal_temp_tem1[a,b]=np.exp(soil_Q10*(tempin-30)/10)
            elif (tempin> -1):
                scal_temp_tem1[a,b]=(tempin+1)*np.exp(soil_Q10*(-30)/10)
            else:
                scal_temp_tem1[a,b]=0
            scal_temp_tem1[a,b]=max(min(scal_temp_tem1[a,b],1),0.01)
    
    # calculate moisture scalar
    
    moist_s=1.0
    moist_coeff = np.array([1.1,  2.4,  0.29])*moist_s
    moistcont_min = 0.25
    
    smoist_orc = np.zeros((len(smoist_d), 32)) # matrix(0,nrow = 2555,ncol = 32)
    smoist_orc[:,:6] = np.tile(smoist_d.loc[:,"Water_soil_0_10cm"].to_numpy().reshape(len(smoist_d), 1), 6)
    smoist_orc[:,6]  = smoist_d.loc[:,"Water_soil_10_20cm"].to_numpy()
    smoist_orc[:,7]  = smoist_d.loc[:,"Water_soil_30_40cm"].to_numpy()
    smoist_orc[:,8]  = smoist_d.loc[:,"Water_soil_50_60cm"].to_numpy()
    smoist_orc[:,9:] = np.tile(smoist_d.loc[:,"Water_soil_90_100cm"].to_numpy().reshape(len(smoist_d), 1), 23)
    
    # scal.smoist.orc=matrix(NA,nrow = 2555,ncol=100)
    scal_water_tem1 = np.full((len(smoist_d), 100), np.nan)
    
    for a in range(len(smoist_d)):
        for b in range(100):
            if(b<=3): # {  # if scalar for litter decomposition
                moist_in=smoist_orc[a,0]
            else:
                layern=(b-3)%32
                if(layern==0):  layern=32
                moist_in=smoist_orc[a,layern-1]
            moistfunc_result = -moist_coeff[0]*moist_in**2+moist_coeff[1]*moist_in-moist_coeff[2]
            scal_water_tem1[a,b]= max( moistcont_min, min(1, moistfunc_result))
    
    # scal.orc=matrix(0,nrow = 2555,ncol = 100)
    scal_env_tem1 = scal_temp_tem1* scal_water_tem1
    # Dateseq=data.frame(Date=dayseq365$Date,scalar_plant=1.0)
    
    # write out
    scal_temp_tem = np.ones((len(stemp_d), 101))
    scal_temp_tem[:,1:] = scal_temp_tem1

    scal_water_tem = np.ones((len(stemp_d), 101))
    scal_water_tem[:,1:] = scal_water_tem1

    scal_env_tem = np.ones((len(stemp_d), 101))
    scal_env_tem[:,1:] = scal_env_tem1
    
    Bscalar_tem = np.ones((len(stemp_d), 101))
    return scal_temp_tem, scal_water_tem, scal_env_tem, Bscalar_tem

def save2csv(df_datetime,dictData,modelName,output_path):
    # dictData: "scal_temp_tem", "scal_water_tem", "scal_env_tem", "Bscalar_tem"
    # output_path = "input/matrix_models_input"
    os.makedirs(output_path+"/Bscalar", exist_ok = True)
    os.makedirs(output_path+"/scalar", exist_ok = True)
    os.makedirs(output_path+"/scalar/tempscalar", exist_ok = True)
    os.makedirs(output_path+"/scalar/waterscalar", exist_ok = True)
    df_res = pd.DataFrame(copy.deepcopy(df_datetime))
    for key, data in dictData.items():
        if data.ndim>1:
            for icolData in range(data.shape[1]):
                df_res["scalar"+str(icolData+1)] = data[:,icolData]
        else:
            df_res["scalar1"] = data
        if key == "scal_temp_tem":
            df_res.to_csv(output_path+"/scalar/tempscalar/"+modelName+".csv", index=False)
        elif key == "scal_water_tem":
            df_res.to_csv(output_path+"/scalar/waterscalar/"+modelName+".csv", index=False)
        elif key == "scal_env_tem":
            df_res.to_csv(output_path+"/scalar/"+modelName+".csv", index=False)
        elif key == "Bscalar_tem":
            df_res.to_csv(output_path+"/Bscalar/"+modelName+".csv", index=False)
        else:
            pass





# def calScalars_allModelMean():
#     # calculate averaged scalar of all models =====================
#     # data.frame.d = data.frame(Date= dayseq365$Date )

#     lsModels = ["TEM","DALEC","TECO","FBDC","CASA","CENTURY","CLM","ORCHIDEE"]
#     Timestep = ["month","day","day","day","month","month","day","day"]
#     Pooln    = [2,6, 8, 13, 14,15, 71, 101]
#     Pooln2   = [2,6, 8, 13, 14, 15, 8, 8]
#     file_matrix = "matrix/matrix.xlsx"

#     # scal.allmodel = data.frame()
#     # for(mn in 1:modn) {
#     for mn, model in enumerate(lsModels):
#         # getwd()
#         pn = Pooln[mn]
#         matrix0 = pd.read_excel(file_matrix,sheet_name = mn)
#         matrix0 = matrix0.iloc[:pn,:]
#         print(model)
#         # OP = as.numeric(matrix0$Pool_order[matrix0$Pool3=="Plant"])
#         OP = matrix0[matrix0["Pool3"]=="Plant"]["Pool_order"].to_numpy()
#         # print(OP)
#         # OLS = as.numeric(matrix0$Pool_order[matrix0$Pool3=="Litter"|matrix0$Pool3=="Soil"]) # order of litter pool 
#         OLS = matrix0[((matrix0["Pool3"] == "Litter") | (matrix0["Pool3"] == "Soil"))]["Pool_order"].to_numpy()
#         print(OLS)
#         # scal.m1 = read.csv(paste("2_cal_envScalar_outputs/scalar/",modsum$Model[mn],"_f",cn,"p",fn,".csv",sep = "")  )
#         scal_env_tem
#         # scal.m1$Date = as.Date(scal.m1$Date)
#         # summary(scal.m1)
#         scal.m2 = merge(scal.m1,data.frame.d, by=c("Date"),all = TRUE )
#         scal.m2 = na.locf(scal.m2)
#         if(length(OP)>1) {
#             scal.m3 = data.frame(Date=scal.m2$Date,scal.plt=rowMeans(scal.m2[,OP+1]))
#         } else {
#             scal.m3 = data.frame(Date=scal.m2$Date,scal.plt=scal.m2[,OP+1])
#         }
#         if(length(OLS)>1) {
#             scal.m3$scal.lsol = rowMeans(scal.m2[,OLS+1])
#         } else {
#             scal.m3$scal.lsol = scal.m2[,OLS+1]
#         }
#         scal.m3$Model = modsum$Model[mn]
#         scal.allmodel = rbind(scal.allmodel,scal.m3)
    
#     }
#     scal.allmodel2 = aggregate(scal.allmodel[,2:3],list(scal.allmodel$Date),mean)
#     colnames(scal.allmodel2)[1] = "Date"
#     write.csv(scal.allmodel2,paste("2_cal_envScalar_outputs/scalar/modelmean_f",cn,"p",fn,".csv",sep = ""),row.names = FALSE )
        
#     #     }
#     # }

def run(inPath, outPath):
    # dictEnvDataPd = readEnvData("output/matrix_models_output/")
    os.makedirs(outPath, exist_ok = True)
    dictEnvDataPd = readEnvData(inPath)
    lsModels = ["TEM","DALEC","TECO","FBDC","CASA","CENTURY","CLM","ORCHIDEE"]
    Timestep = ["month","day","day","day","month","month","day","day"]
    # print(dictEnvDataPd)
    dictData = {}
    print("1. TEM ...")
    res = calScalars_TEM(dictEnvDataPd['tair_m'], dictEnvDataPd['smoist_m'])
    dictData["scal_temp_tem"]  = res[0]
    dictData["scal_water_tem"] = res[1]
    dictData["scal_env_tem"]   = res[2]
    dictData["Bscalar_tem"]    = res[3]
    df_datetime = dictEnvDataPd['tair_m']["Date"]
    save2csv(df_datetime,dictData,"TEM",outPath)
    #------------------------------------------------------
    print("2. CENTURY ...")
    res = calScalars_CENTURY(dictEnvDataPd['tair_d'], dictEnvDataPd['tair_m'], dictEnvDataPd['rain_m'], dictEnvDataPd['stemp_m'])
    dictData["scal_temp_tem"]  = res[0]
    dictData["scal_water_tem"] = res[1]
    dictData["scal_env_tem"]   = res[2]
    dictData["Bscalar_tem"]    = res[3]
    df_datetime = dictEnvDataPd['tair_m']["Date"]
    save2csv(df_datetime,dictData,"CENTURY",outPath)
    #------------------------------------------------------
    print("3. TECO ...")
    res = calScalars_TECO(dictEnvDataPd['stemp_d'], dictEnvDataPd['smoist_d'])
    dictData["scal_temp_tem"]  = res[0]
    dictData["scal_water_tem"] = res[1]
    dictData["scal_env_tem"]   = res[2]
    dictData["Bscalar_tem"]    = res[3]
    df_datetime = dictEnvDataPd['stemp_d']["Date"]
    save2csv(df_datetime,dictData,"TECO",outPath)
    #------------------------------------------------------
    print("4. CASA ...")
    res = calScalars_CASA(dictEnvDataPd['tair_d'], dictEnvDataPd['tair_m'], dictEnvDataPd['stemp_m'], dictEnvDataPd['smoist_m'], dictEnvDataPd['rain_m'])
    dictData["scal_temp_tem"]  = res[0]
    dictData["scal_water_tem"] = res[1]
    dictData["scal_env_tem"]   = res[2]
    dictData["Bscalar_tem"]    = res[3]
    df_datetime = dictEnvDataPd['tair_m']["Date"]
    save2csv(df_datetime,dictData,"CASA",outPath)
    #------------------------------------------------------
    print("5. FBDC ...")
    res = calScalars_FBDC(dictEnvDataPd['tair_d'])
    dictData["scal_temp_tem"]  = res[0]
    dictData["scal_water_tem"] = res[1]
    dictData["scal_env_tem"]   = res[2]
    dictData["Bscalar_tem"]    = res[3]
    df_datetime = dictEnvDataPd['tair_d']["Date"]
    save2csv(df_datetime,dictData,"FBDC",outPath)
    #------------------------------------------------------
    print("6. DALEC2 ...")
    res = calScalars_DALEC2(dictEnvDataPd['stemp_d'])
    dictData["scal_temp_tem"]  = res[0]
    dictData["scal_water_tem"] = res[1]
    dictData["scal_env_tem"]   = res[2]
    dictData["Bscalar_tem"]    = res[3]
    df_datetime = dictEnvDataPd['stemp_d']["Date"]
    save2csv(df_datetime,dictData,"DALEC",outPath)
    #------------------------------------------------------
    print("7. CLM ...")
    res = calScalars_CLM(dictEnvDataPd['stemp_d'], dictEnvDataPd['smoist_d'])
    dictData["scal_temp_tem"]  = res[0]
    dictData["scal_water_tem"] = res[1]
    dictData["scal_env_tem"]   = res[2]
    dictData["Bscalar_tem"]    = res[3]
    df_datetime = dictEnvDataPd['stemp_d']["Date"]
    save2csv(df_datetime,dictData,"CLM",outPath)
    #------------------------------------------------------
    print("8. ORCHIDEE ...")
    res = calScalars_ORCHIDEE(dictEnvDataPd['stemp_d'], dictEnvDataPd['smoist_d'])
    dictData["scal_temp_tem"]  = res[0]
    dictData["scal_water_tem"] = res[1]
    dictData["scal_env_tem"]   = res[2]
    dictData["Bscalar_tem"]    = res[3]
    df_datetime = dictEnvDataPd['stemp_d']["Date"]
    save2csv(df_datetime,dictData,"ORCHIDEE",outPath)
    #------------------------------------------------------


    # tair_d, tair_m,stemp_m, smoist_m, rain_m
    # print(calScalars_ORCHIDEE(dictEnvDataPd['stemp_d'], dictEnvDataPd['smoist_d']))
    # calScalars_allModelMean()


    # dictEnvDataPd = readEnvData("output/matrix_models_output/")
    # # print(dictEnvDataPd)
    # # print(calScalars_TEM(dictEnvDataPd["tair_m"], dictEnvDataPd["rain_m"],dictEnvDataPd["stemp_m"],dictEnvDataPd["smoist_m"]))
    # calScalars_CENTURY(dictEnvDataPd['tair_d'], dictEnvDataPd["rain_d"],dictEnvDataPd["stemp_d"],dictEnvDataPd["smoist_d"])