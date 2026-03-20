module datatypes
    implicit none
    ! settings
    character(50)  :: case_name        ! define the case name
    ! Note: Just set one of do_simu, do_mcmc and do_spinup to be "True"
    logical :: do_simu                 ! simulation mode
    logical :: do_mcmc                 ! MCMC for data assimilation mode
    logical :: do_spinup               ! spinup mode
    logical :: do_s_init
    ! simulation selections 
    logical :: do_matrix               ! whether run matrix or not
    logical :: do_restart              ! whether read restart file or not
    logical :: do_obs_zwt
    logical :: do_obs_Tsoil
    logical :: do_snow                 ! do soil snow process or not
    logical :: do_soilphy              ! do soil physics or not
    logical :: do_EBG                  ! run EBG or not based on Ma et al., 2022
    logical :: do_ndep                 ! N deposit
    logical :: do_leap                 ! judge leap year or not
    ! output selections
    logical :: do_out_hr
    logical :: do_out_day
    logical :: do_out_mon
    logical :: do_out_yr
    ! 
    logical :: do_spruce = .False.    ! set the spruce site handle in 1974 for cutting the vegetation
    logical :: do_sa     = .False.
    logical :: do_annual_mcmc = .True.
    logical :: do_fixed_param = .False.
    real(8) :: Tref_rh = 10.0
    real(8) :: def_Tpro_me = 10.0
    real(8) :: def_Q10rh, def_Q10pro
    real(8) :: def_s_rh = 1.0
    real(8) :: def_s_vea(3), def_Entrpy(3), def_Vcmax0(3), def_Q10(3)
    real(8) :: def_s_npp(3) = 1.0
    ! set the input and output path and files
    character(200) :: inDir
    character(200) :: outDir
    ! input files
    character(300) :: climfile
    character(300) :: watertablefile
    character(300) :: snowdepthfile
    character(300) :: in_restartfile
    character(300) :: defParamsFile
    character(300) :: mcmc_configfile
    character(300) :: spinup_configfile
    ! experiment setting: Jian: not test yet
    real(8) :: Ttreat   = 0.        ! Temperature treatment, warming in air and soil temperature
    real(8) :: CO2treat = 0.        ! CO2 treatmant, up to CO2treat, not add to Ca. CO2
    real(8) :: N_fert   = 0.        ! 5.6 ! (11.2 gN m-2 yr-1, in spring, Duke Forest FACE)
    ! above settings from the nml file --------------------------------------------------
    ! output path
    character(250) :: outDir_nc     = "results_nc_format"
    character(250) :: outDir_csv    = "results_csv_format"
    character(250) :: outDir_mcmc   = "results_mcmc"
    character(250) :: outDir_spinup = "results_spinup"

    ! other paths
    character(300) :: outdir_case, outDir_h, outDir_d, outDir_m, outDir_y
    character(300) :: outDir_mcmc_h, outDir_mcmc_d, outDir_mcmc_m, outfile_restart, restartfile

    ! setttings of fixed parameters -----------------------------------------------------
    integer, parameter :: max_npft = 10               ! maxmum PFT count for reading the parameters
    integer, parameter :: nlayers = 10                ! how many layers
    real(8),    parameter :: pi      = 3.1415926
    ! physical constants
    real(8),    parameter :: tauL(3) = (/0.1, 0.425, 0.00/)  ! leaf transmittance for vis, for NIR, for thermal
    real(8),    parameter :: rhoL(3) = (/0.1, 0.425, 0.00/)  ! leaf reflectance for vis, for NIR, for thermal
    real(8),    parameter :: emleaf  = 0.96
    real(8),    parameter :: emsoil  = 0.94
    real(8),    parameter :: Rconst  = 8.314                 ! universal gas constant (J/mol)
    real(8),    parameter :: sigma   = 5.67e-8               ! Steffan Boltzman constant (W/m2/K4)
    real(8),    parameter :: cpair   = 1010.                 ! heat capapcity of air (J/kg/K)
    real(8),    parameter :: Patm    = 101325. !1.e5         ! atmospheric pressure  (Pa)
    real(8),    parameter :: Trefk   = 293.2                 ! reference temp K for Kc, Ko, Rd
    real(8),    parameter :: H2OLv0  = 2.501e6               ! latent heat H2O (J/kg)
    real(8),    parameter :: AirMa   = 29.e-3                ! mol mass air (kg/mol)
    real(8),    parameter :: H2OMw   = 18.e-3                ! mol mass H2O (kg/mol)
    real(8),    parameter :: chi     = 0.93                  ! gbH/gbw
    real(8),    parameter :: Dheat   = 21.5e-6               ! molecular diffusivity for heat
    ! plant parameters
    real(8),    parameter :: gsw0    = 1.0e-2                ! g0 for H2O in BWB model
    real(8),    parameter :: theta   = 0.9
    real(8),    parameter :: wleaf   = 0.01                  ! leaf width (m)
    ! thermodynamic parameters for Kc and Ko (Leuning 1990)
    real(8),    parameter :: conKc0  = 302.e-6               ! mol mol^-1
    real(8),    parameter :: conKo0  = 256.e-3               ! mol mol^-1
    real(8),    parameter :: Ekc     = 59430.                ! J mol^-1
    real(8),    parameter :: Eko     = 36000.                ! J mol^-1
    ! Erd = 53000.                                        ! J mol^-1
    real(8),    parameter :: o2ci    = 210.e-3               ! mol mol^-1
    ! thermodynamic parameters for Vcmax & Jmax (Eq 9, Harley et al, 1992; #1392)
    real(8),    parameter :: Eavm    = 116300.               ! J/mol  (activation energy)
    real(8),    parameter :: Edvm    = 202900.               ! J/mol  (deactivation energy)
    real(8),    parameter :: Eajm    = 79500.                ! J/mol  (activation energy) 
    real(8),    parameter :: Edjm    = 201000.               ! J/mol  (deactivation energy)
    ! parameters for temperature dependence of gamma* (revised from von Caemmerer et al 1993)
    real(8),    parameter :: gam0    = 28.0e-6               ! mol mol^-1 @ 20C = 36.9 @ 25C
    real(8),    parameter :: gam1    = .0509
    real(8),    parameter :: gam2    = .0010
    real(8),    parameter :: times_storage_use=3*720.        ! 720 hours, 30 days
    real(8) :: rhoS(3) = (/0.1, 0.3,   0.00/)                ! soil reflectance for vis, for NIR, for thermal, update in vegetation?
    ! end of consts parameters -------------------------------------------------------------------
    character(100) :: mc_str_n
    ! climate data type --------------------------------------------------------------------------
    type forcing_data_type
        integer :: year
        integer :: doy
        integer :: hour
        real(8) :: Tair
        real(8) :: Tsoil
        real(8) :: RH                   ! Jian: RH seems confused in forcing and soil respiration
        real(8) :: VPD
        real(8) :: Rain
        real(8) :: WS
        real(8) :: PAR
        real(8) :: CO2
        real(8) :: PBOT                 ! unit patm Pa dynamic atmosphere pressure
        real(8) :: Ndep
        real(8) :: Tsoil_lr(11)!TS_0cm
        real(8) :: zwt
    end type forcing_data_type
    type(forcing_data_type), allocatable, save :: forcing(:)
    integer :: nforcing
    real(8) :: co2ca, radsol 
    ! alternative input variable
    real(8),DIMENSION(:), ALLOCATABLE :: snow_in       ! if not run snow process, then read from the input file
    ! end of forcing data ------------------------------------------------------------------------

    ! parameters or variables for cycle and states -------------------------------------------
    integer :: npft
    character(50), allocatable :: sp_names(:)
    type spec_data_type
        !!! species parameters
        character(50) :: spec_name
        real(8) :: pft_weight 
        ! species parameter sets
        real(8) :: LAImax
        real(8) :: LAImin 
        integer :: stom_n 
        real(8) :: SapS 
        real(8) :: SapR 
        real(8) :: SLA 
        real(8) :: GLmax 
        real(8) :: GRmax 
        real(8) :: Gsmax 
        real(8) :: alpha 
        real(8) :: Vcmax0 
        real(8) :: Ds0  
        real(8) :: xfang 
        real(8) :: rdepth 
        real(8) :: frlen(nlayers)        ! ratio of roots in every layer, Oak Ridge FACE: Shuang
        real(8) :: Rootmax 
        real(8) :: Stemmax
        real(8) :: Tau_Leaf
        real(8) :: Tau_Stem 
        real(8) :: Tau_Root 
        real(8) :: Q10  
        real(8) :: Rl0 
        real(8) :: Rs0 
        real(8) :: Rr0 
        real(8) :: JV  
        real(8) :: Entrpy 
        real(8) :: gddonset 
        real(8) :: f_rg
        ! add for annual different parameters
        real(8) :: s_vea
        real(8) :: s_ved
        real(8) :: s_jea
        real(8) :: s_jed
        real(8) :: s_vea_1, s_vea_2, s_vea_3, s_vea_4, s_vea_5, s_vea_6, s_vea_7, s_vea_8
        real(8) :: s_ved_1, s_ved_2, s_ved_3, s_ved_4, s_ved_5, s_ved_6, s_ved_7, s_ved_8
        real(8) :: Entrpy_1, Entrpy_2, Entrpy_3, Entrpy_4, Entrpy_5, Entrpy_6, Entrpy_7, Entrpy_8
        real(8) :: Vcmax0_1, Vcmax0_2, Vcmax0_3, Vcmax0_4, Vcmax0_5, Vcmax0_6, Vcmax0_7, Vcmax0_8
        real(8) :: Q10_1, Q10_2, Q10_3, Q10_4, Q10_5, Q10_6, Q10_7, Q10_8
        real(8) :: f_rg_1, f_rg_2, f_rg_3, f_rg_4, f_rg_5, f_rg_6, f_rg_7, f_rg_8
        real(8) :: fn2r_1, fn2r_2, fn2r_3, fn2r_4, fn2r_5, fn2r_6, fn2r_7, fn2r_8
        real(8) :: s_npp_1, s_npp_2, s_npp_3, s_npp_4, s_npp_5, s_npp_6, s_npp_7, s_npp_8, s_npp_9
        real(8) :: fn2l_1, fn2l_2, fn2l_3, fn2l_4, fn2l_5, fn2l_6, fn2l_7, fn2l_8
        ! add in plant growth process
        real(8) :: hmax     ! m
        real(8) :: hl0      ! m2/kg C
        real(8) :: LAIMAX0  ! maybe the LAImax
        real(8) :: la0     
        ! add to change the allocate
        real(8) :: fn2l
        real(8) :: fn2r
        ! add the parameters to scale the pools
        real(8) :: s_cLeaf
        real(8) :: s_cStem
        real(8) :: s_cRoot
        real(8) :: s_nsc
        real(8) :: s_nsn
        !!! species-based initial values
        real(8) :: cLeaf  
        real(8) :: cStem 
        real(8) :: cRoot  
        real(8) :: CN0_L  
        real(8) :: CN0_S  
        real(8) :: CN0_R   
        real(8) :: NSCmin  
        real(8) :: Storage  
        real(8) :: nsc   
        real(8) :: nsn             ! 6.0 ! 0.35 according to Ma et al., 2022
        real(8) :: accumulation  
        real(8) :: SNvcmax   
        real(8) :: N_deficit  
        !!! parameters for cycle -----------
        ! carbon and nitrogen flux
        real(8) :: gpp, NPP, NPP_L, NPP_W, NPP_R
        real(8) :: alpha_L, alpha_W, alpha_R           ! allocation of NPP
        real(8) :: outC_L, outC_W, outC_R, L_fall      ! output from carbon pool; L_fall = outC_L
        real(8) :: outN_L, outN_W, outN_R
        real(8) :: RmLeaf, RmStem, RmRoot, Rmain, Rauto ! respiration
        real(8) :: RgLeaf, RgStem, RgRoot, Rgrowth
        real(8) :: Aleaf(3)                             !Jian: sunlit, shadlit, sum
        ! carbon and nitrogen states
        real(8) :: cPlant, nPlant
        real(8) :: CN_L,   CN_W,   CN_R                ! above still has the values of CN0: CN0_L, CN0_W, CN0_R
        real(8) :: N_leaf, N_Stem, N_root              ! NSN used by different parts
        real(8) :: nLeaf,  nStem,  nRoot     ! N pools
        real(8) :: N_LF,   N_WF,   N_RF                    ! N out without resorption 
        real(8) :: bmleaf, bmstem, bmroot, bmplant
        real(8) :: stemsap, rootsap, ht
        real(8) :: stor_use, add, store, NSCmax
        ! carbon or nitrogen  processes variables
        real(8) :: LAI
        real(8) :: Vcmx0, eJmx0
        ! special nitrogen processes
        real(8) :: N_transfer, alphaN, costCreuse      ! N resorption, alphaN: resorption rate, costCresue: the use of C when resorp N
        real(8) :: N_demand, N_uptake, costCuptake    
        real(8) :: N_fixation, costCfix, fnsc, Rnitrogen ! fnsc: scale of NSC
        real(8) :: SNgrowth, SNRauto                     ! scale of N for growth or Rauto
        ! phenology 
        integer :: onset
        ! water processes
        real(8) :: plantup(10), transp                    ! plantup due to re-distribute transpiration
        ! energy or environmental variables
        real(8) :: Esoil, Hsoil 
        real(8) :: Rsoilab1, Rsoilab2                      ! radition to soil   
        real(8) :: QLair, QLleaf 
        ! test
        real(8) :: GPmax, GrowthP, GrowthL, GrowthR, GrowthS
        real(8) :: scalT, CNp0, test1, test2, test3, test4, test5, test6
        real(8) :: test7, test8, test9
    end type spec_data_type

    type site_data_type
        integer :: count_pft   
        type(spec_data_type), allocatable :: sp(:)
        ! site-based parameters
        real(8) :: lat  
        real(8) :: lon  
        real(8) :: wsmax  
        real(8) :: wsmin  
        real(8) :: extkU  
        real(8) :: Tau_F  
        real(8) :: Tau_C  
        real(8) :: Tau_Micro   
        real(8) :: Tau_SlowSOM  
        real(8) :: Tau_Passive  
        real(8) :: Q10rh  
        real(8) :: etaW           !!!! need to be assigned value
        real(8) :: f_F2M  
        real(8) :: f_C2M 
        real(8) :: f_C2S  
        real(8) :: f_M2S  
        real(8) :: f_M2P   
        real(8) :: f_S2P  
        real(8) :: f_S2M  
        real(8) :: f_P2M 
        ! methane
        real(8) :: r_me  
        real(8) :: Q10pro  
        real(8) :: kCH4  
        real(8) :: Omax   
        real(8) :: CH4_thre  
        real(8) :: Tveg  
        real(8) :: Tpro_me  
        real(8) :: Toxi  
        real(8) :: f   
        real(8) :: bubprob  
        real(8) :: Vmaxfraction
        real(8) :: pox
        ! add to modify the pool size
        real(8) :: f_lit_m
        real(8) :: s_lit
        real(8) :: f_fast
        real(8) :: f_slow
        real(8) :: s_soil 
        ! add to modify the water processes
        real(8) :: par_shcap_snow
        real(8) :: par_condu_snow
        real(8) :: par_condu_b
        ! real(8) :: par_albedo_snow
        real(8) :: par_fsub
        real(8) :: par_rho_snow
        real(8) :: par_decay_m
        ! add for annual different parameters
        real(8) :: Q10rh_1
        real(8) :: Q10rh_2
        real(8) :: Q10rh_3
        real(8) :: Q10rh_4
        real(8) :: Q10rh_5
        real(8) :: Q10rh_6
        real(8) :: Q10rh_7
        real(8) :: Q10rh_8
        real(8) :: Q10pro_1
        real(8) :: Q10pro_2
        real(8) :: Q10pro_3
        real(8) :: Q10pro_4
        real(8) :: Q10pro_5
        real(8) :: Q10pro_6
        real(8) :: Q10pro_7
        real(8) :: Q10pro_8
        real(8) :: Tpro_me_1
        real(8) :: Tpro_me_2
        real(8) :: Tpro_me_3
        real(8) :: Tpro_me_4
        real(8) :: Tpro_me_5
        real(8) :: Tpro_me_6
        real(8) :: Tpro_me_7
        real(8) :: Tpro_me_8
        real(8) :: Tref_rh_1
        real(8) :: Tref_rh_2
        real(8) :: Tref_rh_3
        real(8) :: Tref_rh_4
        real(8) :: Tref_rh_5
        real(8) :: Tref_rh_6
        real(8) :: Tref_rh_7
        real(8) :: Tref_rh_8
        !!! site-based initial values
        real(8) :: cLit_m  
        real(8) :: cLit_s   
        real(8) :: cSoil_f   
        real(8) :: cSoil_s  
        real(8) :: cSoil_p   
        real(8) :: CN0_lit_m  
        real(8) :: CN0_lit_s  
        real(8) :: CN0_soil_f  
        real(8) :: CN0_soil_s  
        real(8) :: CN0_soil_p  
        real(8) :: N_deposit    ! Nitrogen input (gN/year/m2, it will be transfered to hourly in simulation) 
        real(8) :: QNminer  
        ! for soil conditions, physical processes
        real(8) :: thksl(nlayers)        ! thickness of every soil layer
        real(8) :: liq_water(nlayers)    ! unit m
        real(8) :: fwsoil                ! update in soilwater module
        real(8) :: topfws 
        real(8) :: omega
        real(8) :: zwt 
        real(8) :: infilt 
        ! soil thermal dynamics in Yuanyuanversion
        real(8) :: sftmp
        real(8) :: Tsnow 
        real(8) :: Twater
        real(8) :: Tice
        real(8) :: G 
        real(8) :: snow_dsim 
        real(8) :: dcount  
        real(8) :: dcount_soil 
        real(8) :: ice_tw 
        real(8) :: Tsoill(nlayers)  ! JJ MS thksl 10 20 30 40 50 70 90 110 130 150...  
        real(8) :: ice(nlayers)
        real(8) :: shcap_snow       ! tuneice worker better
        real(8) :: condu_snow
        real(8) :: condu_b          ! yuanyuan soil thermal version value  ... int: this par is not sensitive to CWE
        real(8) :: depth_ex  
        real(8) :: diff_s  
        real(8) :: diff_snow        ! .. int diffusivity of snow not sensitive for ice
        real(8) :: albedo_snow 
        real(8) :: resht  
        real(8) :: thd_snow_depth  
        real(8) :: b_bound           ! b_bound=0.1     !tuneice  not sensitive for ice
        real(8) :: infilt_rate  
        real(8) :: fa  
        real(8) :: fsub  
        real(8) :: rho_snow        !tuneice
        real(8) :: decay_m   !aging factor on snow melting
        !----------------------------------------------------------------
        ! methane module. update: Shuang methane bog species even more shallowly rooted than the tundra. add initials for methane module Shuang version
        real(8) :: CH4_V(nlayers)
        real(8) :: CH4(nlayers)
        real(8) :: Vp(nlayers)             ! assume in the very beginning no bubbles exist in the first three layers (30cm)
        real(8) :: bubble_methane_tot 
        real(8) :: Nbub  
        ! real(8) :: depth_1                 ! calculate soil depth unit cm
        !!! parameters for cycle and summarys
        ! carbon and nitrogen fluxes
        real(8) :: GPP, NPP, NPP_L, NPP_W, NPP_R
        real(8) :: Rauto, Rmleaf, Rmstem, Rmroot, Rmain,  Rhetero
        real(8) :: Rgleaf, Rgstem, Rgroot, Rgrowth
        real(8) :: Rh_lit_m, Rh_lit_s, Rh_soil_f, Rh_soil_s, Rh_soil_p
        real(8) :: outC_L, outC_S, outC_R
        real(8) :: outN_L, outN_S, outN_R
        real(8) :: outC_lit_m, outC_lit_s, outC_soil_f, outC_soil_s, outC_soil_p
        real(8) :: outN_lit_m, outN_lit_s, outN_soil_f, outN_soil_s, outN_soil_p
        real(8) :: Rnitrogen  ! total C cost from N processes
        ! carbon and nitrogen states
        real(8) :: NSC, NSN, storage, stor_use
        real(8) :: cLeaf, cStem, cRoot, cPlant
        real(8) :: nLeaf, nStem, nRoot, nPlant
        real(8) :: nLit_m, nLit_s, nSoil_f, nSoil_s, nSoil_p
        real(8) :: bmleaf, bmstem, bmroot
        real(8) :: CN_L, CN_S, CN_R
        real(8) :: CN_lit_m, CN_lit_s, CN_soil_f, CN_soil_s, CN_soil_p
        ! special N processes
        real(8) :: N_miner, N_immob
        real(8) :: N_imm_lit_m, N_imm_lit_s, N_imm_soil_f, N_imm_soil_s, N_imm_soil_p
        real(8) :: N_demand, N_uptake, Scalar_N_flow
        real(8) :: N_leach,  N_vol, N_loss, fNnetmin
        real(8) :: N_fixation, N_transfer
        ! carbon or nitrogen  processes variables
        real(8) :: lai, frlen(nlayers), LAImin, LAImax
        real(8) :: rdepth                                  ! weighted by different pfts
        ! water processes
        real(8) :: runoff, melt, transp, evap, snow_depth
        real(8) :: wcl(nlayers), wsc(nlayers)
        real(8) :: depth(10), pwater(10),dpatm,scalW
        ! energy and environmental variables
        real(8) :: rain_d, GDD5, ta, Dair, sublim, eairP 
        real(8) :: tsoil_layer(nlayers+1)
        real(8) :: Rsoilab1, Rsoilab2, Rsoilab3, Rsoilabs 
        real(8) :: Esoil, Hsoil, resdh
        real(8) :: QLair, QLleaf
        ! methane
        real(8) :: simuCH4, Pro_sum, Oxi_sum, methaneP(nlayers), methanebP(nlayers)
        real(8) :: presP(nlayers)
        !
        real(8) :: test1
    end type site_data_type
    ! end of parameters or variables for cycle and states -------------------------------------------

    ! define the output variables --------------------------------------------------
    type spec_outvars_type
        ! carbon fluxes (Kg C m-2 s-1)
        real(8) :: gpp
        real(8) :: Aleaf(3)
        real(8) :: nee
        real(8) :: npp
        real(8) :: nppLeaf
        real(8) :: nppStem
        real(8) :: nppRoot
        real(8) :: nppOther    ! According to SPRUCE-MIP, stem means above ground Stemy tissues which is different from Stem tissues.
        real(8) :: ra
        real(8) :: raLeaf
        real(8) :: raStem
        real(8) :: raRoot
        real(8) :: raOther
        real(8) :: rMaint
        real(8) :: rGrowth
        real(8) :: nbp
        real(8) :: nsc
        ! Carbon Pools  (KgC m-2)
        real(8) :: cLeaf
        real(8) :: cStem
        real(8) :: cRoot
        ! Nitrogen pools (kgN m-2)
        real(8) :: nLeaf
        real(8) :: nStem
        real(8) :: nRoot
        real(8) :: nsn
        ! real(8) :: nOther(:)
        ! water fluxes (kg m-2 s-1)
        real(8) :: tran
        ! other
        real(8) :: lai                     ! m2 m-2, Leaf area index
        ! add out_litter
        real(8) :: outC_L
        real(8) :: outC_W
        real(8) :: outC_R
    end type spec_outvars_type

    ! total outputs
    type outvars_data_type
        integer :: year
        integer :: doy
        integer :: hour
        type(spec_outvars_type), allocatable :: sp(:)
        ! carbon fluxes (Kg C m-2 s-1)
        real(8) :: gpp
        real(8) :: nee
        real(8) :: npp
        real(8) :: nppLeaf
        real(8) :: nppStem
        real(8) :: nppRoot
        real(8) :: nppOther           ! According to SPRUCE-MIP, stem means above ground Stemy tissues which is different from Stem tissues.
        real(8) :: ra
        real(8) :: raLeaf
        real(8) :: raStem
        real(8) :: raRoot
        real(8) :: raOther
        real(8) :: rMaint
        real(8) :: rGrowth            ! maintenance respiration and growth respiration
        real(8) :: rh
        real(8) :: nbp                ! heterotrophic respiration. NBP(net biome productivity) = GPP - Rh - Ra - other losses  
        real(8) :: wetlandCH4
        real(8) :: wetlandCH4prod
        real(8) :: wetlandCH4cons     ! wetland net fluxes of CH4, CH4 production, CH4 consumption
        ! Carbon Pools  (KgC m-2)
        real(8) :: cLeaf
        real(8) :: cStem
        real(8) :: cRoot
        real(8) :: cOther              ! cOther: carbon biomass in other plant organs(reserves, fruits), Jian: maybe NSC storage in TECO?
        real(8) :: cLitter
        real(8) :: cLitterCwd          ! litter (excluding coarse Stemy debris), Jian: fine litter in TECO?, cLitterCwd: carbon in coarse Stemy debris
        real(8) :: cSoil
        real(8) :: cSoilLevels(nlayers)
        real(8) :: cSoilFast
        real(8) :: cSoilSlow
        real(8) :: cSoilPassive        ! cSoil: soil organic carbon (Jian: total soil carbon); cSoilLevels(depth-specific soil organic carbon, Jian: depth?); cSoilPools (different pools without depth)
        real(8) :: CH4(nlayers)              ! methane concentration
        ! Nitrogen fluxes (kgN m-2 s-1)
        real(8) :: fBNF
        real(8) :: fN2O
        real(8) :: fNloss
        real(8) :: fNnetmin
        real(8) :: fNdep               ! fBNF: biological nitrogen fixation; fN2O: loss of nitrogen through emission of N2O; fNloss:Total loss of nitrogen to the atmosphere and from leaching; net mineralizaiton and deposition of N
        ! Nitrogen pools (kgN m-2)
        real(8) :: nLeaf
        real(8) :: nStem
        real(8) :: nRoot
        real(8) :: nOther
        real(8) :: nLitter
        real(8) :: nLitterCwd
        real(8) :: nSoil
        real(8) :: nMineral                ! nMineral: Mineral nitrogen pool
        ! energy fluxes (W m-2)
        real(8) :: hfls
        real(8) :: hfss
        real(8) :: SWnet
        real(8) :: LWnet                   ! Sensible heat flux; Latent heat flux; Net shortwave radiation; Net longwave radiation
        ! water fluxes (kg m-2 s-1)
        real(8) :: ec
        real(8) :: tran
        real(8) :: es                      ! Canopy evaporation; Canopy transpiration; Soil evaporation
        real(8) :: hfsbl                   ! Snow sublimation
        real(8) :: mrro
        real(8) :: mrros
        real(8) :: mrrob                   ! Total runoff; Surface runoff; Subsurface runoff
        ! other
        real(8) :: mrso(nlayers)           ! Kg m-2, soil moisture in each soil layer
        real(8) :: tsl(nlayers)            ! K, soil temperature in each soil layer
        real(8) :: tsland                  ! K, surface temperature
        real(8) :: wtd                     ! m, Water table depth
        real(8) :: snd                     ! m, Total snow depth
        real(8) :: lai                     ! m2 m-2, Leaf area index 
        ! litter and soil decomposition
        real(8) :: outC_lit_m
        real(8) :: outC_lit_s 
        real(8) :: outC_soil_f
        real(8) :: outC_soil_s
        real(8) :: outC_soil_p
        ! heterotrophic respiration from each pool
        real(8) :: Rh_lit_m
        real(8) :: Rh_lit_s 
        real(8) :: Rh_soil_f 
        real(8) :: Rh_soil_s 
        real(8) :: Rh_soil_p
    end type outvars_data_type
    type(outvars_data_type) :: outVars_h, outVars_d, outVars_m, outVars_y
    type(outvars_data_type), allocatable :: tot_outVars_h(:) 
    type(outvars_data_type), allocatable :: tot_outVars_d(:)
    type(outvars_data_type), allocatable :: tot_outVars_m(:)
    type(outvars_data_type), allocatable :: tot_outVars_y(:)
    ! end of define the output variables -----------------------------------------

    ! define the read parameters and initial values from parameters nml file
    type in_site_params
        real(8) :: lat
        real(8) :: lon
        real(8) :: wsmax 
        real(8) :: wsmin
        real(8) :: extkU
        real(8) :: Tau_F
        real(8) :: Tau_C
        real(8) :: Tau_Micro
        real(8) :: Tau_SlowSOM
        real(8) :: Tau_Passive
        real(8) :: Q10rh
        real(8) :: etaW
        real(8) :: f_F2M
        real(8) :: f_C2M
        real(8) :: f_C2S
        real(8) :: f_M2S
        real(8) :: f_M2P 
        real(8) :: f_S2P
        real(8) :: f_S2M
        real(8) :: f_P2M
        ! methane
        real(8) :: r_me 
        real(8) :: Q10pro
        real(8) :: kCH4
        real(8) :: Omax
        real(8) :: CH4_thre
        real(8) :: Tveg
        real(8) :: Tpro_me
        real(8) :: Toxi
        real(8) :: f 
        real(8) :: bubprob 
        real(8) :: Vmaxfraction
        real(8) :: pox
        ! -------------------------
        ! add for modifying the pool size
        real(8) :: f_lit_m
        real(8) :: s_lit
        real(8) :: f_fast   ! the proportion of soil fast pool in total soil pool
        real(8) :: f_slow   ! the proportion of soil slow pool in total soil pool
        real(8) :: s_soil
        ! add for water cycle: same in the parameters of initial values
        real(8) :: par_shcap_snow
        real(8) :: par_condu_snow
        real(8) :: par_condu_b
        ! real(8) :: par_albedo_snow
        real(8) :: par_fsub
        real(8) :: par_rho_snow
        real(8) :: par_decay_m
        ! ------------------------------
        ! add for annual different parameters
        real(8) :: Q10rh_1, Q10rh_2, Q10rh_3, Q10rh_4, Q10rh_5, Q10rh_6, Q10rh_7, Q10rh_8
        real(8) :: Q10pro_1, Q10pro_2, Q10pro_3, Q10pro_4, Q10pro_5, Q10pro_6, Q10pro_7, Q10pro_8
        real(8) :: Tpro_me_1, Tpro_me_2, Tpro_me_3, Tpro_me_4, Tpro_me_5, Tpro_me_6, Tpro_me_7, Tpro_me_8
        real(8) :: Tref_rh_1, Tref_rh_2, Tref_rh_3, Tref_rh_4, Tref_rh_5, Tref_rh_6, Tref_rh_7, Tref_rh_8
    end type in_site_params

    type in_spec_params
        character(50) :: spec_name
        real(8) :: pft_weight
        ! species parameter sets
        real(8) :: LAImax
        real(8) :: LAImin
        real(8) :: stom_n
        real(8) :: SapS
        real(8) :: SapR 
        real(8) :: SLA
        real(8) :: GLmax
        real(8) :: GRmax
        real(8) :: Gsmax
        real(8) :: alpha
        real(8) :: Vcmax0
        real(8) :: Ds0 
        real(8) :: xfang 
        real(8) :: rdepth
        real(8) :: Rootmax 
        real(8) :: Stemmax 
        real(8) :: Tau_Leaf 
        real(8) :: Tau_Stem 
        real(8) :: Tau_Root 
        real(8) :: Q10 
        real(8) :: Rl0 
        real(8) :: Rs0
        real(8) :: Rr0 
        real(8) :: JV 
        real(8) :: Entrpy 
        real(8) :: gddonset
        real(8) :: f_rg
        ! add for annual different parameters
        real(8) :: s_vea
        real(8) :: s_ved
        real(8) :: s_jea
        real(8) :: s_jed
        real(8) :: s_vea_1, s_vea_2, s_vea_3, s_vea_4, s_vea_5, s_vea_6, s_vea_7, s_vea_8
        real(8) :: s_ved_1, s_ved_2, s_ved_3, s_ved_4, s_ved_5, s_ved_6, s_ved_7, s_ved_8
        real(8) :: Entrpy_1, Entrpy_2, Entrpy_3, Entrpy_4, Entrpy_5, Entrpy_6, Entrpy_7, Entrpy_8
        real(8) :: Vcmax0_1, Vcmax0_2, Vcmax0_3, Vcmax0_4, Vcmax0_5, Vcmax0_6, Vcmax0_7, Vcmax0_8
        real(8) :: Q10_1, Q10_2, Q10_3, Q10_4, Q10_5, Q10_6, Q10_7, Q10_8
        real(8) :: f_rg_1, f_rg_2, f_rg_3, f_rg_4, f_rg_5, f_rg_6, f_rg_7, f_rg_8
        real(8) :: fn2r_1, fn2r_2, fn2r_3, fn2r_4, fn2r_5, fn2r_6, fn2r_7, fn2r_8
        real(8) :: s_npp_1, s_npp_2, s_npp_3, s_npp_4, s_npp_5, s_npp_6, s_npp_7, s_npp_8, s_npp_9
        real(8) :: fn2l_1, fn2l_2, fn2l_3, fn2l_4, fn2l_5, fn2l_6, fn2l_7, fn2l_8
        ! add in plant growth process
        real(8) :: hmax 
        real(8) :: hl0 
        real(8) :: LAIMAX0
        real(8) :: la0
        real(8) :: fn2l
        real(8) :: fn2r
        ! add for pool sizes
        real(8) :: s_cLeaf   ! scale the initial leaf C pool
        real(8) :: s_cStem   ! scale the initial stem C pool
        real(8) :: s_cRoot   ! scale the initial root C pool
        real(8) :: s_nsc     ! scale the initial nsc pool
        real(8) :: s_nsn
    end type in_spec_params

    type in_site_init_values
        ! site-based initial values
        real(8) :: cLit_m  
        real(8) :: cLit_s  
        real(8) :: cSoil_f 
        real(8) :: cSoil_s  
        real(8) :: cSoil_p  
        real(8) :: CN0_lit_m 
        real(8) :: CN0_lit_s 
        real(8) :: CN0_soil_f 
        real(8) :: CN0_soil_s 
        real(8) :: CN0_soil_p 
        real(8) :: N_deposit  
        real(8) :: QNminer 
        ! for soil conditions, physical processes
        real(8) :: thksl(nlayers) 
        real(8) :: FRLEN(nlayers)
        real(8) :: liq_water(nlayers) 
        real(8) :: fwsoil 
        real(8) :: topfws 
        real(8) :: omega 
        real(8) :: zwt  
        real(8) :: infilt 
        ! soil thermal dynamics in Yuanyuanversion
        real(8) :: sftmp 
        real(8) :: Tsnow  
        real(8) :: Twater 
        real(8) :: Tice  
        real(8) :: G  
        real(8) :: snow_dsim
        real(8) :: dcount  
        real(8) :: dcount_soil 
        real(8) :: ice_tw 
        real(8) :: Tsoill(nlayers) 
        real(8) :: ice(nlayers)   
        real(8) :: shcap_snow 
        real(8) :: condu_snow 
        real(8) :: condu_b
        real(8) :: depth_ex 
        real(8) :: diff_s  
        real(8) :: diff_snow 
        real(8) :: albedo_snow 
        real(8) :: resht 
        real(8) :: thd_snow_depth
        real(8) :: b_bound 
        real(8) :: infilt_rate
        real(8) :: fa 
        real(8) :: fsub 
        real(8) :: rho_snow 
        real(8) :: decay_m 
        !------------------------------
        real(8) :: CH4_V(nlayers)
        real(8) :: CH4(nlayers) 
        real(8) :: Vp(nlayers)
        real(8) :: bubble_methane_tot
        real(8) :: Nbub
        real(8) :: depth_1 
    end type in_site_init_values

    type in_spec_init_values
        ! species-based initial values
        real(8) :: cLeaf 
        real(8) :: cStem 
        real(8) :: cRoot 
        real(8) :: CN0_L 
        real(8) :: CN0_S 
        real(8) :: CN0_R 
        real(8) :: NSCmin 
        real(8) :: Storage 
        real(8) :: nsc 
        real(8) :: nsn 
        real(8) :: accumulation
        real(8) :: SNvcmax 
        real(8) :: N_deficit 
        real(8) :: alphaN 
        real(8) :: FRLEN(nlayers)
    end type in_spec_init_values

    type in_params_init_values
        type(in_site_params) :: st_params
        type(in_spec_params), allocatable :: sp_params(:)
        type(in_site_init_values) :: st_init_val
        type(in_spec_init_values), allocatable :: sp_init_val(:)
    end type in_params_init_values
    type(in_params_init_values) :: in_params_vals

    contains

    subroutine read_configs_nml(conf_nml_file)
        implicit none
        character(*), intent(in) :: conf_nml_file
        integer io

        namelist /teco_settings/ case_name, & 
            do_simu,   do_mcmc,    do_spinup,&
            do_matrix, do_restart, do_s_init, &
            do_obs_zwt, do_obs_Tsoil, do_snow, do_soilphy, do_EBG,  do_ndep,  do_leap, &
            do_out_hr, do_out_day, do_out_mon, do_out_yr, &
            inDir,     outDir,&
            climfile,  watertablefile, snowdepthfile, in_restartfile, defParamsFile, &
            mcmc_configfile, spinup_configfile
        namelist /exps_settings/ Ttreat, CO2treat, N_fert

        print *, "# Read TECO config nml file ...", adjustl(trim(conf_nml_file))
        open(388, file = conf_nml_file)
        read(388, nml  = teco_settings, iostat=io)
        read(388, nml  = exps_settings, iostat=io)
        close(388)
    end subroutine read_configs_nml

    subroutine initialize_teco(st) !, do_mcmc_cycle)
        implicit none
        real(8) :: cSoil, cLit
        type(site_data_type), intent(inout) :: st
        ! logical,intent(in)::do_mcmc_cycle
        integer :: ipft, i
        ! if(.not. do_mcmc_cycle) then
            call init_site_params(st)
            call init_site_init_values(st)
        if(do_s_init)then
            cLit  = st%cLit_m + st%cLit_s
            st%cLit_m  = st%f_lit_m * cLit
            st%cLit_s  = (1-st%f_lit_m) * cLit
            cSoil = st%cSoil_f + st%cSoil_s + st%cSoil_p
            cSoil = cSoil * st%s_soil
            st%cSoil_f = cSoil * st%f_fast
            st%cSoil_s = cSoil * st%f_slow
            st%cSoil_p = cSoil - st%cSoil_f - st%cSoil_s
        endif
        ! update the water processes
        st%shcap_snow = st%par_shcap_snow
        st%condu_snow = st%par_condu_snow
        st%condu_b    = st%par_condu_b
        st%fsub       = st%par_fsub
        st%rho_snow   = st%par_rho_snow
        st%decay_m    = st%par_decay_m
        
        ! endif
        st%LAImax = 0.
        st%LAImin = 0. 
        st%LAI    = 0.
        st%bmleaf = 0.
        st%bmstem = 0.
        st%bmroot = 0.
        st%frlen  = 0.
        
        do ipft = 1, npft
            ! if(.not. do_mcmc_cycle) then
                call init_spec_params(st%sp(ipft), in_params_vals%sp_params(ipft))
                call init_spec_init_Values(st%sp(ipft), in_params_vals%sp_init_val(ipft))
            ! endif

            ! add the processes to scale the cLeaf, cStem, cRoot, nsn
            if(do_s_init)then
                st%sp(ipft)%cLeaf = st%sp(ipft)%cLeaf * st%sp(ipft)%s_cLeaf
                st%sp(ipft)%cStem = st%sp(ipft)%cStem * st%sp(ipft)%s_cStem
                st%sp(ipft)%cRoot = st%sp(ipft)%cRoot * st%sp(ipft)%s_cRoot
                st%sp(ipft)%nsc   = st%sp(ipft)%nsc   * st%sp(ipft)%s_nsc
                st%sp(ipft)%nsn   = st%sp(ipft)%nsn   * st%sp(ipft)%s_nsn
            endif
            ! end

            st%LAImax = st%LAImax + st%sp(ipft)%pft_weight * st%sp(ipft)%LAImax
            st%LAImin = st%LAImin + st%sp(ipft)%pft_weight * st%sp(ipft)%LAImin
            st%frlen  = st%frlen  + st%sp(ipft)%pft_weight * st%sp(ipft)%frlen
            st%sp(ipft)%Tau_Leaf = st%sp(ipft)%Tau_Leaf * 8760. ! the unit of residence time is transformed from yearly to hourly
            st%sp(ipft)%Tau_Stem = st%sp(ipft)%Tau_Stem * 8760.
            st%sp(ipft)%Tau_Root = st%sp(ipft)%Tau_Root * 8760.
            st%sp(ipft)%SLA      = st%sp(ipft)%SLA/10000.         ! Convert unit from cm2/g to m2/g
            st%sp(ipft)%GLmax    = st%sp(ipft)%GLmax/8760.
            st%sp(ipft)%GRmax    = st%sp(ipft)%GRmax/8760.
            st%sp(ipft)%Gsmax    = st%sp(ipft)%Gsmax/8760.
            st%sp(ipft)%stor_use = st%sp(ipft)%Storage/times_storage_use
            st%sp(ipft)%LAI      = st%sp(ipft)%LAIMIN
            st%lai               = st%lai + st%sp(ipft)%pft_weight * st%sp(ipft)%LAI
            st%sp(ipft)%bmleaf   = st%sp(ipft)%cLeaf/0.48
            st%sp(ipft)%bmstem   = st%sp(ipft)%cStem/0.48
            st%sp(ipft)%bmroot   = st%sp(ipft)%cRoot/0.48
            st%sp(ipft)%bmplant  = st%sp(ipft)%bmstem + st%sp(ipft)%bmroot + st%sp(ipft)%bmleaf
            st%bmleaf            = st%bmleaf + st%sp(ipft)%pft_weight * st%sp(ipft)%bmleaf
            st%bmstem            = st%bmstem + st%sp(ipft)%pft_weight * st%sp(ipft)%bmstem
            st%bmroot            = st%bmroot + st%sp(ipft)%pft_weight * st%sp(ipft)%bmroot
            st%sp(ipft)%CN_L     = st%sp(ipft)%CN0_L
            st%sp(ipft)%CN_W     = st%sp(ipft)%CN0_S
            st%sp(ipft)%CN_R     = st%sp(ipft)%CN0_R
            st%sp(ipft)%nLeaf    = st%sp(ipft)%cLeaf/st%sp(ipft)%CN_L
            st%sp(ipft)%nStem    = st%sp(ipft)%cStem/st%sp(ipft)%CN_W
            st%sp(ipft)%nRoot    = st%sp(ipft)%cRoot/st%sp(ipft)%CN_R

        enddo
    
        st%Tau_C       = st%Tau_C * 8760.
        st%Tau_F       = st%Tau_F * 8760.
        st%Tau_Micro   = st%Tau_Micro * 8760.
        st%Tau_SlowSOM = st%Tau_Micro * 8760.
        st%Tau_Passive = st%Tau_Passive * 8760.
        st%CN_lit_m    = st%CN0_lit_m
        st%CN_lit_s    = st%CN0_lit_s
        st%CN_soil_f   = st%CN0_soil_f
        st%CN_soil_s   = st%CN0_soil_s
        st%CN_soil_p   = st%CN0_soil_p
        st%nLit_m      = st%cLit_m/st%CN0_lit_m
        st%nLit_s      = st%cLit_s/st%CN0_lit_s
        st%nSoil_f     = st%cSoil_f/st%CN0_soil_f
        st%nSoil_s     = st%cSoil_s/st%CN0_soil_s
        st%nSoil_p     = st%cSoil_p/st%CN0_soil_p

        st%wcl   = st%wsmax/100.
        st%Esoil = 0.5*st%G
        do i = 2, nlayers
            st%depth(i) = st%depth(i-1) + st%thksl(i)
        enddo
        do i=1,nlayers
            if (st%depth(i) .le. (-st%zwt)*0.1) then
                st%pwater(i) = 1000*9.81*(st%depth(i)*0.01-(-st%zwt)*0.001)
            else
                st%pwater(i) = 0.
            endif
            st%presP(i) = 101325 + st%pwater(i)  ! unit Pa
            st%methanebP(i) = st%f * st%presP(i) * st%Vp(i)/(8.3144621 * (st%Tsoill(i)+273.15))  !unit mol/layer
            st%methaneP(i) = st%CH4(i)/12
        ! gC/layer  /12   unit molC/layer
        enddo
        st%N_deposit = st%N_deposit/8760. !(gN/h/m2, )

    end subroutine initialize_teco

    subroutine init_site_params(st)
        implicit none
        type(site_data_type), intent(inout) :: st
        call update_site_params(st, in_params_vals%st_params%lat, in_params_vals%st_params%lon, &
            &   in_params_vals%st_params%wsmax, in_params_vals%st_params%wsmin, in_params_vals%st_params%extkU, &
            &   in_params_vals%st_params%Tau_F, in_params_vals%st_params%Tau_C, in_params_vals%st_params%Tau_Micro,&
            &   in_params_vals%st_params%Tau_SlowSOM, in_params_vals%st_params%Tau_Passive, &
            &   in_params_vals%st_params%Q10rh, in_params_vals%st_params%etaW, in_params_vals%st_params%f_F2M, &
            &   in_params_vals%st_params%f_C2M, &
            &   in_params_vals%st_params%f_C2S, in_params_vals%st_params%f_M2S, in_params_vals%st_params%f_M2P, &
            &   in_params_vals%st_params%f_S2P, in_params_vals%st_params%f_S2M, in_params_vals%st_params%f_P2M, &  
            &   in_params_vals%st_params%r_me,  in_params_vals%st_params%Q10pro, in_params_vals%st_params%kCH4, & 
            &   in_params_vals%st_params%Omax,  in_params_vals%st_params%CH4_thre, in_params_vals%st_params%Tveg,& 
            &   in_params_vals%st_params%Tpro_me, in_params_vals%st_params%Toxi, in_params_vals%st_params%f, &
            &   in_params_vals%st_params%bubprob, in_params_vals%st_params%Vmaxfraction, &
            &   in_params_vals%st_params%f_lit_m, in_params_vals%st_params%s_lit, & 
            &   in_params_vals%st_params%f_fast, in_params_vals%st_params%f_slow, in_params_vals%st_params%s_soil, &
            &   in_params_vals%st_params%par_shcap_snow, in_params_vals%st_params%par_condu_snow, &
            &   in_params_vals%st_params%par_condu_b, &!in_params_vals%st_params%par_albedo_snow, &
            &   in_params_vals%st_params%par_fsub, in_params_vals%st_params%par_rho_snow, &
            &   in_params_vals%st_params%par_decay_m, &
            &   in_params_vals%st_params%pox, &
            &   in_params_vals%st_params%Q10rh_1, in_params_vals%st_params%Q10rh_2, in_params_vals%st_params%Q10rh_3, &
            &   in_params_vals%st_params%Q10rh_4, in_params_vals%st_params%Q10rh_5, in_params_vals%st_params%Q10rh_6, &
            &   in_params_vals%st_params%Q10rh_7, in_params_vals%st_params%Q10rh_8, &
            &   in_params_vals%st_params%Q10pro_1, in_params_vals%st_params%Q10pro_2, in_params_vals%st_params%Q10pro_3, &
            &   in_params_vals%st_params%Q10pro_4, in_params_vals%st_params%Q10pro_5, in_params_vals%st_params%Q10pro_6, &
            &   in_params_vals%st_params%Q10pro_7, in_params_vals%st_params%Q10pro_8, &
            &   in_params_vals%st_params%Tpro_me_1, in_params_vals%st_params%Tpro_me_2, in_params_vals%st_params%Tpro_me_3, &
            &   in_params_vals%st_params%Tpro_me_4, in_params_vals%st_params%Tpro_me_5, in_params_vals%st_params%Tpro_me_6, &
            &   in_params_vals%st_params%Tpro_me_7, in_params_vals%st_params%Tpro_me_8, &
            &   in_params_vals%st_params%Tref_rh_1, in_params_vals%st_params%Tref_rh_2, in_params_vals%st_params%Tref_rh_3, &
            &   in_params_vals%st_params%Tref_rh_4, in_params_vals%st_params%Tref_rh_5, in_params_vals%st_params%Tref_rh_6, &
            &   in_params_vals%st_params%Tref_rh_7, in_params_vals%st_params%Tref_rh_8)
    end subroutine init_site_params

    subroutine init_site_init_values(st)
        implicit none
        type(site_data_type), intent(inout) :: st
        call update_site_init_values(st,&
            &   in_params_vals%st_init_val%cLit_m, in_params_vals%st_init_val%cLit_s,&
            &   in_params_vals%st_init_val%cSoil_f, in_params_vals%st_init_val%cSoil_s, &
            &   in_params_vals%st_init_val%cSoil_p, in_params_vals%st_init_val%CN0_lit_m,&
            &   in_params_vals%st_init_val%CN0_lit_s, in_params_vals%st_init_val%CN0_soil_f,& 
            &   in_params_vals%st_init_val%CN0_soil_s, in_params_vals%st_init_val%CN0_soil_p, &
            &   in_params_vals%st_init_val%N_deposit, in_params_vals%st_init_val%QNminer, &
            &   in_params_vals%st_init_val%thksl, in_params_vals%st_init_val%liq_water,& 
            &   in_params_vals%st_init_val%fwsoil, in_params_vals%st_init_val%topfws, &
            &   in_params_vals%st_init_val%omega, in_params_vals%st_init_val%zwt, &
            &   in_params_vals%st_init_val%infilt, in_params_vals%st_init_val%sftmp, &
            &   in_params_vals%st_init_val%Tsnow, in_params_vals%st_init_val%Twater, &
            &   in_params_vals%st_init_val%Tice, in_params_vals%st_init_val%G, &
            &   in_params_vals%st_init_val%snow_dsim, in_params_vals%st_init_val%dcount,& 
            &   in_params_vals%st_init_val%dcount_soil, in_params_vals%st_init_val%ice_tw, &
            &   in_params_vals%st_init_val%Tsoill, in_params_vals%st_init_val%ice,& 
            &   in_params_vals%st_init_val%shcap_snow, in_params_vals%st_init_val%condu_snow, &
            &   in_params_vals%st_init_val%condu_b, in_params_vals%st_init_val%depth_ex, &
            &   in_params_vals%st_init_val%diff_s, in_params_vals%st_init_val%diff_snow, &
            &   in_params_vals%st_init_val%albedo_snow, in_params_vals%st_init_val%resht, &
            &   in_params_vals%st_init_val%thd_snow_depth, in_params_vals%st_init_val%b_bound,& 
            &   in_params_vals%st_init_val%infilt_rate, in_params_vals%st_init_val%fa, &
            &   in_params_vals%st_init_val%fsub, in_params_vals%st_init_val%rho_snow, &
            &   in_params_vals%st_init_val%decay_m, in_params_vals%st_init_val%CH4_V, & 
            &   in_params_vals%st_init_val%CH4, in_params_vals%st_init_val%Vp, &
            &   in_params_vals%st_init_val%bubble_methane_tot, in_params_vals%st_init_val%Nbub,& 
            &   in_params_vals%st_init_val%depth_1)
    end subroutine init_site_init_values

    subroutine init_spec_params(in_spec, in_sp_params)
        implicit none
        type(spec_data_type), intent(inout) :: in_spec
        type(in_spec_params), intent(in)    :: in_sp_params
        call update_spec_params(in_spec, &
            &   in_sp_params%spec_name, int(in_sp_params%stom_n),  in_sp_params%pft_weight, &
            &   in_sp_params%LAImax, in_sp_params%LAImin, in_sp_params%SapS, in_sp_params%SapR, &
            &   in_sp_params%SLA, in_sp_params%GLmax, in_sp_params%GRmax,  in_sp_params%Gsmax, &
            &   in_sp_params%alpha, in_sp_params%Vcmax0, in_sp_params%Ds0, in_sp_params%xfang, &
            &   in_sp_params%rdepth, in_sp_params%Rootmax,  in_sp_params%Stemmax, &
            &   in_sp_params%Tau_Leaf, in_sp_params%Tau_Stem,  in_sp_params%Tau_Root, in_sp_params%Q10, &   
            &   in_sp_params%Rl0, in_sp_params%Rs0, in_sp_params%Rr0, in_sp_params%JV, in_sp_params%Entrpy, &   
            &   in_sp_params%gddonset, in_sp_params%hmax, in_sp_params%hl0, in_sp_params%LAIMAX0,  &
            &   in_sp_params%la0, in_sp_params%fn2l,   in_sp_params%fn2r, &
            &   in_sp_params%s_cLeaf, in_sp_params%s_cStem, in_sp_params%s_cRoot, in_sp_params%s_nsc,in_sp_params%s_nsn, &
            &   in_sp_params%f_rg, in_sp_params%s_vea, in_sp_params%s_ved, in_sp_params%s_jea, in_sp_params%s_jed, &
            &   in_sp_params%s_vea_1, in_sp_params%s_vea_2, in_sp_params%s_vea_3, in_sp_params%s_vea_4, &
            &   in_sp_params%s_vea_5, in_sp_params%s_vea_6, in_sp_params%s_vea_7, in_sp_params%s_vea_8, &
            &   in_sp_params%s_ved_1, in_sp_params%s_ved_2, in_sp_params%s_ved_3, in_sp_params%s_ved_4, &
            &   in_sp_params%s_ved_5, in_sp_params%s_ved_6, in_sp_params%s_ved_7, in_sp_params%s_ved_8, &
            &   in_sp_params%Entrpy_1, in_sp_params%Entrpy_2, in_sp_params%Entrpy_3, in_sp_params%Entrpy_4, &
            &   in_sp_params%Entrpy_5, in_sp_params%Entrpy_6, in_sp_params%Entrpy_7, in_sp_params%Entrpy_8, &
            &   in_sp_params%Vcmax0_1, in_sp_params%Vcmax0_2, in_sp_params%Vcmax0_3, in_sp_params%Vcmax0_4, &
            &   in_sp_params%Vcmax0_5, in_sp_params%Vcmax0_6, in_sp_params%Vcmax0_7, in_sp_params%Vcmax0_8, &
            &   in_sp_params%Q10_1, in_sp_params%Q10_2, in_sp_params%Q10_3, in_sp_params%Q10_4, &
            &   in_sp_params%Q10_5, in_sp_params%Q10_6, in_sp_params%Q10_7, in_sp_params%Q10_8, &
            &   in_sp_params%f_rg_1, in_sp_params%f_rg_2, in_sp_params%f_rg_3, in_sp_params%f_rg_4, &
            &   in_sp_params%f_rg_5, in_sp_params%f_rg_6, in_sp_params%f_rg_7, in_sp_params%f_rg_8, &
            &   in_sp_params%fn2r_1, in_sp_params%fn2r_2, in_sp_params%fn2r_3, in_sp_params%fn2r_4, &
            &   in_sp_params%fn2r_5, in_sp_params%fn2r_6, in_sp_params%fn2r_7, in_sp_params%fn2r_8, &
            &   in_sp_params%s_npp_1,in_sp_params%s_npp_2,in_sp_params%s_npp_3,in_sp_params%s_npp_4,&
            &   in_sp_params%s_npp_5,in_sp_params%s_npp_6,in_sp_params%s_npp_7,in_sp_params%s_npp_8,&
            &   in_sp_params%s_npp_9, &
            &   in_sp_params%fn2l_1, in_sp_params%fn2l_2, in_sp_params%fn2l_3, in_sp_params%fn2l_4, &
            &   in_sp_params%fn2l_5, in_sp_params%fn2l_6, in_sp_params%fn2l_7, in_sp_params%fn2l_8)
    end subroutine init_spec_params

    subroutine init_spec_init_Values(in_spec, in_sp_init_vals)
        implicit none
        type(spec_data_type), intent(inout) :: in_spec
        type(in_spec_init_values), intent(in) :: in_sp_init_vals
        call update_spec_init_values(in_spec, &
            &   in_sp_init_vals%cLeaf, in_sp_init_vals%cStem, in_sp_init_vals%cRoot, &
            &   in_sp_init_vals%CN0_L, in_sp_init_vals%CN0_S, in_sp_init_vals%CN0_R, &
            &   in_sp_init_vals%NSCmin, in_sp_init_vals%Storage, in_sp_init_vals%nsc, in_sp_init_vals%nsn, &
            &   in_sp_init_vals%accumulation, in_sp_init_vals%SNvcmax, in_sp_init_vals%N_deficit, &
            &   in_sp_init_vals%alphaN, in_sp_init_vals%frlen)
    end subroutine init_spec_init_Values

    subroutine update_site_params(in_st, &
        lat, lon,  wsmax,  wsmin,  extkU, &
        Tau_F, Tau_C, Tau_Micro, Tau_SlowSOM, Tau_Passive, &
        Q10rh, etaW, f_F2M, f_C2M, f_C2S, f_M2S, f_M2P, f_S2P, f_S2M, f_P2M, &  
        r_me, Q10pro, kCH4, Omax, CH4_thre, &  
        Tveg, Tpro_me, Toxi, f, bubprob, Vmaxfraction, &
        f_lit_m, s_lit,&
        f_fast, f_slow, s_soil,&
        par_shcap_snow, par_condu_snow, par_condu_b, & !par_albedo_snow, 
        par_fsub, par_rho_snow, par_decay_m, pox, &
        Q10rh_1, Q10rh_2, Q10rh_3, Q10rh_4, Q10rh_5, Q10rh_6, Q10rh_7, Q10rh_8, &
        Q10pro_1, Q10pro_2, Q10pro_3, Q10pro_4, Q10pro_5, Q10pro_6, Q10pro_7, Q10pro_8, &
        Tpro_me_1, Tpro_me_2, Tpro_me_3, Tpro_me_4, Tpro_me_5, Tpro_me_6, Tpro_me_7, Tpro_me_8, &
        Tref_rh_1, Tref_rh_2, Tref_rh_3, Tref_rh_4, Tref_rh_5, Tref_rh_6, Tref_rh_7, Tref_rh_8)
        implicit none
        type(site_data_type), intent(inout) :: in_st
        !!! site-based parameters
        real(8), intent(in) :: lat, lon,  wsmax,  wsmin,  extkU
        real(8), intent(in) :: Tau_F, Tau_C, Tau_Micro, Tau_SlowSOM, Tau_Passive
        real(8), intent(in) :: Q10rh, etaW, f_F2M, f_C2M, f_C2S, f_M2S, f_M2P, f_S2P, f_S2M, f_P2M  
        real(8), intent(in) :: r_me, Q10pro, kCH4, Omax, CH4_thre  
        real(8), intent(in) :: Tveg, Tpro_me, Toxi, f, bubprob, Vmaxfraction 
        real(8), intent(in) :: f_lit_m, s_lit
        real(8), intent(in) :: f_fast, f_slow, s_soil
        real(8), intent(in) :: par_shcap_snow, par_condu_snow, par_condu_b
        real(8), intent(in) :: par_fsub, par_rho_snow, par_decay_m, pox
        real(8), intent(in) :: Q10rh_1, Q10rh_2, Q10rh_3, Q10rh_4, Q10rh_5, Q10rh_6, Q10rh_7, Q10rh_8
        real(8), intent(in) :: Q10pro_1, Q10pro_2, Q10pro_3, Q10pro_4, Q10pro_5, Q10pro_6, Q10pro_7, Q10pro_8
        real(8), intent(in) :: Tpro_me_1, Tpro_me_2, Tpro_me_3, Tpro_me_4, Tpro_me_5, Tpro_me_6, Tpro_me_7, Tpro_me_8
        real(8), intent(in) :: Tref_rh_1, Tref_rh_2, Tref_rh_3, Tref_rh_4, Tref_rh_5, Tref_rh_6, Tref_rh_7, Tref_rh_8
        in_st%lat          = lat
        in_st%lon          = lon
        in_st%wsmax        = wsmax
        in_st%wsmin        = wsmin
        in_st%extkU        = extkU
        in_st%Tau_F        = Tau_F
        in_st%Tau_C        = Tau_C
        in_st%Tau_Micro    = Tau_Micro
        in_st%Tau_SlowSOM  = Tau_SlowSOM
        in_st%Tau_Passive  = Tau_Passive
        in_st%Q10rh        = Q10rh
        in_st%etaW         = etaW
        in_st%f_F2M        = f_F2M
        in_st%f_C2M        = f_C2M
        in_st%f_C2S        = f_C2S
        in_st%f_M2S        = f_M2S
        in_st%f_M2P        = f_M2P
        in_st%f_S2P        = f_S2P
        in_st%f_S2M        = f_S2M
        in_st%f_P2M        = f_P2M
        in_st%r_me         = r_me
        in_st%Q10pro       = Q10pro
        in_st%kCH4         = kCH4
        in_st%Omax         = Omax
        in_st%CH4_thre     = CH4_thre
        in_st%Tveg         = Tveg
        in_st%Tpro_me      = Tpro_me
        in_st%Toxi         = Toxi
        in_st%f            = f
        in_st%bubprob      = bubprob
        in_st%Vmaxfraction = Vmaxfraction
        in_st%pox          = pox
        ! add for annual different parameters
        in_st%Q10rh_1      = Q10rh_1
        in_st%Q10rh_2      = Q10rh_2
        in_st%Q10rh_3      = Q10rh_3
        in_st%Q10rh_4      = Q10rh_4
        in_st%Q10rh_5      = Q10rh_5
        in_st%Q10rh_6      = Q10rh_6
        in_st%Q10rh_7      = Q10rh_7
        in_st%Q10rh_8      = Q10rh_8
        in_st%Q10pro_1     = Q10pro_1
        in_st%Q10pro_2     = Q10pro_2
        in_st%Q10pro_3     = Q10pro_3
        in_st%Q10pro_4     = Q10pro_4
        in_st%Q10pro_5     = Q10pro_5
        in_st%Q10pro_6     = Q10pro_6
        in_st%Q10pro_7     = Q10pro_7
        in_st%Q10pro_8     = Q10pro_8
        in_st%Tpro_me_1    = Tpro_me_1
        in_st%Tpro_me_2    = Tpro_me_2
        in_st%Tpro_me_3    = Tpro_me_3
        in_st%Tpro_me_4    = Tpro_me_4
        in_st%Tpro_me_5    = Tpro_me_5
        in_st%Tpro_me_6    = Tpro_me_6
        in_st%Tpro_me_7    = Tpro_me_7
        in_st%Tpro_me_8    = Tpro_me_8
        in_st%Tref_rh_1    = Tref_rh_1
        in_st%Tref_rh_2    = Tref_rh_2
        in_st%Tref_rh_3    = Tref_rh_3
        in_st%Tref_rh_4    = Tref_rh_4
        in_st%Tref_rh_5    = Tref_rh_5
        in_st%Tref_rh_6    = Tref_rh_6
        in_st%Tref_rh_7    = Tref_rh_7
        in_st%Tref_rh_8    = Tref_rh_8
        ! add new parameters
        in_st%f_lit_m      = f_lit_m
        in_st%s_lit        = s_lit
        in_st%f_fast       = f_fast
        in_st%f_slow       = f_slow
        in_st%s_soil       = s_soil
        in_st%par_shcap_snow   = par_shcap_snow
        in_st%par_condu_snow   = par_condu_snow
        in_st%par_condu_b      = par_condu_b
        ! in_st%par_albedo_snow  = par_albedo_snow
        in_st%par_fsub         = par_fsub
        in_st%par_rho_snow     = par_rho_snow
        in_st%par_decay_m      = par_decay_m
        ! print*, "TauC ssss: ", Tau_C, in_st%Tau_C
        return
    end subroutine update_site_params

    subroutine update_site_init_values(in_st, &
        cLit_m, cLit_s, cSoil_f, cSoil_s, cSoil_p, &
        CN0_lit_m, CN0_lit_s, CN0_soil_f, CN0_soil_s, CN0_soil_p, &
        N_deposit, QNminer, thksl, &
        liq_water, fwsoil, topfws, omega, &
        zwt, infilt, sftmp, Tsnow, Twater, Tice, &
        G, snow_dsim, dcount, dcount_soil, ice_tw, &
        Tsoill, ice, shcap_snow, condu_snow, condu_b, &
        depth_ex, diff_s, diff_snow, albedo_snow, &
        resht, thd_snow_depth, b_bound, infilt_rate, &
        fa, fsub, rho_snow, decay_m, &
        CH4_V, CH4, Vp, bubble_methane_tot, Nbub, depth_1)
        implicit none
        type(site_data_type), intent(inout) :: in_st
        !!! site-based initial values
        real(8), intent(in) :: cLit_m, cLit_s, cSoil_f, cSoil_s, cSoil_p
        real(8), intent(in) :: CN0_lit_m, CN0_lit_s, CN0_soil_f, CN0_soil_s, CN0_soil_p
        real(8), intent(in) :: N_deposit, QNminer, thksl(nlayers)
        real(8), intent(in) :: liq_water(nlayers), fwsoil, topfws, omega
        real(8), intent(in) :: zwt, infilt, sftmp, Tsnow, Twater, Tice
        real(8), intent(in) :: G, snow_dsim, dcount, dcount_soil, ice_tw
        real(8), intent(in) :: Tsoill(nlayers), ice(nlayers), shcap_snow, condu_snow, condu_b
        real(8), intent(in) :: depth_ex, diff_s, diff_snow, albedo_snow
        real(8), intent(in) :: resht, thd_snow_depth, b_bound, infilt_rate
        real(8), intent(in) :: fa, fsub, rho_snow, decay_m
        real(8), intent(in) :: CH4_V(nlayers), CH4(nlayers), Vp(nlayers)
        real(8), intent(in) :: bubble_methane_tot, Nbub, depth_1
        in_st%cLit_m             = cLit_m
        in_st%cLit_s             = cLit_s
        in_st%cSoil_f            = cSoil_f
        in_st%cSoil_s            = cSoil_s
        in_st%cSoil_p            = cSoil_p
        in_st%CN0_lit_m          = CN0_lit_m
        in_st%CN0_lit_s          = CN0_lit_s
        in_st%CN0_soil_f         = CN0_soil_f
        in_st%CN0_soil_s         = CN0_soil_s
        in_st%CN0_soil_p         = CN0_soil_p
        in_st%N_deposit          = N_deposit
        in_st%QNminer            = QNminer
        in_st%thksl              = thksl
        in_st%liq_water          = liq_water
        in_st%fwsoil             = fwsoil
        in_st%topfws             = topfws
        in_st%omega              = omega
        in_st%zwt                = zwt
        in_st%infilt             = infilt
        in_st%sftmp              = sftmp
        in_st%Tsnow              = Tsnow
        in_st%Twater             = Twater
        in_st%Tice               = Tice
        in_st%G                  = G
        in_st%snow_dsim          = snow_dsim
        in_st%dcount             = dcount
        in_st%dcount_soil        = dcount_soil
        in_st%ice_tw             = ice_tw
        in_st%Tsoill             = Tsoill
        in_st%ice                = ice
        in_st%shcap_snow         = shcap_snow
        in_st%condu_snow         = condu_snow
        in_st%condu_b            = condu_b
        in_st%depth_ex           = depth_ex
        in_st%diff_s             = diff_s
        in_st%diff_snow          = diff_snow
        in_st%albedo_snow        = albedo_snow
        in_st%resht              = resht
        in_st%thd_snow_depth     = thd_snow_depth
        in_st%b_bound            = b_bound
        in_st%infilt_rate        = infilt_rate
        in_st%fa                 = fa
        in_st%fsub               = fsub
        in_st%rho_snow           = rho_snow
        in_st%decay_m            = decay_m
        in_st%CH4_V              = CH4_V
        in_st%CH4                = CH4
        in_st%Vp                 = Vp
        in_st%bubble_methane_tot = bubble_methane_tot
        in_st%Nbub               = Nbub
        in_st%depth(1)           = depth_1
        ! -----------------------------------
        return
    end subroutine update_site_init_values

    subroutine update_spec_params(in_spec, &
        spec_name, stom_n,  pft_weight,  LAImax, LAImin, & 
        SapS,     SapR,      SLA, GLmax, GRmax,  Gsmax, &
        alpha,    Vcmax0,    Ds0,  xfang, rdepth, Rootmax,  Stemmax, &
        Tau_Leaf, Tau_Stem,  Tau_Root,    Q10,    Rl0,      Rs0,      Rr0, &
        JV,       Entrpy,    gddonset,    hmax,   hl0,      LAIMAX0,  la0,&
        fn2l, fn2r, s_cLeaf, s_cStem,   s_cRoot,  s_nsc, s_nsn, f_rg,s_vea,s_ved,s_jea,s_jed,&
        s_vea_1, s_vea_2, s_vea_3, s_vea_4, s_vea_5, s_vea_6, s_vea_7, s_vea_8,&
        s_ved_1, s_ved_2, s_ved_3, s_ved_4, s_ved_5, s_ved_6, s_ved_7, s_ved_8,&
        Entrpy_1, Entrpy_2, Entrpy_3, Entrpy_4, Entrpy_5, Entrpy_6, Entrpy_7, Entrpy_8,&
        Vcmax0_1, Vcmax0_2, Vcmax0_3, Vcmax0_4, Vcmax0_5, Vcmax0_6, Vcmax0_7, Vcmax0_8,&
        Q10_1,   Q10_2,   Q10_3,   Q10_4,   Q10_5,   Q10_6,   Q10_7,   Q10_8, &
        f_rg_1,  f_rg_2,  f_rg_3,  f_rg_4,  f_rg_5,  f_rg_6,  f_rg_7,  f_rg_8, &
        fn2r_1,  fn2r_2,  fn2r_3,  fn2r_4,  fn2r_5,  fn2r_6,  fn2r_7,  fn2r_8, &
        s_npp_1, s_npp_2, s_npp_3, s_npp_4, s_npp_5, s_npp_6, s_npp_7, s_npp_8, s_npp_9, &
        fn2l_1, fn2l_2, fn2l_3, fn2l_4, fn2l_5, fn2l_6, fn2l_7, fn2l_8)
        implicit none
        type(spec_data_type), intent(inout) :: in_spec
        integer, intent(in) :: stom_n
        character(*), intent(in) :: spec_name
        real(8), intent(in) :: pft_weight, LAImax, LAImin
        real(8), intent(in) :: SapS,     SapR,     SLA
        real(8), intent(in) :: GLmax,    GRmax,    Gsmax
        real(8), intent(in) :: alpha,    Vcmax0,   Ds0
        real(8), intent(in) :: xfang,    rdepth,   Rootmax,  Stemmax
        real(8), intent(in) :: Tau_Leaf, Tau_Stem, Tau_Root
        real(8), intent(in) :: Q10,      Rl0,      Rs0,      Rr0
        real(8), intent(in) :: JV,       Entrpy,   gddonset 
        real(8), intent(in) ::  hmax,    hl0,      LAIMAX0,  la0, fn2l, fn2r
        real(8), intent(in) :: s_cLeaf, s_cStem,   s_cRoot,  s_nsc, s_nsn, f_rg
        real(8), intent(in) :: s_vea, s_ved, s_jea, s_jed
        real(8), intent(in) :: s_vea_1, s_vea_2, s_vea_3, s_vea_4, s_vea_5, s_vea_6, s_vea_7, s_vea_8
        real(8), intent(in) :: s_ved_1, s_ved_2, s_ved_3, s_ved_4, s_ved_5, s_ved_6, s_ved_7, s_ved_8
        real(8), intent(in) :: Entrpy_1, Entrpy_2, Entrpy_3, Entrpy_4, Entrpy_5, Entrpy_6, Entrpy_7, Entrpy_8
        real(8), intent(in) :: Vcmax0_1, Vcmax0_2, Vcmax0_3, Vcmax0_4, Vcmax0_5, Vcmax0_6, Vcmax0_7, Vcmax0_8
        real(8), intent(in) :: Q10_1,   Q10_2,   Q10_3,   Q10_4,   Q10_5,   Q10_6,   Q10_7,   Q10_8
        real(8), intent(in) :: f_rg_1,  f_rg_2,  f_rg_3,  f_rg_4,  f_rg_5,  f_rg_6,  f_rg_7,  f_rg_8
        real(8), intent(in) :: fn2r_1,  fn2r_2,  fn2r_3,  fn2r_4,  fn2r_5,  fn2r_6,  fn2r_7,  fn2r_8
        real(8), intent(in) :: s_npp_1, s_npp_2, s_npp_3, s_npp_4, s_npp_5, s_npp_6, s_npp_7, s_npp_8, s_npp_9
        real(8), intent(in) :: fn2l_1, fn2l_2, fn2l_3, fn2l_4, fn2l_5, fn2l_6, fn2l_7, fn2l_8
        in_spec%spec_name   = spec_name
        in_spec%pft_weight  = pft_weight
        ! species parameter sets
        in_spec%fn2l        = fn2l
        in_spec%fn2r        = fn2r
        in_spec%LAImax      = LAImax
        in_spec%LAImin      = LAImin
        in_spec%stom_n      = int(stom_n)
        in_spec%SapS        = SapS
        in_spec%SapR        = SapR
        in_spec%SLA         = SLA
        in_spec%GLmax       = GLmax
        in_spec%GRmax       = GRmax
        in_spec%Gsmax       = Gsmax
        in_spec%alpha       = alpha
        in_spec%Vcmax0      = Vcmax0
        in_spec%Ds0         = Ds0
        in_spec%xfang       = xfang
        in_spec%rdepth      = rdepth
        in_spec%Rootmax     = Rootmax
        in_spec%Stemmax     = Stemmax
        in_spec%Tau_Leaf    = Tau_Leaf
        in_spec%Tau_Stem    = Tau_Stem
        in_spec%Tau_Root    = Tau_Root
        in_spec%Q10         = Q10
        in_spec%Rl0         = Rl0 
        in_spec%Rs0         = Rs0
        in_spec%Rr0         = Rr0
        in_spec%JV          = JV
        in_spec%Entrpy      = Entrpy
        in_spec%gddonset    = gddonset
        in_spec%f_rg       = f_rg
        in_spec%s_vea       = s_vea
        in_spec%s_ved       = s_ved
        in_spec%s_jea       = s_jea
        in_spec%s_jed       = s_jed
        in_spec%s_vea_1     = s_vea_1
        in_spec%s_vea_2     = s_vea_2
        in_spec%s_vea_3     = s_vea_3
        in_spec%s_vea_4     = s_vea_4
        in_spec%s_vea_5     = s_vea_5
        in_spec%s_vea_6     = s_vea_6
        in_spec%s_vea_7     = s_vea_7
        in_spec%s_vea_8     = s_vea_8
        in_spec%s_ved_1     = s_ved_1
        in_spec%s_ved_2     = s_ved_2
        in_spec%s_ved_3     = s_ved_3
        in_spec%s_ved_4     = s_ved_4
        in_spec%s_ved_5     = s_ved_5
        in_spec%s_ved_6     = s_ved_6
        in_spec%s_ved_7     = s_ved_7
        in_spec%s_ved_8     = s_ved_8
        in_spec%Entrpy_1     = Entrpy_1
        in_spec%Entrpy_2     = Entrpy_2
        in_spec%Entrpy_3     = Entrpy_3
        in_spec%Entrpy_4     = Entrpy_4
        in_spec%Entrpy_5     = Entrpy_5
        in_spec%Entrpy_6     = Entrpy_6
        in_spec%Entrpy_7     = Entrpy_7
        in_spec%Entrpy_8     = Entrpy_8
        in_spec%Vcmax0_1     = Vcmax0_1
        in_spec%Vcmax0_2     = Vcmax0_2
        in_spec%Vcmax0_3     = Vcmax0_3
        in_spec%Vcmax0_4     = Vcmax0_4
        in_spec%Vcmax0_5     = Vcmax0_5
        in_spec%Vcmax0_6     = Vcmax0_6
        in_spec%Vcmax0_7     = Vcmax0_7
        in_spec%Vcmax0_8     = Vcmax0_8
        in_spec%Q10_1       = Q10_1
        in_spec%Q10_2       = Q10_2
        in_spec%Q10_3       = Q10_3
        in_spec%Q10_4       = Q10_4
        in_spec%Q10_5       = Q10_5
        in_spec%Q10_6       = Q10_6
        in_spec%Q10_7       = Q10_7
        in_spec%Q10_8       = Q10_8
        in_spec%f_rg_1      = f_rg_1
        in_spec%f_rg_2      = f_rg_2
        in_spec%f_rg_3      = f_rg_3
        in_spec%f_rg_4      = f_rg_4
        in_spec%f_rg_5      = f_rg_5
        in_spec%f_rg_6      = f_rg_6
        in_spec%f_rg_7      = f_rg_7
        in_spec%f_rg_8      = f_rg_8
        in_spec%fn2r_1     = fn2r_1
        in_spec%fn2r_2     = fn2r_2
        in_spec%fn2r_3     = fn2r_3
        in_spec%fn2r_4     = fn2r_4
        in_spec%fn2r_5     = fn2r_5
        in_spec%fn2r_6     = fn2r_6
        in_spec%fn2r_7     = fn2r_7
        in_spec%fn2r_8     = fn2r_8
        in_spec%s_npp_1    = s_npp_1
        in_spec%s_npp_2    = s_npp_2
        in_spec%s_npp_3    = s_npp_3
        in_spec%s_npp_4    = s_npp_4
        in_spec%s_npp_5    = s_npp_5
        in_spec%s_npp_6    = s_npp_6
        in_spec%s_npp_7    = s_npp_7
        in_spec%s_npp_8    = s_npp_8
        in_spec%s_npp_9    = s_npp_9
        in_spec%fn2l_1     = fn2l_1
        in_spec%fn2l_2     = fn2l_2
        in_spec%fn2l_3     = fn2l_3
        in_spec%fn2l_4     = fn2l_4
        in_spec%fn2l_5     = fn2l_5
        in_spec%fn2l_6     = fn2l_6
        in_spec%fn2l_7     = fn2l_7
        in_spec%fn2l_8     = fn2l_8
        ! add in plant growth process
        in_spec%hmax        = hmax 
        in_spec%hl0         = hl0 
        in_spec%LAIMAX0     = LAIMAX0 
        in_spec%la0         = la0
        in_spec%fn2l        = fn2l
        in_spec%fn2r        = fn2r
        ! add new parameters
        in_spec%s_cLeaf     = s_cLeaf
        in_spec%s_cStem     = s_cStem
        in_spec%s_cRoot     = s_cRoot
        in_spec%s_nsc       = s_nsc
        in_spec%s_nsn       = s_nsn
        return
    end subroutine update_spec_params

    subroutine update_spec_init_values(in_spec, &
        cLeaf, cStem, cRoot, &
        CN0_L, CN0_S, CN0_R, NSCmin, Storage, nsc, nsn, &
        accumulation, SNvcmax, N_deficit, alphaN, frlen)
        implicit none
        type(spec_data_type), intent(inout) :: in_spec
        real(8), intent(in) :: cLeaf, cStem, cRoot
        real(8), intent(in) :: CN0_L, CN0_S, CN0_R
        real(8), intent(in) :: NSCmin, Storage, nsc, nsn
        real(8), intent(in) :: accumulation, SNvcmax, N_deficit, alphaN
        real(8), intent(in) :: frlen(10)
        in_spec%cLeaf        = cLeaf
        in_spec%cStem        = cStem
        in_spec%cRoot        = cRoot
        in_spec%CN0_L        = CN0_L
        in_spec%CN0_S        = CN0_S
        in_spec%CN0_R        = CN0_R
        in_spec%NSCmin       = NSCmin
        in_spec%Storage      = Storage
        in_spec%nsc          = nsc
        in_spec%nsn          = nsn   
        in_spec%accumulation = accumulation 
        in_spec%SNvcmax      = SNvcmax
        in_spec%N_deficit    = N_deficit
        in_spec%alphaN       = alphaN
        in_spec%frlen        = FRLEN
        return
    end subroutine update_spec_init_values

    subroutine init_hourly()
        implicit none
        if (do_out_hr) call init_outVars(outVars_h)
    end subroutine init_hourly

    subroutine init_daily(st)
        implicit none
        type(site_data_type), INTENT(INOUT) :: st
        st%rain_d = 0
        st%ta     = 0.
        if (do_out_day) call init_outVars(outVars_d)
    end subroutine init_daily

    subroutine init_monthly()
        implicit none
        if(do_out_mon) call init_outVars(outVars_m)
    end subroutine init_monthly

    subroutine init_yearly(st)
        implicit none
        type(site_data_type), intent(inout) :: st
        integer ipft
        st%GDD5 = 0. 
        do ipft = 1, npft
            st%sp(ipft)%onset = 0
        enddo 
        if (do_out_yr) call init_outVars(outVars_y)
    end subroutine init_yearly

    subroutine init_outVars(outVars)
        implicit none
        type(outvars_data_type), intent(inout) :: outVars
        integer :: ipft
        outVars%year = 0
        outVars%doy  = 0
        outVars%hour = 0
        if (allocated(outVars%sp))then
            do ipft = 1, npft
                call init_spec_outVars(outVars%sp(ipft))
            enddo
        endif
        outVars%gpp              = 0.
        outVars%nee              = 0.
        outVars%npp              = 0.
        outVars%nppLeaf          = 0.
        outVars%nppStem          = 0.
        outVars%nppStem          = 0.
        outVars%nppRoot          = 0.
        outVars%nppOther         = 0.  
        outVars%ra               = 0.
        outVars%raLeaf           = 0.
        outVars%raStem           = 0.
        outVars%raRoot           = 0.
        outVars%raOther          = 0.
        outVars%rMaint           = 0.
        outVars%rGrowth          = 0.
        outVars%rh               = 0.
        outVars%nbp              = 0.
        outVars%wetlandCH4       = 0.
        outVars%wetlandCH4prod   = 0.
        outVars%wetlandCH4cons   = 0. 
        ! Carbon Pools  (KgC m-2)
        outVars%cLeaf            = 0.
        outVars%cStem            = 0.
        outVars%cRoot            = 0.
        outVars%cOther           = 0.
        outVars%cLitter          = 0.
        outVars%cLitterCwd       = 0.  
        outVars%cSoil            = 0.
        outVars%cSoilLevels(:)   = 0.
        outVars%cSoilFast        = 0.
        outVars%cSoilSlow        = 0.
        outVars%cSoilPassive     = 0. 
        outVars%CH4(:)           = 0.
        ! Nitrogen fluxes (kgN m-2 s-1)
        outVars%fBNF             = 0.
        outVars%fN2O             = 0.
        outVars%fNloss           = 0.
        outVars%fNnetmin         = 0.
        outVars%fNdep            = 0.  
        ! Nitrogen pools (kgN m-2)
        outVars%nLeaf            = 0.
        outVars%nStem            = 0.
        outVars%nRoot            = 0.
        outVars%nOther           = 0.
        outVars%nLitter          = 0.
        outVars%nLitterCwd       = 0.
        outVars%nSoil            = 0.
        outVars%nMineral         = 0. 
        ! energy fluxes (W m-2)
        outVars%hfls             = 0.
        outVars%hfss             = 0.
        outVars%SWnet            = 0.
        outVars%LWnet            = 0.
        ! water fluxes (kg m-2 s-1)
        outVars%ec               = 0.
        outVars%tran             = 0.
        outVars%es               = 0.   
        outVars%hfsbl            = 0.  
        outVars%mrro             = 0.
        outVars%mrros            = 0.
        outVars%mrrob            = 0.   
        ! other
        outVars%mrso(:)          = 0.  
        outVars%tsl(:)           = 0.
        outVars%tsland           = 0.                 
        outVars%wtd              = 0.           
        outVars%snd              = 0.           
        outVars%lai              = 0.
        ! out from litter and soil
        outVars%outC_lit_m       = 0.
        outVars%outC_lit_s       = 0.
        outVars%outC_soil_f      = 0.
        outVars%outC_soil_s      = 0.
        outVars%outC_soil_p      = 0.
        outVars%Rh_lit_m         = 0.
        outVars%Rh_lit_s         = 0.
        outVars%Rh_soil_f        = 0.
        outVars%Rh_soil_s        = 0.
        outVars%Rh_soil_p        = 0.
    end subroutine init_outVars

    subroutine init_spec_outVars(spec_outVars)
        implicit none
        type(spec_outvars_type), intent(inout) :: spec_outVars
        ! carbon fluxes (Kg C m-2 s-1)
        spec_outVars%gpp      = 0.
        spec_outVars%Aleaf(:)    = 0.
        spec_outVars%nee      = 0.
        spec_outVars%npp      = 0.
        spec_outVars%nppLeaf  = 0.
        spec_outVars%nppStem  = 0.
        spec_outVars%nppStem  = 0.
        spec_outVars%nppRoot  = 0.
        spec_outVars%nppOther = 0.    ! According to SPRUCE-MIP, stem means above ground Stemy tissues which is different from Stem tissues.
        spec_outVars%ra       = 0.
        spec_outVars%raLeaf   = 0.
        spec_outVars%raStem   = 0.
        spec_outVars%raRoot   = 0.
        spec_outVars%raOther  = 0.
        spec_outVars%rMaint   = 0.
        spec_outVars%rGrowth  = 0.
        spec_outVars%nbp      = 0.
        spec_outVars%nsc      = 0.
        ! Carbon Pools  (KgC m-2)
        spec_outVars%cLeaf    = 0.
        spec_outVars%cStem    = 0.
        spec_outVars%cRoot    = 0.
        ! Nitrogen pools (kgN m-2)
        spec_outVars%nLeaf    = 0.
        spec_outVars%nStem    = 0.
        spec_outVars%nRoot    = 0.
        spec_outVars%nsn      = 0.
        ! water fluxes (kg m-2 s-1)
        spec_outVars%tran     = 0.
        ! other
        spec_outVars%lai      = 0. 
        ! out litter
        spec_outVars%outC_L   = 0.
        spec_outVars%outC_W   = 0.
        spec_outVars%outC_R   = 0.
        return
    end subroutine init_spec_outVars

    subroutine get_forcingdata()
        implicit none
        integer STAT, COUNT
        character(150) commts
        ! define variable for each line
        integer :: tmp_yr, tmp_doy, tmp_h
        real(8) :: tmp_Ta, tmp_Ts,  tmp_rh, tmp_vpd, tmp_rain, tmp_ws 
        real(8) :: tmp_par, tmp_co2, tmp_pbot, tmp_ndep
        real(8) :: tmp_tsoil_0, tmp_tsoil_10, tmp_tsoil_20, tmp_tsoil_30, tmp_tsoil_40, tmp_tsoil_50
        real(8) :: tmp_tsoil_70, tmp_tsoil_90, tmp_tsoil_110, tmp_tsoil_130, tmp_tsoil_150, tmp_zwt 

        call ReadLineNumFromFile(climfile, nforcing)  ! get the line number

        allocate(forcing(nforcing))                   ! allocate the array

        COUNT = 0
        OPEN(1,FILE=climfile,status='old',ACTION='read',IOSTAT=STAT)
        read(1,'(a160)') commts
        DO WHILE (.TRUE.)
            COUNT=COUNT+1
            READ(1,*,IOSTAT=STAT, end=993) tmp_yr, tmp_doy, tmp_h,   &
                tmp_Ta,  tmp_Ts,  tmp_rh, tmp_vpd, tmp_rain, tmp_ws, & 
                tmp_par, tmp_co2, tmp_pbot, tmp_ndep,                &
                tmp_tsoil_0, tmp_tsoil_10, tmp_tsoil_20, tmp_tsoil_30, tmp_tsoil_40, tmp_tsoil_50, &
                tmp_tsoil_70, tmp_tsoil_90, tmp_tsoil_110, tmp_tsoil_130, tmp_tsoil_150, tmp_zwt

            IF(STAT .NE. 0) EXIT
            forcing(COUNT)%year  = tmp_yr
            forcing(COUNT)%doy   = tmp_doy
            forcing(COUNT)%hour  = tmp_h
            forcing(COUNT)%Tair  = tmp_Ta
            forcing(COUNT)%Tsoil = tmp_Ts
            forcing(COUNT)%RH    = tmp_rh
            forcing(COUNT)%VPD   = tmp_vpd
            forcing(COUNT)%Rain  = tmp_rain
            forcing(COUNT)%WS    = tmp_ws
            forcing(COUNT)%PAR   = tmp_par
            forcing(COUNT)%CO2   = tmp_co2
            forcing(COUNT)%PBOT  = tmp_pbot
            forcing(COUNT)%Ndep  = tmp_ndep
            forcing(COUNT)%Tsoil_lr(1) = tmp_tsoil_0
            forcing(COUNT)%Tsoil_lr(2) = tmp_tsoil_10 
            forcing(COUNT)%Tsoil_lr(3) = tmp_tsoil_20 
            forcing(COUNT)%Tsoil_lr(4) = tmp_tsoil_30 
            forcing(COUNT)%Tsoil_lr(5) = tmp_tsoil_40 
            forcing(COUNT)%Tsoil_lr(6) = tmp_tsoil_50
            forcing(COUNT)%Tsoil_lr(7) = tmp_tsoil_70 
            forcing(COUNT)%Tsoil_lr(8) = tmp_tsoil_90 
            forcing(COUNT)%Tsoil_lr(9) = tmp_tsoil_110 
            forcing(COUNT)%Tsoil_lr(10) = tmp_tsoil_130 
            forcing(COUNT)%Tsoil_lr(11) = tmp_tsoil_150
            forcing(COUNT)%zwt   = tmp_zwt*1000 ! m to mm
        ENDDO
993     continue
        CLOSE(1)
    end subroutine get_forcingdata

    subroutine get_snowdepth()
        implicit none
        ! real(8) temp_snow_depth(max_nlines)
        integer STAT, COUNT, nrow
        character(50) commts

        ! integer m,n,istat1,lines,yr_length
        real(8) snow_depth_read
        integer tmp_yr, tmp_doy, tmp_hr

        call ReadLineNumFromFile(snowdepthfile, nrow)  ! get the line number
        allocate(snow_in(nrow))

        open(11,file = snowdepthfile, status ='old',ACTION='read', IOSTAT=STAT)
        read(11,'(a160)') commts ! skip 2 lines of input met data file
        COUNT = 0
        do
            COUNT = COUNT + 1
            read (11,*,IOSTAT=STAT, end=1018) tmp_yr,tmp_doy,tmp_hr,snow_depth_read
            IF(STAT .NE. 0) EXIT
            snow_in(COUNT)=snow_depth_read     
        enddo
1018    continue
        close(11)    ! close snow depth file
        return
    end subroutine get_snowdepth

    subroutine ReadLineNumFromFile(filepath, count_lines)
        implicit none
        character(len=*), intent(in) :: filepath
        character(len=100) header, line
        integer STAT, count_lines
        print*, "Reading file path: ", trim(filepath)
        open(38, file=trim(filepath), status="old", action="read", iostat=STAT) ! open file
        read(38, '(a100)') header           ! read the header of the file
        count_lines = 0                     ! initilize the count_lines
        do while(.TRUE.)
            read(38, *, iostat=STAT) line   ! read each line
            if(STAT .ne. 0) exit            ! until the end of the file
            count_lines = count_lines + 1   ! recording the count of the lines
        enddo
        close(38)
        return
    end subroutine ReadLineNumFromFile 

    subroutine deallocate_results(outVars, in_npft)
        implicit none
        type(outvars_data_type), intent(inout) :: outVars
        integer :: ipft, in_npft
        do ipft = 1, in_npft
            if(allocated(outVars%sp)) deallocate(outVars%sp)
        enddo
    end subroutine deallocate_results

    subroutine deallocate_date_type()
        if (allocated(forcing)) deallocate(forcing)
        if (allocated(snow_in)) deallocate(snow_in)
    end subroutine deallocate_date_type
end module datatypes