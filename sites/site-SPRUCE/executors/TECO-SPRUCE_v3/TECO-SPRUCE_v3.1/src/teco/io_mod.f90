!#define USE_NETCDF

module io_mod
    use datatypes
#ifdef USE_NETCDF
        use netcdf
#endif
    implicit none
    CHARACTER(len=4) :: str_startyr, str_endyr
    real(8) convert_g2kg, convert_h2s
    
    contains

    subroutine read_nml_params_initValues(in_paramsNmlFile)
        implicit none
        integer io, ipft
        character(*), intent(in) :: in_paramsNmlFile

        !!! site-based parameters
        real(8) :: lat, lon,  wsmax,  wsmin,  extkU
        real(8) :: Tau_F, Tau_C, Tau_Micro, Tau_SlowSOM, Tau_Passive
        real(8) :: Q10rh, etaW, f_F2M, f_C2M, f_C2S, f_M2S, f_M2P, f_S2P, f_S2M, f_P2M  
        real(8) :: r_me, Q10pro, kCH4, Omax, CH4_thre  
        real(8) :: Tveg, Tpro_me, Toxi, f, bubprob, Vmaxfraction, pox 
        real(8) :: f_lit_m, s_lit
        real(8) :: f_fast, f_slow, s_soil      ! the proportion of soil slow pool in total soil pool
        real(8) :: par_shcap_snow
        real(8) :: par_condu_snow
        real(8) :: par_condu_b
        ! real(8) :: par_albedo_snow
        real(8) :: par_fsub
        real(8) :: par_rho_snow
        real(8) :: par_decay_m
        ! add for annual different parameters
        real(8) :: Q10rh_1, Q10rh_2, Q10rh_3, Q10rh_4, Q10rh_5, Q10rh_6, Q10rh_7, Q10rh_8
        real(8) :: Q10pro_1, Q10pro_2, Q10pro_3, Q10pro_4, Q10pro_5, Q10pro_6, Q10pro_7, Q10pro_8
        real(8) :: Tpro_me_1, Tpro_me_2, Tpro_me_3, Tpro_me_4, Tpro_me_5, Tpro_me_6, Tpro_me_7, Tpro_me_8
        real(8) :: Tref_rh_1, Tref_rh_2, Tref_rh_3, Tref_rh_4, Tref_rh_5, Tref_rh_6, Tref_rh_7, Tref_rh_8
        !!! species-based parameters
        integer :: count_pft, stom_n(max_npft)
        character(20) :: spec_names(max_npft)
        real(8) :: pft_weight(max_npft), LAImax(max_npft), LAImin(max_npft) 
        real(8) :: SapS(max_npft),     SapR(max_npft),     SLAx(max_npft)
        real(8) :: GLmax(max_npft),    GRmax(max_npft),    Gsmax(max_npft)
        real(8) :: alpha(max_npft),    Vcmax0(max_npft),   Ds0(max_npft)
        real(8) :: xfang(max_npft),    rdepth(max_npft),   Rootmax(max_npft),  Stemmax(max_npft)
        real(8) :: Tau_Leaf(max_npft), Tau_Stem(max_npft), Tau_Root(max_npft)
        real(8) :: Q10(max_npft),      Rl0(max_npft),      Rs0(max_npft),      Rr0(max_npft)
        real(8) :: JV(max_npft),       Entrpy(max_npft),   gddonset(max_npft) 
        real(8) :: hmax(max_npft),     hl0(max_npft),      LAIMAX0(max_npft),  la0(max_npft)
        real(8) :: fn2l(max_npft),       fn2r(max_npft)
        real(8) :: s_cLeaf(max_npft), s_cStem(max_npft),   s_cRoot(max_npft),  s_nsc(max_npft), s_nsn(max_npft)        ! scale the initial leaf C pool
        real(8) :: f_rg(max_npft), s_vea(max_npft), s_ved(max_npft), s_jea(max_npft), s_jed(max_npft)
        real(8) :: s_vea_1(max_npft), s_vea_2(max_npft), s_vea_3(max_npft), s_vea_4(max_npft)
        real(8) :: s_vea_5(max_npft), s_vea_6(max_npft), s_vea_7(max_npft), s_vea_8(max_npft)
        real(8) :: s_ved_1(max_npft), s_ved_2(max_npft), s_ved_3(max_npft), s_ved_4(max_npft)
        real(8) :: s_ved_5(max_npft), s_ved_6(max_npft), s_ved_7(max_npft), s_ved_8(max_npft)
        real(8) :: Entrpy_1(max_npft), Entrpy_2(max_npft), Entrpy_3(max_npft), Entrpy_4(max_npft)
        real(8) :: Entrpy_5(max_npft), Entrpy_6(max_npft), Entrpy_7(max_npft), Entrpy_8(max_npft)
        real(8) :: Vcmax0_1(max_npft), Vcmax0_2(max_npft), Vcmax0_3(max_npft), Vcmax0_4(max_npft)
        real(8) :: Vcmax0_5(max_npft), Vcmax0_6(max_npft), Vcmax0_7(max_npft), Vcmax0_8(max_npft)
        real(8) :: Q10_1(max_npft), Q10_2(max_npft), Q10_3(max_npft), Q10_4(max_npft)
        real(8) :: Q10_5(max_npft), Q10_6(max_npft), Q10_7(max_npft), Q10_8(max_npft)
        real(8) :: f_rg_1(max_npft), f_rg_2(max_npft), f_rg_3(max_npft), f_rg_4(max_npft)
        real(8) :: f_rg_5(max_npft), f_rg_6(max_npft), f_rg_7(max_npft), f_rg_8(max_npft)
        real(8) :: fn2r_1(max_npft), fn2r_2(max_npft), fn2r_3(max_npft), fn2r_4(max_npft)
        real(8) :: fn2r_5(max_npft), fn2r_6(max_npft), fn2r_7(max_npft), fn2r_8(max_npft)
        real(8) :: s_npp_1(max_npft), s_npp_2(max_npft), s_npp_3(max_npft), s_npp_4(max_npft)
        real(8) :: s_npp_5(max_npft), s_npp_6(max_npft), s_npp_7(max_npft), s_npp_8(max_npft), s_npp_9(max_npft)
        real(8) :: fn2l_1(max_npft), fn2l_2(max_npft), fn2l_3(max_npft), fn2l_4(max_npft)
        real(8) :: fn2l_5(max_npft), fn2l_6(max_npft), fn2l_7(max_npft), fn2l_8(max_npft)
        !!! site-based initial values
        real(8) :: cLit_m, cLit_s, cSoil_f, cSoil_s, cSoil_p
        real(8) :: CN0_lit_m, CN0_lit_s, CN0_soil_f, CN0_soil_s, CN0_soil_p
        real(8) :: N_deposit, QNminer, thksl(nlayers), FRLEN(nlayers)
        real(8) :: liq_water(nlayers), fwsoil, topfws, omega
        real(8) :: zwt, infilt, sftmp, Tsnow, Twater, Tice
        real(8) :: G, snow_dsim, dcount, dcount_soil, ice_tw
        real(8) :: Tsoill(nlayers), ice(nlayers), shcap_snow, condu_snow, condu_b
        real(8) :: depth_ex, diff_s, diff_snow, albedo_snow
        real(8) :: resht, thd_snow_depth, b_bound, infilt_rate
        real(8) :: fa, fsub, rho_snow, decay_m
        real(8) :: CH4_V(nlayers), CH4(nlayers), Vp(nlayers)
        real(8) :: bubble_methane_tot, Nbub, depth_1
        !!! species-based initial values
        real(8) :: cLeaf(max_npft), cStem(max_npft), cRoot(max_npft)
        real(8) :: CN0_L(max_npft), CN0_S(max_npft), CN0_R(max_npft)
        real(8) :: NSCmin(max_npft), Storage(max_npft), nsc(max_npft), nsn(max_npft)
        real(8) :: accumulation(max_npft), SNvcmax(max_npft), N_deficit(max_npft), alphaN(max_npft)
        real(8) :: FRLEN_1(max_npft), FRLEN_2(max_npft), FRLEN_3(max_npft), FRLEN_4(max_npft)
        real(8) :: FRLEN_5(max_npft), FRLEN_6(max_npft), FRLEN_7(max_npft), FRLEN_8(max_npft)
        real(8) :: FRLEN_9(max_npft), FRLEN_10(max_npft)
        ! namelist
        ! local variables 
        real(8) :: all_pft_weight
        namelist /nml_site_params/ lat, lon,  wsmax,  wsmin,  extkU, &
            Tau_F, Tau_C, Tau_Micro, Tau_SlowSOM, Tau_Passive, &
            Q10rh, etaW, f_F2M, f_C2M, f_C2S, f_M2S, f_M2P, f_S2P, f_S2M, f_P2M, &  
            r_me, Q10pro, kCH4, Omax, CH4_thre, &  
            Tveg, Tpro_me, Toxi, f, bubprob, Vmaxfraction, &
            f_lit_m, s_lit, &
            f_fast, f_slow, s_soil, &
            par_shcap_snow, par_condu_snow, par_condu_b, & !par_albedo_snow, &
            par_fsub, par_rho_snow, par_decay_m, pox, &
            Q10rh_1, Q10rh_2, Q10rh_3, Q10rh_4, Q10rh_5, Q10rh_6, Q10rh_7, Q10rh_8, &
            Q10pro_1, Q10pro_2, Q10pro_3, Q10pro_4, Q10pro_5, Q10pro_6, Q10pro_7, Q10pro_8, &
            Tpro_me_1, Tpro_me_2, Tpro_me_3, Tpro_me_4, Tpro_me_5, Tpro_me_6, Tpro_me_7, Tpro_me_8, &
            Tref_rh_1, Tref_rh_2, Tref_rh_3, Tref_rh_4, Tref_rh_5, Tref_rh_6, Tref_rh_7, Tref_rh_8

        namelist /nml_species_params/ count_pft, stom_n, &
            spec_names, &
            pft_weight, LAImax, LAImin, & 
            SapS,     SapR,     SLAx, &
            GLmax,    GRmax,    Gsmax, &
            alpha,    Vcmax0,   Ds0, &
            xfang,    rdepth,   Rootmax,  Stemmax, &
            Tau_Leaf, Tau_Stem, Tau_Root, &
            Q10,      Rl0,      Rs0,      Rr0, &
            JV,       Entrpy,   gddonset, & 
            hmax,    hl0,      LAIMAX0,  la0, fn2l, fn2r, &
            s_cLeaf, s_cStem,   s_cRoot,  s_nsc, s_nsn, f_rg, s_vea, s_ved, s_jea, s_jed, &
            s_vea_1, s_vea_2, s_vea_3, s_vea_4, s_vea_5, s_vea_6, s_vea_7, s_vea_8, &
            s_ved_1, s_ved_2, s_ved_3, s_ved_4, s_ved_5, s_ved_6, s_ved_7, s_ved_8, &
            Entrpy_1, Entrpy_2, Entrpy_3, Entrpy_4, Entrpy_5, Entrpy_6, Entrpy_7, Entrpy_8, &
            Vcmax0_1, Vcmax0_2, Vcmax0_3, Vcmax0_4, Vcmax0_5, Vcmax0_6, Vcmax0_7, Vcmax0_8, &
            Q10_1,   Q10_2,   Q10_3,   Q10_4,   Q10_5,   Q10_6,   Q10_7,   Q10_8, &
            f_rg_1,  f_rg_2,  f_rg_3,  f_rg_4,  f_rg_5,  f_rg_6,  f_rg_7,  f_rg_8, &
            fn2r_1,  fn2r_2,  fn2r_3,  fn2r_4,  fn2r_5,  fn2r_6,  fn2r_7,  fn2r_8, &
            s_npp_1, s_npp_2, s_npp_3, s_npp_4, s_npp_5, s_npp_6, s_npp_7, s_npp_8, s_npp_9, &
            fn2l_1, fn2l_2, fn2l_3, fn2l_4, fn2l_5, fn2l_6, fn2l_7, fn2l_8

        namelist /nml_site_initial_values/ cLit_m, cLit_s, cSoil_f, cSoil_s, cSoil_p, &
            CN0_lit_m, CN0_lit_s, CN0_soil_f, CN0_soil_s, CN0_soil_p, &
            N_deposit, QNminer, thksl, FRLEN, &
            liq_water, fwsoil, topfws, omega, &
            zwt, infilt, sftmp, Tsnow, Twater, Tice, &
            G, snow_dsim, dcount, dcount_soil, ice_tw, &
            Tsoill, ice, shcap_snow, condu_snow, condu_b, &
            depth_ex, diff_s, diff_snow, albedo_snow, &
            resht, thd_snow_depth, b_bound, infilt_rate, &
            fa, fsub, rho_snow, decay_m, &
            CH4_V, CH4, Vp, bubble_methane_tot, Nbub, depth_1

        namelist /nml_species_initial_values/cLeaf, cStem, cRoot, &
            CN0_L, CN0_S, CN0_R, NSCmin, Storage, nsc, nsn, &
            accumulation, SNvcmax, N_deficit, alphaN, &
            FRLEN_1, FRLEN_2, FRLEN_3, FRLEN_4, FRLEN_5, &
            FRLEN_6, FRLEN_7, FRLEN_8, FRLEN_9, FRLEN_10
        ! -----------------------------------------------------------------------------------
        print *, "# read parameters nml file: ", adjustl(trim(in_paramsNmlFile))
        open(343, file = adjustl(trim(in_paramsNmlFile)))
        read(343, nml  = nml_site_params,            iostat=io)
        read(343, nml  = nml_species_params,         iostat=io)
        read(343, nml  = nml_site_initial_values,    iostat=io)
        read(343, nml  = nml_species_initial_values, iostat=io)
        close(343)
        
        !------------------------------------------------------
        npft = count_pft
        allocate(in_params_vals%sp_init_val(count_pft))
        allocate(in_params_vals%sp_params(count_pft))
        in_params_vals%st_params%lat          = lat
        in_params_vals%st_params%lon          = lon
        in_params_vals%st_params%wsmax        = wsmax
        in_params_vals%st_params%wsmin        = wsmin
        in_params_vals%st_params%extkU        = extkU
        in_params_vals%st_params%Tau_F        = Tau_F
        in_params_vals%st_params%Tau_C        = Tau_C
        in_params_vals%st_params%Tau_Micro    = Tau_Micro
        in_params_vals%st_params%Tau_SlowSOM  = Tau_SlowSOM
        in_params_vals%st_params%Tau_Passive  = Tau_Passive
        in_params_vals%st_params%Q10rh        = Q10rh
        in_params_vals%st_params%etaW         = etaW
        in_params_vals%st_params%f_F2M        = f_F2M
        in_params_vals%st_params%f_C2M        = f_C2M
        in_params_vals%st_params%f_C2S        = f_C2S
        in_params_vals%st_params%f_M2S        = f_M2S
        in_params_vals%st_params%f_M2P        = f_M2P
        in_params_vals%st_params%f_S2P        = f_S2P
        in_params_vals%st_params%f_S2M        = f_S2M
        in_params_vals%st_params%f_P2M        = f_P2M
        in_params_vals%st_params%r_me         = r_me
        in_params_vals%st_params%Q10pro       = Q10pro
        in_params_vals%st_params%kCH4         = kCH4
        in_params_vals%st_params%Omax         = Omax
        in_params_vals%st_params%CH4_thre     = CH4_thre
        in_params_vals%st_params%Tveg         = Tveg
        in_params_vals%st_params%Tpro_me      = Tpro_me
        in_params_vals%st_params%Toxi         = Toxi
        in_params_vals%st_params%f            = f
        in_params_vals%st_params%bubprob      = bubprob
        in_params_vals%st_params%Vmaxfraction = Vmaxfraction
        in_params_vals%st_params%pox          = pox
        in_params_vals%st_params%f_lit_m      = f_lit_m
        in_params_vals%st_params%s_lit        = s_lit
        in_params_vals%st_params%f_fast       = f_fast
        in_params_vals%st_params%f_slow       = f_slow
        in_params_vals%st_params%s_soil       = s_soil
        in_params_vals%st_params%par_shcap_snow   = par_shcap_snow
        in_params_vals%st_params%par_condu_snow   = par_condu_snow
        in_params_vals%st_params%par_condu_b      = par_condu_b
        ! in_params_vals%st_params%par_albedo_snow  = albedo_snow
        in_params_vals%st_params%par_fsub         = par_fsub
        in_params_vals%st_params%par_rho_snow     = par_rho_snow
        in_params_vals%st_params%par_decay_m      = par_decay_m
        ! add for annual different parameters
        in_params_vals%st_params%Q10rh_1        = Q10rh_1
        in_params_vals%st_params%Q10rh_2        = Q10rh_2
        in_params_vals%st_params%Q10rh_3        = Q10rh_3
        in_params_vals%st_params%Q10rh_4        = Q10rh_4
        in_params_vals%st_params%Q10rh_5        = Q10rh_5
        in_params_vals%st_params%Q10rh_6        = Q10rh_6
        in_params_vals%st_params%Q10rh_7        = Q10rh_7
        in_params_vals%st_params%Q10rh_8        = Q10rh_8
        in_params_vals%st_params%Q10pro_1       = Q10pro_1
        in_params_vals%st_params%Q10pro_2       = Q10pro_2
        in_params_vals%st_params%Q10pro_3       = Q10pro_3
        in_params_vals%st_params%Q10pro_4       = Q10pro_4
        in_params_vals%st_params%Q10pro_5       = Q10pro_5
        in_params_vals%st_params%Q10pro_6       = Q10pro_6
        in_params_vals%st_params%Q10pro_7       = Q10pro_7
        in_params_vals%st_params%Q10pro_8       = Q10pro_8
        in_params_vals%st_params%Tpro_me_1      = Tpro_me_1
        in_params_vals%st_params%Tpro_me_2      = Tpro_me_2
        in_params_vals%st_params%Tpro_me_3      = Tpro_me_3
        in_params_vals%st_params%Tpro_me_4      = Tpro_me_4
        in_params_vals%st_params%Tpro_me_5      = Tpro_me_5
        in_params_vals%st_params%Tpro_me_6      = Tpro_me_6
        in_params_vals%st_params%Tpro_me_7      = Tpro_me_7
        in_params_vals%st_params%Tpro_me_8      = Tpro_me_8
        in_params_vals%st_params%Tref_rh_1      = Tref_rh_1
        in_params_vals%st_params%Tref_rh_2      = Tref_rh_2
        in_params_vals%st_params%Tref_rh_3      = Tref_rh_3
        in_params_vals%st_params%Tref_rh_4      = Tref_rh_4
        in_params_vals%st_params%Tref_rh_5      = Tref_rh_5
        in_params_vals%st_params%Tref_rh_6      = Tref_rh_6
        in_params_vals%st_params%Tref_rh_7      = Tref_rh_7
        in_params_vals%st_params%Tref_rh_8      = Tref_rh_8
        ! ---------------------------------------------------
        in_params_vals%st_init_val%cLit_m             = cLit_m
        in_params_vals%st_init_val%cLit_s             = cLit_s
        in_params_vals%st_init_val%cSoil_f            = cSoil_f
        in_params_vals%st_init_val%cSoil_s            = cSoil_s
        in_params_vals%st_init_val%cSoil_p            = cSoil_p
        in_params_vals%st_init_val%CN0_lit_m          = CN0_lit_m
        in_params_vals%st_init_val%CN0_lit_s          = CN0_lit_s
        in_params_vals%st_init_val%CN0_soil_f         = CN0_soil_f
        in_params_vals%st_init_val%CN0_soil_s         = CN0_soil_s
        in_params_vals%st_init_val%CN0_soil_p         = CN0_soil_p
        in_params_vals%st_init_val%N_deposit          = N_deposit
        in_params_vals%st_init_val%QNminer            = QNminer
        in_params_vals%st_init_val%thksl              = thksl
        in_params_vals%st_init_val%liq_water          = liq_water
        in_params_vals%st_init_val%fwsoil             = fwsoil
        in_params_vals%st_init_val%topfws             = topfws
        in_params_vals%st_init_val%omega              = omega
        in_params_vals%st_init_val%zwt                = zwt
        in_params_vals%st_init_val%infilt             = infilt
        in_params_vals%st_init_val%sftmp              = sftmp
        in_params_vals%st_init_val%Tsnow              = Tsnow
        in_params_vals%st_init_val%Twater             = Twater
        in_params_vals%st_init_val%Tice               = Tice
        in_params_vals%st_init_val%G                  = G
        
        in_params_vals%st_init_val%snow_dsim          = snow_dsim
        in_params_vals%st_init_val%dcount             = dcount
        in_params_vals%st_init_val%dcount_soil        = dcount_soil
        in_params_vals%st_init_val%ice_tw             = ice_tw
        in_params_vals%st_init_val%Tsoill             = Tsoill
        in_params_vals%st_init_val%ice                = ice
        in_params_vals%st_init_val%shcap_snow         = shcap_snow
        in_params_vals%st_init_val%condu_snow         = condu_snow
        in_params_vals%st_init_val%condu_b            = condu_b
        in_params_vals%st_init_val%depth_ex           = depth_ex
        in_params_vals%st_init_val%diff_s             = diff_s
        in_params_vals%st_init_val%diff_snow          = diff_snow
        in_params_vals%st_init_val%albedo_snow        = albedo_snow
        in_params_vals%st_init_val%resht              = resht
        in_params_vals%st_init_val%thd_snow_depth     = thd_snow_depth
        in_params_vals%st_init_val%b_bound            = b_bound
        in_params_vals%st_init_val%infilt_rate        = infilt_rate
        in_params_vals%st_init_val%fa                 = fa
        in_params_vals%st_init_val%fsub               = fsub
        in_params_vals%st_init_val%rho_snow           = rho_snow
        in_params_vals%st_init_val%decay_m            = decay_m
        in_params_vals%st_init_val%CH4_V              = CH4_V
        in_params_vals%st_init_val%CH4                = CH4
        in_params_vals%st_init_val%Vp                 = Vp
        in_params_vals%st_init_val%bubble_methane_tot = bubble_methane_tot
        in_params_vals%st_init_val%Nbub               = Nbub
        in_params_vals%st_init_val%depth_1            = depth_1
        ! ------------------------------------
        all_pft_weight = sum(pft_weight(1:count_pft))
        allocate(sp_names(npft))
        do ipft = 1, npft
            sp_names(ipft)      = spec_names(ipft)
            in_params_vals%sp_params(ipft)%spec_name   = spec_names(ipft)
            in_params_vals%sp_params(ipft)%pft_weight  = pft_weight(ipft)/all_pft_weight
            ! species parameter sets
            in_params_vals%sp_params(ipft)%LAImax      = LAImax(ipft)
            in_params_vals%sp_params(ipft)%LAImin      = LAImin(ipft)
            in_params_vals%sp_params(ipft)%stom_n      = stom_n(ipft)
            in_params_vals%sp_params(ipft)%SapS        = SapS(ipft)
            in_params_vals%sp_params(ipft)%SapR        = SapR(ipft)
            in_params_vals%sp_params(ipft)%SLA         = SLAx(ipft)
            in_params_vals%sp_params(ipft)%GLmax       = GLmax(ipft)
            in_params_vals%sp_params(ipft)%GRmax       = GRmax(ipft)
            in_params_vals%sp_params(ipft)%Gsmax       = Gsmax(ipft)
            in_params_vals%sp_params(ipft)%alpha       = alpha(ipft)
            in_params_vals%sp_params(ipft)%Vcmax0      = Vcmax0(ipft)
            in_params_vals%sp_params(ipft)%Ds0         = Ds0(ipft)
            in_params_vals%sp_params(ipft)%xfang       = xfang(ipft)
            in_params_vals%sp_params(ipft)%rdepth      = rdepth(ipft)
            in_params_vals%sp_params(ipft)%Rootmax     = Rootmax(ipft)
            in_params_vals%sp_params(ipft)%Stemmax     = Stemmax(ipft)
            in_params_vals%sp_params(ipft)%Tau_Leaf    = Tau_Leaf(ipft)
            in_params_vals%sp_params(ipft)%Tau_Stem    = Tau_Stem(ipft)
            in_params_vals%sp_params(ipft)%Tau_Root    = Tau_Root(ipft)
            in_params_vals%sp_params(ipft)%Q10         = Q10(ipft)
            in_params_vals%sp_params(ipft)%Rl0         = Rl0(ipft) 
            in_params_vals%sp_params(ipft)%Rs0         = Rs0(ipft)
            in_params_vals%sp_params(ipft)%Rr0         = Rr0(ipft)
            in_params_vals%sp_params(ipft)%f_rg        = f_rg(ipft)
            in_params_vals%sp_params(ipft)%s_vea       = s_vea(ipft)
            in_params_vals%sp_params(ipft)%s_ved       = s_ved(ipft)
            in_params_vals%sp_params(ipft)%s_jea       = s_jea(ipft)
            in_params_vals%sp_params(ipft)%s_jed       = s_jed(ipft)
            in_params_vals%sp_params(ipft)%s_vea_1     = s_vea_1(ipft)
            in_params_vals%sp_params(ipft)%s_vea_2     = s_vea_2(ipft)
            in_params_vals%sp_params(ipft)%s_vea_3     = s_vea_3(ipft)
            in_params_vals%sp_params(ipft)%s_vea_4     = s_vea_4(ipft)
            in_params_vals%sp_params(ipft)%s_vea_5     = s_vea_5(ipft)
            in_params_vals%sp_params(ipft)%s_vea_6     = s_vea_6(ipft)
            in_params_vals%sp_params(ipft)%s_vea_7     = s_vea_7(ipft)
            in_params_vals%sp_params(ipft)%s_vea_8     = s_vea_8(ipft)
            in_params_vals%sp_params(ipft)%s_ved_1     = s_ved_1(ipft)
            in_params_vals%sp_params(ipft)%s_ved_2     = s_ved_2(ipft)
            in_params_vals%sp_params(ipft)%s_ved_3     = s_ved_3(ipft)
            in_params_vals%sp_params(ipft)%s_ved_4     = s_ved_4(ipft)
            in_params_vals%sp_params(ipft)%s_ved_5     = s_ved_5(ipft)
            in_params_vals%sp_params(ipft)%s_ved_6     = s_ved_6(ipft)
            in_params_vals%sp_params(ipft)%s_ved_7     = s_ved_7(ipft)
            in_params_vals%sp_params(ipft)%s_ved_8     = s_ved_8(ipft)
            in_params_vals%sp_params(ipft)%Entrpy_1    = Entrpy_1(ipft)
            in_params_vals%sp_params(ipft)%Entrpy_2    = Entrpy_2(ipft)
            in_params_vals%sp_params(ipft)%Entrpy_3    = Entrpy_3(ipft)
            in_params_vals%sp_params(ipft)%Entrpy_4    = Entrpy_4(ipft)
            in_params_vals%sp_params(ipft)%Entrpy_5    = Entrpy_5(ipft)
            in_params_vals%sp_params(ipft)%Entrpy_6    = Entrpy_6(ipft)
            in_params_vals%sp_params(ipft)%Entrpy_7    = Entrpy_7(ipft)
            in_params_vals%sp_params(ipft)%Entrpy_8    = Entrpy_8(ipft)
            in_params_vals%sp_params(ipft)%Vcmax0_1   = Vcmax0_1(ipft)
            in_params_vals%sp_params(ipft)%Vcmax0_2   = Vcmax0_2(ipft)
            in_params_vals%sp_params(ipft)%Vcmax0_3   = Vcmax0_3(ipft)
            in_params_vals%sp_params(ipft)%Vcmax0_4   = Vcmax0_4(ipft)
            in_params_vals%sp_params(ipft)%Vcmax0_5   = Vcmax0_5(ipft)
            in_params_vals%sp_params(ipft)%Vcmax0_6   = Vcmax0_6(ipft)
            in_params_vals%sp_params(ipft)%Vcmax0_7   = Vcmax0_7(ipft)
            in_params_vals%sp_params(ipft)%Vcmax0_8   = Vcmax0_8(ipft)
            in_params_vals%sp_params(ipft)%Q10_1      = Q10_1(ipft)
            in_params_vals%sp_params(ipft)%Q10_2      = Q10_2(ipft)
            in_params_vals%sp_params(ipft)%Q10_3      = Q10_3(ipft)
            in_params_vals%sp_params(ipft)%Q10_4      = Q10_4(ipft)
            in_params_vals%sp_params(ipft)%Q10_5      = Q10_5(ipft)
            in_params_vals%sp_params(ipft)%Q10_6      = Q10_6(ipft)
            in_params_vals%sp_params(ipft)%Q10_7      = Q10_7(ipft)
            in_params_vals%sp_params(ipft)%Q10_8      = Q10_8(ipft)
            in_params_vals%sp_params(ipft)%f_rg_1     = f_rg_1(ipft)
            in_params_vals%sp_params(ipft)%f_rg_2     = f_rg_2(ipft)
            in_params_vals%sp_params(ipft)%f_rg_3     = f_rg_3(ipft)
            in_params_vals%sp_params(ipft)%f_rg_4     = f_rg_4(ipft)
            in_params_vals%sp_params(ipft)%f_rg_5     = f_rg_5(ipft)
            in_params_vals%sp_params(ipft)%f_rg_6     = f_rg_6(ipft)
            in_params_vals%sp_params(ipft)%f_rg_7     = f_rg_7(ipft)
            in_params_vals%sp_params(ipft)%f_rg_8     = f_rg_8(ipft)
            in_params_vals%sp_params(ipft)%fn2r_1    = fn2r_1(ipft)
            in_params_vals%sp_params(ipft)%fn2r_2    = fn2r_2(ipft)
            in_params_vals%sp_params(ipft)%fn2r_3    = fn2r_3(ipft)
            in_params_vals%sp_params(ipft)%fn2r_4    = fn2r_4(ipft)
            in_params_vals%sp_params(ipft)%fn2r_5    = fn2r_5(ipft)
            in_params_vals%sp_params(ipft)%fn2r_6    = fn2r_6(ipft)
            in_params_vals%sp_params(ipft)%fn2r_7    = fn2r_7(ipft)
            in_params_vals%sp_params(ipft)%fn2r_8    = fn2r_8(ipft)
            in_params_vals%sp_params(ipft)%s_npp_1   = s_npp_1(ipft)
            in_params_vals%sp_params(ipft)%s_npp_2   = s_npp_2(ipft)
            in_params_vals%sp_params(ipft)%s_npp_3   = s_npp_3(ipft)
            in_params_vals%sp_params(ipft)%s_npp_4   = s_npp_4(ipft)
            in_params_vals%sp_params(ipft)%s_npp_5   = s_npp_5(ipft)
            in_params_vals%sp_params(ipft)%s_npp_6   = s_npp_6(ipft)
            in_params_vals%sp_params(ipft)%s_npp_7   = s_npp_7(ipft)
            in_params_vals%sp_params(ipft)%s_npp_8   = s_npp_8(ipft)
            in_params_vals%sp_params(ipft)%s_npp_9   = s_npp_9(ipft)
            in_params_vals%sp_params(ipft)%fn2l_1    = fn2l_1(ipft)
            in_params_vals%sp_params(ipft)%fn2l_2    = fn2l_2(ipft)
            in_params_vals%sp_params(ipft)%fn2l_3    = fn2l_3(ipft)
            in_params_vals%sp_params(ipft)%fn2l_4    = fn2l_4(ipft)
            in_params_vals%sp_params(ipft)%fn2l_5    = fn2l_5(ipft)
            in_params_vals%sp_params(ipft)%fn2l_6    = fn2l_6(ipft)
            in_params_vals%sp_params(ipft)%fn2l_7    = fn2l_7(ipft)
            in_params_vals%sp_params(ipft)%fn2l_8    = fn2l_8(ipft)
            in_params_vals%sp_params(ipft)%JV          = JV(ipft)
            in_params_vals%sp_params(ipft)%Entrpy      = Entrpy(ipft)
            in_params_vals%sp_params(ipft)%gddonset    = gddonset(ipft)
            ! add in plant growth process
            in_params_vals%sp_params(ipft)%hmax        = hmax(ipft) 
            in_params_vals%sp_params(ipft)%hl0         = hl0(ipft) 
            in_params_vals%sp_params(ipft)%LAIMAX0     = LAIMAX0(ipft) 
            in_params_vals%sp_params(ipft)%la0         = la0(ipft)
            in_params_vals%sp_params(ipft)%fn2l         = fn2l(ipft)
            in_params_vals%sp_params(ipft)%fn2r         = fn2r(ipft)
            ! add to scale the initial values of pools
            in_params_vals%sp_params(ipft)%s_cLeaf      = s_cLeaf(ipft)
            in_params_vals%sp_params(ipft)%s_cStem      = s_cStem(ipft)
            in_params_vals%sp_params(ipft)%s_cRoot      = s_cRoot(ipft)
            in_params_vals%sp_params(ipft)%s_nsc        = s_nsc(ipft)
            in_params_vals%sp_params(ipft)%s_nsn        = s_nsn(ipft)
            ! ----------------------------------------------------
            in_params_vals%sp_init_val(ipft)%cLeaf        = cLeaf(ipft)
            in_params_vals%sp_init_val(ipft)%cStem        = cStem(ipft)
            in_params_vals%sp_init_val(ipft)%cRoot        = cRoot(ipft)
            in_params_vals%sp_init_val(ipft)%CN0_L        = CN0_L(ipft)
            in_params_vals%sp_init_val(ipft)%CN0_S        = CN0_S(ipft)
            in_params_vals%sp_init_val(ipft)%CN0_R        = CN0_R(ipft)
            in_params_vals%sp_init_val(ipft)%NSCmin       = NSCmin(ipft)
            in_params_vals%sp_init_val(ipft)%Storage      = Storage(ipft)
            in_params_vals%sp_init_val(ipft)%nsc          = nsc(ipft)
            in_params_vals%sp_init_val(ipft)%nsn          = nsn(ipft)   
            in_params_vals%sp_init_val(ipft)%accumulation = accumulation(ipft) 
            in_params_vals%sp_init_val(ipft)%SNvcmax      = SNvcmax(ipft)
            in_params_vals%sp_init_val(ipft)%N_deficit    = N_deficit(ipft)
            in_params_vals%sp_init_val(ipft)%alphaN       = alphaN(ipft)
            in_params_vals%sp_init_val(ipft)%FRLEN(1)     = FRLEN_1(ipft)
            in_params_vals%sp_init_val(ipft)%FRLEN(2)     = FRLEN_2(ipft)
            in_params_vals%sp_init_val(ipft)%FRLEN(3)     = FRLEN_3(ipft)
            in_params_vals%sp_init_val(ipft)%FRLEN(4)     = FRLEN_4(ipft)
            in_params_vals%sp_init_val(ipft)%FRLEN(5)     = FRLEN_5(ipft)
            in_params_vals%sp_init_val(ipft)%FRLEN(6)     = FRLEN_6(ipft)
            in_params_vals%sp_init_val(ipft)%FRLEN(7)     = FRLEN_7(ipft)
            in_params_vals%sp_init_val(ipft)%FRLEN(8)     = FRLEN_8(ipft)
            in_params_vals%sp_init_val(ipft)%FRLEN(9)     = FRLEN_9(ipft)
            in_params_vals%sp_init_val(ipft)%FRLEN(10)    = FRLEN_10(ipft)
        enddo
        ! ---------------------------------------------------
        def_Q10rh = Q10rh
        def_Q10pro = Q10pro
        def_s_vea = s_vea(1:3)
        def_Entrpy = Entrpy(1:3)
        def_Vcmax0 = Vcmax0(1:3)
        def_Q10 = Q10(1:3)
        ! ---------------------------------------------------
    end subroutine read_nml_params_initValues

! ===============================================================================
    subroutine updateOutVars(st, outvars, ntime, iyear, iday, ihour)
        implicit none
        integer, intent(in) :: ntime, iyear, iday, ihour
        type(outvars_data_type), intent(inout) :: outVars
        type(site_data_type), intent(in) :: st
        integer :: ipft
        ! integer iTotHourly
        outVars%year = iyear
        outVars%doy  = iday
        outVars%hour = ihour
        ! stop
        convert_g2kg = 1 !0.001
        convert_h2s  = 1!1/3600.
        if (allocated(outvars%sp)) then
            ! npft = size(outvars%sp)
            do ipft = 1, npft
                ! carbon fluxes (Kg C m-2 s-1)
                outvars%sp(ipft)%gpp      = outvars%sp(ipft)%gpp     + &
                                                    st%sp(ipft)%gpp*convert_g2kg*convert_h2s/ntime
                outvars%sp(ipft)%Aleaf(:) = outvars%sp(ipft)%Aleaf(:)     + &
                                                    st%sp(ipft)%Aleaf(:)*convert_g2kg*convert_h2s/ntime *1000000
                outvars%sp(ipft)%npp      = outvars%sp(ipft)%npp     + &
                                                    st%sp(ipft)%npp*convert_g2kg*convert_h2s/ntime
                outvars%sp(ipft)%nppLeaf  = outvars%sp(ipft)%nppLeaf + &
                                                    st%sp(ipft)%NPP_L*convert_g2kg*convert_h2s/ntime
                outvars%sp(ipft)%nppStem  = outvars%sp(ipft)%nppStem + &
                                                    st%sp(ipft)%NPP_W*convert_g2kg*convert_h2s/ntime
                ! outvars%sp(ipft)%nppStem  = outvars%sp(ipft)%nppStem + &
                !                                     st%sp(ipft)%test3*convert_g2kg*convert_h2s/ntime
                outvars%sp(ipft)%nppRoot  = outvars%sp(ipft)%nppRoot + &
                                                    st%sp(ipft)%NPP_R*convert_g2kg*convert_h2s/ntime
                outvars%sp(ipft)%nppOther = outvars%sp(ipft)%nppOther + &
                                                    st%sp(ipft)%add*convert_g2kg*convert_h2s/ntime    ! According to SPRUCE-MIP, stem means above ground Stemy tissues which is different from Stem tissues.
                outvars%sp(ipft)%ra       = outvars%sp(ipft)%ra      + &
                                                    st%sp(ipft)%Rauto*convert_g2kg*convert_h2s/ntime
                outvars%sp(ipft)%raLeaf   = outvars%sp(ipft)%raLeaf  + &
                                                   (st%sp(ipft)%RmLeaf + st%sp(ipft)%RgLeaf)*convert_g2kg*convert_h2s/ntime
                outvars%sp(ipft)%raStem   = outvars%sp(ipft)%raStem  + &
                                                   (st%sp(ipft)%RmStem + st%sp(ipft)%RgStem)*convert_g2kg*convert_h2s/ntime
                outvars%sp(ipft)%raRoot   = outvars%sp(ipft)%raRoot  + &
                                                   (st%sp(ipft)%RmRoot + st%sp(ipft)%RgRoot)*convert_g2kg*convert_h2s/ntime
                outvars%sp(ipft)%rMaint   = outvars%sp(ipft)%rMaint  + &
                                                    st%sp(ipft)%Rmain*convert_g2kg*convert_h2s/ntime
                outvars%sp(ipft)%rGrowth  = outvars%sp(ipft)%rGrowth + &
                                                    st%sp(ipft)%Rgrowth*convert_g2kg*convert_h2s/ntime
                outvars%sp(ipft)%raOther  = outvars%sp(ipft)%raOther + &
                                                    st%sp(ipft)%Rnitrogen*convert_g2kg*convert_h2s/ntime
                outVars%sp(ipft)%nsc      = outVars%sp(ipft)%nsc     + &
                                                    st%sp(ipft)%nsc*convert_g2kg*convert_h2s/ntime
                ! Carbon Pools  (KgC m-2)
                outvars%sp(ipft)%cLeaf    = outvars%sp(ipft)%cLeaf  + &
                                                    st%sp(ipft)%cLeaf*convert_g2kg/ntime
                                                    ! st%sp(ipft)%test8*convert_g2kg/ntime
                outvars%sp(ipft)%cStem    = outvars%sp(ipft)%cStem  + &
                                                    st%sp(ipft)%cStem*convert_g2kg/ntime
                ! st%sp(ipft)%costCfix*convert_g2kg/ntime
                outvars%sp(ipft)%cRoot    = outvars%sp(ipft)%cRoot  + &
                                                    st%sp(ipft)%cRoot*convert_g2kg/ntime
                ! st%sp(ipft)%test9*convert_g2kg/ntime
                ! Nitrogen pools (kgN m-2)
                outvars%sp(ipft)%nLeaf    = outvars%sp(ipft)%nLeaf  + &
                                                    st%sp(ipft)%nLeaf*convert_g2kg/ntime
                ! st%sp(ipft)%test1*convert_g2kg/ntime
                outvars%sp(ipft)%nStem    = outvars%sp(ipft)%nStem  + &
                                                    st%sp(ipft)%nStem*convert_g2kg/ntime
                ! st%sp(ipft)%N_Stem*convert_g2kg/ntime
                outvars%sp(ipft)%nRoot    = outvars%sp(ipft)%nRoot  + &
                                                    st%sp(ipft)%nRoot*convert_g2kg/ntime
                ! st%sp(ipft)%test2*convert_g2kg/ntime
                outVars%sp(ipft)%nsn      = outVars%sp(ipft)%nsn    + &
                                                    st%sp(ipft)%nsn*convert_g2kg/ntime
                ! water fluxes (kg m-2 s-1)
                outvars%sp(ipft)%tran     = outvars%sp(ipft)%tran   + &
                                                    st%sp(ipft)%transp*convert_g2kg*convert_h2s/ntime
                ! other
                outvars%sp(ipft)%lai      = outvars%sp(ipft)%lai    + &
                                                    st%sp(ipft)%LAI/ntime
                ! out litter
                outVars%sp(ipft)%outC_L   = outVars%sp(ipft)%outC_L + &
                                                    st%sp(ipft)%outC_L/ntime
                outVars%sp(ipft)%outC_W   = outVars%sp(ipft)%outC_W + &
                                                    st%sp(ipft)%outC_W/ntime
                outVars%sp(ipft)%outC_R   = outVars%sp(ipft)%outC_R + &
                                                    st%sp(ipft)%outC_R/ntime
            enddo
        endif
        ! carbon fluxes (KgC m-2 s-1) Jian: TECO unit is gC m-2 h-1
        outvars%gpp             = outvars%gpp      + st%gpp*convert_g2kg*convert_h2s/ntime
        outvars%npp             = outvars%npp      + st%npp*convert_g2kg*convert_h2s/ntime
        outvars%nppLeaf         = outvars%nppLeaf  + st%NPP_L*convert_g2kg*convert_h2s/ntime
        outvars%nppStem         = outvars%nppStem  + st%NPP_W*convert_g2kg*convert_h2s/ntime  
        ! outvars%nppStem         = outvars%nppStem  + st%NPP_W*convert_g2kg*convert_h2s/ntime 
        outvars%nppRoot         = outvars%nppRoot  + st%NPP_R*convert_g2kg*convert_h2s/ntime
        outvars%nppOther        = outvars%nppOther + st%NSC*convert_g2kg*convert_h2s/ntime 
        outvars%ra              = outvars%ra       + st%Rauto*convert_g2kg*convert_h2s/ntime
        outvars%raLeaf          = outvars%raLeaf   + (st%Rmleaf+st%Rgleaf)*convert_g2kg*convert_h2s/ntime
        outvars%raStem          = outvars%raStem   + (st%Rmstem+st%Rgstem)*convert_g2kg*convert_h2s/ntime
        outvars%raRoot          = outvars%raRoot   + (st%Rmroot+st%Rgroot)*convert_g2kg*convert_h2s/ntime
        outvars%raOther         = outvars%raOther  + st%Rnitrogen *convert_g2kg*convert_h2s/ntime
        outvars%rMaint          = outvars%rMaint   + st%Rmain *convert_g2kg*convert_h2s/ntime
        outvars%rGrowth         = outvars%rGrowth  + st%Rgrowth *convert_g2kg*convert_h2s/ntime 
        outvars%rh              = outvars%rh       + st%Rhetero *convert_g2kg*convert_h2s/ntime 
        outvars%nbp             = outvars%nbp      + &
                                    (st%gpp - st%Rhetero - st%Rauto) *convert_g2kg*convert_h2s/ntime   
        outvars%wetlandCH4      = outvars%wetlandCH4     + st%simuCH4 *convert_g2kg*convert_h2s/ntime   
        outvars%wetlandCH4prod  = outvars%wetlandCH4prod + st%Pro_sum *convert_g2kg*convert_h2s/ntime 
        outvars%wetlandCH4cons  = outvars%wetlandCH4cons + st%Oxi_sum *convert_g2kg*convert_h2s/ntime 
        ! Carbon Pools  (KgC m-2)
        outvars%cLeaf           = outvars%cLeaf + st%cLeaf*convert_g2kg/ntime
        outvars%cStem           = outvars%cStem + st%cStem*convert_g2kg/ntime
        outvars%cRoot           = outvars%cRoot + st%cRoot*convert_g2kg/ntime
        outvars%cOther          = outvars%cOther+ st%NSC*convert_g2kg/ntime
        outvars%cLitter         = outvars%cLitter + st%cLit_m*convert_g2kg/ntime
        outvars%cLitterCwd      = outvars%cLitterCwd + st%cLit_s*convert_g2kg/ntime
        outvars%cSoil           = outvars%cSoil + (st%cSoil_f + st%cSoil_p + st%cSoil_s)*convert_g2kg/ntime
        outvars%cSoilLevels(:)  = outvars%cSoilLevels(:) + (/0.,0.,0.,0.,0.,0.,0.,0.,0.,0./)/ntime
        outvars%cSoilFast       = outvars%cSoilFast + st%cSoil_f*convert_g2kg/ntime 
        outvars%cSoilSlow       = outvars%cSoilSlow + st%cSoil_s*convert_g2kg/ntime 
        outvars%cSoilPassive    = outvars%cSoilPassive + st%cSoil_p*convert_g2kg/ntime 
        outvars%CH4(:)          = outvars%CH4(:) + st%CH4*convert_g2kg/ntime 
        ! Nitrogen fluxes (kgN m-2 s-1)
        outvars%fBNF            = outvars%fBNF + st%N_fixation*convert_g2kg*convert_h2s/ntime 
        outvars%fN2O            = outvars%fN2O + &
                                    (st%N_transfer+st%N_uptake+st%N_fixation)*convert_g2kg*convert_h2s/ntime
        outvars%fNloss          = outvars%fNloss + &
                                    st%N_loss*convert_g2kg*convert_h2s/ntime
        outvars%fNnetmin        = outvars%fNnetmin + st%fNnetmin*convert_g2kg*convert_h2s/ntime 
        outvars%fNdep           = outvars%fNdep    + st%N_deposit*convert_g2kg*convert_h2s/ntime 
        ! Nitrogen pools (kgN m-2) st%N_miner + st%N_deposit-(st%N_uptake+st%N_immob) - st%N_loss
        outvars%nLeaf           = outvars%nLeaf      + st%nLeaf*convert_g2kg/ntime      ! st%nLeaf*convert_g2kg/ntime
        outvars%nStem           = outvars%nStem      + st%nStem*convert_g2kg/ntime        ! st%nStem*convert_g2kg/ntime
        outvars%nRoot           = outvars%nRoot      + st%nRoot*convert_g2kg/ntime          ! st%nRoot*convert_g2kg/ntime
        outvars%nOther          = outvars%nOther     + st%resdh*convert_g2kg/ntime           
        outvars%nLitter         = outvars%nLitter    + st%nLit_m*convert_g2kg/ntime         ! st%nLit_m*convert_g2kg/ntime
        outvars%nLitterCwd      = outvars%nLitterCwd + st%nLit_s*convert_g2kg/ntime    ! st%nLit_s*convert_g2kg/ntime
        outvars%nSoil           = outvars%nSoil      + (st%nSoil_f+st%nSoil_s+st%nSoil_p)*convert_g2kg/ntime
        outvars%nMineral        = outvars%nMineral   + st%QNminer*convert_g2kg/ntime 
        ! energy fluxes (W m-2)
        outvars%hfls            = outvars%hfls + st%Hsoil/ntime ! Sensible heat flux;
        outvars%hfss            = outvars%hfss + st%Esoil/ntime ! Latent heat flux;
        outvars%SWnet           = outvars%SWnet + st%G/ntime       ! Net shortwave radiation;
        outvars%LWnet           = outvars%LWnet + st%sftmp/ntime       ! Net longwave radiation
        ! water fluxes (kg m-2 s-1)
        outvars%ec              = outvars%ec    + st%test1*convert_g2kg*convert_h2s/ntime        ! Canopy evaporation;
        outvars%tran            = outvars%tran  + st%transp*convert_g2kg*convert_h2s/ntime      ! Canopy transpiration;
        outvars%es              = outvars%es    + st%evap*convert_g2kg*convert_h2s/ntime ! Soil evaporation
        outvars%hfsbl           = outvars%hfsbl + st%sublim*convert_g2kg*convert_h2s/ntime ! Snow sublimation
        outvars%mrro            = outvars%mrro  + st%runoff*convert_g2kg*convert_h2s/ntime
        ! outvars%mrros         = forcing(iforcing)%Rain    
        outvars%mrrob           = outvars%mrrob + (st%zwt/1000)/ntime! 0/ntime ! Total runoff; Surface runoff; Subsurface runoff
        ! other
        outvars%mrso(:)         = outvars%mrso(:) + st%liq_water*1000/ntime  ! Kg m-2, soil moisture in each soil layer
        outvars%tsl(:)          = outvars%tsl(:) + (st%tsoil_layer(2:11)+273.15)/ntime                            ! K, soil temperature in each soil layer Jian: not sure the tsoil_layer is correct or not
        outvars%tsland          = outvars%tsland + st%ice(1)/ntime !forcing(iforcing)%Tair+273.15                                   ! K, surface temperature
        outvars%wtd             = outvars%wtd +  (st%zwt/1000)/ntime                                       ! m, Water table depth
        outvars%snd             = outvars%snd +  (st%snow_depth/100)/ntime                               ! m, Total snow depth, Jian: change from m to cm in code, and now change from cm to m
        outvars%lai             = outvars%lai +  (st%lai)/ntime                                           ! m2 m-2, Leaf area index
        ! out from litter and soil
        outVars%outC_lit_m      = outVars%outC_lit_m  + st%outC_lit_m/ntime
        outVars%outC_lit_s      = outVars%outC_lit_s  + st%outC_lit_s/ntime
        outVars%outC_soil_f     = outVars%outC_soil_f + st%outC_soil_f/ntime
        outVars%outC_soil_s     = outVars%outC_soil_s + st%outC_soil_s/ntime
        outVars%outC_soil_p     = outVars%outC_soil_p + st%outC_soil_p/ntime
        outVars%Rh_lit_m        = outVars%Rh_lit_m    + st%Rh_lit_m/ntime
        outVars%Rh_lit_s        = outVars%Rh_lit_s    + st%Rh_lit_s/ntime
        outVars%Rh_soil_f       = outVars%Rh_soil_f   + st%Rh_soil_f/ntime
        outVars%Rh_soil_s       = outVars%Rh_soil_s   + st%Rh_soil_s/ntime
        outVars%Rh_soil_p       = outVars%Rh_soil_p   + st%Rh_soil_p/ntime
    end subroutine updateOutVars

! ========================================================================================================
!  This part is used to write the outputs to the csv-format file
!       open_file_csv, write_data_csv, close_data_csv
!  ---------------------------------------------------------------
    subroutine def_header(header_csv)
        implicit none
        character(*), intent(inout) :: header_csv
        ! character(100) :: spec_names(npft)
        integer :: ipft

        ! Write header line
        header_csv = "year,doy,hour,"
        do ipft = 1, npft
            header_csv = adjustl(trim(header_csv))//"gpp_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"nee_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"npp_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"nppLeaf_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"nppStem_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"nppStem_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"nppRoot_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"nppOther_"//adjustl(trim(sp_names(ipft)))//","    ! According to SPRUCE-MIP, stem means above ground Stemy tissues which is different from Stem tissues.
            header_csv = adjustl(trim(header_csv))//"ra_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"raLeaf_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"raStem_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"raRoot_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"raOther_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"rMaint_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"rGrowth_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"nsc_"//adjustl(trim(sp_names(ipft)))//","
            ! Carbon Pools  (KgC m-2)
            header_csv = adjustl(trim(header_csv))//"cLeaf_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"cStem_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"cRoot_"//adjustl(trim(sp_names(ipft)))//","
            ! Nitrogen pools (kgN m-2)
            header_csv = adjustl(trim(header_csv))//"nLeaf_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"nStem_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"nRoot_"//adjustl(trim(sp_names(ipft)))//","
            header_csv = adjustl(trim(header_csv))//"nsn_"//adjustl(trim(sp_names(ipft)))//","
            ! water fluxes (kg m-2 s-1)
            header_csv = adjustl(trim(header_csv))//"tran_"//adjustl(trim(sp_names(ipft)))//","
            ! other
            header_csv = adjustl(trim(header_csv))//"lai_"//adjustl(trim(sp_names(ipft)))//"," 
            ! Aleaf
            header_csv = adjustl(trim(header_csv))//"Aleaf_sun_"//adjustl(trim(sp_names(ipft)))//"," 
            header_csv = adjustl(trim(header_csv))//"Aleaf_shd_"//adjustl(trim(sp_names(ipft)))//"," 
            header_csv = adjustl(trim(header_csv))//"Aleaf_sum_"//adjustl(trim(sp_names(ipft)))//"," 
            ! litter
            header_csv = adjustl(trim(header_csv))//"outC_L_"//adjustl(trim(sp_names(ipft)))//"," 
            header_csv = adjustl(trim(header_csv))//"outC_W_"//adjustl(trim(sp_names(ipft)))//"," 
            header_csv = adjustl(trim(header_csv))//"outC_R_"//adjustl(trim(sp_names(ipft)))//"," 
        enddo
        header_csv = adjustl(trim(header_csv))//"gpp,nee,npp,nppLeaf,nppStem,nppStem,nppRoot,nppOther,ra,&
            &raLeaf,raStem,raRoot,raOther,rMaint,rGrowth,rh,nbp,wetlandCH4,wetlandCH4prod,&
            &wetlandCH4cons,cLeaf,cStem,cRoot,cOther,cLitter,cLitterCwd,cSoil,&
            &cSoilLevels_1,cSoilLevels_2,cSoilLevels_3,cSoilLevels_4,cSoilLevels_5,&
            &cSoilLevels_6,cSoilLevels_7,cSoilLevels_8,cSoilLevels_9,cSoilLevels_10,&
            &cSoilFast,cSoilSlow,cSoilPassive,CH4_1,CH4_2,CH4_3,CH4_4,CH4_5,CH4_6,CH4_7,&
            &CH4_8,CH4_9,CH4_10,fBNF,fN2O,fNloss,fNnetmin,fNdep,nLeaf,nStem,nRoot,nOther,&
            &nLitter,nLitterCwd,nSoil,nMineral,hfls,hfss,SWnet,LWnet,ec,tran,es,hfsbl,&
            &mrro,mrros,mrrob,mrso_1,mrso_2,mrso_3,mrso_4,mrso_5,mrso_6,mrso_7,mrso_8,&
            &mrso_9,mrso_10,tsl_1,tsl_2,tsl_3,tsl_4,tsl_5,tsl_6,tsl_7,tsl_8,tsl_9,tsl_10,&
            &tsland,wtd,snd,lai,outC_lit_m,outC_lit_s,outC_soil_f,outC_soil_s,outC_soil_p,&
            &Rh_lit_m,Rh_lit_s,Rh_soil_f,Rh_soil_s,Rh_soil_p"
        return
    end subroutine def_header

    subroutine def_csv_fileName(out_path, str_freq, csv_fileName)
        implicit none
        character(*), intent(in) :: out_path, str_freq
        character(*), intent(inout) :: csv_fileName

        if(do_mcmc)then

            csv_fileName = adjustl(trim(out_path))//"/TECO-SPRUCE_"//adjustl(trim(case_name))//"_"//&
                &str_freq//"_"//adjustl(trim(mc_str_n))//".csv"

! windows
#if defined(WIN32) || defined(WIN64) || defined(_WIN32) || defined(_WIN64)
        csv_fileName = adjustl(trim(out_path))//"\TECO-SPRUCE_"//adjustl(trim(case_name))//"_"//&
                &str_freq//"_"//adjustl(trim(mc_str_n))//".csv"
#endif
        elseif(do_spinup) then
            csv_fileName = adjustl(trim(out_path))//"/TECO-SPRUCE_"//adjustl(trim(case_name))//"_"//&
                &str_freq//"_"//adjustl(trim(mc_str_n))//".csv"
! windows
#if defined(WIN32) || defined(WIN64) || defined(_WIN32) || defined(_WIN64)
        csv_fileName = adjustl(trim(out_path))//"\TECO-SPRUCE_"//adjustl(trim(case_name))//"_"//&
                &str_freq//"_"//adjustl(trim(mc_str_n))//".csv"
#endif
        else
            csv_fileName = adjustl(trim(out_path))//"/TECO-SPRUCE_"//adjustl(trim(case_name))//"_"//str_freq//".csv"
! windows
#if defined(WIN32) || defined(WIN64) || defined(_WIN32) || defined(_WIN64)
        csv_fileName = adjustl(trim(out_path))//"\TECO-SPRUCE_"//adjustl(trim(case_name))//"_"//str_freq//".csv"
#endif
        endif

        if(do_sa)then
            csv_fileName = adjustl(trim(out_path))//"/TECO-SPRUCE_"//adjustl(trim(case_name))//"_"//&
                &str_freq//"_"//adjustl(trim(mc_str_n))//".csv"
! windows
#if defined(WIN32) || defined(WIN64) || defined(_WIN32) || defined(_WIN64)
        csv_fileName = adjustl(trim(out_path))//"\TECO-SPRUCE_"//adjustl(trim(case_name))//"_"//&
                &str_freq//"_"//adjustl(trim(mc_str_n))//".csv"
#endif
        elseif(do_spinup) then
            csv_fileName = adjustl(trim(out_path))//"/TECO-SPRUCE_"//adjustl(trim(case_name))//"_"//&
                &str_freq//"_"//adjustl(trim(mc_str_n))//".csv"
! windows
#if defined(WIN32) || defined(WIN64) || defined(_WIN32) || defined(_WIN64)
        csv_fileName = adjustl(trim(out_path))//"\TECO-SPRUCE_"//adjustl(trim(case_name))//"_"//&
                &str_freq//"_"//adjustl(trim(mc_str_n))//".csv"
#endif
        endif
        return
    end subroutine def_csv_fileName

    subroutine write_data_csv(unit, outVars)
        implicit none
        integer, intent(in) :: unit
        type(outvars_data_type), intent(in) :: outVars
        integer :: ipft, ilayer, nformat
        character(len=5000) :: format_string
        
        ! write the date
        nformat       = 31*npft+108 
        format_string = '((i4,",")(i3,",")(i2,",")' // repeat('(f15.4, ",")', nformat-1) // 'f15.4)'
        ! print*,"test:", outVars%sp(1)%Aleaf(1)
        write(unit, adjustl(trim(format_string)))outVars%year, outVars%doy, outVars%hour, &
            (outVars%sp(ipft)%gpp,     outVars%sp(ipft)%nee,      outVars%sp(ipft)%npp,     &     
            outVars%sp(ipft)%nppLeaf,  outVars%sp(ipft)%nppStem,  outVars%sp(ipft)%nppStem, &
            outVars%sp(ipft)%nppRoot,  outVars%sp(ipft)%nppOther, outVars%sp(ipft)%ra,      & 
            outVars%sp(ipft)%raLeaf,   outVars%sp(ipft)%raStem,   outVars%sp(ipft)%raRoot,  & 
            outVars%sp(ipft)%raOther,  outVars%sp(ipft)%rMaint,   outVars%sp(ipft)%rGrowth, &
            outVars%sp(ipft)%nsc,      outVars%sp(ipft)%cLeaf,    outVars%sp(ipft)%cStem,   & 
            outVars%sp(ipft)%cRoot,    outVars%sp(ipft)%nLeaf,    outVars%sp(ipft)%nStem,   &  
            outVars%sp(ipft)%nRoot,    outVars%sp(ipft)%nsn,     outVars%sp(ipft)%tran,    & 
            outVars%sp(ipft)%lai,      outVars%sp(ipft)%Aleaf(1), outVars%sp(ipft)%Aleaf(2),&
            outVars%sp(ipft)%Aleaf(3), outVars%sp(ipft)%outC_L,   outVars%sp(ipft)%outC_W, &
            outVars%sp(ipft)%outC_L,   ipft = 1, npft),& 
            outVars%gpp,     outVars%nee,        outVars%npp,            outVars%nppLeaf,  &        
            outVars%nppStem, outVars%nppStem,    outVars%nppRoot,        outVars%nppOther, &   
            outVars%ra,      outVars%raLeaf,     outVars%raStem,         outVars%raRoot,   &  
            outVars%raOther, outVars%rMaint,     outVars%rGrowth,        outVars%rh,       &
            outVars%nbp,     outVars%wetlandCH4, outVars%wetlandCH4prod, outVars%wetlandCH4cons,   &
            outVars%cLeaf,   outVars%cStem,      outVars%cRoot,          outVars%cOther,   &
            outVars%cLitter, outVars%cLitterCwd, outVars%cSoil,           &
            (outVars%cSoilLevels(ilayer), ilayer = 1, nlayers),           &
            outVars%cSoilFast,  outVars%cSoilSlow, outVars%cSoilPassive,  &
            (outVars%CH4(ilayer), ilayer = 1, nlayers),                   &
            outVars%fBNF,       outVars%fN2O,   outVars%fNloss,   outVars%fNnetmin,   outVars%fNdep,   &
            outVars%nLeaf,      outVars%nStem,  outVars%nRoot,    outVars%nOther,     outVars%nLitter, &
            outVars%nLitterCwd, outVars%nSoil,  outVars%nMineral, outVars%hfls,       outVars%hfss,    &
            outVars%SWnet,      outVars%LWnet,  outVars%ec,       outVars%tran,       outVars%es,      &
            outVars%hfsbl,      outVars%mrro,   outVars%mrros,    outVars%mrrob,                       &     
            (outVars%mrso(ilayer), ilayer = 1, nlayers), &
            (outVars%tsl(ilayer),  ilayer = 1, nlayers), &
            outVars%tsland,     outVars%wtd,    outVars%snd,      outVars%lai,  outVars%outC_lit_m,   &
            outVars%outC_lit_s, outVars%outC_soil_f, outVars%outC_soil_s, outVars%outC_soil_p,        &
            outVars%Rh_lit_m,   outVars%Rh_lit_s, outVars%Rh_soil_f, outVars%Rh_soil_s, outVars%Rh_soil_p
            
        return
    end subroutine write_data_csv

! #ifdef USE_NETCDF
!     subroutine write_outputs_nc(out_path, outVars, nSimuLen, str_freq)
!         ! Daily and monthly
!         ! carbon flux (KgC m-2 s-1): gpp, npp, nppLeaf, nppStem, nppRoot, nppOther,
!         !              ra, raLeaf, raStem, raRoot, raOther, rMaint, rGrowth, rh
!         !              nbp (=gpp - Rh - Ra - other losses)
!         !              wetlandCH4, wetlandCH4prod, wetlandCH4cons
!         ! carbon pools (KgC m-2): cLeaf, cStem, cRoot, cOther, cLitter (excluding coarse Stem debris), cLitterCwd
!         !              cSoil, cSoilLevels, cSoilPools (soil organic carbon for each pool), CH4 (Methane concentration)
!         ! Nitrogen flux (KgN m-2 s-1) : fBNF(biological nitrogen fixation), fN2O, fNloss, fNnetmin, fNdep
!         ! Nitrogen pools (KgN m-2): nleaf, nStem, nRoot, nOther, nLitter, nLitterCwd, nSoil, nMineral
!         ! Energy Fluxes (W m-2): hfls(sensible heat flux), hfss(Latent heat flux), SWnet (Net Shortwave radiation), LWnet(Net Longwave radiation)
!         ! Water Fluxes  (Kg m-2 s-1): ec(canopy evaporation), tran(canopy transpiration), es(soil evaporation), hfsbl (snow sublimation), mrro(total runoff),
!         !                 mrros (surface runoff), mrrob(subsurface runoff)
!         ! other         : mrso (soil moisture in each soil layer, Kg m-2), tsl(soil temperature in each soil layer, K), tsland(surface temperature, K),
!         !                 wtd (Water table depth, m), snd (total snow depth, m), lai(m2 m-2) 
!         ! ===================================================================================================================================================
!         ! carbon fluxes variables
!         ! ----------:-----------:----------:-----------------------
!         implicit none
!         character(*), intent(in) :: out_path, str_freq
!         type(outvars_data_type), allocatable, intent(in) :: outVars
!         integer, intent(in) :: nSimuLen
!         integer :: ipft

!         write(str_startyr,"(I4)")forcing(1)%year
!         write(str_endyr,"(I4)")forcing(nforcing)%year

!         if (allocated(outVars%sp)) then
!             do ipft = 1, count_pft
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%gpp,      "gpp_"//adjustl(trim(spec_names(ipft))),     &
!                     "kgC m-2 s-1", "gross primary productivity",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%nee,      "nee_"//adjustl(trim(spec_names(ipft))),      &
!                     "kgC m-2 s-1", "net ecosystem exchange",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%npp,      "npp_"//adjustl(trim(spec_names(ipft))),      &
!                     "kgC m-2 s-1", "net primary productivity",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%nppLeaf,  "nppLeaf_"//adjustl(trim(spec_names(ipft))),  &
!                     "kgC m-2 s-1", "NPP allocated to leaf tissues",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%nppStem,  "nppStem_"//adjustl(trim(spec_names(ipft))),  &
!                     "kgC m-2 s-1", "NPP allocated to above ground Stemy tissues",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%nppStem,  "nppStem_"//adjustl(trim(spec_names(ipft))),  &
!                     "kgC m-2 s-1", "NPP allocated to stem tissues",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%nppRoot,  "nppRoot_"//adjustl(trim(spec_names(ipft))),  &
!                     "kgC m-2 s-1", "NPP allocated to root tissues",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%nppOther, "nppOther_"//adjustl(trim(spec_names(ipft))), &
!                     "kgC m-2 s-1", "NPP allocated to other plant organs (reserves, fruits, exudates)",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%ra, "ra_"//adjustl(trim(spec_names(ipft))), &
!                     "kgC m-2 s-1", "Plant Autotrophic Respiration",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%raLeaf,   "raLeaf_"//adjustl(trim(spec_names(ipft))),   &
!                     "kgC m-2 s-1", "Ra from leaves",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%raStem,   "raStem_"//adjustl(trim(spec_names(ipft))),   &
!                     "kgC m-2 s-1", "Ra from above ground Stemy tissues",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%raRoot,   "raRoot_"//adjustl(trim(spec_names(ipft))),   &
!                     "kgC m-2 s-1", "Ra from fine roots",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%raOther,   "raOther_"//adjustl(trim(spec_names(ipft))), &
!                     "kgC m-2 s-1", "Ra from other plant organs (reserves, fruits, exudates)",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%rMaint,    "rMaint_"//adjustl(trim(spec_names(ipft))),  &
!                     "kgC m-2 s-1", "Maintenance respiration",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%rGrowth,   "rGrowth_"//adjustl(trim(spec_names(ipft))), &
!                     "kgC m-2 s-1", "Growth respiration",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%nbp,       "nbp_"//adjustl(trim(spec_names(ipft))),     &
!                     "kgC m-2 s-1", "Net Biome productivity (NBP = GPP - Rh - Ra - other losses)",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%cLeaf,     "cLeaf_"//adjustl(trim(spec_names(ipft))),   &
!                     "kgC m-2", "Carbon biomass in leaves",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%cStem,     "cStem_"//adjustl(trim(spec_names(ipft))),   &
!                     "kgC m-2", "Carbon above ground Stemy biomass",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%cRoot,     "cRoot_"//adjustl(trim(spec_names(ipft))),   &
!                     "kgC m-2", "Carbon biomass in roots",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%nLeaf,     "nLeaf_"//adjustl(trim(spec_names(ipft))),   &
!                     "kgN m-2", "Nitrogen in leaves",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%nStem,     "nStem_"//adjustl(trim(spec_names(ipft))),   &
!                     "kgN m-2", "Nitrogen in stems",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%nRoot,     "nRoot_"//adjustl(trim(spec_names(ipft))),   &
!                     "kgN m-2", "Nirogen in roots",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%tran,      "tran_"//adjustl(trim(spec_names(ipft))),    &
!                     "kg m-2 s-1", "Canopy transpiration",str_freq,1)
!                 call write_nc(out_path, nSimuLen, outVars%sp(ipft)%lai,       "lai_"//adjustl(trim(spec_names(ipft))),     &
!                     "m2 m-2", "Leaf area index",str_freq,1)
!             enddo
!         endif

!         ! outputs
!         call write_nc(out_path, nSimuLen, outVars%gpp,"gpp","kgC m-2 s-1", "gross primary productivity",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%npp,"npp","kgC m-2 s-1", "Total net primary productivity",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%nppLeaf,"nppLeaf","kgC m-2 s-1", "NPP allocated to leaf tissues",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%nppStem,"nppStem","kgC m-2 s-1", &
!             & "NPP allocated to above ground Stemy tissues",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%nppStem,"nppStem","kgC m-2 s-1", "NPP allocated to stem tissues",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%nppRoot,"nppRoot","kgC m-2 s-1", "NPP allocated to root tissues",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%nppOther,"nppOther","kgC m-2 s-1", &
!             & "NPP allocated to other plant organs (reserves, fruits, exudates)",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%ra,"ra","kgC m-2 s-1", "Plant Autotrophic Respiration",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%raLeaf,"raLeaf","kgC m-2 s-1", "Ra from leaves",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%raStem,"raStem","kgC m-2 s-1", "Ra from above ground Stemy tissues",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%raRoot,"raRoot","kgC m-2 s-1", "Ra from fine roots",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%raOther,"raOther","kgC m-2 s-1", &
!             & "Ra from other plant organs (reserves, fruits, exudates)",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%rMaint,"rMaint","kgC m-2 s-1", "Maintenance respiration",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%rGrowth,"rGrowth","kgC m-2 s-1", "Growth respiration",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%rh,"rh","kgC m-2 s-1", "Heterotrophic respiration rate",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%nbp,"nbp","kgC m-2 s-1", &
!             &"Net Biome productivity (NBP = GPP - Rh - Ra - other losses)",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%wetlandCH4,"wetlandCH4","kgC m-2 s-1", "Net fluxes of CH4",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%wetlandCH4prod,"wetlandCH4prod","kgC m-2 s-1", "CH4 production",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%wetlandCH4cons,"wetlandCH4cons","kgC m-2 s-1", "CH4 consumption",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%cLeaf,"cLeaf","kgC m-2", "Carbon biomass in leaves",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%cStem,"cStem","kgC m-2", "Carbon above ground Stemy biomass",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%cRoot,"cRoot","kgC m-2", "Carbon biomass in roots",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%cOther,"cOther","kgC m-2", &
!             & "Carbon biomass in other plant organs (reserves, fruits)",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%cLitter,"cLitter","kgC m-2", &
!             & "Carbon in litter (excluding coarse Stemy debris)",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%cLitterCwd,"cLitterCwd","kgC m-2", "Carbon in coarse Stemy debris",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%cSoil,"cSoil","kgC m-2", "Total soil organic carbon",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%cSoilLevels,"cSoilLevels","kgC m-2", &
!             & "Depth-specific soil organic carbon",str_freq,nlayers)
!         call write_nc(out_path, nSimuLen, outVars%cSoilFast,"cSoilFast","kgC m-2", "Fast soil organic carbon",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%cSoilSlow,"cSoilSlow","kgC m-2 s-1", "Slow soil organic carbon",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%cSoilPassive,"cSoilPassive","kgC m-2 s-1", &
!             & "Passive soil organic carbon",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%CH4,"CH4","kgC m-2 s-1", "Methane concentration",str_freq,nlayers)
!         call write_nc(out_path, nSimuLen, outVars%fBNF,"fBNF","kgN m-2 s-1", "biological nitrogen fixation",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%fN2O,"fN2O","kgN m-2 s-1", &
!             & "loss of nitrogen through emission of N2O",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%fNloss,"fNloss","kgN m-2 s-1", &
!             & "Total loss of nitrogen to the atmosphere and from leaching",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%fNnetmin,"fNnetmin","kgN m-2 s-1", "net mineralization of N",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%fNdep,"fNdep","kgN m-2 s-1", "Nitrogen deposition",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%nLeaf,"nLeaf","kgN m-2", "Nitrogen in leaves",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%nStem,"nStem","kgN m-2", "Nitrogen in stems",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%nRoot,"nRoot","kgN m-2", "Nirogen in roots",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%nOther,     "nOther","kgN m-2", &
!             & "nitrogen in other plant organs (reserves, fruits)",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%nLitter,    "nLitter","kgN m-2", &
!             & "Nitrogen in litter (excluding coarse Stemy debris)",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%nLitterCwd, "nLitterCwd","kgN m-2", &
!             & "Nitrogen in coarse Stemy debris",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%nSoil,"nSoil","kgN m-2", "Nitrogen in soil organic matter",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%nMineral,"nMineral","kgN m-2", "Mineral nitrogen pool",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%hfls,"hfls","W m-2", "Sensible heat flux",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%hfss,"hfss","W m-2", "Latent heat flux",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%SWnet,"SWnet","W m-2", "Net shortwave radiation",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%LWnet,"LWnet","W m-2", "Net longwave radiation",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%ec,"ec","kg m-2 s-1", "Canopy evaporation",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%tran,"tran","kg m-2 s-1", "Canopy transpiration",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%es,"es","kg m-2 s-1", "Soil evaporation",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%hfsbl,"hfsbl","kg m-2 s-1", "Snow sublimation",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%mrro,"mrro","kg m-2 s-1", "Total runoff",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%mrros,"mrros","kg m-2 s-1", "Surface runoff",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%mrrob,"mrrob","kg m-2 s-1", "Subsurface runoff",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%mrso,"mrso","kg m-2", "soil moisture in each soil layer",str_freq,nlayers)
!         call write_nc(out_path, nSimuLen, outVars%tsl,"tsl","K", "soil temperature in each soil layer",str_freq,nlayers)
!         call write_nc(out_path, nSimuLen, outVars%tsland,"tsland","K", "surface temperature",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%wtd,"wtd","m", "Water table depth",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%snd,"snd","m", "Total snow depth",str_freq,1)
!         call write_nc(out_path, nSimuLen, outVars%lai,"lai","m2 m-2", "Leaf area index",str_freq,1)
!         ! call write_nc(out_path, nSimuLen, all_gdd5_h,"GDD5","m2 m-2", "GDD5",str_freq,1)
!         ! call write_nc(out_path, nSimuLen, all_onset_h,"onset","m2 m-2", "onset",str_freq,1)
!         ! call write_nc(out_path, nSimuLen, all_storage_h,"storage","m2 m-2", "onset",str_freq,1)
!         ! call write_nc(out_path, nSimuLen, all_add_h,"add","m2 m-2", "onset",str_freq,1)
!         ! call write_nc(out_path, nSimuLen, all_accumulation_h,"accumulation","m2 m-2", "accumulation",str_freq,1)
!         ! call write_nc(out_path, nSimuLen, all_test_h,"test_gpp","m2 m-2", "test_gpp",str_freq,9)
        
!     end subroutine write_outputs_nc

!     subroutine write_nc(outfile, nSimuLen, data, varName, unit, description, str_freq, nSoilLayer)
!         IMPLICIT NONE
!         real(kind=8), Dimension(nSimuLen, nSoilLayer), intent(in) :: data
!         integer(kind=4) :: nSoilLayer
!         integer(KIND=4) :: ncid, timid, dp_dimid, timvarid
!         integer(kind=4) :: varid
!         integer(kind=4), intent(in) :: nSimuLen
!         CHARACTER(LEN=*), INTENT(IN) :: outfile, str_freq
!         CHARACTER(len=*), intent(in) :: varName, unit, description
!         character(len=:), allocatable :: nc_fileName
!         character(len=100) :: timeUnit
!         integer itime
!         real(8), dimension(nSimuLen) :: time_values 
!         integer :: start(1), count(1)
        
!         allocate(character(len=200+len(outfile)) :: nc_fileName)
!         nc_fileName = adjustl(trim(outfile))//"/"//adjustl(trim(varName))//"_"//str_freq//"_TECO-SPRUCE_"//&
!             & adjustl(trim(case_name))//"_"//adjustl(trim(str_startyr))//"-"//adjustl(trim(str_endyr))//".nc"   
        
!         !Create the netCDF file.
!         CALL check(nf90_create(nc_fileName, NF90_CLOBBER, ncid))

!         !Define the dimensions.
!         ! CALL check(nf90_def_dim(ncid, "nSimu", nfreq,    simuid))
!         CALL check(nf90_def_dim(ncid, "time",  nSimuLen, timid))
    
!         if (nSoilLayer>1)then
!             call check(nf90_def_dim(ncid, "depth", nSoilLayer, dp_dimid))
!             CALL check(nf90_def_var(ncid = ncid, name = varName,  xtype = NF90_FLOAT, &
!                 & dimids = (/timid, dp_dimid/),  varID =varid))
!         else
!             CALL check(nf90_def_var(ncid = ncid, name = varName,  xtype = NF90_FLOAT, &
!                 & dimids = (/timid/),  varID =varid))
!         endif

!         call check(nf90_def_var(ncid, "time",  NF90_DOUBLE, timid,  timvarid))
!         !Define data variable
        
!         !Add attributes
!         if (str_freq .eq. "hourly") then
!             timeUnit = "hours since "//adjustl(trim(str_startyr))//"-01-01 00:00:00"
!         else if (str_freq .eq. "daily") then
!             timeUnit = "days since "//adjustl(trim(str_startyr))//"-01-01 00:00:00"
!         else if (str_freq .eq. "monthly") then
!             timeUnit = "months since "//adjustl(trim(str_startyr))//"-01-01 00:00:00"
!         end if
        
!         call check(nf90_put_att(ncid,timvarid,"units",adjustl(trim(timeUnit))))
!         CALL check(nf90_put_att(ncid,varid,"units",unit))
!         CALL check(nf90_put_att(ncid,varid,"description",description))
!         CALL check(nf90_enddef(ncid)) 
!         !End Definitions

!         !Write Data
!         ! if (nSoilLayer>1)then
!         !     do i = 1, nSoilLayer
!         !         CALL check(nf90_put_var(ncid, varid, data, start=[1,i], count=[nSimuLen,1]))
!         !     enddo
!         ! else

!         do itime = 1, nSimuLen
!             time_values(itime) = itime-1
!         enddo
!         start = 1
!         count = nSimuLen

!         CALL check(nf90_put_var(ncid, timvarid, time_values,start,count))
!         CALL check(nf90_put_var(ncid, varid, data))
        
!         CALL check(nf90_close(ncid))
!     end subroutine write_nc

!     ! check (ever so slightly modified from www.unidata.ucar.edu)
!     subroutine check(istatus)
!         ! use netcdf
!         implicit none
!         integer, intent(in) :: istatus
!         if(istatus /= nf90_noerr) then
!             write(*,*) trim(adjustl(nf90_strerror(istatus)))
!         end if
!     end subroutine check
! #endif
end module io_mod