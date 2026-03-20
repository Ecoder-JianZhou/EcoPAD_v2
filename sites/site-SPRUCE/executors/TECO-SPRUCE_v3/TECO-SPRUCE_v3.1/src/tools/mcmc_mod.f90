module mcmc_mod
! create mcmc datatypes and I/O
    use datatypes

    implicit none
    integer, parameter :: numPFT = 3, numObsFiles = 24
    integer, parameter :: numStParams = 73, numSpParams = 111
    integer :: numAllParams = numStParams + numPFT*numSpParams

    ! mcmc setttings
    type :: mcmc_settings
        integer :: nDAsimu
        real(8) :: search_scale
        integer :: ncov
        integer :: nRand
        logical :: do_mc_out_hr
        logical :: do_mc_out_day
        logical :: do_mc_out_mon
        logical :: do_mc_out_yr
    end type mcmc_settings
    type(mcmc_settings) :: mcset

    ! observation files
    type :: mcmc_observation_file
        character(500) :: BNPP
        character(500) :: ANPPshrub
        character(500) :: BNPPshrub
        character(500) :: PhotoShrub
        character(500) :: cLeafShrub
        character(500) :: cStemShrub
        character(500) :: LAIshrub
        character(500) :: NPPsphag
        character(500) :: cPlantSphag
        character(500) :: ANPPtree
        character(500) :: BNPPtree
        character(500) :: LAItree
        character(500) :: PhotoTree
        character(500) :: cPlantTree
        character(500) :: cSoil
        character(500) :: CH4
        character(500) :: ER
        character(500) :: GPP
        character(500) :: NEE
        character(500) :: RH
        character(500) :: RH_y
        character(500) :: CH4_y
        character(500) :: BNPPshrub_d
        character(500) :: BNPPtree_d
    end type mcmc_observation_file
    type(mcmc_observation_file) :: mcObsFiles

    real(8)::obsWt(numObsFiles)

    ! observations data
    type each_mcmc_param_data
        character(50) :: varname
        logical :: existOrNot
        real(8), allocatable :: obsData(:,:)
        real(8), allocatable :: mdData(:,:)
        integer :: mark_idx
    end type each_mcmc_param_data
    type(each_mcmc_param_data) :: mcVarData(numObsFiles)

    ! for reading parameter information
    type each_mcmc_parameter
        character(50) :: parName
        real(8)       :: parVal
        real(8)       :: parmin
        real(8)       :: parmax
    end type each_mcmc_parameter

    type sp_mcmc_parameter
        type(each_mcmc_parameter) :: var(numSpParams)
    end type sp_mcmc_parameter
    
    type all_mcmc_parameter
        type(each_mcmc_parameter) :: st(numStParams)
        type(sp_mcmc_parameter)   :: sp(numPFT)
    end type all_mcmc_parameter

    type(all_mcmc_parameter) :: mcParams
    ! end for reading paramters information to mcParams

    ! define some variables for MCMC
    type index_species_parameters_to_opt
        integer(8), allocatable :: idx(:)
    end type index_species_parameters_to_opt


    contains

    subroutine read_mcmc_conf_nml(in_mcConfNmlFile)
        implicit none
        character(*), intent(in) :: in_mcConfNmlFile
        integer io

        type(each_mcmc_parameter) :: st(numStParams)
        type(sp_mcmc_parameter) :: sp(numPFT)

        namelist /nml_mcmc_settings/ mcset
        namelist /obsFiles/ mcObsFiles
        namelist /obsWeight/ obsWt
        namelist /siteDAParams/ st
        namelist /spDAParams/ sp

        print *, "# Read MCMC config nml file ...", adjustl(trim(in_mcConfNmlFile))
        open(388, file = in_mcConfNmlFile)
        read(388, nml  = nml_mcmc_settings, iostat=io)
        read(388, nml  = obsFiles,          iostat=io)
        read(388, nml  = obsWeight,         iostat=io)
        read(388, nml  = siteDAParams,      iostat=io)
        read(388, nml  = spDAParams,        iostat=io)
        close(388)

        mcParams%st = st
        mcParams%sp = sp
        
        call read_obs_data(mcObsFiles)

    end subroutine read_mcmc_conf_nml

    subroutine read_obs_data(in_mcObsFiles)
        implicit none
        type(mcmc_observation_file), intent(in) :: in_mcObsFiles

        call read_each_obs_file("BNPP",       in_mcObsFiles%BNPP,       mcVarData(1))
        call read_each_obs_file("ANPPshrub",  in_mcObsFiles%ANPPshrub,  mcVarData(2))
        call read_each_obs_file("BNPPshrub",  in_mcObsFiles%BNPPshrub,  mcVarData(3))
        call read_each_obs_file("PhotoShrub", in_mcObsFiles%PhotoShrub, mcVarData(4))
        call read_each_obs_file("cLeafShrub", in_mcObsFiles%cLeafShrub, mcVarData(5))
        call read_each_obs_file("cStemShrub", in_mcObsFiles%cStemShrub, mcVarData(6))
        call read_each_obs_file("NPPsphag",   in_mcObsFiles%NPPsphag,   mcVarData(7))
        call read_each_obs_file("cPlantSphag",in_mcObsFiles%cPlantSphag,mcVarData(8))
        call read_each_obs_file("ANPPtree",   in_mcObsFiles%ANPPtree,   mcVarData(9))
        call read_each_obs_file("BNPPtree",   in_mcObsFiles%BNPPtree,   mcVarData(10))
        call read_each_obs_file("LAItree",    in_mcObsFiles%LAItree,    mcVarData(11))
        call read_each_obs_file("PhotoTree",  in_mcObsFiles%PhotoTree,  mcVarData(12))
        call read_each_obs_file("cPlantTree", in_mcObsFiles%cPlantTree, mcVarData(13))
        call read_each_obs_file("cSoil",      in_mcObsFiles%cSoil,      mcVarData(14))
        call read_each_obs_file("CH4",        in_mcObsFiles%CH4,        mcVarData(15))
        call read_each_obs_file("ER",         in_mcObsFiles%ER,         mcVarData(16))
        call read_each_obs_file("GPP",        in_mcObsFiles%GPP,        mcVarData(17))
        call read_each_obs_file("NEE",        in_mcObsFiles%NEE,        mcVarData(18))
        call read_each_obs_file("RH",         in_mcObsFiles%RH,         mcVarData(19))
        call read_each_obs_file("LAIshrub",   in_mcObsFiles%LAIshrub,   mcVarData(20))
        call read_each_obs_file("RH_y",       in_mcObsFiles%RH_y,       mcVarData(21))
        call read_each_obs_file("CH4_y",      in_mcObsFiles%CH4_y,      mcVarData(22))
        call read_each_obs_file("BNPPshrub_d",in_mcObsFiles%BNPPshrub_d,mcVarData(23))
        call read_each_obs_file("BNPPtree_d", in_mcObsFiles%BNPPtree_d, mcVarData(24))
    end subroutine read_obs_data

    subroutine read_each_obs_file(varname, in_file, ivarData)
        implicit none
        character(*), intent(in) :: varname
        character(*), intent(in) :: in_file
        type(each_mcmc_param_data), intent(inout) :: ivarData
        logical ExistOrNot
        integer toCountLines
        character(500) :: filename

        filename = adjustl(trim(inDir))//"/observation_files/"//adjustl(trim(in_file))
        print*, filename

        ivarData%varname = varname
        print*, ivarData%varname
        INQUIRE(FILE=filename, EXIST=ExistOrNot)
        ivarData%existOrNot = ExistOrNot
        if (ivarData%existOrNot) then
            call ReadLineNumFromFile(filename, toCountLines)
            allocate(ivarData%obsData(toCountLines, 5))
            call ReadObsDataFromFile(filename, toCountLines, ivarData%obsData)
            allocate(ivarData%mdData(toCountLines, 4))
            ivarData%mark_idx = 1
        endif
        return
    end subroutine read_each_obs_file

    subroutine ReadObsDataFromFile(filepath, count_lines, resData)
        ! Jian: note that this subroutine is used to read the observational data. 
        ! The observational file must be .txt format, and with 5 columns: year, doy, hour, value, std.
        implicit none
        character(len=*), intent(in) :: filepath
        character(len=100) header
        integer STAT, count_lines, iline, n
        real(8) resData(count_lines, 5), readData(5) ! 5 colunms: year, doy, hour, value, std.

        OPEN(34, FILE=trim(filepath), status='old', ACTION='read', IOSTAT=STAT) ! open file
        read(34, '(a100)') header
        iline = 1
        do
            read(34,*,iostat=STAT, end=567) (readData(n), n = 1, 5)
            if(STAT .ne. 0) exit
            resData(iline, :) = readData
            iline = iline + 1
        end do
567     continue
        close(34)
        return
    end subroutine ReadObsDataFromFile

    subroutine update_mcParams(DAparVal, idxStPar, idxSpPar)
        implicit none
        real(8), intent(in) :: DAparVal(:)
        integer, intent(in) :: idxStPar(:)
        type(index_species_parameters_to_opt), intent(in) :: idxSpPar(:)
        integer :: i, j, nparSt, nparSp

        nparSt = size(idxStPar)

        do i = 1, nparSt
            mcParams%st(idxStPar(i))%parVal = DAparVal(i)
        enddo

        nparSp = nparSt
        do i = 1, numPFT
            do j = 1, size(idxSpPar(i)%idx)
                nparSp = nparSp + 1
                mcParams%sp(i)%var(idxSpPar(i)%idx(j))%parVal = DAparVal(nparSp)
            enddo
        enddo
    end subroutine update_mcParams

    subroutine update_simuParams()
        implicit none
        integer i, j
        do i = 1, size(mcParams%st)
            select case (trim(adjustl(mcParams%st(i)%parName)))
                case ("wsmax");       in_params_vals%st_params%wsmax       = mcParams%st(i)%parval
                case ("wsmin");       in_params_vals%st_params%wsmin       = mcParams%st(i)%parval
                case ("extkU");       in_params_vals%st_params%extkU       = mcParams%st(i)%parval
                case ("Tau_F");       in_params_vals%st_params%Tau_F       = mcParams%st(i)%parval
                case ("Tau_C");       in_params_vals%st_params%Tau_C       = mcParams%st(i)%parval
                case ("Tau_Micro");   in_params_vals%st_params%Tau_Micro   = mcParams%st(i)%parval
                case ("Tau_SlowSOM"); in_params_vals%st_params%Tau_SlowSOM = mcParams%st(i)%parval
                case ("Tau_Passive"); in_params_vals%st_params%Tau_Passive = mcParams%st(i)%parval
                case ("Q10rh");       in_params_vals%st_params%Q10rh       = mcParams%st(i)%parval
                case ("etaW");        in_params_vals%st_params%etaW        = mcParams%st(i)%parval
                case ("f_F2M");       in_params_vals%st_params%f_F2M       = mcParams%st(i)%parval
                case ("f_C2M");       in_params_vals%st_params%f_C2M       = mcParams%st(i)%parval
                case ("f_C2S");       in_params_vals%st_params%f_C2S       = mcParams%st(i)%parval
                case ("f_M2S");       in_params_vals%st_params%f_M2S       = mcParams%st(i)%parval
                case ("f_M2P");       in_params_vals%st_params%f_M2P       = mcParams%st(i)%parval
                case ("f_S2P");       in_params_vals%st_params%f_S2P       = mcParams%st(i)%parval
                case ("f_S2M");       in_params_vals%st_params%f_S2M       = mcParams%st(i)%parval
                case ("f_P2M");       in_params_vals%st_params%f_P2M       = mcParams%st(i)%parval
                case ("r_me");        in_params_vals%st_params%r_me        = mcParams%st(i)%parval
                case ("Q10pro");      in_params_vals%st_params%Q10pro      = mcParams%st(i)%parval
                case ("kCH4");        in_params_vals%st_params%kCH4        = mcParams%st(i)%parval
                case ("Omax");        in_params_vals%st_params%Omax        = mcParams%st(i)%parval
                case ("CH4_thre");    in_params_vals%st_params%CH4_thre    = mcParams%st(i)%parval
                case ("Tveg");        in_params_vals%st_params%Tveg        = mcParams%st(i)%parval
                case ("Tpro_me");     in_params_vals%st_params%Tpro_me     = mcParams%st(i)%parval
                case ("Toxi");        in_params_vals%st_params%Toxi        = mcParams%st(i)%parval
                case ("f");           in_params_vals%st_params%f           = mcParams%st(i)%parval
                case ("bubprob");     in_params_vals%st_params%bubprob     = mcParams%st(i)%parval
                case ("Vmaxfraction");in_params_vals%st_params%Vmaxfraction= mcParams%st(i)%parval
                case ("f_fast");      in_params_vals%st_params%f_fast      = mcParams%st(i)%parval
                case ("f_slow");      in_params_vals%st_params%f_slow      = mcParams%st(i)%parval
                case ("s_soil");      in_params_vals%st_params%s_soil      = mcParams%st(i)%parval
                case ("par_shcap_snow"); in_params_vals%st_params%par_shcap_snow = mcParams%st(i)%parval
                case ("par_condu_snow"); in_params_vals%st_params%par_condu_snow = mcParams%st(i)%parval
                case ("par_condu_b"); in_params_vals%st_params%par_condu_b = mcParams%st(i)%parval
                case ("par_fsub");    in_params_vals%st_params%par_fsub    = mcParams%st(i)%parval
                case ("par_rho_snow");in_params_vals%st_params%par_rho_snow= mcParams%st(i)%parval
                case ("par_decay_m"); in_params_vals%st_params%par_decay_m = mcParams%st(i)%parval
                case ("pox");         in_params_vals%st_params%pox         = mcParams%st(i)%parval
                case ("f_lit_m");     in_params_vals%st_params%f_lit_m     = mcParams%st(i)%parval
                case ("s_lit");       in_params_vals%st_params%s_lit       = mcParams%st(i)%parval
                case ("Q10rh_1");  in_params_vals%st_params%Q10rh_1  = mcParams%st(i)%parval
                case ("Q10rh_2");  in_params_vals%st_params%Q10rh_2  = mcParams%st(i)%parval
                case ("Q10rh_3");  in_params_vals%st_params%Q10rh_3  = mcParams%st(i)%parval
                case ("Q10rh_4");  in_params_vals%st_params%Q10rh_4  = mcParams%st(i)%parval
                case ("Q10rh_5");  in_params_vals%st_params%Q10rh_5  = mcParams%st(i)%parval
                case ("Q10rh_6");  in_params_vals%st_params%Q10rh_6  = mcParams%st(i)%parval
                case ("Q10rh_7");  in_params_vals%st_params%Q10rh_7  = mcParams%st(i)%parval
                case ("Q10rh_8");  in_params_vals%st_params%Q10rh_8  = mcParams%st(i)%parval
                case ("Q10pro_1"); in_params_vals%st_params%Q10pro_1 = mcParams%st(i)%parval
                case ("Q10pro_2"); in_params_vals%st_params%Q10pro_2 = mcParams%st(i)%parval
                case ("Q10pro_3"); in_params_vals%st_params%Q10pro_3 = mcParams%st(i)%parval
                case ("Q10pro_4"); in_params_vals%st_params%Q10pro_4 = mcParams%st(i)%parval
                case ("Q10pro_5"); in_params_vals%st_params%Q10pro_5 = mcParams%st(i)%parval
                case ("Q10pro_6"); in_params_vals%st_params%Q10pro_6 = mcParams%st(i)%parval
                case ("Q10pro_7"); in_params_vals%st_params%Q10pro_7 = mcParams%st(i)%parval
                case ("Q10pro_8"); in_params_vals%st_params%Q10pro_8 = mcParams%st(i)%parval
                case ("Tpro_me_1"); in_params_vals%st_params%Tpro_me_1 = mcParams%st(i)%parval
                case ("Tpro_me_2"); in_params_vals%st_params%Tpro_me_2 = mcParams%st(i)%parval
                case ("Tpro_me_3"); in_params_vals%st_params%Tpro_me_3 = mcParams%st(i)%parval
                case ("Tpro_me_4"); in_params_vals%st_params%Tpro_me_4 = mcParams%st(i)%parval
                case ("Tpro_me_5"); in_params_vals%st_params%Tpro_me_5 = mcParams%st(i)%parval
                case ("Tpro_me_6"); in_params_vals%st_params%Tpro_me_6 = mcParams%st(i)%parval
                case ("Tpro_me_7"); in_params_vals%st_params%Tpro_me_7 = mcParams%st(i)%parval
                case ("Tpro_me_8"); in_params_vals%st_params%Tpro_me_8 = mcParams%st(i)%parval
                case ("Tref_rh_1"); in_params_vals%st_params%Tref_rh_1 = mcParams%st(i)%parval
                case ("Tref_rh_2"); in_params_vals%st_params%Tref_rh_2 = mcParams%st(i)%parval
                case ("Tref_rh_3"); in_params_vals%st_params%Tref_rh_3 = mcParams%st(i)%parval
                case ("Tref_rh_4"); in_params_vals%st_params%Tref_rh_4 = mcParams%st(i)%parval
                case ("Tref_rh_5"); in_params_vals%st_params%Tref_rh_5 = mcParams%st(i)%parval
                case ("Tref_rh_6"); in_params_vals%st_params%Tref_rh_6 = mcParams%st(i)%parval
                case ("Tref_rh_7"); in_params_vals%st_params%Tref_rh_7 = mcParams%st(i)%parval
                case ("Tref_rh_8"); in_params_vals%st_params%Tref_rh_8 = mcParams%st(i)%parval
            end select
        end do

        do i = 1, npft
            do j = 1, size(mcParams%sp(i)%var)
                select case (trim(adjustl(mcParams%sp(i)%var(j)%parname)))
                    case ("stom_n");   in_params_vals%sp_params(i)%stom_n   = int(mcParams%sp(i)%var(j)%parval)
                    case ("LAImax");   in_params_vals%sp_params(i)%LAImax   = mcParams%sp(i)%var(j)%parval
                    case ("LAImin");   in_params_vals%sp_params(i)%LAImin   = mcParams%sp(i)%var(j)%parval
                    case ("SapS");     in_params_vals%sp_params(i)%SapS     = mcParams%sp(i)%var(j)%parval
                    case ("SapR");     in_params_vals%sp_params(i)%SapR     = mcParams%sp(i)%var(j)%parval
                    case ("SLAx");      in_params_vals%sp_params(i)%SLA      = mcParams%sp(i)%var(j)%parval
                    case ("GLmax");    in_params_vals%sp_params(i)%GLmax    = mcParams%sp(i)%var(j)%parval
                    case ("GRmax");    in_params_vals%sp_params(i)%GRmax    = mcParams%sp(i)%var(j)%parval
                    case ("Gsmax");    in_params_vals%sp_params(i)%Gsmax    = mcParams%sp(i)%var(j)%parval
                    case ("alpha");    in_params_vals%sp_params(i)%alpha    = mcParams%sp(i)%var(j)%parval
                    case ("Vcmax0");   in_params_vals%sp_params(i)%Vcmax0   = mcParams%sp(i)%var(j)%parval
                    case ("Ds0");      in_params_vals%sp_params(i)%Ds0      = mcParams%sp(i)%var(j)%parval
                    case ("xfang");    in_params_vals%sp_params(i)%xfang    = mcParams%sp(i)%var(j)%parval
                    case ("rdepth");   in_params_vals%sp_params(i)%rdepth   = mcParams%sp(i)%var(j)%parval
                    case ("Rootmax");  in_params_vals%sp_params(i)%Rootmax  = mcParams%sp(i)%var(j)%parval
                    case ("Stemmax");  in_params_vals%sp_params(i)%Stemmax  = mcParams%sp(i)%var(j)%parval
                    case ("Tau_Leaf"); in_params_vals%sp_params(i)%Tau_Leaf = mcParams%sp(i)%var(j)%parval
                    case ("Tau_Stem"); in_params_vals%sp_params(i)%Tau_Stem = mcParams%sp(i)%var(j)%parval
                    case ("Tau_Root"); in_params_vals%sp_params(i)%Tau_Root = mcParams%sp(i)%var(j)%parval
                    case ("Q10");      in_params_vals%sp_params(i)%Q10      = mcParams%sp(i)%var(j)%parval
                    case ("Rl0");      in_params_vals%sp_params(i)%Rl0      = mcParams%sp(i)%var(j)%parval
                    case ("Rs0");      in_params_vals%sp_params(i)%Rs0      = mcParams%sp(i)%var(j)%parval
                    case ("Rr0");      in_params_vals%sp_params(i)%Rr0      = mcParams%sp(i)%var(j)%parval
                    case ("JV");       in_params_vals%sp_params(i)%JV       = mcParams%sp(i)%var(j)%parval
                    case ("Entrpy");   in_params_vals%sp_params(i)%Entrpy   = mcParams%sp(i)%var(j)%parval
                    case ("gddonset"); in_params_vals%sp_params(i)%gddonset = mcParams%sp(i)%var(j)%parval
                    case ("hmax");     in_params_vals%sp_params(i)%hmax     = mcParams%sp(i)%var(j)%parval
                    case ("hl0");      in_params_vals%sp_params(i)%hl0      = mcParams%sp(i)%var(j)%parval
                    case ("LAIMAX0");  in_params_vals%sp_params(i)%LAIMAX0  = mcParams%sp(i)%var(j)%parval
                    case ("la0");      in_params_vals%sp_params(i)%la0      = mcParams%sp(i)%var(j)%parval
                    case ("fn2l");     in_params_vals%sp_params(i)%fn2l     = mcParams%sp(i)%var(j)%parval
                    case ("fn2r");     in_params_vals%sp_params(i)%fn2r     = mcParams%sp(i)%var(j)%parval
                    case ("s_cLeaf");  in_params_vals%sp_params(i)%s_cLeaf  = mcParams%sp(i)%var(j)%parval
                    case ("s_cStem");  in_params_vals%sp_params(i)%s_cStem  = mcParams%sp(i)%var(j)%parval
                    case ("s_cRoot");  in_params_vals%sp_params(i)%s_cRoot  = mcParams%sp(i)%var(j)%parval
                    case ("s_nsc");    in_params_vals%sp_params(i)%s_nsc    = mcParams%sp(i)%var(j)%parval
                    case ("s_nsn");    in_params_vals%sp_params(i)%s_nsn    = mcParams%sp(i)%var(j)%parval
                    case ("f_rg");     in_params_vals%sp_params(i)%f_rg     = mcParams%sp(i)%var(j)%parval
                    case ("s_vea_1");  in_params_vals%sp_params(i)%s_vea_1  = mcParams%sp(i)%var(j)%parval
                    case ("s_vea_2");  in_params_vals%sp_params(i)%s_vea_2  = mcParams%sp(i)%var(j)%parval
                    case ("s_vea_3");  in_params_vals%sp_params(i)%s_vea_3  = mcParams%sp(i)%var(j)%parval
                    case ("s_vea_4");  in_params_vals%sp_params(i)%s_vea_4  = mcParams%sp(i)%var(j)%parval
                    case ("s_vea_5");  in_params_vals%sp_params(i)%s_vea_5  = mcParams%sp(i)%var(j)%parval
                    case ("s_vea_6");  in_params_vals%sp_params(i)%s_vea_6  = mcParams%sp(i)%var(j)%parval
                    case ("s_vea_7");  in_params_vals%sp_params(i)%s_vea_7  = mcParams%sp(i)%var(j)%parval
                    case ("s_vea_8");  in_params_vals%sp_params(i)%s_vea_8  = mcParams%sp(i)%var(j)%parval
                    case ("s_ved_1");  in_params_vals%sp_params(i)%s_ved_1  = mcParams%sp(i)%var(j)%parval
                    case ("s_ved_2");  in_params_vals%sp_params(i)%s_ved_2  = mcParams%sp(i)%var(j)%parval
                    case ("s_ved_3");  in_params_vals%sp_params(i)%s_ved_3  = mcParams%sp(i)%var(j)%parval
                    case ("s_ved_4");  in_params_vals%sp_params(i)%s_ved_4  = mcParams%sp(i)%var(j)%parval
                    case ("s_ved_5");  in_params_vals%sp_params(i)%s_ved_5  = mcParams%sp(i)%var(j)%parval
                    case ("s_ved_6");  in_params_vals%sp_params(i)%s_ved_6  = mcParams%sp(i)%var(j)%parval
                    case ("s_ved_7");  in_params_vals%sp_params(i)%s_ved_7  = mcParams%sp(i)%var(j)%parval
                    case ("s_ved_8");  in_params_vals%sp_params(i)%s_ved_8  = mcParams%sp(i)%var(j)%parval
                    case ("Entrpy_1"); in_params_vals%sp_params(i)%Entrpy_1 = mcParams%sp(i)%var(j)%parval
                    case ("Entrpy_2"); in_params_vals%sp_params(i)%Entrpy_2 = mcParams%sp(i)%var(j)%parval
                    case ("Entrpy_3"); in_params_vals%sp_params(i)%Entrpy_3 = mcParams%sp(i)%var(j)%parval
                    case ("Entrpy_4"); in_params_vals%sp_params(i)%Entrpy_4 = mcParams%sp(i)%var(j)%parval
                    case ("Entrpy_5"); in_params_vals%sp_params(i)%Entrpy_5 = mcParams%sp(i)%var(j)%parval
                    case ("Entrpy_6"); in_params_vals%sp_params(i)%Entrpy_6 = mcParams%sp(i)%var(j)%parval
                    case ("Entrpy_7"); in_params_vals%sp_params(i)%Entrpy_7 = mcParams%sp(i)%var(j)%parval
                    case ("Entrpy_8"); in_params_vals%sp_params(i)%Entrpy_8 = mcParams%sp(i)%var(j)%parval
                    case ("Vcmax0_1"); in_params_vals%sp_params(i)%Vcmax0_1 = mcParams%sp(i)%var(j)%parval
                    case ("Vcmax0_2"); in_params_vals%sp_params(i)%Vcmax0_2 = mcParams%sp(i)%var(j)%parval
                    case ("Vcmax0_3"); in_params_vals%sp_params(i)%Vcmax0_3 = mcParams%sp(i)%var(j)%parval
                    case ("Vcmax0_4"); in_params_vals%sp_params(i)%Vcmax0_4 = mcParams%sp(i)%var(j)%parval
                    case ("Vcmax0_5"); in_params_vals%sp_params(i)%Vcmax0_5 = mcParams%sp(i)%var(j)%parval
                    case ("Vcmax0_6"); in_params_vals%sp_params(i)%Vcmax0_6 = mcParams%sp(i)%var(j)%parval
                    case ("Vcmax0_7"); in_params_vals%sp_params(i)%Vcmax0_7 = mcParams%sp(i)%var(j)%parval
                    case ("Vcmax0_8"); in_params_vals%sp_params(i)%Vcmax0_8 = mcParams%sp(i)%var(j)%parval
                    case ("Q10_1");    in_params_vals%sp_params(i)%Q10_1 = mcParams%sp(i)%var(j)%parval
                    case ("Q10_2");    in_params_vals%sp_params(i)%Q10_2 = mcParams%sp(i)%var(j)%parval
                    case ("Q10_3");    in_params_vals%sp_params(i)%Q10_3 = mcParams%sp(i)%var(j)%parval
                    case ("Q10_4");    in_params_vals%sp_params(i)%Q10_4 = mcParams%sp(i)%var(j)%parval
                    case ("Q10_5");    in_params_vals%sp_params(i)%Q10_5 = mcParams%sp(i)%var(j)%parval
                    case ("Q10_6");    in_params_vals%sp_params(i)%Q10_6 = mcParams%sp(i)%var(j)%parval
                    case ("Q10_7");    in_params_vals%sp_params(i)%Q10_7 = mcParams%sp(i)%var(j)%parval
                    case ("Q10_8");    in_params_vals%sp_params(i)%Q10_8 = mcParams%sp(i)%var(j)%parval  
                    case ("f_rg_1");   in_params_vals%sp_params(i)%f_rg_1 = mcParams%sp(i)%var(j)%parval
                    case ("f_rg_2");   in_params_vals%sp_params(i)%f_rg_2 = mcParams%sp(i)%var(j)%parval
                    case ("f_rg_3");   in_params_vals%sp_params(i)%f_rg_3 = mcParams%sp(i)%var(j)%parval
                    case ("f_rg_4");   in_params_vals%sp_params(i)%f_rg_4 = mcParams%sp(i)%var(j)%parval
                    case ("f_rg_5");   in_params_vals%sp_params(i)%f_rg_5 = mcParams%sp(i)%var(j)%parval
                    case ("f_rg_6");   in_params_vals%sp_params(i)%f_rg_6 = mcParams%sp(i)%var(j)%parval
                    case ("f_rg_7");   in_params_vals%sp_params(i)%f_rg_7 = mcParams%sp(i)%var(j)%parval
                    case ("f_rg_8");   in_params_vals%sp_params(i)%f_rg_8 = mcParams%sp(i)%var(j)%parval
                    case ("fn2r_1"); in_params_vals%sp_params(i)%fn2r_1 = mcParams%sp(i)%var(j)%parval
                    case ("fn2r_2"); in_params_vals%sp_params(i)%fn2r_2 = mcParams%sp(i)%var(j)%parval
                    case ("fn2r_3"); in_params_vals%sp_params(i)%fn2r_3 = mcParams%sp(i)%var(j)%parval
                    case ("fn2r_4"); in_params_vals%sp_params(i)%fn2r_4 = mcParams%sp(i)%var(j)%parval
                    case ("fn2r_5"); in_params_vals%sp_params(i)%fn2r_5 = mcParams%sp(i)%var(j)%parval
                    case ("fn2r_6"); in_params_vals%sp_params(i)%fn2r_6 = mcParams%sp(i)%var(j)%parval
                    case ("fn2r_7"); in_params_vals%sp_params(i)%fn2r_7 = mcParams%sp(i)%var(j)%parval
                    case ("fn2r_8"); in_params_vals%sp_params(i)%fn2r_8 = mcParams%sp(i)%var(j)%parval
                    case ("s_npp_1"); in_params_vals%sp_params(i)%s_npp_1 = mcParams%sp(i)%var(j)%parval
                    case ("s_npp_2"); in_params_vals%sp_params(i)%s_npp_2 = mcParams%sp(i)%var(j)%parval
                    case ("s_npp_3"); in_params_vals%sp_params(i)%s_npp_3 = mcParams%sp(i)%var(j)%parval
                    case ("s_npp_4"); in_params_vals%sp_params(i)%s_npp_4 = mcParams%sp(i)%var(j)%parval
                    case ("s_npp_5"); in_params_vals%sp_params(i)%s_npp_5 = mcParams%sp(i)%var(j)%parval
                    case ("s_npp_6"); in_params_vals%sp_params(i)%s_npp_6 = mcParams%sp(i)%var(j)%parval
                    case ("s_npp_7"); in_params_vals%sp_params(i)%s_npp_7 = mcParams%sp(i)%var(j)%parval
                    case ("s_npp_8"); in_params_vals%sp_params(i)%s_npp_8 = mcParams%sp(i)%var(j)%parval
                    case ("s_npp_9"); in_params_vals%sp_params(i)%s_npp_9 = mcParams%sp(i)%var(j)%parval
                    case ("fn2l_1"); in_params_vals%sp_params(i)%fn2l_1 = mcParams%sp(i)%var(j)%parval
                    case ("fn2l_2"); in_params_vals%sp_params(i)%fn2l_2 = mcParams%sp(i)%var(j)%parval
                    case ("fn2l_3"); in_params_vals%sp_params(i)%fn2l_3 = mcParams%sp(i)%var(j)%parval
                    case ("fn2l_4"); in_params_vals%sp_params(i)%fn2l_4 = mcParams%sp(i)%var(j)%parval
                    case ("fn2l_5"); in_params_vals%sp_params(i)%fn2l_5 = mcParams%sp(i)%var(j)%parval
                    case ("fn2l_6"); in_params_vals%sp_params(i)%fn2l_6 = mcParams%sp(i)%var(j)%parval
                    case ("fn2l_7"); in_params_vals%sp_params(i)%fn2l_7 = mcParams%sp(i)%var(j)%parval
                    case ("fn2l_8"); in_params_vals%sp_params(i)%fn2l_8 = mcParams%sp(i)%var(j)%parval
                end select
            end do
        end do
    end subroutine update_simuParams

    subroutine get_simu_data(in_year, in_doy, in_hour, in_st)
        implicit none
        type(site_data_type), intent(in) :: in_st
        integer, intent(in) :: in_year, in_doy, in_hour
        integer :: i, itime, nObs
        real(8) :: simu_value

        do i = 1, numObsFiles
            if(mcVarData(i)%existOrNot) then
                itime = mcVarData(i)%mark_idx
                nObs  = size(mcVarData(i)%obsData, dim=1)
                if (itime > nObs) cycle
                if(itime .eq. 1) then
                    ! check and skip early year data
                    do while (itime <= nObs .and. mcVarData(i)%obsData(itime, 1) < in_year)
                        mcVarData(i)%mdData(itime, 1:3) = mcVarData(i)%obsData(itime,1:3)
                        mcVarData(i)%mdData(itime, 4) = -9999
                        itime = itime + 1
                        if(itime>nObs)exit
                    end do
                    mcVarData(i)%mark_idx = itime
                endif

                if(itime <= nObs) then
                    ! not beyond the range, then
                    if (mcVarData(i)%obsData(itime, 1) == in_year .and. &
                        mcVarData(i)%obsData(itime, 2) == in_doy  .and. &
                        mcVarData(i)%obsData(itime, 3) == in_hour) then

                    ! select variable depend on varname
                    simu_value = -9999
                    select case(trim(adjustl(mcVarData(i)%varname)))
                        case ("BNPP")
                            ! simu_value = (in_st%sp(1)%pft_weight*outVars_y%sp(1)%nppRoot + &
                            !               in_st%sp(2)%pft_weight*outVars_y%sp(2)%nppRoot) * 24 * 365
                            simu_value = in_st%sp(2)%pft_weight*outVars_y%sp(2)%gpp * 24 * 365
                        case ("ANPPshrub")
                            simu_value = (outVars_y%sp(2)%nppLeaf + outVars_y%sp(2)%nppStem) * 24 * 365 * in_st%sp(2)%pft_weight
                        case ("BNPPshrub")
                            simu_value = in_st%sp(2)%pft_weight*outVars_y%sp(2)%nppRoot * 24 * 365
                        case ("PhotoShrub")
                            simu_value = outVars_h%sp(2)%Aleaf(3)
                        case ("cLeafShrub")
                            simu_value = in_st%sp(2)%pft_weight * outVars_h%sp(2)%cleaf
                        case ("cStemShrub")
                            simu_value = in_st%sp(2)%pft_weight * outVars_h%sp(2)%cStem
                        case ("NPPsphag")
                            simu_value = in_st%sp(3)%pft_weight*outVars_y%sp(3)%npp * 24 * 365
                        case ("cPlantSphag")
                            simu_value = in_st%sp(3)%pft_weight * &
                                         (outVars_h%sp(3)%cLeaf + outVars_h%sp(3)%cStem + outVars_h%sp(3)%cRoot)
                        case ("ANPPtree")
                            simu_value = (outVars_y%sp(1)%nppLeaf + outVars_y%sp(1)%nppStem) * 24 * 365 * in_st%sp(1)%pft_weight
                        case ("BNPPtree")
                            simu_value = in_st%sp(1)%pft_weight*outVars_y%sp(1)%nppRoot * 24 * 365
                        case ("LAItree")
                            simu_value = outVars_h%sp(1)%LAI
                        case ("PhotoTree")
                            simu_value = outVars_h%sp(1)%Aleaf(3)
                        case ("cPlantTree")
                            simu_value = in_st%sp(1)%pft_weight * (outVars_h%sp(1)%cLeaf + outVars_h%sp(1)%cStem)
                        case ("cSoil")
                            simu_value = outVars_h%cSoilFast + outVars_h%cSoilSlow + outVars_h%cSoilPassive
                        case ("CH4")
                            simu_value = outVars_h%wetlandCH4
                        case ("ER")
                            simu_value = (in_st%sp(2)%pft_weight*outVars_h%sp(2)%ra + &
                                          in_st%sp(3)%pft_weight*outVars_h%sp(3)%ra + outVars_h%rh) 
                        case ("GPP")
                            simu_value = (in_st%sp(2)%pft_weight*outVars_h%sp(2)%gpp + &
                                          in_st%sp(3)%pft_weight*outVars_h%sp(3)%gpp)
                        case ("NEE")
                            simu_value = ((in_st%sp(2)%pft_weight*outVars_h%sp(2)%ra  + &
                                           in_st%sp(3)%pft_weight*outVars_h%sp(3)%ra  + outVars_h%rh)-&
                                          (in_st%sp(2)%pft_weight*outVars_h%sp(2)%gpp + &
                                           in_st%sp(3)%pft_weight*outVars_h%sp(3)%gpp))
                        case ("RH")
                            simu_value = outVars_h%rh !outVars_y%rh * 24 * 365
                        case ("LAIshrub")
                            simu_value = outVars_h%sp(2)%LAI
                        case ("RH_y")
                            simu_value = outVars_y%rh * 24 * 365
                        case ("CH4_y")
                            simu_value = outVars_y%wetlandCH4 * 24 * 365
                        case ("BNPPshrub_d")
                            simu_value = in_st%sp(2)%pft_weight*outVars_d%sp(2)%nppRoot * 24
                        case ("BNPPtree_d")
                            simu_value = in_st%sp(1)%pft_weight*outVars_d%sp(1)%nppRoot * 24
                        case default
                            simu_value = -9999 
                    end select

                    ! update variables
                        mcVarData(i)%mdData(mcVarData(i)%mark_idx, 1) = in_year
                        mcVarData(i)%mdData(mcVarData(i)%mark_idx, 2) = in_doy
                        mcVarData(i)%mdData(mcVarData(i)%mark_idx, 3) = in_hour
                        mcVarData(i)%mdData(mcVarData(i)%mark_idx, 4) = simu_value
                        mcVarData(i)%mark_idx = mcVarData(i)%mark_idx + 1
                    endif
                end if
            endif
        enddo
    end subroutine get_simu_data


    subroutine deallocate_mcmc()
        implicit NONE
        integer :: i
        do i = 1, numObsFiles
            if(allocated(mcVarData(i)%obsData)) deallocate(mcVarData(i)%obsData)
            if(allocated(mcVarData(i)%mdData))  deallocate(mcVarData(i)%mdData)
        enddo
    end subroutine deallocate_mcmc
end module mcmc_mod