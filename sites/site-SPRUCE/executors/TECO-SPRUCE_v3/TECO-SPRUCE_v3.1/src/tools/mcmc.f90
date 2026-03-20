module mcmc
    use datatypes
    use driver ! run teco simulation
    use mcmc_mod

    integer npar4DA ! number of parameters to be optimize
    integer upgraded, iDAsimu

    logical :: do_cov2create_new_params ! if do cov to create new parameters
    real(8), allocatable :: DAparVal(:)
    real(8), allocatable :: DAparMin(:)
    real(8), allocatable :: DAparMax(:)
    real(8), allocatable :: DAparOldVal(:)
    integer, allocatable :: idxStPar(:), idxFixStep(:)
    ! ------- divide different parameters for different observations ---------
    integer, allocatable :: idxSpecialPars_st(:), idxSpecialPars_sp(:,:)

    type(index_species_parameters_to_opt) :: idxSpPar(numPFT) ! type defined in MCMC_mod
    real(8), allocatable ::  search_scale(:) ! fixed step for parameters

    real(8) J_last(numObsFiles), J_new(numObsFiles)
    ! save parameters in MCMC processes
    real(8), allocatable :: totMcParamsets(:,:) ! upgraded, nParams
    ! mark paramters or observation after 100 cyles no paramters updated
    integer, allocatable :: mark_idx_obs(:) ! for each observation file, 1: updated, 0: not updated
    integer :: mark_simu, isave
    real(8) :: scale_sel_params
    real(8), allocatable :: mark_obs(:,:)

    integer, parameter :: num_steps = 15
    integer, dimension(num_steps) :: nsteps
    ! integer, parameter :: istep_1 = 1001, istep_2 = 2001, istep_3 = 3001 ! all, tree, shrub
    ! integer, parameter :: istep_4 = 4001, istep_5 = 5001, istep_6 = 6001 ! sphag, soil, annual ch4+Rh
    ! integer, parameter :: istep_7 = 7001, istep_8 = 8001, istep_9 = 9001 ! annual Tree, Shrub, Sphag
    ! integer, parameter :: istep_10 = 10001 
    ! integer, parameter :: istep_11 = 11001, istep_12 = 12001, istep_13 = 13001
    ! integer, parameter :: istep_14 = 14001, istep_15 = 15001


    integer, parameter :: istep_1 = 4001, istep_2 = 8001, istep_3 = 12001 ! all, tree, shrub
    integer, parameter :: istep_4 = 16001, istep_5 = 20001, istep_6 = 24001 ! sphag, soil, annual ch4+Rh
    integer, parameter :: istep_7 = 28001, istep_8 = 32001, istep_9 = 36001 ! annual Tree, Shrub, Sphag
    integer, parameter :: istep_10 = 40001 
    integer, parameter :: istep_11 = 44001, istep_12 = 12001, istep_13 = 13001
    integer, parameter :: istep_14 = 14001, istep_15 = 15001
    

    contains

    subroutine run_mcmc(st)
        implicit none
        type(site_data_type), intent(inout) :: st
        integer :: temp_upgraded, i, nDAsimu
        real(8) :: upgraded_rate, sum_j_new, sum_j_last, temp_new_parval(npar4DA)

        print*, "# Start to run MCMC ... "
        call random_seed()
        ! 1. generate new parameters
        ! call generate_new_parameters()
        allocate(mark_obs(100, numObsFiles))
        upgraded = 0
        mark_simu = 0
        scale_sel_params = 0.1
        ! 2. do cylces
        ! nDAsimu = istep_4-1 !mcset%nDAsimu
        nDAsimu = 50000
        nsteps = (/ istep_1, istep_2, istep_3, istep_4, istep_5, &
                    istep_6, istep_7, istep_8, istep_9, istep_10, &
                    istep_11, istep_12, istep_13, istep_14, istep_15/)
        do iDAsimu = 1, nDAsimu !mcset%nDAsimu
            upgraded_rate = (real(upgraded, 8) / real(iDAsimu, 8))!*100

            sum_j_new = J_last(15)
            sum_j_last = J_last(19)

            write(*, '(I6, A1, I6, 2X, F12.2, 2X, F12.2, 2X, F12.2, 2X, F12.2, 2X, I6, 2X, F6.2, 2X, F8.6, 2X, F8.6)') &
            iDAsimu, '/', nDAsimu, sum_j_new, sum_j_last,sum(J_new), sum(J_last), upgraded, upgraded_rate, search_scale(1), &
            search_scale(idxFixStep(1))

            do i = 1, numObsFiles
                mcVarData(i)%mark_idx = 1
            end do

            ! update mcparams
            if(iDAsimu > 1) call generate_new_parameters()
            temp_new_parval = DAparVal
            DAparVal = DAparOldVal            


            if(iDAsimu < istep_1) then ! 1000
                ! shrub 
                obsWt = [0, 1000, 0, 100, 1000, 1000, 10, 10, & 
                         10, 0, 10, 10, 10, 10, &
                         10, 100, 100, 100, 10, 100, 0, 0, 1000,10]
                DAparVal(159:164) = temp_new_parval(159:164)
                DAparVal(166:168) = temp_new_parval(166:168)
                DAparVal(170:173) = temp_new_parval(170:173)
                DAparVal(175)     = temp_new_parval(175)
                DAparVal(178:182) = temp_new_parval(178:182)
                DAparVal(184:256) = temp_new_parval(184:256)
            elseif(iDAsimu < istep_2) then ! 1000
                ! tree
                obsWt = [0, 10, 0, 10, 10, 10, 10, 10, & 
                         1000, 0, 1000, 1000, 100, 10, &
                         10, 100, 100, 100, 10, 10, 0, 0, 10,1000]
                DAparVal(61:66)   = temp_new_parval(61:66)
                DAparVal(68:70)   = temp_new_parval(68:70)
                DAparVal(72:75)   = temp_new_parval(72:75)
                DAparVal(77)      = temp_new_parval(77)
                DAparVal(80:84)   = temp_new_parval(80:84)
                DAparVal(86:158)  = temp_new_parval(86:158)
            elseif(iDAsimu < istep_3) then ! 1000
                ! sphagnum 
                obsWt = [0, 10, 0, 10, 10, 10, 1000, 1000, & 
                         10, 0, 10, 10, 10, 10, &
                         10, 100, 100, 100, 10, 10, 0, 0, 10,10]
                DAparVal(257:262)   = temp_new_parval(257:262)
                DAparVal(264:266)   = temp_new_parval(264:266)
                DAparVal(268:271)   = temp_new_parval(268:271)
                DAparVal(273)       = temp_new_parval(273)
                DAparVal(276:280)   = temp_new_parval(276:280)
                DAparVal(282:354)   = temp_new_parval(282:354)
            elseif(iDAsimu < istep_4) then ! 1000
                obsWt = [0, 10, 0, 10, 10, 10, 10, 10, & 
                         10, 0, 10, 10, 10, 10, &
                         100, 1000, 1000, 1000, 100, 10, 0, 0, 10,10]
                ! er
                DAparVal(170:172) = temp_new_parval(170:172)
                DAparVal(184:256) = temp_new_parval(184:256)
                DAparVal(268:270) = temp_new_parval(268:270)
                DAparVal(282:354) = temp_new_parval(282:354)
                ! gpp shrub
                DAparVal(184:199) = temp_new_parval(184:199)
                DAparVal(208:231) = temp_new_parval(208:231)
                DAparVal(240:248) = temp_new_parval(240:248)
               ! gpp sphagnum
                DAparVal(282:297) = temp_new_parval(282:297)
                DAparVal(306:329) = temp_new_parval(306:329)
                DAparVal(338:346) = temp_new_parval(338:346) ! s_npp
            elseif(iDAsimu < istep_5)then ! 1000
                ! shrub npp
                obsWt = [0, 1000, 0, 100, 100, 100, 10, 10, & 
                         10,  0, 10, 10, 10, 10, &
                         10, 100, 100, 100, 10, 100, 0, 0, 1000,10]
                DAparVal(161:164) = temp_new_parval(161:164)
                DAparVal(166:168) = temp_new_parval(166:168)
                DAparVal(170:172) = temp_new_parval(170:172)
                DAparVal(232:239) = temp_new_parval(232:239)
                DAparVal(240:248) = temp_new_parval(240:248)
                DAparVal(249:256) = temp_new_parval(249:256)
            elseif(iDAsimu < istep_6)then ! 1000
                ! tree npp
                obsWt = [0, 10, 0, 10, 10, 10, 10, 10, & 
                         1000, 0, 1000, 100, 100, 10, &
                         10, 100, 100, 100, 10, 10, 0, 0, 10,1000]
                DAparVal(63:66)   = temp_new_parval(63:66)
                DAparVal(68:70)   = temp_new_parval(68:70)
                DAparVal(72:74)   = temp_new_parval(72:74)
                DAparVal(134:141) = temp_new_parval(134:141)
                ! npp tree
                DAparVal(142:150) = temp_new_parval(142:150)
                DAparVal(151:158) = temp_new_parval(151:158)
            elseif(iDAsimu < istep_7)then 
                ! bnpp shrub
                obsWt = [0, 100, 0, 10, 10, 10, 10, 10, & 
                         100, 0, 10, 10, 10, 10, &
                         10, 10, 10, 10, 10, 10, 0, 0, 1000,10]
            
                DAparVal(232:256) = temp_new_parval(232:256)

                DAparVal(159:164) = temp_new_parval(159:164)
                DAparVal(166:168) = temp_new_parval(166:168)
                DAparVal(170:172) = temp_new_parval(170:172)
            elseif(iDAsimu < istep_8)then 
                ! bnpp tree
                obsWt = [0, 100, 0, 10, 10, 10, 10, 10, & 
                         100, 0, 10, 10, 10, 10, &
                         10, 10, 10, 10, 10, 10, 0, 0, 10,1000]
                DAparVal(61:66)   = temp_new_parval(61:66)
                DAparVal(68:70)   = temp_new_parval(68:70)
                DAparVal(72:74)   = temp_new_parval(72:74)
                DAparVal(134:158) = temp_new_parval(134:158)
                
            elseif(iDAsimu < istep_9)then ! 1000
                ! sphagnum npp
                obsWt = [0, 10, 0, 10, 10, 10, 1000, 1000, & 
                         10, 0, 10, 10, 10, 10, &
                         10, 100, 100, 100, 10, 10, 0, 0, 10,10]
                DAparVal(259:262) = temp_new_parval(259:262)
                DAparVal(264:266) = temp_new_parval(264:266)
                DAparVal(268:270) = temp_new_parval(268:270)
                DAparVal(330:337) = temp_new_parval(330:337)
                DAparVal(338:346) = temp_new_parval(338:346)
                DAparVal(347:354) = temp_new_parval(347:354)

            elseif(iDAsimu < istep_10)then ! 1000
                ! soil
                obsWt = [0, 10, 0, 10, 10, 10, 10, 10, & 
                         10, 0, 10, 10, 10, 1000, &
                         100, 100, 100, 100, 100, 10, 0, 0, 10,10]
                DAparVal(1:5)   = temp_new_parval(1:5)
                DAparVal(7:15)  = temp_new_parval(7:15)
                DAparVal(17:22) = temp_new_parval(17:22)
                DAparVal(23:25) = temp_new_parval(23:25)
                DAparVal(26)    = temp_new_parval(26)
                DAparVal(27:28) = temp_new_parval(27:28)
                DAparVal(29:60) = temp_new_parval(29:60)
            elseif(iDAsimu < istep_11) then
                ! CH4 and Rh
                obsWt = [0, 10, 0, 10, 10, 10, 10, 10, & 
                         10, 0, 10, 10, 10, 100, &
                         100000, 100, 100, 100, 1000000, 10, 0, 0, 10,10]
                DAparVal(1:14)  = temp_new_parval(1:14)
                ! DAparVal(15:22) = temp_new_parval(15:22)
                DAparVal(26)    = temp_new_parval(26)
                ! DAparVal(29:44) = temp_new_parval(29:44)
                DAparVal(29:52) = temp_new_parval(29:52)
                ! DAparVal(45)    = temp_new_parval(45)
                ! DAparVal(53)    = temp_new_parval(53)
                
            else
                ! CH4 and Rh
                obsWt = [0, 10, 0, 10, 10, 10, 10, 10, & 
                         10, 0, 10, 10, 10, 100, &
                         10000, 100, 100, 100, 10000, 10, 0, 0, 10,10]
                DAparVal(15:22) = temp_new_parval(15:22)
                DAparVal(26)    = temp_new_parval(26)
                DAparVal(29:52) = temp_new_parval(29:52)
                
            endif

            
            ! make sure the consistent cost function results if changing the weights
            ! 1. parameters equal old parameters
            do i = 1, num_steps
                if(iDAsimu == nsteps(i)) DAparVal = DAparOldVal
            enddo

            call update_mcParams(DAparVal, idxStPar, idxSpPar)
            call update_simuParams() ! update parameters to simulate

            call initialize_teco(st)
            call teco_simu(st, .False.)

            temp_upgraded = upgraded
            call cal_cost_function() ! check if update upgraded
            if(iDAsimu == 1) J_last = J_new
            ! make sure the consistent cost function results if changing the weights
            ! 2. J_last = J_new
            do i = 1, num_steps
                if(iDAsimu == nsteps(i)) J_last = J_new
            enddo
            
            ! do i = 1, size(idxFixStep)
            !     search_scale(idxFixStep(i)) = scale_sel_params
            ! enddo
            if(iDAsimu == 1)       search_scale = 0.05! mcset%search_scale
            if(iDAsimu == istep_1) search_scale = 0.01! mcset%search_scale
            if(iDAsimu == istep_2) search_scale = 0.08! mcset%search_scale
            if(iDAsimu == istep_3) search_scale = 0.08! mcset%search_scale
            if(iDAsimu == istep_4) search_scale = 0.08! mcset%search_scale
            if(iDAsimu == istep_5) search_scale = 0.08! mcset%search_scale
            if(iDAsimu == istep_6) search_scale = 0.01! mcset%search_scale
            if(iDAsimu == istep_7) search_scale = 0.01! mcset%search_scale
            if(iDAsimu == istep_8) search_scale = 0.08! mcset%search_scale
            if(iDAsimu == istep_9) search_scale = 0.08! mcset%search_scale
            if(iDAsimu == istep_10) search_scale = 0.08! mcset%search_scale
            if(iDAsimu == istep_11) search_scale = 0.08
            if(iDAsimu == istep_12) search_scale = 0.08
            if(iDAsimu == istep_13) search_scale = 0.1
            if(iDAsimu == istep_14) search_scale = 0.1
            if(iDAsimu == istep_15) search_scale = 0.1



            search_scale = search_scale * (1.0 + 0.01*(upgraded_rate - 0.25))
            search_scale = max(0.000001, min(0.5, search_scale))

            search_scale(134:158) = 0.1
            search_scale(232:256) = 0.1
            ! search_scale(61) = 0.1
            ! search_scale(63) = 0.1
            ! search_scale(68) = 0.1
            ! search_scale(80) = 0.1
            
            if(upgraded > temp_upgraded)then
                ! DAparOldVal = DAparVal
                ! totMcParamsets(upgraded,:) = DAparVal
                totMcParamsets(upgraded,:) = DAparOldVal
                ! mark_simu = 0
            endif

            

            if(upgraded > 5)then
                isave = 1
                if(mod(iDAsimu, 4000) == 0) then
                    call mcmc_param_outputs(upgraded, st)
                endif
                ! do i = 1,num_step
                !     if(iDAsimu == nsteps(i)-1)then
                !         if(iDAsimu < istep_9) then 
                !             isave = 1
                !         else
                !             isave = 2
                !         endif
                !         call mcmc_param_outputs(upgraded, st)
                !     endif
                ! enddo
            endif
            
            ! if(iDAsimu > 5000) then
            !     if(mark_simu > 100) then
            !         scale_sel_params = scale_sel_params * 0.998
            !     else
            !         scale_sel_params = scale_sel_params * 1.002
            !     endif
            ! endif
            ! scale_sel_params = max(min(scale_sel_params, 0.2), 0.001)
            
        enddo
        isave = 1
        call mcmc_param_outputs(upgraded, st)
        ! 3. outputs 
    end subroutine run_mcmc

    subroutine init_mcmc(in_mcConfNmlFile)
        implicit none
        character(*), intent(in) :: in_mcConfNmlFile
        integer :: i

        call read_mcmc_conf_nml(in_mcConfNmlFile)
        ! get paramters to optimize
        call filter_params_to_optimize()
        ! print*, "idxFixStep: ", idxFixStep
        allocate(search_scale(npar4DA))
        search_scale = mcset%search_scale
        do i = 1, size(idxFixStep)
            search_scale(idxFixStep(i)) = 0.02
        enddo

        J_last = 9000000.0
        ! initilize outputs
        allocate(totMcParamsets(mcset%nDAsimu, npar4DA))
    end subroutine init_mcmc

    subroutine filter_params_to_optimize()
        implicit none
        real(8), dimension(numAllParams) :: temp_parmin, temp_parmax, temp_parval
        integer :: temp_idx_st(numStParams)
        integer :: i, j, nSpPar4DA(numPFT), nStPar4DA, iFixStep
        type(index_species_parameters_to_opt) :: temp_idx_sp(numPFT)
        ! setting different parameters for different observations
        integer :: temp_sp_params(npft, numSpParams), idx_mark, idxPar_st

        character(50) :: arr_keyParams(15)
        allocate(idxFixStep(24))! 6 for site parameters, 9 * 2 for species parameters

        ! arr_keyParams=["Tau_F       ", "Tau_C       ", "Tau_Micro   ", "Tau_SlowSOM ", &
        !                "Tau_Passive ", "Q10rh       ", "Q10pro      ", "Vmaxfraction", &
        !                "SLAx        ", "GLmax       ", "GRmax       ", "Gsmax       ", &
        !                "Vcmax0      ", "Tau_Leaf    ", "Q10         ", "Rl0         ", &
        !                "JV          ", "fn2r        "]

        arr_keyParams=["Tau_F       ", "Tau_C       ", "Tau_Micro   ", &
                       "Q10rh       ", "Q10pro      ", "r_me        ",  &
                       "SLAx        ", "GLmax       ", "GRmax       ", "Gsmax       ", &
                       "Vcmax0      ", "Q10         ", "Rl0         ", &
                       "JV          ", "fn2r        "]!, "s_cLeaf     ", "s_cStem     "]

        npar4DA = 0
        idxPar_st  = 0
        iFixStep = 0
        print*, "# check all parameters to be optimized ..."
        do i = 1, numStParams
            if(mcParams%st(i)%parmax > mcParams%st(i)%parmin) then
                npar4DA = npar4DA + 1
                temp_parmin(npar4DA) = mcParams%st(i)%parmin
                temp_parmax(npar4DA) = mcParams%st(i)%parmax
                temp_parval(npar4DA) = mcParams%st(i)%parVal
                temp_idx_st(npar4DA) = i
                print*, npar4DA, trim(mcParams%st(i)%parName), mcParams%st(i)%parVal, &
                        mcParams%st(i)%parmin, mcParams%st(i)%parmax
                if(any(mcParams%st(i)%parName == arr_keyParams))then
                    iFixStep = iFixStep + 1
                    idxFixStep(iFixStep) = npar4DA
                    print*, iFixStep,"fixed step for ", trim(mcParams%st(i)%parName), " at ", npar4DA
                endif
            endif
        enddo

        nStPar4DA = npar4DA
        ! site-based parameters
        allocate(idxSpecialPars_st(nStPar4DA))
        idxPar_st = idxPar_st + 1
        idxSpecialPars_st(idxPar_st) = npar4DA
        do i = 1, nStPar4DA
            idxSpecialPars_st(i) = i
        enddo

        do i = 1, numPFT
            nSpPar4DA(i) = 0
            allocate(temp_idx_sp(i)%idx(numSpParams))
            idx_mark = 0
            do j = 1, numSpParams
                if(mcParams%sp(i)%var(j)%parmax > mcParams%sp(i)%var(j)%parmin)then
                    npar4DA = npar4DA + 1
                    nSpPar4DA(i) = nSpPar4DA(i) + 1
                    temp_parmin(npar4DA) = mcParams%sp(i)%var(j)%parmin
                    temp_parmax(npar4DA) = mcParams%sp(i)%var(j)%parmax
                    temp_parval(npar4DA) = mcParams%sp(i)%var(j)%parVal
                    temp_idx_sp(i)%idx(nSpPar4DA(i)) = j
                    print*, npar4DA, trim(mcParams%sp(i)%var(j)%parName), mcParams%sp(i)%var(j)%parVal, &
                        mcParams%sp(i)%var(j)%parmin, mcParams%sp(i)%var(j)%parmax
                    
                    idx_mark = idx_mark + 1
                    temp_sp_params(i, idx_mark) = npar4DA
                    if(i>1)then ! except tree
                        if(any(mcParams%sp(i)%var(j)%parName == arr_keyParams))then
                            iFixStep = iFixStep + 1
                            idxFixStep(iFixStep) = npar4DA
                            print*, iFixStep, "fixed step for ", trim(mcParams%sp(i)%var(j)%parName), " at ", npar4DA
                        endif
                    endif
                endif
            enddo
            allocate(idxSpPar(i)%idx(nSpPar4DA(i)))
            idxSpPar(i)%idx = temp_idx_sp(i)%idx(:nSpPar4DA(i))
            if(allocated(temp_idx_sp(i)%idx)) deallocate(temp_idx_sp(i)%idx)
        enddo

        allocate(idxSpecialPars_sp(3, nSpPar4DA(1)))
        idxSpecialPars_sp = temp_sp_params(:,:nSpPar4DA(1))

        allocate(DAparMax(npar4DA), DAparMin(npar4DA), DAparVal(npar4DA), DAparOldVal(npar4DA), idxStPar(nStPar4DA))

        DAparMin = temp_parmin(:npar4DA)
        DAparMax = temp_parmax(:npar4DA)
        DAparVal = temp_parval(:npar4DA)
        idxStPar = temp_idx_st(:nStPar4DA)
        DAparOldVal = DAparVal

    end subroutine filter_params_to_optimize

    subroutine generate_new_parameters()
        implicit NONE
        integer :: i
        real(8) :: rand_harvest, rand

        
        if (do_cov2create_new_params) then
            
        else
        ! do normal update parameters
            do i = 1, npar4DA
                do
                    call random_number(rand_harvest)
                    rand = rand_harvest - 0.5 
                    ! Ensure parameter is within bounds
                    if(DAparOldVal(i) > DAparMax(i) .or. DAparOldVal(i) < DAparMin(i))then
                        DAparOldVal(i) = DAparMin(i) + rand_harvest*(DAparMax(i)-DAparMin(i))
                    endif
                    if(DAparMin(i) .eq. DAparMax(i)) then
                        DAparOldVal(i) = DAparMax(i)
                    endif
                    ! generate new parameters
                    DAparVal(i) = DAparOldVal(i) + rand*(DAparMax(i)-DAparMin(i))*search_scale(i)
                    ! if out of bounds, retry with a new random number
                    if (DAparVal(i) >= DAparMin(i) .and. DAparVal(i) <= DAparMax(i)) then
                        exit
                    else
                        DAparVal(i) = DAparOldVal(i)
                    endif
                enddo
            end do
        endif

    end subroutine

    subroutine cal_cost_function()
        implicit none
        integer i, mark_upgrade
        real(8) :: JCost, delta_J(numObsFiles), cs_rand, mark_jcost(numObsFiles)
        ! real(8) :: delta_J_norm(numObsFiles),mean_delta_J,std_delta_J
        ! real(8) :: mean_obs, std_obs

        J_new = 0.
        do i = 1, numObsFiles
            if(mcVarData(i)%existOrNot)then
                call calculate_cost(mcVarData(i)%obsData(:,4), mcVarData(i)%mdData(:,4),JCost)
                J_new(i) = J_new(i) + JCost*100*obsWt(i)
                mark_jcost(i) = JCost
            end if
            ! if(mod(iDAsimu, 1000) == 0) then 
                ! print*, "test_", i,": ", J_new(i) - J_last(i), J_last(i),J_new(i)
            ! endif
        enddo

        delta_J = J_new - J_last

        mark_upgrade = 0

        call random_number(cs_rand)
        if(AMIN1(1.0, exp(-sum(delta_J))) .gt. cs_rand)then
            DAparOldVal = DAparVal
            mark_upgrade = 1
            J_last = J_new
        endif
        
        if(mark_upgrade == 1) upgraded = upgraded + 1

    end subroutine

    ! subroutine calculate_cost(datObs, datMd, stdObs, JCost)
    !     implicit none
    !     real(8), intent(in) :: datObs(:), datMd(:), stdObs(:)
    !     real(8) :: JCost, dObsSimu, std4cal
    !     integer :: nLine, iLine, nCost
    !     nLine = size(datObs)
    !     nCost = 0
    !     JCost = 0.
    !     do iLine = 1, nLine
    !         if(datObs(iLine)>-999. .and. datMd(iLine)>-999.)then
    !             nCost = nCost+1
    !             dObsSimu = datMd(iLine) - datObs(iLine)
    !             std4cal  = max(0.2 * abs(datObs(iLine)), 0.5)
    !             if (std4cal <= 0) std4cal = 1.0
    !             JCost = JCost + (dObsSimu*dObsSimu)/(2.*std4cal*std4cal)
    !         endif
    !     enddo
    !     if(nCost .gt. 0) JCost=JCost/real(nCost)
    ! end subroutine

    subroutine calculate_cost(datObs, datMd, JCost)
        implicit none
        real(8), intent(in) :: datObs(:), datMd(:)!, stdObs(:)
        real(8) :: JCost, obs_mean, numerator, denominator
        integer :: n, i
        n = size(datObs)
        obs_mean = sum(datObs)/n
        numerator = 0.0
        denominator = 0.0
        do i = 1, n
            if(datObs(i)>-999. .and. datMd(i)>-999.)then
                numerator = numerator + (datMd(i) - datObs(i))**2
                denominator = denominator + (datObs(i) - obs_mean)**2
            endif
        end do
        if (denominator > 0.0) then
            JCost = (numerator / denominator)
        elseif (size(datObs) .eq. 1) then
            Jcost = numerator/(datObs(1) - 83094)**2
        elseif (size(datObs) .eq. 2) then
            ! if(denominator < 0.01*numerator)then
                JCost = numerator/2
            ! endif
        else
            JCost = numerator/n
        endif
        ! if(nCost .gt. 0) JCost=JCost/real(nCost)
        return
    end subroutine calculate_cost


    subroutine mcmc_param_outputs(nUpgraded, st)
        implicit none
        integer, intent(in) :: nUpgraded!, npar4DA, idxStPar(:)
        ! type(index_species_parameters_to_opt), intent(in) :: idxSpPar(:)
        type(site_data_type), intent(inout) :: st

        integer :: npar4st, npar4sp
        integer :: nBuilt_in, ipar, inum, ipft, isimu!, iline
        ! character(250) :: outfile_mc_ParamSets
        character(6000) :: header_line
        integer, allocatable :: rand_number(:)
        real(8), allocatable :: nonBuiltParamsets(:,:), randSelParamsets(:,:)
        character(100) :: mc_str_params

        npar4st = size(idxStPar)
        ! keep unless 1 parameter set
        nBuilt_in = max(1, int(0.1 * nUpgraded))
        ! get csv header
        header_line = ""
        do ipar = 1, npar4st
            if(ipar .eq. 1) then 
                header_line=trim(mcParams%st(idxStPar(ipar))%parname)
            else
                header_line = trim(header_line) // "," // trim(mcParams%st(idxStPar(ipar))%parname)
            endif
        end do

        do ipft = 1, npft
            npar4sp = size(idxSpPar(ipft)%idx)
            do ipar = 1, npar4sp
                header_line = trim(header_line) // "," // &
                    trim(mcParams%sp(ipft)%var(idxSpPar(ipft)%idx(ipar))%parname) // "_" // sp_names(ipft)
            end do
        end do
        ! copy parameters set (drop built-in part)
        allocate(nonBuiltParamsets(nUpgraded - nBuilt_in, npar4DA))
        nonBuiltParamsets = totMcParamsets(nBuilt_in:nUpgraded, :)
        ! write parameter sets to csv
        write(mc_str_params, "(I0.3)") isave
        call write_parameter_sets(trim(outDir_mcmc) // "/total_parameter_sets_"//adjustl(trim(mc_str_params))//".csv", &
            header_line, nonBuiltParamsets)
        ! random select 100 parameter set
        allocate(rand_number(mcset%nRand))
        call generate_random_numbers(1, nUpgraded - nBuilt_in, rand_number)
        allocate(randSelParamsets(mcset%nRand, npar4DA))
        do inum = 1, mcset%nRand
            randSelParamsets(inum, :) = nonBuiltParamsets(rand_number(inum), :)
        end do
        ! write selected parameter sets
        call write_parameter_sets(trim(outDir_mcmc) // "/sel_parameter_sets.csv", header_line, randSelParamsets)

        ! run teco model according to the select variables
        ! do_out_hr   = .True.
        ! do_out_day  = .True.
        ! do_out_mon  = .False.
        ! do_out_yr   = .False.
        do isimu = 1, 1!mcset%nRand
            write(mc_str_n, "(I0.3)") isave
            ! call update_mcParams(randSelParamsets(isimu, :), idxStPar, idxSpPar)
            call update_mcParams(totMcParamsets(nUpgraded, :), idxStPar, idxSpPar)
            call update_simuParams() ! update parameters to simulate

            call initialize_teco(st)
            call teco_simu(st, .True.)
        end do

        deallocate(rand_number)
        deallocate(nonBuiltParamsets)
        deallocate(randSelParamsets)
    end subroutine mcmc_param_outputs

    subroutine generate_random_numbers(min_value, max_value, res_rand)
        implicit none
        integer, dimension(:), intent(inout) :: res_rand
        integer, intent(in) :: min_value, max_value
        integer :: i, j, temp!, range_size, available_numbers
        integer, dimension(max_value - min_value + 1) :: all_numbers
        real(8) :: r

        ! initialize the random
        ! call random_seed()

        ! initilize all_numbers array
        do i = 1, size(all_numbers)
            all_numbers(i) = min_value - 1 + i
        end do

        ! using Fisher-Yates method
        do i = size(all_numbers), 2, -1
            call random_number(r)
            j = int(r * i) + 1
            temp = all_numbers(i)
            all_numbers(i) = all_numbers(j)
            all_numbers(j) = temp
        end do

        ! get the before N random number 
        res_rand = all_numbers(1:size(res_rand,1))
    end subroutine generate_random_numbers

    subroutine write_parameter_sets(filename, header, paramsets)
        implicit none
        character(*), intent(in) :: filename, header
        real(8), intent(in) :: paramsets(:, :)
        integer :: iline
    
        open(118, file=filename, status='replace')
        write(118, *) trim(header)
        do iline = 1, size(paramsets, 1)
            write(118, '(*(ES20.12,:,","))') paramsets(iline,:)
        end do
        close(118)
    end subroutine write_parameter_sets

    subroutine mcmc_vars_deallocate()
        implicit none
        integer i

        do i = 1, numPFT
            if (allocated(idxSpPar(i)%idx)) deallocate(idxSpPar(i)%idx)
        enddo
        if(allocated(totMcParamsets)) deallocate(totMcParamsets)
        if(allocated(DAparVal)) deallocate(DAparVal)
        if(allocated(DAparMin)) deallocate(DAparMin)
        if(allocated(DAparMax)) deallocate(DAparMax)
        if(allocated(DAparOldVal)) deallocate(DAparOldVal)
        if(allocated(idxStPar))    deallocate(idxStPar)
        if(allocated(idxFixStep))  deallocate(idxFixStep)
        if(allocated(search_scale)) deallocate(search_scale)
        if(allocated(mark_obs)) deallocate(mark_obs)
        if(allocated(idxSpecialPars_st)) deallocate(idxSpecialPars_st)
        if(allocated(idxSpecialPars_sp)) deallocate(idxSpecialPars_sp)
    end subroutine mcmc_vars_deallocate
    
end module mcmc