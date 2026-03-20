module transfer
    use datatypes
    USE, INTRINSIC :: IEEE_ARITHMETIC
    implicit none

    contains

    subroutine TCS_CN(st, iforcing)  
        implicit none
        type(site_data_type),    intent(inout) :: st
        type(forcing_data_type), intent(in)    :: iforcing
        integer :: i, j, ipft
        real(8) :: frac_soc(10), Q10h(5) ! soil thermal
        real(8) :: Qroot0, Nfix0, Nup0, Cfix0, ksye
        real(8) :: Creuse0, ScNloss, N_deN0, LDON0
        real(8) :: CNmin, CNmax, S_w_min, S_omega, S_t(5)
        real(8) :: SNfine,SNcoarse,SNmicr,SNslow,SNpass
        real(8) :: Scalar_N_T, kappaVcmax!, w_QNminer

        frac_soc = (/0.75,0.2,0.02,0.015,0.005,0.0,0.0,0.0,0.0,0.0/)  ! Jian: calculate the scales of Temperature and Scale_N_T
        Q10h     = (/st%Q10rh,st%Q10rh,st%Q10rh,st%Q10rh,st%Q10rh/)   ! Lit_m, lit_s, soil_f, soil_s, soil_p
        Qroot0   = 500.
        Nfix0    = 1./60.          ! maximum N fix ratio, N/C
        Nup0     = 0.02            ! nitrogen uptake rate
        Cfix0    = 12.             ! C cost per N for fixation
        ksye     = 0.05            ! C cost per N for uptake
        Creuse0  = 2.              ! C cost per N for resorption
        ScNloss  = 1.
        N_deN0   = 1.E-3*ScNloss  
        LDON0    = 1.E-3*ScNloss 
        ! for N scalars
        CNmin    = 40.0
        CNmax    = 200.0
        ! calculating soil scaling factors, S_omega and S_tmperature
        S_w_min  = 0.08 ! minimum decomposition rate at zero soil moisture
        S_omega  = S_w_min + (1.-S_w_min) * Amin1(1.0, 0.3*st%omega)
        ! calculate the scale of temperature
        if (do_soilphy) then
            S_t=(/0.0,0.0,0.0,0.0,0.0/)
            do i=1,5
                if(i.lt.3) then    ! couarse and fine litter use surface layer soil temperature
                    S_t(i)=Q10h(i)**((st%tsoil_layer(2)-Tref_rh)/10.)  ! Duke
                else 
                    do j=1,10       ! fast,slow and passive pool use weighed soil temperature in layers according to soc distribution
                        S_t(i)=S_t(i)+frac_soc(j)*Q10h(i)**((st%tsoil_layer(j+1)-Tref_rh)/10.)  ! Duke
                    enddo
                endif
            enddo
        else
            do i=1,5
                S_t(i)=Q10h(i)**((iforcing%Tsoil-Tref_rh)/10.)  ! Duke
            enddo  
        endif 
        ! calculate the allocation of NPP
        st%outC_L = 0.
        st%outC_S = 0.
        st%outC_R = 0.
        do ipft = 1, npft
            
            st%sp(ipft)%NPP_L = st%sp(ipft)%alpha_L * st%sp(ipft)%NPP
            st%sp(ipft)%NPP_W = st%sp(ipft)%alpha_W * st%sp(ipft)%NPP
            st%sp(ipft)%NPP_R = st%sp(ipft)%alpha_R * st%sp(ipft)%NPP
            ! the carbon leaving the vegetation pools
            st%sp(ipft)%outC_L = st%sp(ipft)%L_fall
            st%sp(ipft)%outC_W = st%sp(ipft)%cStem/st%sp(ipft)%Tau_Stem*S_omega !*exp(CN(2)/CN0(2)-1.) 
            st%sp(ipft)%outC_R = st%sp(ipft)%cRoot/st%sp(ipft)%Tau_Root*S_omega
            ! summary
            st%outC_L = st%outC_L + st%sp(ipft)%outC_L * st%sp(ipft)%pft_weight
            st%outC_S = st%outC_S + st%sp(ipft)%outC_W * st%sp(ipft)%pft_weight
            st%outC_R = st%outC_R + st%sp(ipft)%outC_R * st%sp(ipft)%pft_weight
        enddo
        ! N scalars on decomposition
        SNfine   = exp(-(st%CN0_lit_m  - st%CN_lit_m) /st%CN0_lit_m) 
        SNcoarse = exp(-(st%CN0_lit_s  - st%CN_lit_s) /st%CN0_lit_s) 
        SNmicr   = exp(-(st%CN0_soil_f - st%CN_soil_f)/st%CN0_soil_f) 
        SNslow   = exp(-(st%CN0_soil_s - st%CN_soil_s)/st%CN0_soil_s) 
        SNpass   = exp(-(st%CN0_soil_p - st%CN_soil_p)/st%CN0_soil_p) 
        ! the carbon leaving litter pools and soil pools
        st%outC_lit_m  = st%cLit_m/st%Tau_F       *S_omega* S_T(1)*st%CN_lit_m/st%CN0_lit_m * def_s_rh !*SNfine
        st%outC_lit_s  = st%cLit_s/st%Tau_C       *S_omega* S_T(2)*st%CN_lit_s/st%CN0_lit_s * def_s_rh !*SNcoarse
        st%outC_soil_f = st%cSoil_f/st%Tau_Micro  *S_omega* S_T(3) * def_s_rh !*SNmicr
        st%outC_soil_s = st%cSoil_s/st%Tau_SlowSOM*S_omega* S_T(4) * def_s_rh !*SNslow
        st%outC_soil_p = st%cSoil_p/st%Tau_Passive*S_omega* S_T(5) * def_s_rh !*SNpass
        ! heterotrophic respiration from each pool
        st%Rh_lit_m  = st%outC_lit_m  * (1. - st%f_F2M)
        st%Rh_lit_s  = st%outC_lit_s  * (1. - st%f_C2M - st%f_C2S)
        st%Rh_soil_f = st%outC_soil_f * (1. - st%f_M2S - st%f_M2P)
        st%Rh_soil_s = st%outC_soil_s * (1. - st%f_S2P - st%f_S2M)
        st%Rh_soil_p = st%outC_soil_p * (1. - st%f_P2M)
        ! ==================================================================
        ! Nitrogen part
        !!! Nitrogen leaving the pools and resorption
        st%outN_L = 0.
        st%outN_S = 0.
        st%outN_R = 0.
        do ipft = 1, npft
            st%sp(ipft)%outN_L = st%sp(ipft)%outC_L/st%sp(ipft)%CN_L
            st%sp(ipft)%outN_W = st%sp(ipft)%outC_W/st%sp(ipft)%CN_W
            st%sp(ipft)%outN_R = st%sp(ipft)%outC_R/st%sp(ipft)%CN_R
            ! summary:
            st%outN_L = st%outN_L + st%sp(ipft)%pft_weight * st%sp(ipft)%outN_L * (1 - st%sp(ipft)%alphaN)
            st%outN_S = st%outN_S + st%sp(ipft)%pft_weight * st%sp(ipft)%outN_W * (1 - st%sp(ipft)%alphaN)
            st%outN_R = st%outN_R + st%sp(ipft)%pft_weight * st%sp(ipft)%outN_R * (1 - st%sp(ipft)%alphaN)
        enddo
        st%outN_lit_m  = st%outC_lit_m/st%CN_lit_m
        st%outN_lit_s  = st%outC_lit_s/st%CN_lit_s
        st%outN_soil_f = st%outC_soil_f/st%CN_soil_f
        st%outN_soil_s = st%outC_soil_s/st%CN_soil_s
        st%outN_soil_p = st%outC_soil_p/st%CN_soil_p
        ! nitrogen mineralization
        st%N_miner = st%outN_lit_m  * (1. - st%f_F2M)  &
            &      + st%outN_lit_s  * (1. - st%f_C2M - st%f_C2S) &
            &      + st%outN_soil_f * (1. - st%f_M2S - st%f_M2P) &
            &      + st%outN_soil_s * (1. - st%f_S2P - st%f_S2M) &
            &      + st%outN_soil_p * (1. - st%f_P2M)
        ! Nitrogen immobilization
        st%N_imm_lit_m = 0.
        st%N_imm_lit_s = 0.
        st%N_imm_soil_f = 0.
        st%N_imm_soil_s = 0.
        st%N_imm_soil_p = 0.
        st%N_immob      = 0.
        if(st%QNminer>0)then
            st%N_imm_lit_m  = Amin1(st%cLit_m/st%CN0_lit_m   - st%cLit_m/st%CN_lit_m,   0.1*st%QNminer)
            st%N_imm_lit_s  = Amin1(st%cLit_s/st%CN0_lit_s   - st%cLit_s/st%CN_lit_s,   0.1*st%QNminer)
            st%N_imm_soil_f = Amin1(st%cSoil_f/st%CN0_soil_f - st%cSoil_f/st%CN_soil_f, 0.1*st%QNminer)
            st%N_imm_soil_s = Amin1(st%cSoil_s/st%CN0_soil_s - st%cSoil_s/st%CN_soil_s, 0.1*st%QNminer)
            st%N_imm_soil_p = Amin1(st%cSoil_p/st%CN0_soil_p - st%cSoil_p/st%CN_soil_p, 0.1*st%QNminer)
            st%N_immob      = st%N_immob + st%N_imm_lit_m  + st%N_imm_lit_s &
                            + st%N_imm_soil_f + st%N_imm_soil_s + st%N_imm_soil_p 
        endif
        ! Let plant itself choose the strategy between using C to uptake
        ! or fix N2 by comparing C invest.
        ! N demand
        st%N_uptake      = 0.
        st%Scalar_N_flow = 0. ! Loss of mineralized N and dissolved organic N
        st%N_fixation    = 0.
        st%N_transfer    = 0.
        st%NSN           = 0.
        ! if (st%QNminer <0) st%QNminer = 0.
        do ipft = 1, npft
            st%sp(ipft)%N_transfer  = 0.
            st%sp(ipft)%N_uptake    = 0.
            st%sp(ipft)%N_fixation  = 0.
            st%sp(ipft)%costCuptake = 0.
            st%sp(ipft)%costCfix    = 0.
            st%sp(ipft)%costCreuse  = 0.
            ! st%sp(ipft)%N_demand    = 0.
            st%sp(ipft)%N_demand = st%sp(ipft)%NPP_L/st%sp(ipft)%CN0_L &
                                 + st%sp(ipft)%NPP_W/st%sp(ipft)%CN0_S &
                                 + st%sp(ipft)%NPP_R/st%sp(ipft)%CN0_R !+ st%sp(ipft)%N_deficit
            ! Jian: nsn to match the nsc
            ! CNp0 = bmP/(bmL/st%sp(ipft)%CN0_L + bmR/st%sp(ipft)%CN0_R + bmS/st%sp(ipft)%CN0_S)  ! Plant CN ratio
            ! st%sp(ipft)%N_demand = st%sp(ipft)%N_demand + AMAX1(st%sp(ipft)%nsc/st%sp(ipft)%CNp0 - st%sp(ipft)%nsn, 0.)
            ! st%sp(ipft)%N_demand = st%sp(ipft)%N_demand + AMAX1(st%sp(ipft)%fnsc*Nfix0*st%sp(ipft)%NSC - st%sp(ipft)%nsn, 0.)
            ! st%sp(ipft)%N_demand = st%sp(ipft)%N_demand + st%sp(ipft)%N_deficit
            ! N input:  1. Nitrogen resorption
            st%sp(ipft)%test4 = st%sp(ipft)%N_demand
            st%sp(ipft)%N_transfer = Amax1((st%sp(ipft)%outN_L + st%sp(ipft)%outN_W &
                                     + st%sp(ipft)%outN_R)*st%sp(ipft)%alphaN, 0.)
            st%sp(ipft)%costCreuse = Amax1(Creuse0*st%sp(ipft)%N_transfer, 0.)
            st%sp(ipft)%N_demand   = st%sp(ipft)%N_demand - st%sp(ipft)%N_transfer
            ! w_QNminer = st%sp(ipft)%pft_weight*st%QNminer
            if(st%sp(ipft)%N_demand > 0.)then
                if(ksye/st%QNminer < Cfix0)then
                    ! N input: 2. N uptake
                    ! st%sp(ipft)%test5 = st%sp(ipft)%N_demand
                    ! st%sp(ipft)%test6 = st%sp(ipft)%N_deficit
                    ! st%sp(ipft)%test7 = w_QNminer
                    ! if(w_QNminer .le. 0) then
                    !     st%sp(ipft)%N_uptake = 0.
                    ! else
                        ! st%sp(ipft)%N_uptake = Amax1(AMIN1(st%sp(ipft)%N_demand + st%sp(ipft)%N_deficit, &
                        !                 &    st%QNminer * st%sp(ipft)%cRoot/(st%sp(ipft)%cRoot +Qroot0), &
                        !                 &    Nup0*st%sp(ipft)%NSC/(ksye/st%QNminer)), 0.) 
                    st%sp(ipft)%N_uptake = Amax1(AMIN1(st%sp(ipft)%N_demand + st%sp(ipft)%N_deficit, &
                                        &    st%QNminer * st%sp(ipft)%cRoot/(st%sp(ipft)%cRoot +Qroot0), &
                                        &    Nup0*st%sp(ipft)%NSC/(ksye/st%QNminer)), 0.)
                    ! endif
                    ! st%sp(ipft)%costCuptake = Amax1(st%sp(ipft)%N_uptake*(ksye/st%QNminer),0.)
                    st%sp(ipft)%costCuptake = st%sp(ipft)%N_uptake*(ksye/st%QNminer)
                    st%sp(ipft)%N_demand    = st%sp(ipft)%N_demand - st%sp(ipft)%N_uptake
                elseif(st%sp(ipft)%nsn < 24.*30.*st%sp(ipft)%N_demand)then
                    ! N input: 3. N fixation
                    st%sp(ipft)%test8 = st%sp(ipft)%N_demand
                    st%sp(ipft)%test9 = st%sp(ipft)%fnsc
                    
                    st%sp(ipft)%N_fixation = Amin1(st%sp(ipft)%N_demand,st%sp(ipft)%fnsc*Nfix0*st%sp(ipft)%NSC)
                    st%sp(ipft)%costCfix   = Cfix0*st%sp(ipft)%N_fixation
                    st%sp(ipft)%N_demand   = st%sp(ipft)%N_demand - st%sp(ipft)%N_fixation
                endif
            endif
            st%sp(ipft)%N_deficit = st%sp(ipft)%N_deficit + st%sp(ipft)%N_demand
            ! !!added by Chris to regulate NPP when N is limited
            ! st%sp(ipft)%NPP = st%sp(ipft)%NPP - st%sp(ipft)%N_deficit /&
            !      (st%sp(ipft)%alpha_L/st%sp(ipft)%CN_L + st%sp(ipft)%alpha_W/st%sp(ipft)%CN_W &
            !       + st%sp(ipft)%alpha_R/st%sp(ipft)%CN_R)
            ! st%sp(ipft)%GPP = st%sp(ipft)%GPP - st%sp(ipft)%N_deficit / &
            !      (st%sp(ipft)%alpha_L/st%sp(ipft)%CN_L + st%sp(ipft)%alpha_W/st%sp(ipft)%CN_W &
            !      + st%sp(ipft)%alpha_R/st%sp(ipft)%CN_R)
            ! st%sp(ipft)%NPP_L = st%sp(ipft)%alpha_L * st%sp(ipft)%NPP
            ! st%sp(ipft)%NPP_W = st%sp(ipft)%alpha_W * st%sp(ipft)%NPP
            ! st%sp(ipft)%NPP_R = st%sp(ipft)%alpha_R * st%sp(ipft)%NPP
            ! st%sp(ipft)%N_deficit = 0.
            ! update NSN
            st%sp(ipft)%NSN = st%sp(ipft)%NSN + st%sp(ipft)%N_transfer + st%sp(ipft)%N_uptake + st%sp(ipft)%N_fixation
            ! st%sp(ipft)%test1 = st%sp(ipft)%N_transfer + st%sp(ipft)%N_uptake + st%sp(ipft)%N_fixation
            ! Total C cost for nitrogen
            st%sp(ipft)%Rnitrogen = st%sp(ipft)%costCuptake + st%sp(ipft)%costCfix + st%sp(ipft)%costCreuse
            ! Nitrogen using, non-structural nitrogen pool, NSN
            st%sp(ipft)%N_leaf = Amax1(AMIN1(st%sp(ipft)%NPP*st%sp(ipft)%alpha_L/st%sp(ipft)%CN_L &
                                     + st%sp(ipft)%cLeaf/st%sp(ipft)%CN0_L &
                                     - st%sp(ipft)%cLeaf/st%sp(ipft)%CN_L, 0.2*st%sp(ipft)%NSN), 0.)
            ! st%sp(ipft)%test1  = st%sp(ipft)%NPP*st%sp(ipft)%alpha_L/st%sp(ipft)%CN_L &
            ! + st%sp(ipft)%cLeaf/st%sp(ipft)%CN0_L &
            ! - st%sp(ipft)%cLeaf/st%sp(ipft)%CN_L
            ! st%sp(ipft)%test2  =   0.2*st%sp(ipft)%NSN
            ! st%sp(ipft)%test3  = st%sp(ipft)%NPP*st%sp(ipft)%alpha_L   
            st%sp(ipft)%N_Stem = amax1(AMIN1(st%sp(ipft)%NPP*st%sp(ipft)%alpha_W/st%sp(ipft)%CN_W, 0.1*st%sp(ipft)%NSN),0.)
            st%sp(ipft)%N_root = AMAX1(AMIN1(st%sp(ipft)%NPP*st%sp(ipft)%alpha_R/st%sp(ipft)%CN_R &
                                     + st%sp(ipft)%cRoot/st%sp(ipft)%CN0_R &
                                     - st%sp(ipft)%cRoot/st%sp(ipft)%CN_R, 0.2*st%sp(ipft)%NSN), 0.)
            st%sp(ipft)%NSN    = st%sp(ipft)%NSN-(st%sp(ipft)%N_leaf+st%sp(ipft)%N_Stem+st%sp(ipft)%N_root)
            st%sp(ipft)%test2  = st%sp(ipft)%N_leaf+st%sp(ipft)%N_Stem+st%sp(ipft)%N_root

            st%sp(ipft)%N_LF   = st%sp(ipft)%outN_L * (1. - st%sp(ipft)%alphaN)
            st%sp(ipft)%N_WF   = st%sp(ipft)%outN_W * (1. - st%sp(ipft)%alphaN)
            st%sp(ipft)%N_RF   = st%sp(ipft)%outN_R * (1. - st%sp(ipft)%alphaN)

            ! summary and update N_uptake
            st%N_uptake      = st%N_uptake      + st%sp(ipft)%pft_weight * st%sp(ipft)%N_uptake
            
            st%N_fixation    = st%N_fixation    + st%sp(ipft)%pft_weight * st%sp(ipft)%N_fixation
            st%N_transfer    = st%N_transfer    + st%sp(ipft)%pft_weight * st%sp(ipft)%N_transfer
            st%NSN           = st%NSN           + st%sp(ipft)%pft_weight * st%sp(ipft)%nsn
        enddo
        ! update QNminer
        ! st%N_deposit =  2.34/8760.
        st%QNminer = st%QNminer + st%N_miner + st%N_deposit - (st%N_uptake + st%N_immob)
        ! commented line for soil thermal       
        ! Scalar_N_T=N_deN0*exp((Tsoil-25.)/10.)
        ! added lines for soil thermal
        if (do_soilphy) then 
            Scalar_N_T = 0.0 
            do j=1,10
                Scalar_N_T = Scalar_N_T + frac_soc(j)*N_deN0*exp((st%tsoil_layer(j+1)-25.)/10.)  
            enddo
        else
            Scalar_N_T=N_deN0*exp((iforcing%Tsoil-25.)/10.)
        endif 
        ! -------------------------------------------------------
        st%Scalar_N_flow = 0.5*st%runoff/st%rdepth
        st%N_leach = AMAX1(amin1(st%Scalar_N_flow*st%QNminer+st%Scalar_N_flow*st%cSoil_f*LDON0, 0.1*st%QNminer), 0.)
        st%N_vol   = AMAX1(amin1(Scalar_N_T*st%QNminer, 0.002*st%QNminer), 0.) !, 0.002*st%QNminer
        st%N_loss  = st%N_leach + st%N_vol
        if(ISNAN(st%N_loss))then
            return
        endif
        ! update QNminer
        st%QNminer  = st%QNminer - st%N_loss
        ! if (st%QNminer < 0 )then
        !     print*, "QNminer is error: ",st%QNminer, st%N_miner, st%N_deposit, st%N_uptake, st%N_immob
        !     print*, st%N_leach, st%N_vol
        !     stop
        ! endif
        st%fNnetmin = st%N_miner + st%N_deposit-(st%N_uptake+st%N_immob) - st%N_loss
        ! ------------------------------------------------------
        !!! update pools
        st%cLeaf  = 0.
        st%cStem  = 0.
        st%cRoot  = 0.
        st%cPlant = 0.
        st%nLeaf  = 0.
        st%nStem  = 0.
        st%nRoot  = 0.
        st%nPlant = 0.
        do ipft = 1, npft
            st%sp(ipft)%cLeaf = st%sp(ipft)%cLeaf - st%sp(ipft)%outC_L + st%sp(ipft)%NPP_L
            st%sp(ipft)%cStem = st%sp(ipft)%cStem - st%sp(ipft)%outC_W + st%sp(ipft)%NPP_W
            st%sp(ipft)%cRoot = st%sp(ipft)%cRoot - st%sp(ipft)%outC_R + st%sp(ipft)%NPP_R
            ! update the N pools
            st%sp(ipft)%nLeaf = st%sp(ipft)%nLeaf - st%sp(ipft)%outN_L + st%sp(ipft)%N_leaf
            st%sp(ipft)%nStem = st%sp(ipft)%nStem - st%sp(ipft)%outN_W + st%sp(ipft)%N_Stem
            st%sp(ipft)%nRoot = st%sp(ipft)%nRoot - st%sp(ipft)%outN_R + st%sp(ipft)%N_root
            ! calculate the vegetation C and N
            st%sp(ipft)%cPlant = st%sp(ipft)%cLeaf + st%sp(ipft)%cStem + st%sp(ipft)%cRoot
            st%sp(ipft)%nPlant = st%sp(ipft)%nLeaf + st%sp(ipft)%nStem + st%sp(ipft)%nRoot
            ! update the site-based vegetation pools
            st%cLeaf  = st%cLeaf  + st%sp(ipft)%pft_weight * st%sp(ipft)%cLeaf
            st%cStem  = st%cStem  + st%sp(ipft)%pft_weight * st%sp(ipft)%cStem
            st%cRoot  = st%cRoot  + st%sp(ipft)%pft_weight * st%sp(ipft)%cRoot
            st%cPlant = st%cPlant + st%sp(ipft)%pft_weight * st%sp(ipft)%cPlant
            ! 
            st%nLeaf  = st%nLeaf  + st%sp(ipft)%pft_weight * st%sp(ipft)%nLeaf
            st%nStem  = st%nStem  + st%sp(ipft)%pft_weight * st%sp(ipft)%nStem
            st%nRoot  = st%nRoot  + st%sp(ipft)%pft_weight * st%sp(ipft)%nRoot
            st%nPlant = st%nPlant + st%sp(ipft)%pft_weight * st%sp(ipft)%nPlant  
        enddo
        st%cLit_m  = st%cLit_m  - st%outC_lit_m  + st%outC_L + st%etaW * st%outC_S + st%outC_R
        st%cLit_s  = st%cLit_s  - st%outC_lit_s  + (1.- st%etaW) * st%outC_S
        st%cSoil_f = st%cSoil_f - st%outC_soil_f + st%f_F2M*st%outC_lit_m + st%f_C2M*st%outC_lit_s &         
                &  + st%f_S2M*st%outC_soil_s + st%f_P2M * st%outC_soil_p
        st%cSoil_s = st%cSoil_s - st%outC_soil_s + st%f_C2S * st%outC_lit_s  + st%f_M2S*st%outC_soil_f
        st%cSoil_p = st%cSoil_p - st%outC_soil_p + st%f_M2P * st%outC_soil_f + st%f_S2P*st%outC_soil_s
        
        do ipft = 1, npft
            if (st%sp(ipft)%cStem <- huge(1.))then
                print*, "Species of ", ipft, " is huge(1)"
                ! stop
                return
            endif
        enddo
        st%nLit_m  = st%nLit_m  - st%outN_lit_m  + st%N_imm_lit_m  + st%outN_L + st%etaW*st%outN_S + st%outN_R  ! already * (1-alphaN)      
        st%nLit_s  = st%nLit_s  - st%outN_lit_s  + st%N_imm_lit_s  + (1. - st%etaW)*st%outN_S
        st%nSoil_f = st%nSoil_f - st%outN_soil_f + st%N_imm_soil_f - st%Scalar_N_flow*st%nSoil_f*LDON0  &
               &   + st%f_F2M*st%outN_lit_m + st%f_C2M*st%outN_lit_s + st%f_S2M*st%outN_soil_s + st%f_P2M*st%outN_soil_p
        st%nSoil_s = st%nSoil_s - st%outN_soil_s + st%N_imm_soil_s + st%f_C2S*st%outN_lit_s + st%f_M2S*st%outN_soil_f
        st%nSoil_p = st%nSoil_p - st%outN_soil_p + st%N_imm_soil_p + st%f_M2P*st%outN_soil_f+ st%f_S2P*st%outN_soil_s
        ! update C/N ratio
        do ipft = 1, npft
            st%sp(ipft)%CN_L = st%sp(ipft)%cLeaf/st%sp(ipft)%nLeaf
            st%sp(ipft)%CN_W = st%sp(ipft)%cStem/st%sp(ipft)%nStem
            st%sp(ipft)%CN_R = st%sp(ipft)%cRoot/st%sp(ipft)%nRoot
            if(st%sp(ipft)%CN_L < 0) then
                print*, "CN_L is error: ", st%sp(ipft)%CN_L, st%sp(ipft)%cLeaf, st%sp(ipft)%nLeaf
                ! stop
                return
            endif
        enddo
        st%CN_L      = st%cLeaf   / st%nLeaf
        st%CN_S      = st%cStem   / st%nStem
        st%CN_R      = st%cRoot   / st%nRoot
        st%CN_lit_m  = st%cLit_m  / st%nLit_m
        st%CN_lit_s  = st%cLit_s  / st%nLit_s
        st%CN_soil_f = st%cSoil_f / st%nSoil_f
        st%CN_soil_s = st%cSoil_s / st%nSoil_s
        st%CN_soil_p = st%cSoil_p / st%nSoil_p
        ! calculate N related scalars
        do ipft = 1, npft
            kappaVcmax           = st%sp(ipft)%CN0_L/1.
            st%sp(ipft)%SNvcmax  = exp(-kappaVcmax*(st%sp(ipft)%CN_L-st%sp(ipft)%CN0_L)/st%sp(ipft)%CN0_L)
            if(.not.ieee_is_finite(st%sp(ipft)%SNvcmax)) then
                print*, kappaVcmax*(st%sp(ipft)%CN_L-st%sp(ipft)%CN0_L)/st%sp(ipft)%CN0_L
                print*, "SNvcmax: ", kappaVcmax, st%sp(ipft)%CN_L, st%sp(ipft)%CN0_L, st%sp(ipft)%CN0_L
                ! stop
                return
            endif
            st%sp(ipft)%SNgrowth = exp(-(st%sp(ipft)%CN_L-st%sp(ipft)%CN0_L)/st%sp(ipft)%CN0_L)
            st%sp(ipft)%SNRauto  = exp(-(st%sp(ipft)%CN_L-st%sp(ipft)%CN0_L)/st%sp(ipft)%CN0_L)
        enddo
        return
    end subroutine TCS_CN

end module transfer