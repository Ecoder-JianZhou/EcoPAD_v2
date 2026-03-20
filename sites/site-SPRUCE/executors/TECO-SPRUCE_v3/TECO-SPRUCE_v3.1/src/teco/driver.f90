module driver

    use datatypes
    use vegetation
    use soil
    use transfer
#ifdef USE_MCMC
    use mcmc_mod, only: get_simu_data
#endif
    use io_mod
    USE, INTRINSIC :: IEEE_ARITHMETIC
    implicit none
    integer :: unit_h, unit_d, unit_m, unit_y
    logical :: do_out_csv

    contains
    subroutine teco_simu(st, in_do_out_csv)
        implicit none
        type(site_data_type), intent(inout) :: st
        logical, intent(in) :: in_do_out_csv
        integer year0, first_year                               ! year0: record the current year to judge whether a new year
        real(8)    Difference                                      ! GPP-Rauto-NPP. Jian: no sure whether for balance? 
        real(8)    :: esat1
        integer :: iclim, iyear, iday, ihour
        integer :: iTotHourly, iTotDaily, iTotMonthly, iTotYearly
        integer :: daysOfyear, daysOfmonth(12), hoursOfYear, hoursOfmonth
        integer :: ipft, iostat, i
        real(8)    :: snow_depth_e, RH
        character(len=:), allocatable :: csv_fileName
        character(4000) :: header_csv

        ! Jian: start the cycle of the forcing data
        first_year = forcing(1)%year
        ! initilize the output 
        do_out_csv = in_do_out_csv
        if(do_out_csv) then
            allocate(character(len=300+len(outDir_csv)) :: csv_fileName)
            call def_header(header_csv)
            if(do_out_hr) then
                if(.not. do_sa) then
                unit_h = 3987
                call def_csv_fileName(outDir_csv, "Hourly", csv_fileName)
                open(newunit=unit_h, file=csv_fileName, status='replace', action='write', iostat=iostat)
                write(unit_h, *) adjustl(trim(header_csv))
                endif
            endif
            if(do_out_day)then
                unit_d = 3988
                call def_csv_fileName(outDir_csv, "Daily", csv_fileName)
                open(newunit=unit_d, file=csv_fileName, status='replace', action='write', iostat=iostat)
                write(unit_d, *) adjustl(trim(header_csv))
            endif
            if(do_out_mon) then
                unit_m = 3989
                call def_csv_fileName(outDir_csv, "Monthly", csv_fileName)
                open(newunit=unit_m, file=csv_fileName, status='replace', action='write', iostat=iostat)
                write(unit_m, *) adjustl(trim(header_csv))
            endif
            if(do_out_yr)then
                unit_y = 3990
                call def_csv_fileName(outDir_csv, "Yearly", csv_fileName)
                open(newunit=unit_y, file=csv_fileName, status='replace', action='write', iostat=iostat)
                write(unit_y, *) adjustl(trim(header_csv))
            endif
            deallocate(csv_fileName)
        endif


        do iclim = 1, nforcing 
            ! if(iclim > 10) stop
            ! if(iclim .eq. 1) print*, "beginning: ", st%cLit_m, st%nLit_m, st%nLit_m, st%nLit_s
            if (iclim .eq. 1) then
                year0       = first_year             ! Jian: record whether it is a new year.
                iTotHourly  = 1
                iTotDaily   = 1
                iTotMonthly = 1
                iTotYearly  = 1
            endif
            iyear = forcing(iclim)%year                      ! force%year
            iday  = forcing(iclim)%doy                    
            ihour = forcing(iclim)%hour

            !annual data assimilation
            if(do_annual_mcmc) then
                ! Tref_rh = st%Tref_rh_1
                st%Tpro_me = st%Tpro_me_1
                ! 2011-2013: set 7 to pretreat
                if(do_fixed_param) then
                    st%Q10rh   = st%Q10rh_7
                    st%Q10pro  = st%Q10pro_7                        
                    st%Tpro_me = st%Tpro_me_7 ! just one 
                    ! Tref_rh    = st%Tref_rh_7
                    ! def_s_rh = st%Tref_rh_7
                    do i = 1, 3
                        st%sp(i)%s_vea = st%sp(i)%s_vea_7
                        ! st%sp(i)%s_ved = st%sp(i)%s_ved_1
                        st%sp(i)%s_jea = st%sp(i)%s_vea_7
                        ! st%sp(i)%s_jed = st%sp(i)%s_ved_1
                        st%sp(i)%Entrpy = st%sp(i)%Entrpy_7
                        st%sp(i)%Vcmax0 = st%sp(i)%Vcmax0_7
                        st%sp(i)%Q10    = st%sp(i)%Q10_7
                        st%sp(i)%f_rg   = st%sp(i)%f_rg_7
                        st%sp(i)%fn2r   = st%sp(i)%fn2r_1
                        st%sp(i)%fn2l   = st%sp(i)%fn2l_1
                        def_s_npp(i)    = st%sp(i)%s_npp_9
                    enddo
                else
                if (iyear < 2014) then
                    st%Q10rh   = st%Q10rh_7
                    st%Q10pro  = st%Q10pro_7                        
                    st%Tpro_me = st%Tpro_me_7 ! just one 
                    ! Tref_rh    = st%Tref_rh_7
                    ! def_s_rh = st%Tref_rh_7
                    do i = 1, 3
                        st%sp(i)%s_vea = st%sp(i)%s_vea_7
                        ! st%sp(i)%s_ved = st%sp(i)%s_ved_1
                        st%sp(i)%s_jea = st%sp(i)%s_vea_7
                        ! st%sp(i)%s_jed = st%sp(i)%s_ved_1
                        st%sp(i)%Entrpy = st%sp(i)%Entrpy_7
                        st%sp(i)%Vcmax0 = st%sp(i)%Vcmax0_7
                        st%sp(i)%Q10    = st%sp(i)%Q10_7
                        st%sp(i)%f_rg   = st%sp(i)%f_rg_7
                        st%sp(i)%fn2r   = st%sp(i)%fn2r_1
                        st%sp(i)%fn2l   = st%sp(i)%fn2l_1
                        def_s_npp(i)    = st%sp(i)%s_npp_9
                    enddo
                    ! def_s_npp(1) = 1.0
                    ! if (iyear .eq. 2011 .and. iday .eq. 1 .and. ihour .eq. 0) then
                    !     st%sp(1)%fn2r  = st%sp(1)%fn2r_5
                    ! elseif (iyear .eq. 2012 .and. iday .eq. 1 .and. ihour .eq. 0) then
                    !     st%sp(1)%fn2r  = st%sp(1)%fn2r_6 !each year
                    ! elseif  (iyear .eq. 2013 .and. iday .eq. 1 .and. ihour .eq. 0) then
                    !     st%sp(1)%fn2r  = st%sp(1)%fn2r_7 !each year
                    ! endif
                else
                    ! tree
                    st%sp(1)%s_vea  = st%sp(1)%s_vea_1
                    ! st%sp(i)%s_ved = st%sp(i)%s_ved_1
                    st%sp(1)%s_jea  = st%sp(1)%s_vea_1
                    ! st%sp(i)%s_jed = st%sp(i)%s_ved_1
                    st%sp(1)%Entrpy = st%sp(1)%Entrpy_1
                    st%sp(1)%Vcmax0 = st%sp(1)%Vcmax0_1
                    st%sp(1)%Q10    = st%sp(1)%Q10_1
                    st%sp(1)%f_rg   = st%sp(1)%f_rg_1
                    def_s_npp(1)    = 1.0

                    ! st%Q10rh  = st%Q10rh_1
                    ! st%Q10pro = st%Q10pro_1
                    ! st%Tpro_me = st%Tpro_me_1
                    ! Tref_rh = st%Tref_rh_1
                    if (iyear .eq. 2014 .and. iday .eq. 1 .and. ihour .eq. 0) then
                        st%Q10rh  = st%Q10rh_1
                        st%Q10pro = st%Q10pro_1
                        st%Tpro_me = st%Tpro_me_1
                        ! Tref_rh = st%Tref_rh_1
                        ! species
                        st%sp(1)%fn2r = st%sp(1)%fn2r_1
                        st%sp(1)%fn2l = st%sp(1)%fn2l_1
                        def_s_npp(1)  = st%sp(1)%s_npp_1
                        ! def_s_rh = st%Tref_rh_1
                        do i = 2,3
                            st%sp(i)%s_vea = st%sp(i)%s_vea_1
                            ! st%sp(i)%s_ved = st%sp(i)%s_ved_1
                            st%sp(i)%s_jea = st%sp(i)%s_vea_1
                            ! st%sp(i)%s_jed = st%sp(i)%s_ved_1
                            st%sp(i)%Entrpy = st%sp(i)%Entrpy_1
                            st%sp(i)%Vcmax0 = st%sp(i)%Vcmax0_1
                            st%sp(i)%Q10    = st%sp(i)%Q10_1
                            st%sp(i)%f_rg   = st%sp(i)%f_rg_1
                            st%sp(i)%fn2r   = st%sp(i)%fn2r_1
                            st%sp(i)%fn2l   = st%sp(i)%fn2l_1
                            def_s_npp(i)    = st%sp(i)%s_npp_1
                        enddo
                    elseif (iyear .eq. 2015 .and. iday .eq. 1 .and. ihour .eq. 0) then
                        st%Q10rh = st%Q10rh_2
                        st%Q10pro = st%Q10pro_2 
                        st%Tpro_me = st%Tpro_me_2
                        ! Tref_rh = st%Tref_rh_2
                        ! def_s_rh = st%Tref_rh_2
                        ! species
                        st%sp(1)%fn2r = st%sp(1)%fn2r_2
                        st%sp(1)%fn2l = st%sp(1)%fn2l_2
                        def_s_npp(1)  = st%sp(1)%s_npp_2
                        do i = 2,3
                            st%sp(i)%s_vea = st%sp(i)%s_vea_2
                            ! st%sp(i)%s_ved = st%sp(i)%s_ved_2
                            st%sp(i)%s_jea = st%sp(i)%s_vea_2
                            ! st%sp(i)%s_jed = st%sp(i)%s_ved_2
                            st%sp(i)%Entrpy = st%sp(i)%Entrpy_2
                            st%sp(i)%Vcmax0 = st%sp(i)%Vcmax0_2
                            st%sp(i)%Q10    = st%sp(i)%Q10_2
                            st%sp(i)%f_rg   = st%sp(i)%f_rg_2
                            st%sp(i)%fn2r   = st%sp(i)%fn2r_2
                            st%sp(i)%fn2l   = st%sp(i)%fn2l_2
                            def_s_npp(i)    = st%sp(i)%s_npp_2
                        enddo
                    elseif (iyear .eq. 2016 .and. iday .eq. 1 .and. ihour .eq. 0) then
                        st%Q10rh = st%Q10rh_3
                        st%Q10pro = st%Q10pro_3
                        st%Tpro_me = st%Tpro_me_3
                        ! Tref_rh = st%Tref_rh_3
                        ! def_s_rh = st%Tref_rh_3
                        ! species
                        st%sp(1)%fn2r = st%sp(1)%fn2r_3
                        st%sp(1)%fn2l = st%sp(1)%fn2l_3
                        def_s_npp(1)  = st%sp(1)%s_npp_3
                        do i = 1,3
                            st%sp(i)%s_vea = st%sp(i)%s_vea_3
                            ! st%sp(i)%s_ved = st%sp(i)%s_ved_3
                            st%sp(i)%s_jea = st%sp(i)%s_vea_3
                            ! st%sp(i)%s_jed = st%sp(i)%s_ved_3
                            st%sp(i)%Entrpy = st%sp(i)%Entrpy_3
                            st%sp(i)%Vcmax0 = st%sp(i)%Vcmax0_3
                            st%sp(i)%Q10    = st%sp(i)%Q10_3
                            st%sp(i)%f_rg   = st%sp(i)%f_rg_3
                            st%sp(i)%fn2r   = st%sp(i)%fn2r_3
                            st%sp(i)%fn2l   = st%sp(i)%fn2l_3
                            def_s_npp(i)    = st%sp(i)%s_npp_3
                        enddo
                    elseif (iyear .eq. 2017 .and. iday .eq. 1 .and. ihour .eq. 0) then
                        st%Q10rh = st%Q10rh_4
                        st%Q10pro = st%Q10pro_4
                        st%Tpro_me = st%Tpro_me_4
                        ! Tref_rh = st%Tref_rh_4
                        ! def_s_rh = st%Tref_rh_4
                        ! species
                        st%sp(1)%fn2r = st%sp(1)%fn2r_4
                        st%sp(1)%fn2l = st%sp(1)%fn2l_4
                        def_s_npp(1)  = st%sp(1)%s_npp_4
                        do i = 1,3
                            st%sp(i)%s_vea = st%sp(i)%s_vea_4
                            ! st%sp(i)%s_ved = st%sp(i)%s_ved_4
                            st%sp(i)%s_jea = st%sp(i)%s_vea_4
                            ! st%sp(i)%s_jed = st%sp(i)%s_ved_4
                            st%sp(i)%Entrpy = st%sp(i)%Entrpy_4
                            st%sp(i)%Vcmax0 = st%sp(i)%Vcmax0_4
                            st%sp(i)%Q10    = st%sp(i)%Q10_4
                            st%sp(i)%f_rg   = st%sp(i)%f_rg_4
                            st%sp(i)%fn2r   = st%sp(i)%fn2r_4
                            st%sp(i)%fn2l   = st%sp(i)%fn2l_4
                            def_s_npp(i)    = st%sp(i)%s_npp_4
                        enddo
                    elseif (iyear .eq. 2018 .and. iday .eq. 1 .and. ihour .eq. 0) then
                        st%Q10rh = st%Q10rh_5
                        st%Q10pro = st%Q10pro_5
                        st%Tpro_me = st%Tpro_me_5
                        ! Tref_rh = st%Tref_rh_5
                        ! def_s_rh = st%Tref_rh_5
                        ! species
                        st%sp(1)%fn2r = st%sp(1)%fn2r_5
                        st%sp(1)%fn2l = st%sp(1)%fn2l_5
                        def_s_npp(1)  = st%sp(1)%s_npp_5
                        do i = 2,3
                            st%sp(i)%s_vea = st%sp(i)%s_vea_5
                            ! st%sp(i)%s_ved = st%sp(i)%s_ved_5
                            st%sp(i)%s_jea = st%sp(i)%s_vea_5
                            ! st%sp(i)%s_jed = st%sp(i)%s_ved_5
                            st%sp(i)%Entrpy = st%sp(i)%Entrpy_5
                            st%sp(i)%Vcmax0 = st%sp(i)%Vcmax0_5
                            st%sp(i)%Q10    = st%sp(i)%Q10_5
                            st%sp(i)%f_rg   = st%sp(i)%f_rg_5
                            st%sp(i)%fn2r   = st%sp(i)%fn2r_5  ! no data this year, so the same to previous year
                            st%sp(i)%fn2l   = st%sp(i)%fn2l_5
                            def_s_npp(i)    = st%sp(i)%s_npp_5
                        enddo
                    elseif (iyear .eq. 2019 .and. iday .eq. 1 .and. ihour .eq. 0) then
                        st%Q10rh = st%Q10rh_6
                        st%Q10pro = st%Q10pro_6
                        st%Tpro_me = st%Tpro_me_6
                        ! Tref_rh = st%Tref_rh_6
                        ! def_s_rh = st%Tref_rh_6
                        ! species
                        st%sp(1)%fn2r = st%sp(1)%fn2r_6
                        st%sp(1)%fn2l = st%sp(1)%fn2l_6
                        def_s_npp(1)  = st%sp(1)%s_npp_6
                        do i = 1,3
                            st%sp(i)%s_vea = st%sp(i)%s_vea_6
                            ! st%sp(i)%s_ved = st%sp(i)%s_ved_6
                            st%sp(i)%s_jea = st%sp(i)%s_vea_6
                            ! st%sp(i)%s_jed = st%sp(i)%s_ved_6
                            st%sp(i)%Entrpy = st%sp(i)%Entrpy_6
                            st%sp(i)%Vcmax0 = st%sp(i)%Vcmax0_6
                            st%sp(i)%Q10    = st%sp(i)%Q10_6
                            st%sp(i)%f_rg   = st%sp(i)%f_rg_6
                            st%sp(i)%fn2r   = st%sp(i)%fn2r_6
                            st%sp(i)%fn2l   = st%sp(i)%fn2l_6
                            def_s_npp(i)    = st%sp(i)%s_npp_6
                        enddo
                    elseif (iyear .eq. 2020 .and. iday .eq. 1 .and. ihour .eq. 0) then
                        st%Q10rh = st%Q10rh_6 ! def_Q10rh !
                        st%Q10pro = st%Q10pro_6 ! def_Q10pro !
                        st%Tpro_me = st%Tpro_me_6 !def_Tpro_me !
                        ! Tref_rh = st%Tref_rh_6 ! 10.0 
                        ! def_s_rh = st%Tref_rh_6
                        ! species
                        st%sp(1)%fn2r = st%sp(1)%fn2r_7
                        st%sp(1)%fn2l = st%sp(1)%fn2l_7
                        def_s_npp(1)  = st%sp(1)%s_npp_7
                        do i = 2,3
                            st%sp(i)%s_vea = st%sp(i)%s_vea_6 ! def_s_vea(i)    !
                            ! st%sp(i)%s_ved = st%sp(i)%s_ved_7
                            st%sp(i)%s_jea = st%sp(i)%s_vea_6 ! def_s_vea(i) !
                            ! st%sp(i)%s_jed = st%sp(i)%s_ved_7
                            st%sp(i)%Entrpy = st%sp(i)%Entrpy_6 ! def_Entrpy(i) !
                            st%sp(i)%Vcmax0 = st%sp(i)%Vcmax0_6 ! def_Vcmax0(i) !
                            st%sp(i)%Q10    = st%sp(i)%Q10_6    ! def_Q10(i)  !
                            st%sp(i)%f_rg   = st%sp(i)%f_rg_6   ! 0.5 !
                            st%sp(i)%fn2r   = st%sp(i)%fn2r_7
                            st%sp(i)%fn2l   = st%sp(i)%fn2l_7
                            def_s_npp(i)    = st%sp(i)%s_npp_7
                        enddo
                    elseif (iyear .eq. 2021 .and. iday .eq. 1 .and. ihour .eq. 0) then
                        st%Q10rh = st%Q10rh_8
                        st%Q10pro = st%Q10pro_8
                        st%Tpro_me = st%Tpro_me_8
                        ! Tref_rh = st%Tref_rh_8
                        ! def_s_rh = st%Tref_rh_8
                        ! species
                        st%sp(1)%fn2r = st%sp(1)%fn2r_8
                        st%sp(1)%fn2l = st%sp(1)%fn2l_8
                        def_s_npp(1)  = st%sp(1)%s_npp_8
                        do i = 2,3
                            st%sp(i)%s_vea = st%sp(i)%s_vea_8
                            ! st%sp(i)%s_ved = st%sp(i)%s_ved_8
                            st%sp(i)%s_jea = st%sp(i)%s_vea_8
                            ! st%sp(i)%s_jed = st%sp(i)%s_ved_8
                            st%sp(i)%Entrpy = st%sp(i)%Entrpy_8
                            st%sp(i)%Vcmax0 = st%sp(i)%Vcmax0_8
                            st%sp(i)%Q10    = st%sp(i)%Q10_8
                            st%sp(i)%f_rg   = st%sp(i)%f_rg_8
                            st%sp(i)%fn2r   = st%sp(i)%fn2r_8
                            st%sp(i)%fn2l   = st%sp(i)%fn2l_8
                            def_s_npp(i)    = st%sp(i)%s_npp_8
                        enddo
                    end if
                endif
                endif
            endif

            ! if ((iday .eq. 1) .and. (ihour .eq. 0)) print*, st%sp(1)%Vcmax0
        
            ! if it is a new year
            if ((iday .eq. 1) .and. (ihour .eq. 0)) call init_yearly(st)
            if (do_simu .and. (iday .eq. 1) .and. (ihour .eq. 0)) write(*,*)iyear
            if (do_spruce) then
                if ((iyear .eq. 1974) .and. (iday .eq. 1) .and. (ihour .eq. 0))then
                    ! 1974 remove 99% of tree biomass
                    do ipft = 1, npft
                        st%sp(ipft)%cLeaf    = 0.1 * st%sp(ipft)%cLeaf
                        st%sp(ipft)%cStem    = 0.1 * st%sp(ipft)%cStem
                        st%sp(ipft)%cRoot    = 0.1 * st%sp(ipft)%cRoot
                        st%sp(ipft)%nLeaf    = 0.1 * st%sp(ipft)%nLeaf
                        st%sp(ipft)%nStem    = 0.1 * st%sp(ipft)%nStem
                        st%sp(ipft)%nRoot    = 0.1 * st%sp(ipft)%nRoot
                        st%sp(ipft)%bmleaf   = 0.1 * st%sp(ipft)%bmleaf
                        st%sp(ipft)%bmstem   = 0.1 * st%sp(ipft)%bmstem
                        st%sp(ipft)%bmroot   = 0.1 * st%sp(ipft)%bmroot
                        st%sp(ipft)%nsc      = 0.1 * st%sp(ipft)%nsc
                        st%sp(ipft)%nsn      = 0.1 * st%sp(ipft)%nsn
                        st%sp(ipft)%storage  = 0.1 * st%sp(ipft)%storage
                        st%sp(ipft)%lai      = st%sp(ipft)%LAIMIN!0.1 * lai
                        st%sp(ipft)%stor_use = 0.1 * st%sp(ipft)%stor_use
                    enddo
                    st%cLeaf      = 0.1 * st%cLeaf
                    st%cStem      = 0.1 * st%cStem
                    st%cRoot      = 0.1 * st%cRoot
                    st%nLeaf      = 0.1 * st%nLeaf
                    st%nStem      = 0.1 * st%nStem
                    st%nRoot      = 0.1 * st%nRoot
                    st%bmleaf   = 0.1 * st%bmleaf
                    st%bmstem   = 0.1 * st%bmstem
                    st%bmroot   = 0.1 * st%bmroot
                    st%nsc      = 0.1 * st%nsc
                    st%nsn      = 0.1 * st%nsn
                    st%storage  = 0.1 * st%storage
                    st%lai      = st%LAIMIN!0.1 * lai
                    st%stor_use = 0.1 * st%stor_use
                endif
            endif

            ! leap year
            if ((iday .eq. 1) .and. (ihour .eq. 0))then
                if (do_leap) then
                    if(iday .eq. 1) call isLeap_update_daysOfyear(iyear, daysOfyear)
                else
                    daysOfyear = 365
                endif
                ! for update the results of monthly and yearly
                call update_hoursOfYear_daysOfmonth_initMonthly(iday, ihour, &
                        daysOfyear, daysOfmonth, hoursOfYear, hoursOfmonth)!, iTotMonthly)
            endif

            call update_summary_monthly(iday, ihour, daysOfmonth, iTotMonthly)

            ! initialize the daily variables to run hourly simulaiton.
            if (ihour .eq. 0) then
                ! a new day simulation.
                if (do_snow) then 
                    if (iyear .eq. first_year .and. iday .eq. 1.) then
                        st%ta     = -12.85                      ! since changed the ta criteria (0. to 1.e-10)) in calculating melt
                        st%rain_d = 0.                          ! dbmemo
                    endif
                    call snow_d(st, iday)                               ! Jian: update snow_dsim snow_d(rain_d,lat,days,ta,snow_dsim,fa,fsub,rho_snow,melt,dcount,decay_m)                          
                    snow_depth_e = st%snow_dsim
                endif
                do ipft = 1, npft
                    st%sp(ipft)%StemSap = AMIN1(st%sp(ipft)%Stemmax,st%sp(ipft)%SapS*st%sp(ipft)%bmStem)            ! Stemmax and SapS were input from parameter file, what are they? Unit? Maximum stem biomass? -JJJJJJJJJJJJJJJJJJJJJJ 
                    st%sp(ipft)%RootSap = AMIN1(st%sp(ipft)%Rootmax,st%sp(ipft)%SapR*st%sp(ipft)%bmRoot)
                    st%sp(ipft)%NSCmax  = 0.05*(st%sp(ipft)%StemSap+st%sp(ipft)%RootSap+st%sp(ipft)%cLeaf)          ! Jian: update the NSCmax each step? and fixed NSCmin  = 5.? 
                enddo
                if(st%Ta.gt.5.0) st%GDD5 = st%GDD5+st%Ta
                call init_daily(st)                                 ! Jian: initilize the daily data.
            endif

            ! forcing data --------------------------------------------------------------------------------
            ! Tair  = forcing(iclim)%Tair                      ! Tair
            ! Tsoil = forcing(iclim)%Tsoil                     ! SLT
            co2ca = forcing(iclim)%CO2*1.0E-6                       ! CO2 concentration,ppm-->1.0E-6
            if (co2ca .lt. 0) co2ca = 380.0*1.0E-6                  ! Jian: if no CO2 (-9999), then use the default value 
            forcing(iclim)%Tair  = forcing(iclim)%Tair  + Ttreat    ! Jian: whether it has the treatment
            forcing(iclim)%Tsoil = forcing(iclim)%Tsoil + Ttreat
            if (CO2treat .ne. 0.) co2ca = CO2treat*1.0E-6 
            ! ----------------------------------------------------------                
            RH       = forcing(iclim)%RH
            st%Dair  = forcing(iclim)%VPD                      ! air water vapour defficit? Unit Pa
            radsol   = forcing(iclim)%PAR                      ! unit ? PAR actually  Jian: or incoming shortwave/longwave radiation?
            st%dpatm = forcing(iclim)%PBOT
            if (do_ndep) st%N_deposit = forcing(iclim)%Ndep*3600
            ! Ajust some unreasonable values 
            RH       = AMAX1(0.01,AMIN1(99.99,RH))                ! relative humidity
            esat1    = 610.78*exp(17.27*forcing(iclim)%Tair/(forcing(iclim)%Tair + 237.3))      ! intermediate parameter
            st%eairP = esat1*RH/100.                              ! Added for SPRUCE, due to lack of VPD data. Jian: ? SPRUCE has the data? !air water vapour pressure
            st%Dair  = esat1-st%eairP                                ! Jian: confused that SPRUCE has the VPD data, why calculate it again?
            radsol   = AMAX1(radsol,0.01)

            ! intially added for soil thermal/ soil water
            if (do_snow) then
                st%snow_depth = snow_depth_e
            else
                st%snow_depth = snow_in(iclim)                  ! read from input file
            endif
            if (st%snow_depth .lt. 0.0) st%snow_depth = 0.0   
            st%snow_depth = st%snow_depth*100.                        ! change from m to cm  
            
            ! Jian: G and Esoil?
            ! if(radsol.gt.10.0) then
            !     st%G = -25.0
            ! else
            !     st%G = 20.5
            ! endif
            if (do_soilphy) then 
                GOTO 160
            endif
            if(radsol.gt.10.0) then
                st%G = -25.0
            else
                st%G = 20.5
            endif
            if (isnan(st%G)) then
                print *, "st%G is nan", st%G
                stop
            endif
            st%Esoil = 0.0
            do ipft = 1, npft
                st%sp(ipft)%Esoil = 0.05 * radsol
                st%Esoil = st%Esoil + st%sp(ipft)%pft_weight * st%sp(ipft)%Esoil
            enddo
            if(radsol.LE.10.0) then
                st%Esoil = 0.0
                do ipft = 1, npft
                    st%sp(ipft)%Esoil = 0.5 * st%G
                    st%Esoil = st%Esoil + st%sp(ipft)%pft_weight * st%sp(ipft)%Esoil                    
                enddo
            endif
160 continue        
            ! for daily mean conditions 
            st%ta     = st%ta + forcing(iclim)%Tair/24.0                             ! sum of a day, for calculating daily mean temperature, snow_d and soilwater
            st%rain_d = st%rain_d + forcing(iclim)%rain                                
            ! calculating scaling factor of NSC
            do ipft = 1, npft
                if(st%sp(ipft)%NSC.le.st%sp(ipft)%NSCmin) st%sp(ipft)%fnsc=0.0
                if(st%sp(ipft)%NSC.ge.st%sp(ipft)%NSCmax) st%sp(ipft)%fnsc=1.0
                if((st%sp(ipft)%NSC.lt.st%sp(ipft)%NSCmax).and.(st%sp(ipft)%NSC.gt.st%sp(ipft)%NSCmin))then 
                    st%sp(ipft)%fnsc = (st%sp(ipft)%NSC-st%sp(ipft)%NSCmin) / &
                                            (st%sp(ipft)%NSCmax-st%sp(ipft)%NSCmin)
                endif
                ! update vcmx0 and eJmx0 according to C/N of leaves
                st%sp(ipft)%Vcmx0 = st%sp(ipft)%Vcmax0*st%sp(ipft)%SNvcmax*1.0e-6
                if(.not.ieee_is_finite(st%sp(ipft)%Vcmx0)) then
                    print*,"Vcmx0 is finite: ", st%sp(ipft)%Vcmx0, st%sp(ipft)%Vcmax0, st%sp(ipft)%SNvcmax
                    ! stop
                    return
                 endif
                ! st%sp(ipft)%eJmx0 = 1.67*st%sp(ipft)%Vcmx0 ! Weng 02/21/2011 Medlyn et al. 2002 
                st%sp(ipft)%eJmx0 = st%sp(ipft)%JV*st%sp(ipft)%Vcmx0   ! added for acclimation study,replace 1.67 with JV Feb 19 2019 Shuang  
            enddo
            ! print*,"before LAI: ", st%sp(1)%LAI
            call vegn_canopy(st, forcing(iclim))      ! run canopy module
            
            if(ISNAN(st%Rsoilab3)) then
                return
            endif 
            ! run soil water processes
            if(do_obs_zwt)then
                call soilwater_obs(st, forcing(iclim))
            else
                call soilwater(st, forcing(iclim)) 
            endif                     
            ! st%ET = st%evap + st%transp
            
            ! ! Jian: to update module
            call respiration(st, forcing(iclim))
            ! THE Third Part: update LAI
            call plantgrowth(st, forcing(iclim))

            ! THE Fourth PART: simulating C influx allocation in pools
            ! print*, "before TCS_CN: ", st%sp(1)%npp
            call TCS_CN(st, forcing(iclim)) 
            ! print*, "after TCS_CN: ", st%sp(1)%npp  
            ! if (do_matrix) call matrix_struct() 
            call methane(st, forcing(iclim))        !update single value of Rh_pools,Tsoil,zwt,wsc 
            ! update NSC
            st%Rauto = 0.0
            st%NSC   = 0.0
            st%NSN   = 0.0
            st%NPP    = 0.0
            st%bmleaf = 0.0
            st%bmstem = 0.0
            st%bmroot = 0.0
            st%LAI    = 0.0
            st%GPP    = 0.0
            st%LAImax = 0.0
            st%LAImin = 0.0
            do ipft = 1, npft
                st%sp(ipft)%Rauto = st%sp(ipft)%Rmain + st%sp(ipft)%Rgrowth + st%sp(ipft)%Rnitrogen
                st%sp(ipft)%NSC   = st%sp(ipft)%NSC   + st%sp(ipft)%GPP     - st%sp(ipft)%Rauto - &
                            &      (st%sp(ipft)%NPP   - st%sp(ipft)%add)    - st%sp(ipft)%store
                Difference = st%sp(ipft)%GPP - st%sp(ipft)%Rauto - st%sp(ipft)%NPP
                if(st%sp(ipft)%NSC < 0)then
                    st%sp(ipft)%bmstem = st%sp(ipft)%bmstem + st%sp(ipft)%NSC/0.48
                    st%sp(ipft)%NPP    = st%sp(ipft)%NPP    + st%sp(ipft)%NSC
                    st%sp(ipft)%NSN    = st%sp(ipft)%NSN    - st%sp(ipft)%NSC/st%sp(ipft)%CN_W
                    st%sp(ipft)%NSC    = 0.
                endif
                st%sp(ipft)%bmleaf  = st%sp(ipft)%cLeaf/0.48
                st%sp(ipft)%bmstem  = st%sp(ipft)%cStem/0.48
                st%sp(ipft)%bmroot  = st%sp(ipft)%cRoot/0.48
                st%sp(ipft)%bmplant = st%sp(ipft)%bmleaf + st%sp(ipft)%bmroot + st%sp(ipft)%bmstem
                ! print *, "check LAI: ",st%sp(ipft)%bmleaf, st%sp(ipft)%SLA
                st%sp(ipft)%LAI     = st%sp(ipft)%bmleaf*st%sp(ipft)%SLA
                ! summary
                st%Rauto  = st%Rauto  + st%sp(ipft)%pft_weight * st%sp(ipft)%Rauto
                st%NSC    = st%NSC    + st%sp(ipft)%pft_weight * st%sp(ipft)%nsc
                st%NSN    = st%NSN    + st%sp(ipft)%pft_weight * st%sp(ipft)%NSN
                st%NPP    = st%NPP    + st%sp(ipft)%pft_weight * st%sp(ipft)%NPP
                st%bmleaf = st%bmleaf + st%sp(ipft)%pft_weight * st%sp(ipft)%bmleaf
                st%bmstem = st%bmstem + st%sp(ipft)%pft_weight * st%sp(ipft)%bmstem
                st%bmroot = st%bmroot + st%sp(ipft)%pft_weight * st%sp(ipft)%bmroot
                st%LAI    = st%LAI    + st%sp(ipft)%pft_weight * st%sp(ipft)%LAI
                st%LAImax = st%LAImax + st%sp(ipft)%pft_weight * st%sp(ipft)%LAImax
                st%LAImin = st%LAImin + st%sp(ipft)%pft_weight * st%sp(ipft)%LAImin
                st%GPP    = st%GPP    + st%sp(ipft)%pft_weight * st%sp(ipft)%gpp
            enddo 
            st%Rhetero = st%Rh_lit_m + st%Rh_lit_s + st%Rh_soil_f + st%Rh_soil_s + st%Rh_soil_p-st%Pro_sum
            
            ! call updateHourly(st, iclim, iyear, iday, ihour)    ! hourly simulation
            call init_hourly()
            call updateOutVars(st, outvars_h, 1, iyear, iday, ihour)

            ! call updateDaily(st, iTotDaily, iyear, iday, ihour)
            call updateOutVars(st, outvars_d, 24, iyear, iday, ihour)
            ! call updateMonthly(st, iTotMonthly, hoursOfmonth, iyear, iday, ihour)
            call updateOutVars(st, outvars_m, hoursOfmonth, iyear, iday, ihour)
            ! call updateYearly(st, iTotYearly, hoursOfYear, iyear, iday, ihour)
            call updateOutVars(st, outvars_y, hoursOfYear, iyear, iday, ihour)

            ! if (do_mcmc) call GetSimuData(iyear, iday, ihour, st, iclim, iTotDaily, iTotMonthly, iTotYearly)
            if(do_mcmc) call get_simu_data(iyear, iday, ihour, st)

            if(do_out_csv) then
                if(do_out_hr) then
                    if (.not. do_sa) then
                        call write_data_csv(unit_h, outVars_h)
                    endif
                endif
            endif
            
            if (ihour .eq. 23) then
                if(do_out_csv) then 
                    if(do_out_day) call write_data_csv(unit_d, outVars_d)
                endif
                iTotDaily = iTotDaily + 1 
            endif
                 
            if (iclim < nforcing)then
                if (forcing(iclim+1)%year>iyear) then            
                    year0        = iyear                      ! update the record of year (year0)
                    if(do_out_csv) then 
                        if(do_out_yr) call write_data_csv(unit_y, outVars_y)
                    endif
                    iTotYearly   = iTotYearly + 1
                    do ipft = 1, npft
                        st%sp(ipft)%storage      = st%sp(ipft)%accumulation
                        st%sp(ipft)%stor_use     = st%sp(ipft)%Storage/times_storage_use
                        st%sp(ipft)%accumulation = 0.0
                        st%sp(ipft)%onset        = 0
                    enddo
                endif
            else
                year0        = iyear                          ! update the record of year (year0)
                if(do_out_csv) then
                    if(do_out_yr) call write_data_csv(unit_y, outVars_y)
                endif
                do ipft = 1, npft
                    st%sp(ipft)%storage      = st%sp(ipft)%accumulation
                    st%sp(ipft)%stor_use     = st%sp(ipft)%Storage/times_storage_use
                    st%sp(ipft)%accumulation = 0.0
                    st%sp(ipft)%onset        = 0
                enddo
            endif
            ! if(iclim .eq. 1) print*, "beginning: ", st%cLit_m, st%nLit_m, st%nLit_m, st%nLit_s
            ! print*, st%sp(1)%pft_weight, st%sp(2)%pft_weight, st%sp(3)%pft_weight
            ! stop
            ! print*,"test Aleaf: ",st%sp(1)%Aleaf
        enddo

        if(do_out_csv)then
            if(do_out_hr)  close(unit_h)
            if(do_out_day) close(unit_d)
            if(do_out_mon) close(unit_m)
            if(do_out_yr)  close(unit_y)
        endif
    end subroutine teco_simu

    subroutine isLeap_update_daysOfyear(iyear, daysOfyear)    
        implicit none
        integer, intent(in) :: iyear
        integer, intent(inout) :: daysOfyear
        if (MOD(iyear, 4) .eq. 0)then
            if (MOD(iyear, 100) .eq. 0)then
                if (MOD(iyear, 400) .eq. 0)then
                    daysOfyear = 366
                else
                    daysOfyear = 365
                endif
            else
                daysOfyear = 366
            endif
        else
            daysOfyear = 365
        endif
        return
    end subroutine isLeap_update_daysOfyear

    subroutine update_hoursOfYear_daysOfmonth_initMonthly(iday, ihour, &
        daysOfyear, daysOfmonth, hoursOfYear, hoursOfmonth)!, iTotMonthly)
        implicit none
        integer, intent(in)    :: iday, ihour
        integer, intent(inout) :: daysOfyear, daysOfmonth(12)
        integer, intent(inout) :: hoursOfYear, hoursOfmonth!, iTotMonthly
        if (daysOfyear .eq. 365) then ! common year
            hoursOfYear = 365*24
            daysOfmonth = (/31,59,90,120,151,181,212,243,273,304,334,365/)
        else
            hoursOfYear = 366*24
            daysOfmonth = (/31,60,91,121,152,182,213,244,274,305,335,366/)
        endif
        ! hours of month
        ! January:
        if (iday .eq. 1)then 
            hoursOfmonth = (daysOfmonth(1)-0)*24
            if (ihour .eq. 0) call init_monthly()
        endif
        ! Feburay:
        if (iday .eq. daysOfmonth(1)+1)then
            hoursOfmonth = (daysOfmonth(2)-daysOfmonth(1))*24
            if (ihour .eq. 0) call init_monthly()
        endif
        ! March
        if (iday .eq. daysOfmonth(2)+1)then
            hoursOfmonth = (daysOfmonth(3)-daysOfmonth(2))*24
            if (ihour .eq. 0) call init_monthly()
        endif
        ! April
        if (iday .eq. daysOfmonth(3)+1)then
            hoursOfmonth = (daysOfmonth(4)-daysOfmonth(3))*24
            if (ihour .eq. 0) call init_monthly()
        endif
        ! May
        if (iday .eq. daysOfmonth(4)+1)then
            hoursOfmonth = (daysOfmonth(5)-daysOfmonth(4))*24
            if (ihour .eq. 0) call init_monthly()
        endif
        ! June
        if (iday .eq. daysOfmonth(5)+1)then
            hoursOfmonth = (daysOfmonth(6)-daysOfmonth(5))*24
            if (ihour .eq. 0) call init_monthly()
        endif
        ! July
        if(iday .eq. daysOfmonth(6)+1)then
            hoursOfmonth = (daysOfmonth(7)-daysOfmonth(6))*24
            if (ihour .eq. 0) call init_monthly()
        endif
        ! Auguest
        if(iday .eq. daysOfmonth(7)+1)then
            hoursOfmonth = (daysOfmonth(8)-daysOfmonth(7))*24
            if (ihour .eq. 0) call init_monthly()
        endif
        ! Septemble
        if(iday .eq. daysOfmonth(8)+1)then
            hoursOfmonth = (daysOfmonth(9)-daysOfmonth(8))*24
            if (ihour .eq. 0) call init_monthly()
        endif
        ! October
        if(iday .eq. daysOfmonth(9)+1)then
            hoursOfmonth = (daysOfmonth(10)-daysOfmonth(9))*24
            if (ihour .eq. 0) call init_monthly()
        endif
        ! November
        if(iday .eq. daysOfmonth(10)+1)then
            hoursOfmonth = (daysOfmonth(11)-daysOfmonth(10))*24
            if (ihour .eq. 0) call init_monthly()
        endif
        ! December
        if(iday .eq. daysOfmonth(11)+1)then
            hoursOfmonth = (daysOfmonth(12)-daysOfmonth(11))*24
            if (ihour .eq. 0) call init_monthly()
        endif
        return
    end subroutine update_hoursOfYear_daysOfmonth_initMonthly

    subroutine update_summary_monthly(iday, ihour, daysOfmonth, iTotMonthly)
        implicit none
        integer, intent(in) :: iday, ihour, daysOfmonth(12)
        integer, intent(inout) :: iTotMonthly
        ! January
        if ((iday .eq. daysOfmonth(1))  .and. (ihour .eq. 23)) then
            if(do_out_csv) then
                if(do_out_mon) call write_data_csv(unit_m, outVars_m)
            endif
            iTotMonthly = iTotMonthly + 1
        endif
        if ((iday .eq. daysOfmonth(2))  .and. (ihour .eq. 23)) then
            if(do_out_csv) then
                if(do_out_mon) call write_data_csv(unit_m, outVars_m)
            endif
            iTotMonthly = iTotMonthly + 1
        endif
        if ((iday .eq. daysOfmonth(3))  .and. (ihour .eq. 23)) then
            if(do_out_csv) then
                if(do_out_mon) call write_data_csv(unit_m, outVars_m)
            endif
            iTotMonthly = iTotMonthly + 1
        endif
        if ((iday .eq. daysOfmonth(4))  .and. (ihour .eq. 23)) then
            if(do_out_csv) then
                if(do_out_mon) call write_data_csv(unit_m, outVars_m)
            endif
            iTotMonthly = iTotMonthly + 1
        endif
        if ((iday .eq. daysOfmonth(5))  .and. (ihour .eq. 23)) then
            if(do_out_csv) then
                if(do_out_mon) call write_data_csv(unit_m, outVars_m)
            endif
            iTotMonthly = iTotMonthly + 1
        endif
        if ((iday .eq. daysOfmonth(6))  .and. (ihour .eq. 23)) then
            if(do_out_csv) then
                if(do_out_mon) call write_data_csv(unit_m, outVars_m)
            endif
            iTotMonthly = iTotMonthly + 1
        endif
        if ((iday .eq. daysOfmonth(7))  .and. (ihour .eq. 23)) then
            if(do_out_csv) then
                if(do_out_mon) call write_data_csv(unit_m, outVars_m)
            endif
            iTotMonthly = iTotMonthly + 1
        endif
        if ((iday .eq. daysOfmonth(8))  .and. (ihour .eq. 23)) then
            if(do_out_csv) then
                if(do_out_mon) call write_data_csv(unit_m, outVars_m)
            endif
            iTotMonthly = iTotMonthly + 1
        endif
        if ((iday .eq. daysOfmonth(9))  .and. (ihour .eq. 23)) then
            if(do_out_csv) then
                if(do_out_mon) call write_data_csv(unit_m, outVars_m)
            endif
            iTotMonthly = iTotMonthly + 1
        endif
        if ((iday .eq. daysOfmonth(10)) .and. (ihour .eq. 23)) then
            if(do_out_csv) then
                if(do_out_mon) call write_data_csv(unit_m, outVars_m)
            endif
            iTotMonthly = iTotMonthly + 1
        endif
        if ((iday .eq. daysOfmonth(11)) .and. (ihour .eq. 23)) then
            if(do_out_csv) then
                if(do_out_mon) call write_data_csv(unit_m, outVars_m)
            endif
            iTotMonthly = iTotMonthly + 1
        endif
        if ((iday .eq. daysOfmonth(12)) .and. (ihour .eq. 23)) then
            if(do_out_csv) then
                if(do_out_mon) call write_data_csv(unit_m, outVars_m)
            endif
            iTotMonthly = iTotMonthly + 1
        endif
    end subroutine update_summary_monthly
    
end module driver