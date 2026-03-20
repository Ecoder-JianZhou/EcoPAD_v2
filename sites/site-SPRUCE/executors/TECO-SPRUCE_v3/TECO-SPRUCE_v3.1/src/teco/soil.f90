module soil
    use datatypes
    implicit none

    contains

    subroutine soilwater(st, iforcing)
        ! All of inputs, the unit of water is 'mm', soil moisture or soil water content is a ratio
        implicit none
        type(site_data_type), intent(inout) :: st
        type(forcing_data_type), intent(in) :: iforcing
        real(8) :: infilt_max, WILTPT_x, FLDCAP
        real(8) :: dwcl(10), evapl(10), wupl(10), srdt(10)!, depth(10)
        real(8) :: twtadd, wtadd, supply, demand, exchangeL
        real(8) :: omegaL(10), Tsrdt, tr_allo, Tr_ratio(10)!, w_wsc(10)
        real(8) :: phi, zmax, thetasmin, zthetasmin, az, zwt1,zwt2,zwt3, vtot
        real(8) :: fw(10), ome(10)
        integer :: i, ipft, nfr

        infilt_max = 15.
        WILTPT_x   = st%wsmin/100.000
        FLDCAP     = st%wsmax/100.000
        do i = 1,10
            dwcl(i)  = 0.0
            evapl(i) = 0.0
            wupl(i)  = 0.0
            srdt(i)  = 0.0
            st%depth(i) = 0.0
            if (i>3) st%wcl(i) = FLDCAP    ! Jian: set underground 30cm to saturated.
        enddo
        st%depth(1) = st%thksl(1) ! Layer volume (cm3): Determine which layers are reached by the root system. 
        do i = 2, 10
            st%depth(i) = st%depth(i-1) + st%thksl(i)
        enddo
        st%rdepth = 0.
        do ipft = 1, npft
            st%rdepth = st%rdepth + st%sp(ipft)%pft_weight * st%sp(ipft)%rdepth
        enddo
        do i = 1, 10
            if(st%rdepth .gt. st%depth(i)) nfr = i+1
        enddo
        IF (nfr.GT.10) nfr=10
        ! ---------- added for soil thermal    
        if (do_soilphy) then 
            st%infilt   = st%infilt + st%melt/24 + iforcing%rain
            if (st%ice(1) .gt. 0.0) then        ! Jian: no use in ice? 2022/11/15
                ! st%infilt = 0.0
            endif
        else
            st%infilt = st%infilt + iforcing%rain           ! ..int commented lines for soil thermal module, included in the previous loop; !mm/hour  
        endif 
        ! ------------------------------------------
        ! water infiltration through layers; Loop over all soil layers.
        twtadd = 0.
        IF(st%infilt.GE.0.0)THEN
            ! Add water to this layer, pass extra water to the next.
            wtadd     = AMIN1(st%infilt, infilt_max, AMAX1((FLDCAP - st%wcl(1))*st%thksl(1)*10.0, 0.0)) ! from cm to mm
            st%wcl(1) = (st%wcl(1)*(st%thksl(1)*10.0) + wtadd)/(st%thksl(1)*10.0)
            twtadd    = twtadd    + wtadd       !calculating total added water to soil layers (mm)
            st%infilt = st%infilt - wtadd       !update infilt
        ENDIF
        st%runoff = 0.
        if (do_soilphy) then 
            st%runoff = st%infilt*0.005   !(infilt_rate = 0.0017 defined earlier by Yuan, changed  to 0.001 by shuang )
        else
            st%runoff = st%infilt*0.001    ! Shuang added this elseif line! Shuang Modifed  Mar16 used to be 0.0019, the water lose too much lowest wt was >400
        endif
        st%infilt = st%infilt-st%runoff
        ! ---------------------------------------------------
        ! add the infilt to balance the water
        if (st%transp .gt. 0.2 .and. st%transp .le. 0.22) then
            st%infilt = st%infilt + st%transp*0.4
        else if (st%transp .gt. 0.22) then
            st%infilt = st%infilt + st%transp*0.8
        else
            st%infilt = st%infilt + st%transp*0.001
        endif

        if (st%evap .ge. 0.1 .and. st%evap .le. 0.15) then
            st%infilt = st%infilt + st%evap*0.4
        else if (st%evap .gt. 0.15) then
            st%infilt = st%infilt + st%evap*0.8
        else
            st%infilt = st%infilt + st%evap*0.001
        endif
        !----------------------------------------------------------------------------------------------------   
        ! water redistribution among soil layers
        do i=1,10
            st%wsc(i) = Amax1(0.00,(st%wcl(i)-WILTPT_x)*st%thksl(i)*10.0)
            if (do_soilphy) then ! ..int commented lines for soil thermal 
                omegaL(i) = Amax1(0.001,(st%liq_water(i)*100./st%thksl(i)-WILTPT_x)/(FLDCAP-WILTPT_x))
            else
                omegaL(i) = Amax1(0.001,(st%wcl(i)-WILTPT_x)/(FLDCAP-WILTPT_x))
            endif        
        enddo
        supply = 0.0
        demand = 0.0
        do i=1,9
            if(omegaL(i).gt.0.3)then
                supply      = st%wsc(i)*(omegaL(i)-0.3)   ! supply=wsc(i)*omegaL(i)
                demand      = (FLDCAP-st%wcl(i+1))*st%THKSL(i+1)*10.0*(1.0-omegaL(i+1))
                exchangeL   = AMIN1(supply,demand)
                st%wsc(i)   = st%wsc(i)   - exchangeL
                st%wsc(i+1) = st%wsc(i+1) + exchangeL
                st%wcl(i)   = st%wsc(i)/(st%THKSL(i)*10.0)+WILTPT_x
                st%wcl(i+1) = st%wsc(i+1)/(st%THKSL(i+1)*10.0)+WILTPT_x
            endif
        enddo
        st%wsc(10) = st%wsc(10) - st%wsc(10)*0.00001
        st%runoff  = st%runoff  + st%wsc(10)*0.00001
        st%wcl(10) = st%wsc(10)/(st%THKSL(10)*10.0)+WILTPT_x
        ! re-calculate evap
        
        Tsrdt = 0.0
        do i = 1, 10
            srdt(i) = EXP(-6.73*(st%depth(i)-st%thksl(i)/2.0)/100.0)
            Tsrdt   = Tsrdt+srdt(i)
        enddo
        do i=1,10
            evapl(i)  = Amax1(AMIN1(st%evap*srdt(i)/Tsrdt, st%wsc(i)), 0.0)  !mm
            dwcl(i)   = evapl(i)/(st%thksl(i)*10.0) !ratio
            st%wcl(i) = st%wcl(i)-dwcl(i)
        enddo
        st%evap = 0.0 
        do i=1,10
            st%evap = st%evap + evapl(i)
        enddo
        
        ! Redistribute transpiration according to root biomass and available water in each layer
        st%transp = 0.
        do ipft = 1, npft
            tr_allo = 0.0
            
            do i=1,nfr
                tr_ratio(i) = st%sp(ipft)%frlen(i) * st%wsc(i) !*(wcl(i)-wiltpt)) !*THKSL(I))
                tr_allo     = tr_allo + tr_ratio(i)
            enddo
            do i=1,nfr
                st%sp(ipft)%plantup(i) = AMIN1(st%sp(ipft)%transp*tr_ratio(i)/tr_allo, st%wsc(i)) !mm          
                wupl(i)    = st%sp(ipft)%plantup(i)/(st%thksl(i)*10.0)
                st%wcl(i)  = st%wcl(i) - st%sp(ipft)%pft_weight * wupl(i)
            enddo
            st%sp(ipft)%transp = 0.0
            do i=1,nfr
                st%sp(ipft)%transp = st%sp(ipft)%transp + st%sp(ipft)%plantup(i)
            enddo
            st%transp = st%transp + st%sp(ipft)%pft_weight * st%sp(ipft)%transp
        enddo
        ! Jian: readd water according to evaporation, transpiration and soil moisture.
        st%infilt = st%infilt + amax1(0.0,(1-st%omega)*st%transp)
        st%infilt = st%infilt + amax1(0.0,(1-st%omega)*st%evap)
        do i=1,10
            st%wsc(i) = Amax1(0.00,(st%wcl(i) - WILTPT_x)*st%THKSL(i)*10.0)
        enddo
        ! ---------------------------------------------------------------------------    
        ! water table module starts here
        if (do_soilphy) then
            vtot = st%wsc(1) + st%wsc(2) + st%wsc(3) + st%infilt ! modified based on Ma et al., 2022
        else 
            vtot = st%wsc(1) + st%wsc(2) + st%wsc(3) + st%infilt!+wsc(4)+wsc(5) 
        endif
        ! infilt means standing water according to jiangjiang                          
        phi        = 0.85                           ! soil porosity   mm3/mm3   the same unit with theta
        zmax       = 300                            ! maximum water table depth   mm
        thetasmin  = 0.25                           ! minimum volumetric water content at the soil surface   cm3/cm3
        zthetasmin = 100                            ! maximum depth where evaporation influences soil moisture   mm
        az         = (phi-thetasmin)/zthetasmin     ! gradient in soil moisture resulting from evaporation at the soil surface    mm-1

        zwt1 = -sqrt(3.0*(phi*zmax-vtot)/(2.0*az))
        zwt2 = -(3.0*(phi*zmax-vtot)/(2.0*(phi-thetasmin)))
        zwt3 = vtot-phi*zmax                                  
        if ((zwt1 .ge. -100) .and. (zwt1 .le. 0))   st%zwt = zwt1  !the non-linear part of the water table changing line
        if (zwt2 .lt. -100)                         st%zwt = zwt2  !the linear part of the water table changing line
        if (phi*zmax .lt. vtot)                     st%zwt = zwt3  !the linear part when the water table is above the soil surface   
        ! water table module ends here
        ! ---------------------------------------------------------------------------------------------------------------
        do i=1,nfr       
            if (do_soilphy) then 
                ome(i)=(st%liq_water(i)*100./st%THKSL(i)-WILTPT_x)/(FLDCAP-WILTPT_x)
                ome(i)=AMIN1(1.0,AMAX1(0.0,ome(i)))
            else 
                ome(i)=(st%wcl(i)-WILTPT_x)/(FLDCAP-WILTPT_x)
                ome(i)=AMIN1(1.0,AMAX1(0.0,ome(i)))
            endif 
            fw(i)=amin1(1.0,3.333*ome(i))
        enddo

        if (do_soilphy) then 
            st%topfws = amax1(0.0, st%topfws)
        else 
            st%topfws = amin1(1.0,(st%wcl(1)-WILTPT_x)/((FLDCAP-WILTPT_x)))
        endif     

        st%fwsoil = 0.0
        st%omega  = 0.0
        do i=1,nfr
            st%fwsoil = st%fwsoil + fw(i) *st%FRLEN(i)
            st%omega  = st%omega  + ome(i)*st%FRLEN(i)
        enddo
    end subroutine soilwater 

    subroutine soilwater_obs(st, iforcing)
        ! All of inputs, the unit of water is 'mm', soil moisture or soil water content is a ratio
        implicit none
        type(site_data_type), intent(inout) :: st
        type(forcing_data_type), intent(in) :: iforcing
        real(8) :: infilt_max, WILTPT_x, FLDCAP
        real(8) :: dwcl(10), evapl(10), wupl(10), srdt(10), depth_top(10)
        real(8) :: twtadd, wtadd, supply, demand, exchangeL
        real(8) :: omegaL(10), Tsrdt, tr_allo, Tr_ratio(10)!, w_wsc(10)
        ! real(8) :: phi, zmax, thetasmin, zthetasmin, az, zwt1,zwt2,zwt3, vtot
        real(8) :: fw(10), ome(10)
        integer :: i, ipft, nfr

        infilt_max = 15.
        WILTPT_x   = st%wsmin/100.000
        FLDCAP     = st%wsmax/100.000
        do i = 1,10
            dwcl(i)  = 0.0
            evapl(i) = 0.0
            wupl(i)  = 0.0
            srdt(i)  = 0.0
            st%depth(i) = 0.0
            if (i>3) st%wcl(i) = FLDCAP    ! Jian: set underground 30cm to saturated.
        enddo
        st%depth(1) = st%thksl(1) ! Layer volume (cm3): Determine which layers are reached by the root system.
        depth_top(1) = 0  ! Jian: mark for top depth for each layer
        do i = 2, 10
            st%depth(i) = st%depth(i-1) + st%thksl(i)
            depth_top(i) = st%depth(i-1) 
        enddo
        st%rdepth = 0.
        do ipft = 1, npft
            st%rdepth = st%rdepth + st%sp(ipft)%pft_weight * st%sp(ipft)%rdepth
        enddo
        do i = 1, 10
            if(st%rdepth .gt. st%depth(i)) nfr = i+1
        enddo
        IF (nfr.GT.10) nfr=10
        ! ---------- added for soil thermal    
        if (do_soilphy) then 
            st%infilt   = st%infilt + st%melt/24 + iforcing%rain
            if (st%ice(1) .gt. 0.0) then        ! Jian: no use in ice? 2022/11/15
                ! st%infilt = 0.0
            endif
        else
            st%infilt = st%infilt + iforcing%rain           ! ..int commented lines for soil thermal module, included in the previous loop; !mm/hour  
        endif 
        ! ------------------------------------------
        ! update the wcl and wsc based on the input water table: iforcing%zwt (mm)
        
        do i = 1, 10
            if (iforcing%zwt >= 0.0) then
                st%wcl(i) = FLDCAP
            else if ( -iforcing%zwt <= depth_top(i)*10.) then
                st%wcl(i) = FLDCAP
            else if ((-iforcing%zwt <= st%depth(i)*10.) .and. (-iforcing%zwt > depth_top(i)*10.)) then
                st%wcl(i) = Amax1(st%wcl(i), & 
                WILTPT_x + (FLDCAP - WILTPT_x)*(st%depth(i)*10. - (-iforcing%zwt))/(st%thksl(i)*10.))
            else if (-iforcing%zwt > st%depth(i)*10.) then
                st%wcl(i) = Amax1(st%wcl(i), WILTPT_x)
            endif
        enddo
        !---------- end update ---------------------
        ! water infiltration through layers; Loop over all soil layers.
        twtadd = 0.
        IF(st%infilt.GE.0.0)THEN
            ! Add water to this layer, pass extra water to the next.
            wtadd     = AMIN1(st%infilt, infilt_max, AMAX1((FLDCAP - st%wcl(1))*st%thksl(1)*10.0, 0.0)) ! from cm to mm
            st%wcl(1) = (st%wcl(1)*(st%thksl(1)*10.0) + wtadd)/(st%thksl(1)*10.0)
            twtadd    = twtadd    + wtadd       !calculating total added water to soil layers (mm)
            st%infilt = st%infilt - wtadd       !update infilt
        ENDIF
        st%runoff = 0.
        if (do_soilphy) then 
            st%runoff = st%infilt*0.005   !(infilt_rate = 0.0017 defined earlier by Yuan, changed  to 0.001 by shuang )
        else
            st%runoff = st%infilt*0.001    ! Shuang added this elseif line! Shuang Modifed  Mar16 used to be 0.0019, the water lose too much lowest wt was >400
        endif
        st%infilt = st%infilt-st%runoff
        ! ---------------------------------------------------
        ! add the infilt to balance the water
        if (st%transp .gt. 0.2 .and. st%transp .le. 0.22) then
            st%infilt = st%infilt + st%transp*0.4
        else if (st%transp .gt. 0.22) then
            st%infilt = st%infilt + st%transp*0.8
        else
            st%infilt = st%infilt + st%transp*0.001
        endif

        if (st%evap .ge. 0.1 .and. st%evap .le. 0.15) then
            st%infilt = st%infilt + st%evap*0.4
        else if (st%evap .gt. 0.15) then
            st%infilt = st%infilt + st%evap*0.8
        else
            st%infilt = st%infilt + st%evap*0.001
        endif
        !----------------------------------------------------------------------------------------------------   
        ! water redistribution among soil layers
        do i=1,10
            st%wsc(i) = Amax1(0.00,(st%wcl(i)-WILTPT_x)*st%thksl(i)*10.0)
            if (do_soilphy) then ! ..int commented lines for soil thermal 
                omegaL(i) = Amax1(0.001,(st%liq_water(i)*100./st%thksl(i)-WILTPT_x)/(FLDCAP-WILTPT_x))
            else
                omegaL(i) = Amax1(0.001,(st%wcl(i)-WILTPT_x)/(FLDCAP-WILTPT_x))
            endif        
        enddo
        supply = 0.0
        demand = 0.0
        do i=1,9
            if(omegaL(i).gt.0.3)then
                supply      = st%wsc(i)*(omegaL(i)-0.3)   ! supply=wsc(i)*omegaL(i)
                demand      = (FLDCAP-st%wcl(i+1))*st%THKSL(i+1)*10.0*(1.0-omegaL(i+1))
                exchangeL   = AMIN1(supply,demand)
                st%wsc(i)   = st%wsc(i)   - exchangeL
                st%wsc(i+1) = st%wsc(i+1) + exchangeL
                st%wcl(i)   = st%wsc(i)/(st%THKSL(i)*10.0)+WILTPT_x
                st%wcl(i+1) = st%wsc(i+1)/(st%THKSL(i+1)*10.0)+WILTPT_x
            endif
        enddo
        st%wsc(10) = st%wsc(10) - st%wsc(10)*0.00001
        st%runoff  = st%runoff  + st%wsc(10)*0.00001
        st%wcl(10) = st%wsc(10)/(st%THKSL(10)*10.0)+WILTPT_x
        ! re-calculate evap
        
        Tsrdt = 0.0
        do i = 1, 10
            srdt(i) = EXP(-6.73*(st%depth(i)-st%thksl(i)/2.0)/100.0)
            Tsrdt   = Tsrdt+srdt(i)
        enddo
        do i=1,10
            evapl(i)  = Amax1(AMIN1(st%evap*srdt(i)/Tsrdt, st%wsc(i)), 0.0)  !mm
            dwcl(i)   = evapl(i)/(st%thksl(i)*10.0) !ratio
            st%wcl(i) = st%wcl(i)-dwcl(i)
        enddo
        st%evap = 0.0 
        do i=1,10
            st%evap = st%evap + evapl(i)
        enddo
        
        ! Redistribute transpiration according to root biomass and available water in each layer
        st%transp = 0.
        do ipft = 1, npft
            tr_allo = 0.0
            ! w_wsc   = st%sp(ipft)%pft_weight
            do i=1,nfr
                tr_ratio(i) = st%sp(ipft)%frlen(i) * st%wsc(i) !*(wcl(i)-wiltpt)) !*THKSL(I))
                tr_allo     = tr_allo + tr_ratio(i)
            enddo
            do i=1,nfr
                st%sp(ipft)%plantup(i) = AMIN1(st%sp(ipft)%transp*tr_ratio(i)/tr_allo, st%wsc(i)) !mm          
                wupl(i)    = st%sp(ipft)%plantup(i)/(st%thksl(i)*10.0)
                st%wcl(i)  = st%wcl(i) - st%sp(ipft)%pft_weight * wupl(i)
            enddo
            st%sp(ipft)%transp = 0.0
            do i=1,nfr
                st%sp(ipft)%transp = st%sp(ipft)%transp + st%sp(ipft)%plantup(i)
            enddo
            st%transp = st%transp + st%sp(ipft)%pft_weight * st%sp(ipft)%transp
        enddo
        ! Jian: readd water according to evaporation, transpiration and soil moisture.
        st%infilt = st%infilt + amax1(0.0,(1-st%omega)*st%transp)
        st%infilt = st%infilt + amax1(0.0,(1-st%omega)*st%evap)

        ! ---- again update wcl and wsc ----
        do i = 1, 10
            if (iforcing%zwt >= 0.0) then
                st%wcl(i) = FLDCAP
            else if ( -iforcing%zwt <= depth_top(i)*10.) then
                st%wcl(i) = FLDCAP
            else if ((-iforcing%zwt <= st%depth(i)*10.) .and. (-iforcing%zwt > depth_top(i)*10.)) then
                st%wcl(i) = Amax1(st%wcl(i), WILTPT_x + (FLDCAP - WILTPT_x)*(st%depth(i)*10. - (-iforcing%zwt))/(st%thksl(i)*10.))
            else if (-iforcing%zwt > st%depth(i)*10.) then
                st%wcl(i) = Amax1(st%wcl(i), WILTPT_x)
            endif
        enddo

        do i=1,10
            st%wsc(i) = Amax1(0.00,(st%wcl(i) - WILTPT_x)*st%THKSL(i)*10.0)
        enddo
        ! ---------------------------------------------------------------------------    
        ! water table module starts here
        st%zwt = iforcing%zwt
        ! if (do_soilphy) then
        !     vtot = st%wsc(1) + st%wsc(2) + st%wsc(3) + st%infilt ! modified based on Ma et al., 2022
        ! else 
        !     vtot = st%wsc(1) + st%wsc(2) + st%wsc(3) + st%infilt!+wsc(4)+wsc(5) 
        ! endif
        ! ! infilt means standing water according to jiangjiang                          
        ! phi        = 0.85                           ! soil porosity   mm3/mm3   the same unit with theta
        ! zmax       = 300                            ! maximum water table depth   mm
        ! thetasmin  = 0.25                           ! minimum volumetric water content at the soil surface   cm3/cm3
        ! zthetasmin = 100                            ! maximum depth where evaporation influences soil moisture   mm
        ! az         = (phi-thetasmin)/zthetasmin     ! gradient in soil moisture resulting from evaporation at the soil surface    mm-1

        ! zwt1 = -sqrt(3.0*(phi*zmax-vtot)/(2.0*az))
        ! zwt2 = -(3.0*(phi*zmax-vtot)/(2.0*(phi-thetasmin)))
        ! zwt3 = vtot-phi*zmax                                  
        ! if ((zwt1 .ge. -100) .and. (zwt1 .le. 0))   st%zwt = zwt1  !the non-linear part of the water table changing line
        ! if (zwt2 .lt. -100)                         st%zwt = zwt2  !the linear part of the water table changing line
        ! if (phi*zmax .lt. vtot)                     st%zwt = zwt3  !the linear part when the water table is above the soil surface   
        ! water table module ends here
        ! ---------------------------------------------------------------------------------------------------------------
        
        
        do i=1,nfr       
            if (do_soilphy) then 
                ome(i)=(st%liq_water(i)*100./st%THKSL(i)-WILTPT_x)/(FLDCAP-WILTPT_x)
                ome(i)=AMIN1(1.0,AMAX1(0.0,ome(i)))
            else 
                ome(i)=(st%wcl(i)-WILTPT_x)/(FLDCAP-WILTPT_x)
                ome(i)=AMIN1(1.0,AMAX1(0.0,ome(i)))
            endif 
            fw(i)=amin1(1.0,3.333*ome(i))
        enddo

        if (do_soilphy) then 
            st%topfws = amax1(0.0, st%topfws)
        else 
            st%topfws = amin1(1.0,(st%wcl(1)-WILTPT_x)/((FLDCAP-WILTPT_x)))
        endif     

        st%fwsoil = 0.0
        st%omega  = 0.0
        do i=1,nfr
            st%fwsoil = st%fwsoil + fw(i) *st%FRLEN(i)
            st%omega  = st%omega  + ome(i)*st%FRLEN(i)
        enddo

    end subroutine soilwater_obs

    subroutine Tsoil_simu(st, iforcing)          
        implicit none 
        type(site_data_type), intent(inout) :: st
        type(forcing_data_type), intent(in) :: iforcing
        ! local variables
        integer :: i
        real(8) :: ice_density, thkns1, shcap_ice, condu_ice
        real(8) :: condu_water, shcap_water, condu_soil, shcap_soil, condu_s
        real(8) :: thd_t, diff_snow, diff_s, latent_heat_fusion
        real(8) :: condu_air, shcap_air, diff_air
        real(8) :: water_tw,  water_table_depth, snow_depth_t
        real(8) :: albedo_water, depth_z(10), ufw(10)
        real(8) :: WILTPT, FILDCP, TairK, wind, raero
        real(8) :: QLsoil, rhocp, H2OLv, slope, psyc, Cmolar, fw1, Rsoil
        real(8) :: flait, rLAI, resht_lai
        real(8) :: condu(10), shcap(10), difsv1, difsv2
        real(8) :: dnr, dsh, dgh, dle, drsdh
        real(8) :: delta, sftmp_pre, tsoill_0
        real(8) :: Tsoill_pre, thkns2, temph2, temph1
        real(8) :: temph_snow, temph_water, heat_excess, ice_incr, inter_var
        real(8) :: heat_adjust

        wind   = iforcing%WS
        if (wind .lt. 0.01) wind = 0.01   ! set windspeed to the minimum speed to avoid zero Gb
        ! aerodynamic resistance
        raero = 50./wind
        ! soil thermal conductivity W m-2 K-1
        ice_density        = 916.!916.
        thkns1             = st%thksl(1)/4.             ! thkns1=thksl(1)/2.
        shcap_ice          = 2117.27*ice_density
        condu_ice          = 2.29
        condu_water        = 0.56!0.56
        shcap_water        = 4188000.
        condu_soil         = 0.25
        shcap_soil         = 2600000.
        condu_s            = 0.25
        thd_t              = -1.0        
        diff_snow          = 3600.*st%condu_snow/st%shcap_snow*10000.
        diff_s             = 3600.*st%condu_b/shcap_soil*10000.
        latent_heat_fusion = 333700.   ! j kg-1
        condu_air          = 0.023
        shcap_air          = 1255.8
        diff_air           = 3600.*condu_air/shcap_air*10000.      
        water_tw           = st%zwt*0.001 - st%ice_tw ! might means total water that is liquid, add up all layers
        water_table_depth  = st%zwt*0.1
        snow_depth_t       = st%snow_depth - 0.46*0.0     ! warming in Tair impact on snow_depth
        ! soil water conditions
        albedo_water       = 0.1 
        WILTPT             = st%wsmin/100.
        FILDCP             = st%wsmax/100.
        TairK              = iforcing%Tair + 273.2
        depth_z            = (/0., 0., 0., 0., 0., 0., 0.,0.,0.,0./)
        ufw                = (/0.0163,0.0263,0.0563,0.0563,0.0563,0.1162,0.1162,0.1162,0.1162,0.1162/) !..int add unfrozen water ratio 
        if (snow_depth_t .lt. st%thd_snow_depth) snow_depth_t =0.0
        if (snow_depth_t .gt. 0.) then
            st%dcount_soil = st%dcount_soil +1./24.
        else 
            st%dcount_soil = 0.
        endif
        if (water_table_depth .lt. 4. .and. water_table_depth .gt. 0.0) water_table_depth =0.    ! avoid numerical issues
        ! calculate
        QLsoil      = emsoil*sigma*((st%sftmp+273.2)**4)
        st%Rsoilab3 = (st%QLair + st%QLleaf)*(1.0-rhoS(3))-QLsoil 

        ! if(isnan(st%sftmp)) then
        !     write(*,*)"test sftmp: ", st%sftmp, delta, sftmp_pre, st%resdh,&
        !          drsdh, st%Rsoilabs, st%Hsoil, st%Esoil, st%G, condu(1),st%tsoill(1), st%thksl(1)
        !     return
        ! endif

        if(ISNAN(st%Rsoilab3)) then
            write(*,*)"Rsoilab3 is NaN: ", st%Rsoilab3, st%QLair, st%QLleaf, rhoS, QLsoil
            print*, st%sp(1)%QLleaf, st%sp(2)%QLleaf, st%sp(3)%QLleaf
            print*, emsoil, sigma, st%sftmp
            ! stop
            return
        endif 
        ! Total radiation absorbed by soil
        if (snow_depth_t .gt. 0.0) then 
            st%Rsoilabs = (st%Rsoilab1 + st%Rsoilab2)*(1 - st%albedo_snow)/(1-0.1) + st%Rsoilab3  
        elseif (water_table_depth .gt. 0.0) then 
            st%Rsoilabs = (st%Rsoilab1 + st%Rsoilab2)*(1 - albedo_water)/(1-0.1)   + st%Rsoilab3  
        else
            st%Rsoilabs = st%Rsoilab1 + st%Rsoilab2 + st%Rsoilab3
        endif 
        ! thermodynamic parameters for air
        rhocp  = cpair*Patm*AirMa/(Rconst*TairK)      
        H2OLv  = H2oLv0-2.365e3*iforcing%Tair
        slope  = (esat(iforcing%Tair+0.01)-esat(iforcing%Tair))/0.01
        psyc   = Patm*cpair*AirMa/(H2OLv*H2OMw)
        Cmolar = Patm/(Rconst*TairK)
        fw1    = AMIN1(AMAX1((FILDCP-st%wcl(1))/(FILDCP-WILTPT),0.3),1.0)    
        if (water_table_depth .gt. 0.0) then 
            Rsoil = 0. 
        else 
            Rsoil=30.*exp(0.2/fw1)
        endif 
        ! latent heat flux into air from soil
        flait     = st%lai
        rLAI      = exp(flait) 
        st%Esoil  = (slope*(st%Rsoilabs - st%G) + rhocp*st%Dair/(raero+rLAI))/       &
                &      (slope+psyc*(Rsoil/(raero+rLAI)+1.))
        ! jian: limit the Esoil to 0.0
        if(st%Esoil .lt. -10000000.0) st%Esoil = 0.0
        if(st%Esoil .gt. 10000000.0) st%Esoil = 0.0
        ! end 
        resht_lai = st%resht*flait
        st%Hsoil  = rhocp*(st%sftmp-iforcing%Tair)/resht_lai
        condu(1) = (FILDCP - st%wcl(1)) * condu_air + st%liq_water(1)/(st%thksl(1)*0.01)*condu_water + &
              &  st%ice(1)/(st%thksl(1)*0.01) * condu_ice + (1-FILDCP)*condu_soil
        shcap(1) = (FILDCP - st%wcl(1)) * shcap_air + st%liq_water(1)/(st%thksl(1)*0.01)*shcap_water + &
              &  st%ice(1)/(st%thksl(1)*0.01) * shcap_ice + (1-FILDCP)*shcap_soil
        difsv1 = 3600.*condu(1)/shcap(1)*10000.   
        
        ! st%tsoill(1) = iforcing%Tair
        st%G   = condu(1)*(st%sftmp - st%tsoill(1))/(st%thksl(1)/2.*0.01)
        ! st%G   = condu(1)*(st%sftmp - iforcing%Tair)/(st%thksl(1)/2.*0.01)
        ! st%test1 = condu(1) !iforcing%Tair
        if (snow_depth_t .gt. 0.0) then
            st%G = st%condu_snow*(st%sftmp-st%Tsnow)/(snow_depth_t/2.*0.01)
        endif
        ! Residual heat energy.
        st%resdh = st%Rsoilabs - st%Hsoil - st%Esoil - st%G
        ! First derivative of net radiation; sensible heat; ground heat;
        DNR   = 4.*emsoil*sigma*(st%sftmp+273.2)**3
        DSH   = rhocp/resht_lai 
        DGH   = condu_s/(st%thksl(1)/2.*0.01)
        DLE   = (DNR+DGH)*slope/(slope+psyc*(Rsoil/(raero+rLAI)+1.))      
        drsdh = -DNR-DSH-DGH-DLE
        ! Calculate increment DELTA.
        delta     = st%resdh/drsdh
        sftmp_pre = st%sftmp
        st%sftmp  = st%sftmp - delta
        if (ABS(sftmp_pre - st%sftmp) .gt. 20. ) then 
            st%sftmp = sftmp_pre  
        ! if (st%sftmp > iforcing%Tair) Then
        else if (ABS(st%sftmp - iforcing%Tair) .gt. 30.) then
            st%sftmp = sftmp_pre ! Jian : limit the sftmp based on air temperature
        endif
        ! endif
        tsoill_0  = st%sftmp
        ! The residual heat energy is used to increase the soil temperature
        do i = 1, 10
            Tsoill_pre = st%tsoill(i) 
            ! update the liq_water
            if (water_table_depth .lt. 0.0 .and. -water_table_depth .lt. depth_z(i)) then
                st%liq_water(i) = FILDCP*st%thksl(i)*0.01 - st%ice(i)
            else
                st%liq_water(i) = st%wcl(i)*st%thksl(i)*0.01 - st%ice(i)
            endif 
            ! update depth_z         
            if (i .eq. 1) then 
                depth_z(1) = st%thksl(1)
            else 
                depth_z(i) = depth_z(i-1) + st%thksl(i)
            endif
            ! Jian: thksl is 10 layers, so thkns2 is calculated as following:
            if (i<10) then
                thkns2 = (st%thksl(i) + st%thksl(i+1))/2.
            else
                thkns2 = (st%thksl(i-1) + st%thksl(i))/2.
            endif
            ! update condu, shcap and difsv2
            if (i < 10) then
                condu(i+1) = (FILDCP-st%wcl(i+1))*condu_air+st%liq_water(i+1)/(st%thksl(i+1)*0.01)*condu_water+ &
                                &  st%ice(i+1)/(st%thksl(i+1)*0.01)*condu_ice +(1-FILDCP)*condu_soil
                shcap(i+1) = (FILDCP-st%wcl(i+1))*shcap_air+st%liq_water(i+1)/(st%thksl(i+1)*0.01)*shcap_water+ &
                                &  st%ice(i+1)/(st%thksl(i+1)*0.01)*shcap_ice +(1-FILDCP)*shcap_soil 
                difsv2     = 3600.*condu(i+1)/shcap(i+1)*10000.
            else
                difsv2 = 3600.*condu(i)/shcap(i)*10000. 
            endif 
            ! update temph2
            if (i<10)then
                temph2 = (difsv1+difsv2)*(st%Tsoill(i)-st%Tsoill(i+1))/thkns2 
            else
                temph2 = (difsv1+difsv2)*(st%Tsoill(i-1)-st%Tsoill(i))/thkns2
            endif
            ! start first layer, ajust if there are snow or water layer above
            if (i .eq. 1)then
                if (snow_depth_t .gt. 0.) then   
                    temph_snow = Amin1(diff_snow, difsv1) * (st%Tsnow - st%Tsoill(1))/((snow_depth_t + st%thksl(1))/2.)
                    st%Tsnow   = st%Tsnow + (exp(-st%depth_ex*snow_depth_t)*diff_snow*(st%sftmp-st%Tsnow)/(snow_depth_t/2.) &
                        &        - temph_snow)/(snow_depth_t/2.+(snow_depth_t + st%thksl(1))/2.) 
                    st%Tsoill(1) = st%Tsoill(1)+(temph_snow - temph2)/((snow_depth_t + st%thksl(1))/2.+thkns2) 
                    if(st%Tsnow .gt.0.0) then 
                        st%Tsnow     = 0.0   
                        st%Tsoill(1) = 0.
                    endif
                    drsdh    = 0.0    ! temporarily set drsdh =0 for heat adjustment of soil when  
                    tsoill_0 = (st%Tsoill(1) + st%Tsnow)/2.
                elseif (water_table_depth .gt. 0.) then  
                    temph_water = (3600.*condu_water/shcap_water*10000.+difsv1)*(st%Twater-st%Tsoill(1))/&
                                  ((water_table_depth+st%thksl(1))/2.)! there is snow layer 
                    st%Twater   = st%Twater + (2.*3600.*condu_water/shcap_water*10000.*(st%sftmp - st%Twater)/&
                                  (water_table_depth/2.)-temph_water)/(water_table_depth/2.+(water_table_depth+st%thksl(1))/2.) 
                    ! Phase change surface water 
                    if(st%Twater .lt. 0.0 .and. water_tw .gt. 0.0) then  ! freeze 
                        heat_excess = -(shcap_water/360000.*water_tw*100.-drsdh)*st%Twater
                        ice_incr    = heat_excess*3600./latent_heat_fusion/ice_density
                        if (ice_incr .lt. water_tw) then
                            st%ice_tw = st%ice_tw +ice_incr
                            water_tw  = water_tw-ice_incr
                            st%Twater = 0.0
                            st%Tice   = 0.0
                        else
                            st%ice_tw = st%ice_tw + water_tw
                            water_tw  = 0.0
                            st%Tice   = st%Tice - latent_heat_fusion*(ice_incr-water_tw)*ice_density/(shcap_ice*st%ice_tw)
                        endif     
                    elseif (st%Twater .gt. 0.0 .and. st%ice_tw .gt. 0.0) then    ! thraw              
                        heat_excess  = (shcap_water/360000.*st%ice_tw*100.-drsdh)*st%Twater
                        ice_incr     = heat_excess*3600./latent_heat_fusion/ice_density
                        if (ice_incr .lt. st%ice_tw) then
                            st%ice_tw = st%ice_tw - ice_incr
                            water_tw  = water_tw  + ice_incr
                            st%Twater = 0.0
                            st%Tice   = 0.0
                        else
                            water_tw  = water_tw + st%ice_tw
                            st%ice_tw = 0.0
                            st%Twater = st%Twater + latent_heat_fusion*(ice_incr-st%ice_tw)*ice_density/(shcap_water*water_tw)
                        endif
                    endif
                    temph2=(difsv1+3600.*condu_water/shcap_water*10000.)*(st%Tsoill(i)-st%Tsoill(i+1))/thkns2 
                    if (water_tw .eq. 0.0 .and. st%ice_tw .gt. 0.0) then 
                        st%Tsoill(1) = st%Tsoill(1)+(2.*3600.*condu_ice/shcap_ice*10000.*(st%Tice-st%Tsoill(1))/thkns1 &
                                    &     -temph2)/(thkns1+thkns2) 
                    else 
                        st%Tsoill(1) = st%Tsoill(1)+(2.*3600.*condu_water/shcap_water*10000.*(st%Twater-st%Tsoill(1))/thkns1 &
                                    &     -temph2)/(thkns1+thkns2) 
                    endif
                    drsdh = 0.0    ! temporarily set drsdh =0 for heat adjustment of soil 
                else
                    st%Tsoill(1) = st%Tsoill(1)+(diff_s*(st%sftmp-st%Tsoill(1))/thkns1 &
                                &     -temph2)/(thkns1+thkns2)
                endif
                heat_excess = drsdh*(thd_t-st%Tsoill(i))+shcap(i)*st%thksl(i)*(st%Tsoill(i)-thd_t)/360000.         
                ice_incr    = heat_excess*3600./latent_heat_fusion/ice_density
                inter_var   = st%ice(i)   
                if (ice_incr .lt. 0.) then     ! freeze             
                    st%ice(i) = Amin1(st%liq_water(i)+inter_var,st%ice(i)-ice_incr, 0.05)            
                else 
                    st%ice(i) = Amax1(st%ice(i)-ice_incr,0.0)              
                endif
                !! readjust energy and temp 
                heat_adjust = heat_excess-latent_heat_fusion*(inter_var-st%ice(i))*ice_density/3600.
                st%Tsoill(i)   = thd_t+heat_adjust/(shcap(i)*st%thksl(i)/360000.-drsdh)
            else
                if ( i .gt. 9) then 
                    temph2 = 0.00003
                    thkns2 = 500  ! boundary conditions, rethink
                endif            
                st%Tsoill(i)   = st%Tsoill(i)+(temph1-temph2)/(thkns1+thkns2)       ! temph1  not initilizd values?
                heat_excess = shcap(i)*st%thksl(i)*(st%Tsoill(i)-thd_t)/360000.        
                ice_incr    = heat_excess*3600./latent_heat_fusion/ice_density
                inter_var   = st%ice(i) 
                if (ice_incr .lt. 0.) then     ! freeze             
                    st%ice(i) = Amin1(st%liq_water(i)+inter_var,st%ice(i)-ice_incr)             
                else 
                    st%ice(i) = Amax1(st%ice(i)-ice_incr,0.0)              
                endif         
                !! readjust energy and temp 
                heat_adjust  = heat_excess-latent_heat_fusion*(inter_var-st%ice(i))*ice_density/3600.
                st%Tsoill(i) = thd_t+heat_adjust/(shcap(i)/360000.*st%thksl(i))
            endif
            if (ABS(tsoill_pre -st%tsoill(i)) .gt. 5. )  st%Tsoill(i)=tsoill_pre
            ! else if (ABS(iforcing%Tair -st%tsoill(i)) .gt. 30.) then
            !     st%Tsoill(i)=tsoill_pre
            ! endif
            ! if (isnan(st%tsoill(i))) st%Tsoill(i) = iforcing%Tsoil ! Jian: limit it 
            TEMPH1 = TEMPH2
            THKNS1 = THKNS2
            DIFSV1 = DIFSV2 
        enddo
        if(do_obs_Tsoil)then
            st%tsoil_layer = iforcing%Tsoil_lr  ! use observation
        else
            st%tsoil_layer(1)    = tsoill_0
            st%tsoil_layer(2:11) = st%Tsoill(1:10)
        endif
        return 
    end subroutine Tsoil_simu

    subroutine snow_d(st, iday)
        implicit none
        type(site_data_type), intent(inout) :: st
        integer, intent(in) :: iday
        real(8) tr,daylength,dec,dsnow, in_snow
        real(8) snow_dsim_pre

        tr        = 0.0174532925
        dec       = sin(((real(iday)-70.)/365.)*360.*tr)*23.44  ! dec=sin(((real(8)(days)-70.)/365.)*360.*tr)*23.44
        daylength = acos(-tan(st%lat*tr)*tan(dec*tr))/7.5 
        daylength = daylength/tr/24.
                
        if (st%snow_dsim .ge. 0.) then
            st%dcount = st%dcount +1.
        else 
            st%dcount =0.
        endif
        st%sublim=0.
        if (st%ta .gt. 0. .and. st%snow_dsim .gt. 0.) st%sublim=st%fsub*715.5*daylength*esat(st%ta)/(st%ta+273.2)*0.001   ! 0.001 from Pa to kPa
        st%melt=0.
        if (st%ta .gt. 1.0e-10 .and. st%snow_dsim .gt. 0.) st%melt=st%fa*(2.63+2.55*st%ta+0.0912*st%ta*st%rain_d)   !dbmemo updated version   
        if (st%dcount .gt.0. .and. st%ta .lt.5.) then
            st%melt=st%melt*EXP(-st%decay_m*st%dcount/365.)  !dbmemo dbice
        endif
        if (st%ta .le. 0.) then         ! dbmemo second bug in dbmemo
            in_snow =st%rain_d
        else
            in_snow = 0.
        endif
        dsnow         = in_snow-st%sublim-st%melt 
        snow_dsim_pre = st%snow_dsim
        st%snow_dsim     = st%snow_dsim + dsnow/st%rho_snow 
        if (st%snow_dsim .le. 0.0) then 
            st%snow_dsim=0.0 
            st%melt = snow_dsim_pre*st%rho_snow +in_snow-st%sublim    !! for water part
        endif 
        st%melt=AMAX1(st%melt, 0.)
        return
    end subroutine snow_d

    subroutine methane(st, iforcing) 
        implicit none
        type(site_data_type), intent(inout) :: st
        type(forcing_data_type), intent(in) :: iforcing
        integer i
        real(8) consum
        !****************************************************************************************************************************** 
        !     set values for MEMCMC
        !******************************************************************************************************************************       
        ! integer,parameter :: miterms=17
        integer,parameter :: miterms=29         ! modified based on Ma et al., 2022
        integer,parameter :: ilines=9000      
        !******************************************************************************************************************************
        !     CH4 Production      
        !******************************************************************************************************************************      
        !      real(8) Rhetero
        real(8) Rh_resp(nlayers), Rh_h,ProCH4(nlayers)!,Pro_sum
        ! real(8) r_me         !release ratio of CH4 to CO2
        ! real(8) Q10pro
        real(8) fSTP(nlayers)         !CH4 production factor of soil temperature
        ! real(8) vt,xt
        real(8) Tmax_me!,Tpro_me
        real(8) fpH          !CH4 production factor of soil pH
        real(8) fEhP         !CH4 production factor of soil redox potential
        ! real(8) FRLEN(nlayers)        !fraction of root in each layer
        real(8) FRLEN_PMT(nlayers)     ! modified based on Ma et al., 2022
        !****************************************************************************************************************************** 
        !     CH4 Oxidation
        !******************************************************************************************************************************      
        !      real(8) CH4(nlayers),CH4_V(nlayers+1)          !both are CH4 concentration: CH4(nlayers)unit gC/m2, CH4_V(nlayers) unit g C/ m3
        ! real(8) CH4(nlayers),CH4_V(nlayers)          !both are CH4 concentration: CH4(nlayers)unit gC/m2, CH4_V(nlayers) unit g C/ m3
        ! real(8) wsc(nlayers)      
        real(8) OxiCH4(nlayers)!,Oxi_sum       !CH4 oxidation
        real(8) Omax_layers(nlayers)!,Omax       !maximum oxidation rate
        real(8) kCH4_layers(nlayers)!,kCH4       !system specific half saturation constant
        real(8) Q10oxi
        real(8) fCH4(nlayers)         !CH4 oxidation factor of CH4 concentration
        real(8) fSTO(nlayers)         !CH4 oxidation factor of soil temperature
        real(8) fEhO         !CH4 oxidation factor of soil redox potential
        ! real(8) st%Toxi
        !****************************************************************************************************************************** 
        !     CH4 Diffusion
        !******************************************************************************************************************************      
        real(8) Deff(nlayers)     !CH4 effective diffusivity !!used in mineral soil  v1.1 
        real(8) D_CH4_a           !CH4 diffusion coefficient in air  !unit cm2 s-1   diffusivity of CH4 in air
        real(8) D_CH4_w           !CH4 diffusion coefficient in water  !unit cm2 s-1   diffusivity of CH4 in water
        real(8) phi          !soil porosity  also used in water table part
        real(8) fwater(nlayers),fair(nlayers)
        real(8) D_CH4_soil(nlayers),D_CH4_soil_a(nlayers),D_CH4_soil_b(nlayers)      !!used in organic peat soil  v1.2
        real(8) fcoarse      !relative volume of coarse pores depending on soil texture  Zhuang 2004
        real(8) ftort        !tortuosity coefficient with a value of 0.66    Walter and Heimann 2000
        !suggesting that the distance covered by diffusion is about two thirds of the length of the real(8) average path
        real(8) SAND         !relative contents of sand (%) in the soil
        real(8) PVSAND       !relative volume of coarse pores in sandy soils     set to 0.45     value from Walter 2001
        real(8) SILT         !relative contents of silt (%) in the soil
        real(8) PVSILT       !relative volume of coarse pores in silty soils     set to 0.20     value from Walter 2001
        real(8) CLAY         !relative contents of clay (%) in the soil
        real(8) PVCLAY       !relative volume of coarse pores in clayish soils     set to 0.14   value from Walter 2001
        ! real(8) DEPTH(10)        !depth in soil  will define it inside this subroutine again      resolution 100mm 200mm
        ! real(8) THKSL(10)        !will define it inside this subroutine again  
        ! real(8) Fdifu(nlayers+1)
        real(8) Fdifu(nlayers)
        real(8) CH4_atm      !concentration of CH4 in atmosphere     seen as 0 cause the value is too low someone use 0.076
        ! real(8) simuCH4      !simulated CH4 emission
        ! ***********  Boundary condition parameters    *************      
        real(8) ScCH4                                 !Schmidt numbers for methane Wania
        real(8) pistonv                               !Piston velocity
        real(8) Ceq                                   !equilibrium concentration of gas in the atmosphere
        real(8) kHinv                                 !Henry's coefficient dependent variable on left side of equation, T is the independent variable
        real(8) kH_CH4         !Henry's constant at standard temperature (CH4) Unit L atm mol-1
        real(8) CHinv          !Coefficient in Henry's Law Unit K      
        real(8) Tsta           !standard temperature Unit K
        real(8) Ppartial       !CH4 partial pressure in air Unit atm
        real(8) Cold           !last time step of ch4 concentration on first soil layer
        integer jwt    !index of the soil layer right above the water table (-)
        !****************************************************************************************************************************** 
        !     Ebullition 
        !******************************************************************************************************************************      
        real(8) CH4_thre_ly(nlayers),EbuCH4(nlayers),Kebu !CH4_thre,
        real(8) Ebu_sum_unsat,Ebu_sum_sat,Ebu_sum          !sum value one dimension is enough 
        integer wtlevelindex
        real(8) rouwater,V   ! local paras for ebullition concentration threshold calculation
        real(8) mebu_out2(nlayers)
        !****************************************************************************************************************************** 
        !     Plant transport
        !******************************************************************************************************************************      
        real(8) PlaCH4(nlayers),Pla_sum
        ! real(8) LAIMIN,LAIMAX
        real(8) Tgr,Tmat,fgrow,Pox,Kpla
        real(8) rh_layert, g, Rgas
        !****************************************************************************************************************************** 
        Rh_h       = st%Rh_lit_m + st%Rh_lit_s + st%Rh_soil_f + st%Rh_soil_s + st%Rh_soil_p
        FRLEN_PMT  = (/0.75,0.2,0.02,0.02,0.01,0.0,0.0,0.0,0.0,0.0/)
        st%simuCH4 = 0.0                 ! v1.2 
        rh_layert  = 0.0
        do i = 1, nlayers
            ! the empirical method used here is from CLM4.5
            Rh_resp(i)= 0.5*Rh_h*st%FRLEN(i) + 0.5*Rh_h*(st%thksl(i)/st%depth(10))
            rh_layert=rh_layert+Rh_resp(i)
        enddo   
           
        Tmax_me=45.0
        do i = 1,nlayers          
            if (do_soilphy) then
                if (st%tsoil_layer(i+1) .lt. 0.0) then
                    fSTP(i) = 0.0
                else if (st%tsoil_layer(i+1) .gt. Tmax_me) then
                    fSTP(i) = 0.0
                else if (st%tsoil_layer(i+1) .ge. 0.0 .and. st%tsoil_layer(i) .le. Tmax_me) then
                    fSTP(i) = st%Q10pro**((st%tsoil_layer(i+1)-st%Tpro_me)/10) 
                endif
            else 
                if (iforcing%Tsoil .lt. 0.0) then
                    fSTP(i) = 0.0
                else if (iforcing%Tsoil .gt. Tmax_me) then
                    fSTP(i) = 0.0
                else if (iforcing%Tsoil .ge. 0.0 .and. iforcing%Tsoil .le. Tmax_me) then
                    fSTP(i) = st%Q10pro**((iforcing%Tsoil-st%Tpro_me)/10)        
                endif
            endif
        enddo

        ! fpH assignment
        fpH=1.0
        ! fEhP assignment
        fEhP=1.0
        
        st%depth(1)=10.0                                  !calculate soil depth unit cm
        do i=2,nlayers
            st%depth(i)=st%depth(i-1)+st%thksl(i)
        enddo
          
        st%Pro_sum=0.0
        do i = 1,nlayers
            if ((st%depth(i)*10) .le. - st%zwt) then
                ProCH4(i)=0.0
            else
                if (((st%depth(i)*10.0)-(st%thksl(i)*10.0)) .lt. -st%zwt) then
                    ProCH4(i)=Rh_resp(i)*st%r_me*fSTP(i)*fpH*fEhP*(((st%depth(i)*10.0)-(-st%zwt))/(st%thksl(i)*10.0))     ! *percent
                elseif (((st%depth(i)*10.0)-(st%thksl(i)*10.0)) .ge. -st%zwt) then
                    ProCH4(i)=Rh_resp(i)*st%r_me*fSTP(i)*fpH*fEhP
                endif
            endif
            st%Pro_sum=st%Pro_sum+ProCH4(i)
        enddo
    
        !**************************************************
        !Add CH4 production to CH4 pool    (gC layer -1)=(gC m-2)
        !**************************************************
        do i=1,nlayers
            st%CH4(i) = st%CH4(i) + ProCH4(i)
        enddo  
        ! END OF METHANE PRODUCTION
        ! ********************************************************************************************************************
        ! B. methane oxidation      hourly  unit gC m-2 h-1     !!!!!!!!!!!method of CLM and Zhuang!!!!!!!!!!!!
        ! Methane oxidation is modeled as an aerobic process that occurs in the unsaturated zone of the soil profile ZHUANG
        ! ********************************************************************************************************************
        ! fSTO assignment
        ! ***************          
        Q10oxi=2.0      !Zhu 2014 results from previous studies  unit 1  also used by zhang
        ! st%Toxi=10.0       !Zhuang 2004 table1 Boreal Forest Wetland
        do i=1,nlayers
            if (do_soilphy) then
                fSTO(i)=Q10oxi**((st%tsoil_layer(i+1)-st%Toxi)/10.0)
            else
                fSTO(i)=Q10oxi**((iforcing%Tsoil-st%Toxi)/10.0)
            endif
        enddo
        ! fEhO assignment
        fEhO=1.0        !Walter 2000  did not consider it, equal to value of 1
    
        ! Omax assignment
        st%Oxi_sum=0.0
        do i = 1,nlayers
            Omax_layers(i)=(st%Omax/(1000000))*12*1000*(st%wsc(i)*0.001)     !convert the unit of Omax from μmol L-1 h-1 to gC m-2 h-1
            if (st%wsc(i).le.0) then
                kCH4_layers = 0.
            else
                kCH4_layers(i)=(st%kCH4/(1000000))*12*1000*(st%wsc(i)*0.001)    !convert the unit of kCH4 from μmol L-1 to gC m-2
            endif
                ! then calculate fCH4 with CH4(i) and kCH4_layers(i) 
            if ((kCH4_layers(i)+st%CH4(i)) .eq. 0)then
                fCH4(i)=0.
            else
                fCH4(i)=st%CH4(i)/(kCH4_layers(i)+st%CH4(i))   !  CH4 concentration factor
            endif

            if ((st%depth(i)*10.0) .le. -st%zwt) then                !unit of Omax: gC m-2 h-1
                OxiCH4(i)=Omax_layers(i)*fCH4(i)*fSTO(i)*fEhO!*0.1      !wrong:*(THKSL(i)/1000)!mm to m account for the thickness
            else
                if (((st%depth(i)*10.0)-(st%thksl(i)*10.0)) .lt. -st%zwt) then
                    if (i .eq. 1) then
                        OxiCH4(i)=Omax_layers(i)*fCH4(i)*fSTO(i)*fEhO*((-st%zwt)/(st%thksl(i)*10.0))
                    else
                        OxiCH4(i)=Omax_layers(i)*fCH4(i)*fSTO(i)*fEhO*(((-st%zwt)-(st%depth(i-1)*10.0))/(st%thksl(i)*10.0))      !  *percent
                    endif
                    
                else if (((st%depth(i)*10.0)-(st%thksl(i)*10.0)) .ge. -st%zwt) then
                    OxiCH4(i)= 0.0
                endif
            endif            
            if (OxiCH4(i) .gt. st%CH4(i)) then
                OxiCH4(i)=st%CH4(i)
            endif  
            st%Oxi_sum=st%Oxi_sum+OxiCH4(i) 
            ! if(isnan(st%Oxi_sum))   st%Oxi_sum = 0
        enddo 
    
        !*******************************************************************
        !minus CH4 oxidation from CH4 pool     
        !*******************************************************************
        do i=1,nlayers
            st%CH4(i) = st%CH4(i) - OxiCH4(i)               !minus CH4 oxidation from CH4 pool
            if (st%wsc(i) .le. 0) then
                st%CH4_V(i) = 0.
            else
                st%CH4_V(i) = st%CH4(i)/(st%wsc(i)*0.001)          !convert concentration from gC/m2 to gC/m3
            endif                                        !CH4_V(i) can be used for DA with observation data in soil layers
        enddo
        ! END OF METHANE OXIDATION
          
        ! ****************************************************
        ! C. methane diffusion
        ! ****************************************************
        ! Parameters assignment 
        D_CH4_a=0.2                             !unit cm2 s-1   D_CH4_a is the molecular diffusion coefficient of methane in air
        D_CH4_a=(D_CH4_a/10000.0)*3600.0        !unit m2 h-1
        D_CH4_w=0.00002                         !unit cm2 s-1   D_CH4_a is the molecular diffusion coefficient of methane in water
        D_CH4_w=(D_CH4_w/10000.0)*3600.0        !unit m2 h-1          
        ftort=0.66                              !tortuosity coefficient with a value of 0.66    Walter and Heimann 2000
        ! parameters for fcoarse algorithm      
        SAND=0.4          !   %   SPRUCE site value    0.4
        SILT=0.4          !   %   SPRUCE site value   0.4
        CLAY=0.2          !   %   SPRUCE site value   0.2
        PVSAND=0.45       !relative volume of coarse pores in sandy soils       set to 0.45     value from Walter 2001 zhuang
        PVSILT=0.20       !relative volume of coarse pores in silty soils       set to 0.20     value from Walter 2001 zhuang
        PVCLAY=0.14       !relative volume of coarse pores in clayish soils     set to 0.14     value from Walter 2001 zhuang  
        fcoarse=SAND*PVSAND+SILT*PVSILT+CLAY*PVCLAY
        CH4_atm=0.076       !unit umol L-1
        ! CH4_atm=0.0       !unit umol L-1      
        ! ******************************************************************************************************
        ! * Peat soil solution for diffusion coefficient: Equations for D_CH4_soil *         v1.2    Millington and Quirk Model
        ! ******************************************************************************************************
        phi = 0.85
        do i=1,nlayers
            fwater(i) = st%wsc(i)/(st%thksl(i)*10)      
            fair(i) = phi-fwater(i)
                        
            D_CH4_soil_a(i) = (((fair(i))**(10./3.))/((phi)**2))*D_CH4_a
            D_CH4_soil_b(i) = D_CH4_W
            if (fair(i) .ge. 0.05) then
                D_CH4_soil(i) = D_CH4_soil_a(i)
            else
                D_CH4_soil(i) = D_CH4_soil_b(i)
            endif
            Deff(i) = D_CH4_soil(i)
        enddo 
    
            
        ! ******************************************************************************************************
        ! * Mineral soil solution for diffusion coefficient: Equations for D_CH4_soil *         v1.1   Three-porosity-model
        ! ******************************************************************************************************
        CH4_atm = (CH4_atm/1000000)*12*1000 
        kH_CH4 = 714.29
        CHinv = 1600.0
        Tsta = 298.15
        Ppartial = 1.7E-6     !unit atm  partial pressure of methane in atmosphere
        ! *****  before Tsoil layers were added
        if (.not. do_soilphy) then
            ScCH4 = 1898 - 110.1*iforcing%Tsoil + 2.834*iforcing%Tsoil**2 - 0.02791*iforcing%Tsoil**3
            ScCH4   = Amax1(ScCH4, 1.) ! add by Jian, to limit it to be negative
            pistonv = 2.07 * (ScCH4/600)**(-1./2)  !n=-1/2   pistonv unit=m/s in Wania's paper but cm/h in the code
            pistonv = pistonv*0.01   ! convert from cm/h to m/h
            kHinv = kH_CH4 /(exp(CHinv*(1.0/(iforcing%Tsoil+273.15)-1.0/Tsta)))
            Ceq = Ppartial / kHinv    ! Ceq: mol L-1   p_partial: atm  kHinv：L atm mol-1
            Ceq = Ceq * 12 * 1000    ! Ceq mol/L to g/m3
        endif
        ! ********************************
        if (st%zwt .ge. -100) then   !index j, water table is right below the jth layer
            jwt=0.
        elseif (st%zwt .ge. -500.0) then  !layer 1-5
            jwt=int(-st%zwt/100)
        else
            jwt=int((-st%zwt-500)/200+5)
        endif

        do i=1,nlayers
            if (do_soilphy) then
                ScCH4 = 1898 - 110.1*st%tsoil_layer(i+1) + 2.834*(st%tsoil_layer(i+1))**2 - 0.02791*(st%tsoil_layer(i+1))**3
                ScCH4   = Amax1(ScCH4, 1.) ! add by Jian, to limit it to be negative
                pistonv = 2.07 * (ScCH4/600)**(-1./2)  !n=-1/2   pistonv unit=m/s in Wania's paper but cm/h in the code
                pistonv = pistonv*0.01   ! convert from cm/h to m/h
                kHinv   = kH_CH4 /((exp(CHinv*(1.0/(st%tsoil_layer(i+1)+273.15)-1.0/Tsta))))
                Ceq     = Ppartial / kHinv    ! Ceq: mol L-1   p_partial: atm  kHinv：L atm mol-1
                Ceq     = Ceq * 12 * 1000    ! Ceq mol/L to g/m3
            endif
            if (i .eq. 1 .and. jwt .ge. 1) then
                if (st%wsc(i) .le. 0) then
                    Fdifu(i) = 0.
                else
                    Fdifu(i) = Deff(i)*(st%CH4_V(i)-CH4_atm)/(st%wsc(i)*0.01)
                endif
            elseif (i .eq. 1) then !.and. jwt .eq. 0
                Cold = st%CH4_V(i)
                
                if (st%wsc(i) .le. 0) then
                    st%CH4_V(i) = 0.
                    Fdifu(i) = 0.
                else
                    st%CH4_V(i) = Ceq + (Cold-Ceq)*exp(-pistonv/(st%wsc(i)*0.001)) !pistonv/wsc m/m unit=1
                    Fdifu(i) = (Cold-st%CH4_V(i))*(st%wsc(i)*0.001)
                endif
            elseif (i .le. nlayers .and. i .ne. jwt+1) then
                
                if (st%wsc(i) .le. 0) then
                    Fdifu(i)=0.
                else
                    Fdifu(i)= Deff(i)*(st%CH4_V(i)-st%CH4_V(i-1))/(st%wsc(i)*0.01)
                endif
            elseif (i .le. nlayers .and. i .eq. jwt+1) then
                Cold = st%CH4_V(i)
                
                if (st%wsc(i) .le. 0) then
                    st%CH4_V(i) = 0.
                    Fdifu(i) = 0.
                else
                    st%CH4_V(i) = Ceq + (Cold-Ceq)*exp(-pistonv/(st%wsc(i)*0.001)) !pistonv/wsc m/m unit=1
                    Fdifu(i) = (Cold-st%CH4_V(i))*(st%wsc(i)*0.001)
                endif
            endif
        enddo

        do i=1,nlayers
            if (Fdifu(i) .gt. 0.0 .and. (Fdifu(i)) .gt. st%CH4(i)) then
                Fdifu(i)=0.999*st%CH4(i)
            endif
           
            if (Fdifu(i) .lt. 0.0) then
                if (i>1) then
                    if ((abs(Fdifu(i))) .gt. st%CH4(i-1)) Fdifu(i)=-0.999*st%CH4(i-1)
                else
                    Fdifu(i)=0
                endif
            endif
            IF (i>1)THEN
                if (st%CH4(i-1) .EQ.0) Fdifu=0.
            else
                if (st%CH4(1) .Eq. 0) Fdifu(i)=0
            endif
        enddo

        do i = 1,nlayers-1                                  !loop of time
            st%CH4(i) = st%CH4(i) + (Fdifu(i+1)-Fdifu(i))*1  
            if (st%CH4(i) .lt. 0.0) then                     ! this part need to be improved until deleted   V1.2
                st%CH4(i) = 0.0
            endif
        enddo  
        st%CH4(10) = st%CH4(10) - Fdifu(10)                                   !MODIFIED ON 07/25/2016
        if (st%CH4(10) .lt. 0.0) then                                    !defined the Fdifu(11) to be 0.0
            st%CH4(10)= 0.0                                              ! switch on/off
        endif
       
        st%simuCH4 = st%simuCH4 + (Fdifu(1)-0.0) 
        ! ********************************************************************************************************************      
        ! D. methane ebullition     !assume bubbles can reach the water table within 1 h&
                                    !& the bubbles is added to the methane concentration in the soil layer just above the wt
                                    !& and then diffused through layers   ??not correct
        ! this subroutine is modified on 02132017 by deleting the unsat from bubble and add unsat to concentration so as to increase diffusion
        ! just by searching "switch" you can switch from old to new mode by adding or deleting "!"
        ! modified threshold value to 100 for testing
        ! ********************************************************************************************************************
        !& and then diffused through layers
        !mechanisms selectable: ECT (concentration threshold) and EBG (bubble growth)
        !EBG is added as a subroutine on Nov 26th 2018, so that minimum changes to the original code
        ! Modified based on Ma et al., 2022
        Ebu_sum_unsat = 0.0
        Ebu_sum_sat   = 0.0                                      !initial value
        Ebu_sum       = 0.0

        rouwater = 1000.   !kg/m3
        g        = 9.81; ! m/s2
        Rgas     = 8.3145; ! m3 Pa/K/mol
        
        if (do_EBG) then
            call ebullition_EBG(st, rouwater, mebu_out2, Ebu_sum_sat, Ebu_sum_unsat)
        else
            !use ECT mechanisms for ebullition
            Kebu=1.0                    !unit  h-1   rate constant               

            do i=1,nlayers
                V = 0.05708 - 0.001545*MAX(0.0, st%tsoil_layer(i+1)) + 0.00002069*(MAX(0.0, st%tsoil_layer(i+1)))**2 ![m3 CH4 m-3 H2O in soil]
                ! ******   water height not correct, change to this:
                if (st%depth(i) .le. (-st%zwt)*0.1) then
                    st%pwater(i) = rouwater*g*(st%depth(i)*0.01-(-st%zwt)*0.001)   ![kg m s-2 or N], pwater/m2 = N/m2 = Pa
                else
                    st%pwater(i) = 0.
                endif
                st%CH4_thre    = ((st%dpatm + st%pwater(i))*V/(Rgas*(st%tsoil_layer(i+1)+273.15)))*1000!-200  ! mol CH4 /m3 to mmol CH4 /m3
                CH4_thre_ly(i) = (st%CH4_thre*1.0e-6)*12*1000*(st%wsc(i)*0.001)    !convert the unit of st%CH4_thre from µmol L-1 to gC m-2
            enddo
        
            if (st%zwt .ge. 0.0) then                                  !when water table is above the soil surface
                do i=1,nlayers
                    if (st%CH4(i) .gt. CH4_thre_ly(i)) then                  
                        EbuCH4(i)=Kebu*(st%CH4(i)-CH4_thre_ly(i))     !only if the concentration is larger than threshold
                    else 
                        EbuCH4(i)=0.0
                    endif
                    Ebu_sum_sat=Ebu_sum_sat+EbuCH4(i)               !& the bubbles are directly added into CH4 efflux into atmosphere
                    ! if(ISNAN(EbuCH4(i)))then ! add by Jian
                    !     EbuCH4(i) = 0.
                    ! endif

                    st%CH4(i)=st%CH4(i)- EbuCH4(i)                        !& update the concentration at the end of this hour in each layers
                enddo
            endif
            
            if (st%zwt .lt. 0.0) then                                  !when water table is below the soil surface
                do i=1,nlayers
                    if ((st%depth(i)*10.0) .le. -st%zwt) then               !acrotelm layers
                        EbuCH4(i)=0.0
                        Ebu_sum_unsat=Ebu_sum_unsat+EbuCH4(i)   
                        ! if(ISNAN(EbuCH4(i)))then ! add by Jian
                        !     EbuCH4(i) = 0.
                        ! endif      
                        st%CH4(i)=st%CH4(i)- EbuCH4(i) 
                    else
                        wtlevelindex = i
                        if (((st%depth(i)*10.0)-(st%THKSL(i)*10.0)) .lt. -st%zwt) then       !partly acrotelm layer
                            
                            if (st%CH4(i) .gt. CH4_thre_ly(i)) then                  
                                EbuCH4(i)=Kebu*(st%CH4(i)-CH4_thre_ly(i))!*(((st%depth(i)*10.0)-(-zwt))/(THKSL(i)*10.0))        ! * percent
                            else 
                                EbuCH4(i)=0.0
                            endif 
                            ! if(ISNAN(EbuCH4(i)))then ! add by Jian
                            !     EbuCH4(i) = 0.
                            ! endif
                            st%CH4(i)=st%CH4(i)- EbuCH4(i)
                            Ebu_sum_unsat=Ebu_sum_unsat+EbuCH4(i)                ! !  modified by Mary on 02132017
                            ! Jian: not sure why wtlevelindex-1? 20230123
                            if (wtlevelindex>1)then ! Jian: maybe > 1????
                                st%CH4(wtlevelindex-1)=st%CH4(wtlevelindex-1)+EbuCH4(i)    !!!!!-1-!!!! !switch on in new mode should be added add burst bubbles below surface to diffusion modified by Mary on 02132017
                            end if
                            ! ：the problem is the resolution of soil layer is 10cm and EbuCH4(i) is directly added to the upper layer of boundary layer   02152017                    
                        else if (((st%depth(i)*10.0)-(st%THKSL(i)*10.0)) .ge. -st%zwt) then   !catotelm layers
                            if (st%CH4(i) .gt. CH4_thre_ly(i)) then                  
                                EbuCH4(i)=Kebu*(st%CH4(i)-CH4_thre_ly(i))
                            else 
                                EbuCH4(i)=0.0
                            endif 
                            st%CH4(i)=st%CH4(i)- EbuCH4(i)    
                            
                            if (wtlevelindex>1) then
                            st%CH4(wtlevelindex-1)=st%CH4(wtlevelindex-1)+EbuCH4(i)     !!!!!-2-!!!! !switch on in new mode should be added
                            endif
                            !modified by Mary on 02152017
                            Ebu_sum_unsat=Ebu_sum_unsat+EbuCH4(i)                  ! modified by Mary on 02132017
                        endif
                    endif
                enddo
            endif
          
            Ebu_sum= Ebu_sum_sat
            st%simuCH4=st%simuCH4+Ebu_sum_sat                         !& the bubbles are directly added into CH4 efflux into atmosphere
        endif    
     
        ! ******************************************************************************************************
        ! E. plant mediated methane transportation      totoally used Walter's model also used by Zhuang et. al
        ! ******************************************************************************************************
        Kpla=0.01         !unit h-1
        ! Kpla=0.01         !unit h-1
        ! Tveg=0.3 ! a factor describing the quality of plant-mediated transport depend on the density of plant stands and plant types 
        ! 0 for boreal forest and 0.5 for tundra
        ! find in parafile !
        ! the Tsoil used here would be better if refer to the 20cm soil temperature after &
        ! & the accomplishment of soil heat dynamics module. according to Zhuang. however Walter used 50cm soil temp.
        Tgr=2.0               !unit degree Celsius if annual mean temp is below 5 (otherwise 7)
        Tmat=Tgr+10.0         !unit degree Celsius
        Pox=0.5               !50% of mediated methane are oxidised 
        ! define fgrow
        if (.not. do_soilphy) then
            if (iforcing%Tsoil .lt. Tgr) then
                fgrow=st%LAImin
            else if (iforcing%Tsoil .ge. Tgr .and. iforcing%Tsoil .le. Tmat) then
                fgrow=st%LAImin+st%LAImax*(1-((Tmat-iforcing%Tsoil)/(Tmat-Tgr))**2)
            else if (iforcing%Tsoil .gt. Tmat) then
                fgrow=st%LAImax
            endif
        else
            if (st%tsoil_layer(3) .lt. Tgr) then
                fgrow=st%LAImin
            else if (st%tsoil_layer(3) .ge. Tgr .and. st%tsoil_layer(3) .le. Tmat) then
                fgrow=st%LAImin+(st%LAImax)*(1-((Tmat-st%tsoil_layer(3))/(Tmat-Tgr))**2)
            else if (st%tsoil_layer(3) .gt. Tmat) then
                fgrow=st%LAImax
            endif
        endif  
        Pla_sum=0.0
        do i=1,nlayers
            PlaCH4(i) = Kpla*st%Tveg*st%FRLEN(i)*fgrow*st%CH4(i)*(1-st%Pox)         !not sensitive at all to this change, but better
            Pla_sum   = Pla_sum+PlaCH4(i)
            st%CH4(i)=st%CH4(i)-Kpla*st%Tveg*FRLEN_PMT(i)*fgrow*st%CH4(i)!PlaCH4(i)/(1-Pox)
            ! if(ISNAN(st%CH4(i)))then ! add by Jian
            !     st%CH4(i) = 0.
            ! endif
            if (st%wsc(i) .le. 0) then
                st%CH4_V(i) = 0.
            else
                st%CH4_V(i) = st%CH4(i)/(st%wsc(i)*0.001)          !convert concentration from gC/m2 to gC/m3
            endif
        enddo  
        st%simuCH4=st%simuCH4+Pla_sum
        ! Jian: check the CH4?
        ! if (ISNAN(st%simuCH4)) then
        !     st%simuCH4 = st%Pro_sum
        ! endif
        consum=st%simuCH4+OxiCH4(1)+OxiCH4(2)+OxiCH4(3)+OxiCH4(4)+OxiCH4(5)+OxiCH4(6)+OxiCH4(7)+OxiCH4(8)+OxiCH4(9)+OxiCH4(10) 
        return
    end subroutine methane

    ! *************************************************************************************
    ! subroutine EBG used by methane submodel
    subroutine ebullition_EBG(st,rouwater,mebu_out2,Ebu_sum_sat,Ebu_sum_unsat)
        ! INPUT:
        !		CH4 = porewater methane in the whole layer, unit=gC/m2, size=nlayers,CH4_V(i) = CH4(i)/(wsc(i)*0.001)
        !		CH4_V(nlayers) = porewater methane concentration, unit=gC/m3, size=nlayers,CH4_V(i) = CH4(i)/(wsc(i)*0.001)
        !		simuCH4 = methane efflux, added up with diffusion, ebullition, and PMT
        !		nlayers = number of layers, 10 by default
        !		zwt = water table level, unit = mm, size=1, below surface when zwt<0
        !		dpatm = atm pressure, input from the atmospheric forcing file, unit=Pa, estimated value from CNCEPT, used by CLM
        !		tsoil_layer = soil temperature of different layers, unit=Celsius, size = nlayers
        !       wsc = volumetric content of soil water in a layer, unit=mm, size = nlayers
        !		THKSL = thickness of the (i)th soil layer in the model, unit=cm, size = nlayers
        !     	depth = depth of the (i)th soil layer, unit=cm, size = nlayers
        !		rouwater = density of water, 1000. kg/m3
        !		g = 9.81 ! m/s2
        !		Rgas = 8.3145 ! m3 Pa/K/mol
        !OUTPUT:
        !		Ebu_sum = sum of ebullited methane finally get into the atm, size = 1,unit=gC/m2
        !		EbuCH4(nlayers) = ebullited methane each time step from one layer, size = nlayers,unit=gC/m2
        !		Ebu_sum_sat = ebullited methane each time step from one layer when water table is higher than soil surface, added
            !up to calculate Ebu_sum
        !		Ebu_sum_unsat = ebullited methane each time step from one layer when water table is below the soil surface, add to
            !CH4 and diffused up

        implicit none
        type(site_data_type), intent(inout) :: st
        integer mwt,ind, i
        ! INPUT parameters:
        real(8) rouwater,g,Rgas
        ! integer, parameter :: nlayers=10
        ! real(8) CH4(nlayers),CH4_V(nlayers),simuCH4,zwt,dpatm,tsoil_layer(11),wsc(nlayers),THKSL(nlayers),depth(nlayers)
        ! OUTPUT parameters:
        ! real(8) Ebu_sum,Ebu_sum_sat,Ebu_sum_unsat,EbuCH4(nlayers)
        real(8) Ebu_sum_sat,Ebu_sum_unsat!,EbuCH4(nlayers)
        ! INTERNAL parameters:
        ! xP: amount of x in each layer [mol], really [mol] not [mol m-3]
        ! real(8) Vp(nlayers),
        real(8) Vtmp(nlayers) !total volume of the bubbles in a layer, size(nlayers), unit m3/layer
        ! real(8) methanebP(nlayers) !amount of CH4 moles in bubbles, size(nlayers), unit mol/layer, not concentration, not m-3
        ! real(8) methaneP(nlayers)  !amount of CH4 moles in porewater, size (nlayers), unit mol/layer  CH4(i) gC/layer
        real(8) mrateP(nlayers)    !The rate at which gas transfer with bubbles modifies gas at each layer, [mol s-1 layer-1]
        real(8) met_db(nlayers)    !calculated in the exceeded bubble module, pore water CH4 concentration change rate due to
        !interaction with bubbles, unit = mol hr-1, size=nz
        real(8) mCwP         !CH4 molar density in the water (mol m-3), size=1

        real(8) dc
        real(8) Dw,Hcp,mebu_out,r,randbub
        real(8), parameter :: peat_coeff_w = 0.9
        real(8), parameter :: Dw_298 = 1.5e-9 ! [m2 s-1]
        real(8) mnv! CH4 molar density in the bubble (mol m-3)           ! Shuang mnv=cb in their paper

        ! real(8) bubble_methane_tot	!amount of CH4 stored in bubbles
        real(8) mebu_rate_out      !pore water CH4 concentration change rate due to interaction with bubbles, unit = mol s-1, size=nz
        real(8) mebu_out2(nlayers) ! ebullition from the layers are temporarily saved here
        real(8) Vout       ! in the equation 8), the exceeded volume of methane in bubbles
        real(8) nout       !nout = Febu in equation 8) EBG, unit mol. = bubble conc * volume
        !real(8) gases_out(2)  !1 for ebu into air, 2 for ebu into the lowest air layer
        ! real(8) presP(nlayers) ! total pressure of air and water in layers
        ! real(8) pwater(nlayers)! water pressure in layers
        real(8) tempP(nlayers)  ! Kalvin soil temperature
        real(8) met_alphaP(nlayers) !methane solubility
        real(8) a,b,c  !constants used to calculate methane solubility
        ! half-life of supersaturated dissolved methane
        !integer, parameter :: dp = selected_real_kind(15, 300)
        real(8), parameter :: ebu_hl = 1800. ! 30 minutes, 1800 s
        real(8), parameter :: k = log(2.0)/ebu_hl  ! turnover rate s-1
        ! real(8), parameter :: pi = 3.141592653589793
        !          !  1 = methane ebullition into air
        !          !  2 = methane ebullition total (when WTD is below peat surface, ebullition is
        !          !      released in the lowest air layer inside peat)
        !        integer, parameter :: mebuair = 1, mebutot = 2
        !*******************
        ! input parameters
        !*******************

        ! real(8) f !threshold fraction of pore space filled with gas bubbles needed for ebullition or
                        ! CH4 mixing ratio in bubbles (mol mol-1)
        ! real(8) Nbub ! Amount of bubbles in one model layer. This directly impacts the CH4 exchange
                            ! rate between pore water and bubbles
        ! real(8) Vmaxfraction ! Maximum fraction of volume occupied by bubbles (10 %)
                                                    ! If this is exceeded, then the extra volume is released
        ! real(8) bubprob ! probability that a bubble will get stuck at one layer
        !********************************************************************************


        !******************************************************************************
        !******************************************************************************
        ! inside the time loop starts here, add to methane module
        !******************************************************************************

        !****  #2. add mwt as index for the wt layer  *************
        if (st%zwt .ge. -100) then   !index mwt, water table is right at the mwt th layer
            mwt=1.
        elseif (st%zwt .ge. -500.0) then  !layer 1-5
            mwt=int(-st%zwt/100)+1
        else
            mwt=int((-st%zwt-500)/200+5)+1
        endif
        !       if (zwt .le. -100.) then
        !           write (*,*) 'mwt',mwt,'zwt',zwt
        !       endif
        !****  #2. end of mwt as index for the wt layer  *************


        !****  #3. update value, might not be necessary
        do i=1,nlayers
            st%methaneP(i) = st%CH4(i)/12      !##need to make sure CH4 is updated at the end of EBG
            !				  gC/layer  /12   unit molC/layer
            tempP(i) = st%tsoil_layer(i+1)+273.15 !unit Kelvin
        g = 9.81 ! m/s2
        if (i .ge. mwt) then
            st%pwater(i) = rouwater*g*(st%depth(i)*0.01-(-st%zwt)*0.001)
        else
            st%pwater(i)  = 0.
        endif
        
        st%presP(i) = st%dpatm + st%pwater(i)  ! unit Pa
        Rgas = 8.3145; ! m3 Pa/K/mol
        st%Vp(i)    = st%methanebP(i) * Rgas * tempP(i)/(st%presP(i) * st%f)  !Vp is updated since methanebP is updated in the middle of the EBG

        !*******#4. bubble_methane_tot, bubble_methane_tot is accumulated total of all the 10 layers,
        st%bubble_methane_tot = st%bubble_methane_tot+st%f * st%presP(i)/(Rgas * tempP(i)) * st%Vp(i)
        !
        !******************************************************************************


        !*******#5. methane_solubility (alpha) ****************************************
        !calculate methane_solubility (alpha) using tempP,a,b,c,Rgas and Hcp
        !alpha: unit [mol(CH4,water) m(water)-3 mol(CH4,air)-1 m(air)3]
        !Hcp: unit [mol(CH4) m(H2O)-3 Pa-1]
        !Rgas = 8.3144621_dp ! [J mol-1 K-1]
        a = 1.3e-3
        b = 1700.
        c = 298.0
        ! Tang et al. 2010, eq. A4
        ! see also
        Hcp = a * exp(b * (1./tempP(i) - 1./c)) * 9.86923266716e-3
        met_alphaP(i) = Rgas * tempP(i) * Hcp
        enddo
        !******************************************************************************
        !    gases_out = 0
        mebu_rate_out = 0
        !*******#6. *******************************************************************
        ! methane_to_bubble - Transferring CH4 between bubbles and the surrounding pore water
        ! input
        ! INPUT:
        !       presP = pressure vector, unit=Pa, size=nz
        !       tempP = pore water temperature vector, unit=K, size=nz
        !       methaneP = CH4 pore water concentration vector, unit=mol/layer, size=nz
        !       met_alphaP = CH4 solubility (dimensionless Henry solubility) vector, unit=-, size=nz
        !       nz = amount of model layers, size=1
        !       geom = model geometry
        !       por = peat porosity, unitless, size=1
        !       Vp = bubble volume vector, unit=m3, size=nz
        ! OUTPUT:
        !       mrateP = mebu_rateP = methane_d = met_d = pore water CH4 concentration change rate due to interaction with bubbles,
        !unit = mol s-1, size=nz
        !   grows (and shrinks) bubbles in each layer based on Henry's law
        !  equation 6
        !    write (*,*) "1 stopping point"
        mrateP = 0
        !! if layers with water peat mixture exist
        do i = mwt,nlayers
        ! CH4 molar density in the water (mol m-3)
            if (st%wsc(i) .eq. 0) then
                mCwP = 0.
            else
                mCwP = st%methaneP(i) / ((st%wsc(i)*0.001)*1);
            endif
        ! concentration difference between pore water and bubble surface
        dc = (mCwP - met_alphaP(i) * st%f * st%presP(i)/(Rgas * tempP(i))) !  Shuang this is part 2 of equation (5)

        if (st%Vp(i) == 0) then           !Shuang equation (6)
        ! creating a bubble from the excess CH4
        if (dc > 0) then
        ! making sure that CH4 exchange FROM bubble TO water does not exist when there is no bubble
        mrateP(i) = -k * dc * ((st%wsc(i)*0.001)*1)
        !            write (*,*) 'Vp=0, dc>0 i',i, 'mrateP(i)',mrateP(i)
        !(geom % dzP(i) * por) thickness of the layer * porosity (*1 m2) = water volume
        !			mol s-1		s-1	 mol m-3	m3
        !when mrateP is negative it means transfer from water to bubble
        end if
        !"Nbub" no need to consider Nbub because mrateP is already the total exchange amount

        else
        ! growing the bubble only if it already exists

        ! radius of one bubble (m)
        r = (3.0/4.0 * st%Vp(i)/st%Nbub/pi)**(1.0/3.0)     ! r updated with Vp, calculated from the last time step in EBG

        ! CH4 diffusion coefficient in water & peat mixture
        !          Dw = methane_D_water(tempP(i)) !this function outputs Dw
        !

        Dw = peat_coeff_w * Dw_298 * (tempP(i)/298.)
        ! if (temperature < 273.15) Dw = 0

        ! change in pore water CH4 due to mass transfer with one bubble   !Shuang part of equation (5)
        mrateP(i) = -4.0 * pi * r * Dw * dc
        !          write (*,*) 'mrateP(i)',mrateP(i),'r',r,'dc',dc
        !            write (*,*) 'Vp/=0 i',i, 'mrateP(i)',mrateP(i)
        ! Nbub bubbles
        mrateP(i) = mrateP(i) * st%Nbub      !mol s-1
        end if
        ! ***** end of #6. methane_to_bubble output is mrateP = mebu_rateP = methane_d=met_d
        !******************************************************************************
        !******************************************************************************
        ! ***** #7. now we are calculating methanebP, amount of CH4 moles in bubbles, size(nlayers), unit mol
        ! removing/adding the same amount of CH4 to the bubbles that was added/removed
        ! to the pore water due to interaction with the bubbles
        st%methanebP(i) = st%methanebP(i) - 3600 * mrateP(i) ! %% the unit of mrateP is mol s-1  %%%%%%%%%%%
        ! making sure that the concentrations do not go negative
        if (st%methanebP(i) < 0) then
        st%methaneP(i) = st%methaneP(i) + st%methanebP(i)
        st%methanebP(i) = 0
        end if
        ! updating bubble volumes
        st%Vp(i) = st%methanebP(i) * Rgas * tempP(i)/(st%presP(i) * st%f)

        ! ***** #8. *********************************************************************
        ! pore water CH4 concentration change rate due to interaction with bubbles    negative mrateP: lost methane from water to bubbles
        st%methaneP(i) = st%methaneP(i) + 3600 * mrateP(i)    ! %%%%%%%%%%%%%%%%%%%%%%
        mebu_rate_out = mebu_rate_out + 3600 * mrateP(i)      ! total of all the layers
        !mebu_rate_out=pore water CH4 concentration change rate due to interaction with bubbles, unit = mol s-1, size=nz

        ! change rate of pore water CH4 due to exchange with bubble
        mebu_rate_out = mebu_rate_out/1  !1 hr for big_dt... it is only an output
        !written as mebu_rate_out in the bigstep module, changed to mebu_rateP, but never used, just an output
        ! *****************************************************************************
        enddo
        !    write (*,*) "2 stopping point"
        ! *****************************************************************************
        ! releasing part of the gas volume if it exceeds certain size
        ! INPUT:
        !       Vp = bubble volume vector, unit=m3, size=nz
        !       presP = pressure vector, unit=Pa, size=nz
        !       tempP = pore water temperature vector, unit=K, size=nz
        !       geom = model geometry
        !       por = peat porosity, unitless, size=1
        !       dt = model time step, unit=s,size=1
        !       nz = amount of model layers, size=1
        !       methanebP = CH4 bubble concentration vector, unit=mol, size=nz
        ! OUTPUT:
        !       met_db = pore water CH4 concentration change rate due to interaction with bubbles, unit = mol hr-1, size=nz
        !       mebu_out = CH4 released in bubbles to deepest air layer, unit = mol hr-1, size=1

        ! If bubble in a certain layer grows larger than Vmax then it is transported to the deepest air layer
        ! Currently considers only CH4
        do i = 1,nlayers
        met_db(i) = 0   !met_db: due to bubble
        mebu_out = 0    !mebu_out: one dimension
        mebu_out2(i) = 0;    ! mebu_out2 = ebullition from the layers are temporarily saved here
        Vtmp(i) = st%Vp(i)
        enddo

        ! layers with water peat mixture exist
        ! looping from bottom to top layer of water-peat mix
        do i = 10, mwt, -1
        ! CH4 molar density in the bubble (mol m-3)           ! Shuang mnv=cb in their paper
        mnV = st%f * st%presP(i)/(Rgas * tempP(i))
        if ((Vtmp(i) - st%Vmaxfraction * (st%wsc(i)*0.001)*1) > 1e-11) then

        ! bubble was released

        !Equation 8)
        Vout = Vtmp(i) - st%Vmaxfraction * ((st%wsc(i)*0.001)*1)
        Vtmp(i) = st%Vmaxfraction * ((st%wsc(i)*0.001)*1)

        ! the size of the bubble increases as it ascends, but the amount of moles in the bubble stay the same
        nout = mnV * Vout                             ! Shuang nout=Febu in the paper, unit mol, mnV=cb, Vout=the rest
        st%methanebP(i) = st%methanebP(i) - nout
        !          write (*,*) 'mnV * Vout  i',i,'mnV',mnV,'Vout',Vout,'nout',nout
        !***** this is 'if bubble got stuck loop'
        ind = i
        do while (Vout > 0 .AND. ind >= mwt + 1)
        ind = ind - 1

        call RANDOM_NUMBER(randbub)
        !            write (*,*) 'randbub',randbub
        if (randbub <= st%bubprob) then
            ! bubble got stuck, and bubble is added back to the upper i-1 layer
        st%methanebP(ind) = st%methanebP(ind) + nout
        Vtmp(ind) = st%methanebP(ind) * Rgas * tempP(ind)/(st%f * st%presP(ind))

        Vout = 0
        nout = 0
        end if

        end do

        ! bubble did not get stuck => it is released to the lowest air layer
        ! This 'if' loop and the 'do while' loop is either or, Vout will be 0 if the do while loop worked
        if (Vout > 0) then
        mebu_out2(i) = nout/1    !dt  here I assign 1 hour

        end if
        !     write (*,*) 'Vout > 0  i',i,'Vout',Vout,'mebu_out2(i)',mebu_out2(i)
        end if
        !        write (*,*) 'i',i,'Vout',Vout,'mnV',mnV,'nout',nout
        end do   !end of do i = 10, 2, -1
        !     write (*,*) "3 stopping point"


        ! updating the bubble volume profile
        st%Vp = Vtmp
        ! a1: First box of peat-air. If 0, there is no peat-air, a2 has no meaning.
        ! a2: Last box of peat-air.
        ! w1: First box of peat-water. If 0, no peat-water, w2 has no meaning.
        ! w2: Last box of peat-water.
        if (mwt .gt. 1) then! if air-peat layer is present
        met_db(mwt-1) = sum(mebu_out2) ! bubbles released to deepest air layer, sum of all the water layers
        ! ebu_out is already 0, no need to set again
        else              ! if all layers are flooded, porewater CH4 was already updated
        mebu_out = sum(mebu_out2);
        !        write (*,*) 'mwt mebu_out',mebu_out,mebu_out2
        end if

        ! end of releasing part of the gas volume if it exceeds certain size
        ! *****************************************************************************


        ! *****************************************************************************
        do i = 1,nlayers
        ! bubbles are released to the lowest air layer if wtd is below surface
        !   if wtd is above surfacr all the met_db(i)=0
        st%methaneP(i) = st%methaneP(i) + 1 * met_db(i) !big_dt replaced with 1hr  %%%%%%%%%%%% need to edit: add to with layer
        !  integer, parameter :: mebuair = 1, mebutot = 2
        !  1 = methane ebullition into air
        !  2 = methane ebullition total (when WTD is below peat surface, ebullition is
        !      released in the lowest air layer inside peat)

        ! for output
        st%CH4(i)=12*st%methaneP(i)
        if (st%wsc(i) .eq. 0) then
            st%CH4_V(i) = 0.
        else
            st%CH4_V(i)=st%CH4(i)/(st%wsc(i)*0.001)
        endif
        
        enddo

        ! mebu_out and met_db are larger than 0. the amount of CH4 gets out
        ! amount of CH4 released to the atmosphere  !%%%% I changed gases_out_bigstep into gases_out=Ebu_sum_unsat  sat
        Ebu_sum_sat = Ebu_sum_sat + (1 * mebu_out)*12       !big_dt replaced with 1hr  %%%% mol/layer to gC/layer %%%%%%%%
        ! amount of CH4 released to the lowest air layer if WTD is below the surface
        Ebu_sum_unsat = Ebu_sum_unsat + 1 * sum(met_db)*12   !big_dt replaced with 1hr  %%%% mol/layer to gC/layer %%%%%%%%

        if (Ebu_sum_sat .ne. 0.) then
        !        write (*,*) 'Ebu_sum_sat', Ebu_sum_sat
        endif
        if (Ebu_sum_unsat .ne. 0.) then
        !        write (*,*) 'Ebu_sum_unsat', Ebu_sum_unsat
        endif

        st%simuCH4=st%simuCH4+Ebu_sum_sat
        !    write (*,*) 'mebu_out',mebu_out,'Ebu_sum_sat',Ebu_sum_sat
        ! 	gases_out=gases_out vector containing CH4 flux caused by ebullition
        ! *****************************************************************************
        !    gases_out_rates = gases_out / dt    !Shuang dt=24*3600

        ! amount of CH4 stored in bubbles
        st%bubble_methane_tot = sum(st%f * st%presP/(Rgas * tempP) * st%Vp)   !this is equation (10)
        !    methanebub2 = bubble_methane_tot
        return
    end subroutine ebullition_EBG
    !   *************************************************************************************

    real(8) function esat(T)   ! returns saturation vapour pressure in Pa
        real(8) T
        esat = 610.78*exp(17.27*T/(T+237.3))
        return
    end
end module soil