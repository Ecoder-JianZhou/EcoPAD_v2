module vegetation
   !=================================================================================
   !  main subroutines  :  canopy,  respiration, plantgrowth 
   !             canopy => yrday,   xlayers,     Tsoil_simu
   !            xlayers => Radiso,  goudriaan,   agsean_day,  agsean_ngt
   !         agsean_day => photosyn
   !           photosyn => ciandA
   ! functions:
   !           sinbet, esat, VJtemp, fJQres, EnzK
   !=================================================================================
   use datatypes
   use soil
   
   USE, INTRINSIC :: IEEE_ARITHMETIC
   implicit none
   real(8) :: fbeam, coszen, Radabv(2), raero
   
   contains
   subroutine vegn_canopy(st, iforcing)
      implicit none
      type(site_data_type), intent(inout) :: st
      type(forcing_data_type), intent(in) :: iforcing
      ! local
      real(8) :: Acan1, Acan2, Ecan1, Ecan2
      integer :: ipft
      
      radsol = iforcing%PAR
      radsol = AMAX1(radsol,0.01)
      call  yrday(iforcing%doy,iforcing%hour,st%lat)!,radsol,fbeam)  ! calculate beam fraction in incoming solar radiation
      coszen = sinbet(iforcing%doy, iforcing%hour+1, st%lat)       ! cos zenith angle of sun
      ! calculate soil albedo for NIR as a function of soil water (Garratt pp292)
      if(st%topfws.gt.0.5) then
         rhoS(2)=0.18
      else
         rhoS(2)=0.52-0.68*st%topfws
      endif
      radabv(1) = 0.5*radsol                    !(1) - solar radn
      radabv(2) = 0.5*radsol                    !(2) - NIR
      ! summary the variables in the xlayers that are needed to use in Tsoil
      st%QLair    = 0.
      st%QLleaf   = 0.
      st%Rsoilab1 = 0.
      st%Rsoilab2 = 0.
      st%gpp      = 0.
      st%transp   = 0.
      do ipft = 1, npft
         call xlayers(st, st%sp(ipft), iforcing, Acan1, Acan2, Ecan1, Ecan2)
         st%sp(ipft)%gpp    = (Acan1 + Acan2)*3600.0*12.0 ! every hour, g C m-2 h-1
         st%sp(ipft)%transp = AMAX1((Ecan1 + Ecan2)*3600.0/(1.0e6*(2.501-0.00236*iforcing%Tair)),0.) ! mm H2O /hour
         ! site-based summary
         st%QLair    = st%QLair    + st%sp(ipft)%pft_weight * st%sp(ipft)%QLair
         st%QLleaf   = st%QLleaf   + st%sp(ipft)%pft_weight * st%sp(ipft)%QLleaf
         st%Rsoilab1 = st%Rsoilab1 + st%sp(ipft)%pft_weight * st%sp(ipft)%Rsoilab1
         st%Rsoilab2 = st%Rsoilab2 + st%sp(ipft)%pft_weight * st%sp(ipft)%Rsoilab2
         st%gpp      = st%gpp      + st%sp(ipft)%pft_weight * st%sp(ipft)%gpp
         st%transp   = st%transp   + st%sp(ipft)%pft_weight * st%sp(ipft)%transp
      enddo
      if(do_soilphy) call Tsoil_simu(st, iforcing)  ! also update the st%Esoil

      if(ISNAN(st%Rsoilab3)) then
            return
      endif 
      st%evap   = AMAX1(st%Esoil*3600.0/(1.0e6*(2.501-0.00236*iforcing%Tair)),0.)
   end subroutine vegn_canopy

   subroutine yrday(doy,hour,lat)
      integer :: doy, hour
      real(8) :: lat !, radsol, fbeam
      real(8) pidiv, slatx, sindec, cosdec
      real(8) a, b, sinbet0, solext, tmprat, tmpR, tmpK, fdiff
      pidiv = pi/180.0
      slatx = lat*pidiv
      sindec = -sin(23.4*pidiv)*cos(2.0*pi*(doy + 10.0)/365.0)
      cosdec = sqrt(1.-sindec*sindec)
      a = sin(slatx)*sindec
      b = cos(slatx)*cosdec
      sinbet0 = a + b*cos(2*pi*(hour - 12.)/24.)
      solext = 1370.0*(1.0 + 0.033*cos(2.0*pi*(doy - 10.)/365.0))*sinbet0
      tmprat = radsol/solext
      tmpR = 0.847 - 1.61*sinbet0 + 1.04*sinbet0*sinbet0
      tmpK = (1.47 - tmpR)/1.66
      if (tmprat .le. 0.22) fdiff = 1.0
      if (tmprat .gt. 0.22 .and. tmprat .le. 0.35) then
         fdiff = 1.0 - 6.4*(tmprat - 0.22)*(tmprat - 0.22)
      end if
      if (tmprat .gt. 0.35 .and. tmprat .le. tmpK) then
         fdiff = 1.47 - 1.66*tmprat
      end if
      if (tmprat .ge. tmpK) then
         fdiff = tmpR
      end if
      fbeam = 1.0 - fdiff
      if (fbeam .lt. 0.0) fbeam = 0.0
      return
   end subroutine yrday

   ! autotrophic respiration
   subroutine respiration(st, iforcing)
      ! calculate plant and soil respiration by the following equation:
      ! RD=BM*Rd*Q10**((T-25)/10) (Sun et al. 2005. Acta Ecologica Sinica)
      implicit none
      type(site_data_type), intent(inout) :: st
      type(forcing_data_type), intent(in) :: iforcing
      integer :: ipft
      ! local variables
      real(8) :: conv                  ! converter from "umol C /m2/s" to "gC/m2/hour"
 
      conv = 3600.*12./1000000.    ! umol C /m2/s--> gC/m2/hour
      st%Rmleaf = 0.0
      st%Rmstem = 0.0
      st%Rmroot = 0.0
      st%Rmain  = 0.0
      do ipft = 1, npft
         if(st%sp(ipft)%LAI.gt.st%sp(ipft)%LAIMIN) then
            st%sp(ipft)%RmLeaf = st%sp(ipft)%Rl0*st%sp(ipft)%SNRauto*st%sp(ipft)%bmleaf*0.48*st%sp(ipft)%SLA*0.1  &
               &                *st%sp(ipft)%Q10**((iforcing%Tair-10.)/10.)*st%sp(ipft)%fnsc*conv
            st%sp(ipft)%RmStem = st%sp(ipft)%Rs0*st%sp(ipft)%SNRauto*st%sp(ipft)%StemSap*0.001 &
               &                *st%sp(ipft)%Q10**((iforcing%Tair-25.)/10.)*st%sp(ipft)%fnsc*conv
            st%sp(ipft)%RmRoot = st%sp(ipft)%Rr0*st%sp(ipft)%SNRauto*st%sp(ipft)%RootSap*0.001 &
               &                *st%sp(ipft)%Q10**((iforcing%Tair-25.)/10.)*st%sp(ipft)%fnsc*conv
         else
            st%sp(ipft)%RmLeaf = 0.3*st%sp(ipft)%GPP
            st%sp(ipft)%RmStem = 0.3*st%sp(ipft)%GPP
            st%sp(ipft)%RmRoot = 0.4*st%sp(ipft)%GPP
         endif
         st%sp(ipft)%Rmain = st%sp(ipft)%Rmleaf + st%sp(ipft)%Rmstem + st%sp(ipft)%Rmroot
         if(st%sp(ipft)%Rmain > 0.0015*st%sp(ipft)%NSC)then             ! If Total autotropic respiration greater than 0.15% of Nonstructure Carbon, rescale.
            st%sp(ipft)%Rmleaf = st%sp(ipft)%Rmleaf/st%sp(ipft)%Rmain*0.0015*st%sp(ipft)%NSC
            st%sp(ipft)%Rmstem = st%sp(ipft)%Rmstem/st%sp(ipft)%Rmain*0.0015*st%sp(ipft)%NSC
            st%sp(ipft)%Rmroot = st%sp(ipft)%RmRoot/st%sp(ipft)%Rmain*0.0015*st%sp(ipft)%NSC
            st%sp(ipft)%Rmain  = st%sp(ipft)%Rmleaf + st%sp(ipft)%Rmstem + st%sp(ipft)%Rmroot
         endif
         st%Rmleaf = st%Rmleaf + st%sp(ipft)%pft_weight * st%sp(ipft)%RmLeaf
         st%Rmstem = st%Rmstem + st%sp(ipft)%pft_weight * st%sp(ipft)%RmStem
         st%Rmroot = st%Rmroot + st%sp(ipft)%pft_weight * st%sp(ipft)%RmRoot
         st%Rmain  = st%Rmain  + st%sp(ipft)%pft_weight * st%sp(ipft)%Rmain
      enddo
      return
   end subroutine respiration

   !     plant growth model
   subroutine plantgrowth(st, iforcing)
      implicit none
      type(site_data_type), intent(inout) :: st
      type(forcing_data_type), intent(in) :: iforcing
      ! local variables
      real(8) :: Tcold, phiN
      real(8) :: bmL, bmR, bmS, bmP, acP, CNp0
      real(8) :: scalW!,scalT
      ! real(8) :: GrowthP, GrowthL, GrowthR, GrowthS
      real(8) :: gamma_Wmax, gamma_Tmax
      real(8) :: bW, bT, beta_T, gamma_W, gamma_T, gamma_N
      integer :: ipft

      Tcold = 5.0      !    Tcold=0.0       ! For SPRUCE
      phiN  = 0.33
      ! Leaf litter
      gamma_Wmax = 0.12/24. ! maxmum leaf fall rate per hour
      gamma_Tmax = 0.12/24.
      bW = 4.0
      bT = 2.0
      st%Rgleaf = 0.
      st%Rgstem = 0.
      st%Rgroot = 0.
      st%Rgrowth = 0.

      do ipft = 1, npft
         bmL = st%sp(ipft)%bmleaf * 0.48   ! Carbon
         bmR = st%sp(ipft)%bmRoot * 0.48
         bmS = st%sp(ipft)%bmStem * 0.48
         if(bmL.lt.st%sp(ipft)%NSC/0.333) bmL = st%sp(ipft)%NSC/0.333
         if(bmR.lt.st%sp(ipft)%NSC/0.333) bmR = st%sp(ipft)%NSC/0.333
         if(bmS.lt.st%sp(ipft)%NSC/0.334) bmS = st%sp(ipft)%NSC/0.334
         st%sp(ipft)%StemSap = st%sp(ipft)%SapS*bmS  ! Weng 12/05/2008
         st%sp(ipft)%RootSap = st%sp(ipft)%SapR*bmR
         if(st%sp(ipft)%StemSap.lt.0.001) st%sp(ipft)%StemSap=0.001
         if(st%sp(ipft)%RootSap.lt.0.001) st%sp(ipft)%RootSap=0.001
         bmP  = bmL + bmR + bmS          ! Plant C biomass
         acP  = bmL + st%sp(ipft)%StemSap + bmS      ! Plant available sapStem C
         CNp0 = bmP/(bmL/st%sp(ipft)%CN0_L + bmR/st%sp(ipft)%CN0_R + bmS/st%sp(ipft)%CN0_S)  ! Plant CN ratio
         st%sp(ipft)%ht     = st%sp(ipft)%hmax*(1.-exp(-st%sp(ipft)%hl0*bmP))    ! Scaling plant C biomass to height
         st%sp(ipft)%LAIMAX = AMAX1(st%sp(ipft)%LAIMAX0 * &
            &                 (1.-exp(-st%sp(ipft)%la0*st%sp(ipft)%ht)),st%sp(ipft)%LAIMIN+0.1)  ! Scaling plant height to maximum LAI

         !   Phenology
         if((st%GDD5.gt.st%sp(ipft)%gddonset).and.st%sp(ipft)%onset.eq.0.and.&
           & st%sp(ipft)%storage.gt.st%sp(ipft)%stor_use) then
            st%sp(ipft)%onset=1
         endif
         if((st%sp(ipft)%onset.eq.1).and.(st%sp(ipft)%storage.gt.st%sp(ipft)%stor_use))then
            if(st%sp(ipft)%LAI.lt.st%sp(ipft)%LAIMAX) st%sp(ipft)%add=st%sp(ipft)%stor_use
            st%sp(ipft)%storage = st%sp(ipft)%storage - st%sp(ipft)%add
         else
            st%sp(ipft)%add   = 0.0
            st%sp(ipft)%onset = 0
         endif
         if(st%sp(ipft)%accumulation.lt.(st%sp(ipft)%NSCmax+0.005*st%sp(ipft)%RootSap)&
            .and. st%sp(ipft)%NSC .gt. st%sp(ipft)%NSCmin)then
            st%sp(ipft)%store=AMAX1(0.,0.0001*st%sp(ipft)%NSC)  ! 0.5% of nonstructure carbon is stored
         else
            st%sp(ipft)%store=0.0
         endif
         st%sp(ipft)%accumulation = st%sp(ipft)%accumulation + st%sp(ipft)%store
         ! !   Scalars for plant growth
         ! !      Sps=Amin1(1.0,3.33*AMAX1(0.0,1.0 - fnsc))
         ! Sps=Sps*(1.-exp(-phiN*NSN))							! Sps is not assigned previous, something is wrong. -JJJJJJJJJJJJJJJJJJJJJ
         ! Ss=AMIN1(1.0,2.*fnsc)
         ! RS0   = 1.0
         ! RS    = bmR/bmL
         ! SL_rs = RS/(RS+RS0*(2.-W))
         ! SR_rs = (RS0*(2.-W))/(RS+RS0*(2.-W))
         ! Slai  = amin1(1.0,2.333*(LAIMAX-LAI)/(LAIMAX-LAIMIN))
         st%sp(ipft)%scalT = AMAX1(0.0, 1.0-exp(-(iforcing%Tair-st%sp(ipft)%gddonset/10.)/5.0))  !0.5 !
         !      Sw=AMAX1(0.333, 0.333+omega)
         scalW = AMIN1(0.5, AMAX1(0.333, 0.333+st%omega))
         !     Plant growth and allocation, based on LM3V
         st%sp(ipft)%CNp0    = CNp0
         st%sp(ipft)%GPmax   = (st%sp(ipft)%GLmax*bmL &
                               +st%sp(ipft)%GSmax*st%sp(ipft)%StemSap &
                               +st%sp(ipft)%GRmax*bmR) !/acP
         ! st%sp(ipft)%test1   = st%sp(ipft)%GPmax*st%sp(ipft)%fnsc*st%sp(ipft)%scalT*(1.-exp(-st%sp(ipft)%NSN))
         ! st%sp(ipft)%test2   = 0.004*st%sp(ipft)%NSC
         ! st%sp(ipft)%test3   = 0.004*st%sp(ipft)%NSN*CNp0
         ! st%sp(ipft)%test1   = AMIN1(st%sp(ipft)%GPmax*st%sp(ipft)%fnsc*st%sp(ipft)%scalT*(1.-exp(-st%sp(ipft)%NSN)),&
         ! &            0.004*st%sp(ipft)%NSC, 0.004*st%sp(ipft)%NSN*CNp0)

         if(ipft .eq. 3) then
         st%sp(ipft)%GrowthP = Amax1(AMIN1(st%sp(ipft)%GPmax*st%sp(ipft)%fnsc*st%sp(ipft)%scalT*(1.-exp(-st%sp(ipft)%NSN)),&
                              &            0.004*st%sp(ipft)%NSC, & 
                              &            0.004*st%sp(ipft)%NSN*CNp0),0.) 
         
         else
         st%sp(ipft)%GrowthP = Amax1(AMIN1(st%sp(ipft)%GPmax*st%sp(ipft)%fnsc*st%sp(ipft)%scalT*(1.-exp(-st%sp(ipft)%NSN)),&
                              &            0.004*st%sp(ipft)%NSC, & 
                              &            0.004*st%sp(ipft)%NSN*CNp0),0.)
         endif
         st%sp(ipft)%GrowthP = st%sp(ipft)%GrowthP*def_s_npp(ipft)
         ! st%sp(ipft)%GrowthL = MAX(0.0,st%sp(ipft)%GrowthP*0.5)      ! updated when QC leaf and Stem changed due to the change of plot area for tree biomass
         st%sp(ipft)%GrowthL = MAX(0.0,st%sp(ipft)%GrowthP*st%sp(ipft)%fn2l)
         ! st%sp(ipft)%GrowthR = Amax1(MIN(st%sp(ipft)%GrowthP*0.4,MAX(0.0,0.75/scalW*bmL-bmR)), 0.)  ! *c1/(1.+c1+c2)
         st%sp(ipft)%GrowthR = Amax1(MIN(st%sp(ipft)%GrowthP*st%sp(ipft)%fn2r,MAX(0.0,0.75/scalW*bmL-bmR)), 0.)
         st%sp(ipft)%GrowthS = MAX(0.0,st%sp(ipft)%GrowthP - (st%sp(ipft)%GrowthL+st%sp(ipft)%GrowthR) )         ! *c2/(1.+c1+c2)  
         ! if(st%sp(ipft)%LAI .gt. st%sp(ipft)%LAIMAX .and. st%sp(ipft)%GrowthR+st%sp(ipft)%GrowthS .ne. 0)then ! added by Mary Chris
         !    st%sp(ipft)%GrowthR = st%sp(ipft)%GrowthR/(st%sp(ipft)%GrowthR+st%sp(ipft)%GrowthS)*&
         !                         (st%sp(ipft)%GrowthL+st%sp(ipft)%GrowthR+st%sp(ipft)%GrowthS)
         !    st%sp(ipft)%GrowthS = st%sp(ipft)%GrowthS/(st%sp(ipft)%GrowthR+st%sp(ipft)%GrowthS)*&
         !                         (st%sp(ipft)%GrowthL+st%sp(ipft)%GrowthR+st%sp(ipft)%GrowthS)
         !    st%sp(ipft)%GrowthL = 0
         ! endif 
         st%sp(ipft)%NPP = st%sp(ipft)%GrowthL + st%sp(ipft)%GrowthR + st%sp(ipft)%GrowthS + st%sp(ipft)%add       ! Modified by Jiang Jiang 2015/10/13  
         ! st%sp(ipft)%NPP = st%sp(ipft)%NPP * 0
         if (st%sp(ipft)%NPP < 0) then
            print*, "NPP is error: ", st%sp(ipft)%GrowthL, st%sp(ipft)%GrowthR, st%sp(ipft)%GrowthS, st%sp(ipft)%add
            ! stop
            return
         endif

         ! if (st%sp(1)%NPP > 0) then
         !    print*,"test1: ",st%sp(1)%GrowthL, st%sp(1)%GrowthR, st%sp(1)%GrowthS, st%sp(1)%add 
         !    print*,"test2: ",st%sp(1)%GrowthP, st%sp(1)%fn2l,  st%sp(1)%fn2r, 0.75/scalW*bmL-bmR
         !    print*,"test3: ",st%sp(1)%GPmax*st%sp(1)%fnsc*st%sp(1)%scalT*(1.-exp(-st%sp(1)%NSN)),&
         !                      &            0.004*st%sp(1)%NSC, & 
         !                      &            0.004*st%sp(1)%NSN*CNp0

         !    print*,"test4: ",st%sp(1)%GLmax, st%sp(1)%GRmax, st%sp(1)%Gsmax
         !    stop
         ! endif


         
         if(st%sp(ipft)%NPP.eq.0.0)then
            st%sp(ipft)%alpha_L = 0.333
            st%sp(ipft)%alpha_W = 0.333
            st%sp(ipft)%alpha_R = 0.333
         else
            st%sp(ipft)%alpha_L = (st%sp(ipft)%GrowthL+st%sp(ipft)%add)/st%sp(ipft)%NPP
            st%sp(ipft)%alpha_W = st%sp(ipft)%GrowthS/st%sp(ipft)%NPP
            st%sp(ipft)%alpha_R = st%sp(ipft)%GrowthR/st%sp(ipft)%NPP
         endif  
         ! Carbon cost for growth; Rgrowth,Rgroot,Rgleaf,Rgstem, 0.5 is from IBIS and Amthor, 1984
         if(do_annual_mcmc)then
            st%sp(ipft)%RgLeaf  = st%sp(ipft)%f_rg * st%sp(ipft)%GrowthL ! * 0.5
            st%sp(ipft)%RgStem  = st%sp(ipft)%f_rg * st%sp(ipft)%GrowthS
            st%sp(ipft)%RgRoot  = st%sp(ipft)%f_rg * st%sp(ipft)%GrowthR
         else
            st%sp(ipft)%RgLeaf  = 0.5 * st%sp(ipft)%GrowthL ! * 0.5
            st%sp(ipft)%RgStem  = 0.5 * st%sp(ipft)%GrowthS
            st%sp(ipft)%RgRoot  = 0.5 * st%sp(ipft)%GrowthR
         endif
         ! st%sp(ipft)%RgLeaf  = st%sp(ipft)%f_rg * st%sp(ipft)%GrowthL ! * 0.5
         ! st%sp(ipft)%RgStem  = st%sp(ipft)%f_rg * st%sp(ipft)%GrowthS
         ! st%sp(ipft)%RgRoot  = st%sp(ipft)%f_rg * st%sp(ipft)%GrowthR
         st%sp(ipft)%Rgrowth = st%sp(ipft)%Rgleaf + st%sp(ipft)%Rgstem + st%sp(ipft)%Rgroot
         st%Rgleaf  = st%Rgleaf  + st%sp(ipft)%pft_weight * st%sp(ipft)%RgLeaf
         st%Rgstem  = st%Rgstem  + st%sp(ipft)%pft_weight * st%sp(ipft)%RgStem
         st%Rgroot  = st%Rgroot  + st%sp(ipft)%pft_weight * st%sp(ipft)%RgRoot
         st%Rgrowth = st%Rgrowth + st%sp(ipft)%pft_weight * st%sp(ipft)%Rgrowth
         ! Jian: some scales not used
         if(iforcing%Tair.gt.(Tcold+10.)) then
            beta_T=1.
         else
            if(iforcing%Tair.gt.Tcold) beta_T=(iforcing%Tair-Tcold)/10.
            if(iforcing%Tair.LE.Tcold)beta_T=0.0
         endif
         if (st%sp(ipft)%Tau_Leaf < 8760.)then
            gamma_W = (1. - AMIN1(1.0,3.333*st%omega))**bW * gamma_Wmax
            gamma_T = (1. - beta_T)**bT * gamma_Tmax
         else
            gamma_W=0.
            gamma_T=0.
         endif
         gamma_N = 1.0/st%sp(ipft)%Tau_Leaf*scalW      ! Modify by Jiang Jiang 2015/10/20
         if(st%sp(ipft)%LAI < st%sp(ipft)%LAIMIN) then
            gamma_W=0.
            gamma_T=0.
            gamma_N=0.
         endif
         !  L_fall=bmleaf*0.48*AMIN1((gamma_T+gamma_N),0.99)
         st%sp(ipft)%L_fall=st%sp(ipft)%bmleaf*0.48*gamma_N
      enddo
      return
   end subroutine plantgrowth

   subroutine xlayers(st, sp, iforcing, Acan1, Acan2, Ecan1, Ecan2)
      ! the multi-layered canopy model developed by Ray Leuning with the new radiative transfer scheme
      ! implemented by Y.P. Wang (from Sellers 1986) 12/Sept/96 (YPW) correction for mean surface temperature of sunlit
      ! and shaded leaves Tleaf,i=sum{Tleaf,i(n)*fslt*Gaussw(n)}/sum{fslt*Gaussw(n)}
      ! ----------------------------------------------------------------------
      implicit none
      type(site_data_type),    intent(inout) :: st
      type(spec_data_type),    intent(inout) :: sp
      type(forcing_data_type), intent(in)    :: iforcing
      real(8) :: Acan1, Acan2, Ecan1, Ecan2
      ! local variables
      real(8) :: Gaussx(5),Gaussw(5)
      real(8) :: wiltpt, fildcp, Rnst1, Rnst2
      real(8) :: Qcan1, Qcan2, Rcan1, Rcan2, Hcan1, Hcan2
      real(8) :: Gbwc1, Gbwc2, Gswc1, Gswc2, Tleaf1, Tleaf2
      real(8) :: wind, xphi1, xphi2, funG, extKb
      real(8) :: pi180, cozen15, cozen45, cozen75, xK15, xK45, xK75
      real(8) :: transd, flait, extkd, extkn
      real(8) :: scatt(2), kpr(3,2), rhoch, rhoc15, rhoc45, rhoc75, rhoc(3, 2)
      real(8) :: reff(3,2), TairK, flai
      real(8) :: Qabs(3,2)  ! calculated in goudriaan
      real(8) :: emair, Rnstar(2), grdn, windUx, scalex, Vcmxx, eJmxx
      real(8) :: Tleaf(2), Aleaf(2), Eleaf(2), Hleaf(2), gbleaf(2), gsleaf(2)
      real(8) :: fslt, fshd, FLAIT1, Tlk1, Tlk2
      real(8) :: Qd0, Qb0
      integer :: nw, ng

      ! Normalised Gaussian points and weights (Goudriaan & van Laar, 1993, P98)
      data Gaussx/0.0469101,0.2307534,0.5,0.7692465,0.9530899/ 
      data Gaussw/0.1184635,0.2393144,0.2844444,0.2393144,0.1184635/
      ! soil water conditions
      wiltpt = st%wsmin/100.
      fildcp = st%wsmax/100.
      ! reset the vairables
      Rnst1  = 0.0        !net rad, sunlit
      Rnst2  = 0.0        !net rad, shaded
      Qcan1  = 0.0        !vis rad
      Qcan2  = 0.0
      Rcan1  = 0.0        !NIR rad
      Rcan2  = 0.0
      Acan1  = 0.0        !CO2
      Acan2  = 0.0
      Ecan1  = 0.0        !Evap
      Ecan2  = 0.0
      Hcan1  = 0.0        !Sens heat
      Hcan2  = 0.0
      Gbwc1  = 0.0        !Boundary layer conductance
      Gbwc2  = 0.0
      Gswc1  = 0.0        !Canopy conductance
      Gswc2  = 0.0
      Tleaf1 = 0.0       !Leaf Temp
      Tleaf2 = 0.0 
      sp%Aleaf(:) = 0.0
      wind   = iforcing%WS
      if(wind.lt.0.01) wind=0.01 
      raero  = 50./wind   ! aerodynamic resistance
      ! Ross-Goudriaan function for G(u) (see Sellers 1985, Eq 13)
      xphi1 = 0.5 - 0.633*sp%xfang -0.33*sp%xfang*sp%xfang
      xphi2 = 0.877 * (1.0 - 2.0*xphi1)
      funG  = xphi1 + xphi2*coszen               ! G-function: Projection of unit leaf area in direction of beam
      if(coszen.gt.0) then                       ! check if day or night
         extKb = funG/coszen                     ! beam extinction coeff - black leaves
      else
         extKb = 100.
      end if
      ! Goudriaan theory as used in Leuning et al 1995 (Eq Nos from Goudriaan & van Laar, 1994)
      ! Effective extinction coefficient for diffuse radiation Goudriaan & van Laar Eq 6.6)
      pi180   = 3.1416/180.
      cozen15 = cos(pi180*15)
      cozen45 = cos(pi180*45)
      cozen75 = cos(pi180*75)
      xK15    = xphi1/cozen15+xphi2
      xK45    = xphi1/cozen45+xphi2
      xK75    = xphi1/cozen75+xphi2
      flait   = sp%LAI
      transd  = 0.308*exp(-xK15*flait)+0.514*exp(-xK45*flait)+     &
         &      0.178*exp(-xK75*flait)
      ! extkd   = (-1./flait)*real(alog(real(transd, 4)),8)
      extkd   = (-1./flait) * real(alog(max(real(transd,4), 1.0e-30)), 8)
      ! if(ieee_is_finite(extkd))then 
      if(extkd > huge(1.0) .or. extkd < -huge(1.0))then
         print*, "extkd1: ",extkd, flait, real(alog(real(transd, 4)),8)
         print*,"extkd2: ", transd
      endif
      extkn   = extkd
      do nw = 1, 2
         scatt(nw)  = tauL(nw) + rhoL(nw)                  ! scattering coeff
         if((1.-scatt(nw))<0.0) scatt(nw) = 0.9999         ! Weng 10/31/2008
         kpr(nw,1)  = extKb*sqrt(1.-scatt(nw))             ! modified k beam scattered (6.20)
         kpr(nw,2)  = extkd*sqrt(1.-scatt(nw))             ! modified k diffuse (6.20)
         rhoch      = (1.-sqrt(1.-scatt(nw)))/(1.+sqrt(1.-scatt(nw)))            ! canopy reflection black horizontal leaves (6.19)
         rhoc15     = 2.*xK15*rhoch/(xK15+extkd)                                 ! canopy reflection (6.21) diffuse
         rhoc45     = 2.*xK45*rhoch/(xK45+extkd)
         rhoc75     = 2.*xK75*rhoch/(xK75+extkd)
         rhoc(nw,2) = 0.308*rhoc15+0.514*rhoc45+0.178*rhoc75
         rhoc(nw,1) = 2.*extKb/(extKb+extkd)*rhoch                                ! canopy reflection (6.21) beam 
         reff(nw,1) = rhoc(nw,1)+(rhoS(nw)-rhoc(nw,1))*exp(-2.*kpr(nw,1)*FLAIT)   ! effective canopy-soil reflection coeff - beam (6.27)           
         reff(nw,2) = rhoc(nw,2)+(rhoS(nw)-rhoc(nw,2))*exp(-2.*kpr(nw,2)*FLAIT)   ! effective canopy-soil reflection coeff - diffuse (6.27)
      enddo
      ! isothermal net radiation & radiation conductance at canopy top - needed to calc emair
      ! Jian: flai and Qabs are not assigned here
      ! call Radiso(st, iforcing, extkd, flai, flait, Qabs, Rnstar, grdn) ! Jian: some parameters not initialization.
      TairK = iforcing%Tair + 273.2
      do ng = 1, 5
         flai = gaussx(ng)*FLAIT
         ! radiation absorption for visible and near infra-red
         ! Goudriaan theory as used in Leuning et al 1995 (Eq Nos from Goudriaan & van Laar, 1994)
         do nw = 1,2
            Qd0        = (1.-fbeam)*radabv(nw)                                    ! diffuse incident radiation
            Qb0        = fbeam*radabv(nw)                                         ! beam incident radiation
            Qabs(nw,2) = Qd0*(kpr(nw,2)*(1.-reff(nw,2))*exp(-kpr(nw,2)*FLAI))+  & ! absorbed radiation - shaded leaves, diffuse
               &         Qb0*(kpr(nw,1)*(1.-reff(nw,1))*exp(-kpr(nw,1)*FLAI)-   & ! beam scattered
               &         extKb*(1.-scatt(nw))*exp(-extKb*FLAI))
            Qabs(nw,1) = Qabs(nw,2)+extKb*Qb0*(1.-scatt(nw))                      ! absorbed radiation - sunlit leaves
         end do
         ! isothermal net radiation & radiation conductance at canopy top
         call Radiso(st, iforcing, extkd, flai, flait, Qabs, emair, Rnstar, grdn)           
         windUx = wind*exp(-st%extkU*flai)             !windspeed at depth xi
         scalex = exp(-extkn*flai)                    !scale Vcmx0 & Jmax0
         Vcmxx  = sp%Vcmx0*scalex
         eJmxx  = sp%eJmx0*scalex
         if(.not.ieee_is_finite(Vcmxx)) then
            print*,"vcmxx is finite: ",ieee_is_finite(Vcmxx), Vcmxx, sp%Vcmx0, scalex, extkn, flai
            ! stop
            return
         endif
         if(radabv(1).ge.10.0) then                          !check solar Radiation > 10 W/m2
            ! leaf stomata-photosynthesis-transpiration model - daytime
            call agsean_day(st, sp, iforcing, windUx, Qabs, grdn, Vcmxx, eJmxx, Rnstar, &
                  & Tleaf, Aleaf, Eleaf, Hleaf, gbleaf, gsleaf)      
         else
            call agsean_ngt(st, sp, iforcing, windUx, Qabs, grdn, Vcmxx, Rnstar, &
                  & Tleaf, Aleaf, Eleaf, Hleaf, gbleaf, gsleaf)
         endif  
         fslt      = exp(-extKb*flai)                        !fraction of sunlit leaves
         fshd      = 1.0-fslt                                !fraction of shaded leaves
         Rnst1     = Rnst1+fslt*Rnstar(1)*Gaussw(ng)*FLAIT  !Isothermal net rad`
         Rnst2     = Rnst2+fshd*Rnstar(2)*Gaussw(ng)*FLAIT
         ! RnstL(ng) = Rnst1+Rnst2

         Qcan1     = Qcan1+fslt*Qabs(1,1)*Gaussw(ng)*FLAIT  !visible
         Qcan2     = Qcan2+fshd*Qabs(1,2)*Gaussw(ng)*FLAIT
         ! QcanL(ng) = Qcan1+Qcan2

         Rcan1     = Rcan1+fslt*Qabs(2,1)*Gaussw(ng)*FLAIT  !NIR
         Rcan2     = Rcan2+fshd*Qabs(2,2)*Gaussw(ng)*FLAIT
         ! RcanL(ng) = Rcan1+Rcan2

         if(Aleaf(1).lt.0.0)Aleaf(1)=0.0      !Weng 2/16/2006
         if(Aleaf(2).lt.0.0)Aleaf(2)=0.0      !Weng 2/16/2006

         Acan1 = Acan1+fslt*Aleaf(1)*Gaussw(ng)*FLAIT*sp%stom_n    !amphi/hypostomatous
         Acan2 = Acan2+fshd*Aleaf(2)*Gaussw(ng)*FLAIT*sp%stom_n

         sp%Aleaf(1) = sp%Aleaf(1) + Aleaf(1)*Gaussw(ng)/sum(Gaussw)
         sp%Aleaf(2) = sp%Aleaf(2) + Aleaf(2)*Gaussw(ng)/sum(Gaussw)
         sp%Aleaf(3) = sp%Aleaf(3) + Aleaf(1)*Gaussw(ng)/sum(Gaussw) + Aleaf(2)*Gaussw(ng)/sum(Gaussw)

         ! AcanL(ng)  = Acan1+Acan2

         ! layer1(ng) = Aleaf(1)
         ! layer2(ng) = Aleaf(2)

         Ecan1     = Ecan1+fslt*Eleaf(1)*Gaussw(ng)*FLAIT
         Ecan2     = Ecan2+fshd*Eleaf(2)*Gaussw(ng)*FLAIT
         ! EcanL(ng) = Ecan1+Ecan2

         Hcan1     = Hcan1+fslt*Hleaf(1)*Gaussw(ng)*FLAIT
         Hcan2     = Hcan2+fshd*Hleaf(2)*Gaussw(ng)*FLAIT
         ! HcanL(ng) = Hcan1+Hcan2

         Gbwc1 = Gbwc1+fslt*gbleaf(1)*Gaussw(ng)*FLAIT*sp%stom_n
         Gbwc2 = Gbwc2+fshd*gbleaf(2)*Gaussw(ng)*FLAIT*sp%stom_n

         Gswc1 = Gswc1+fslt*gsleaf(1)*Gaussw(ng)*FLAIT*sp%stom_n
         Gswc2 = Gswc2+fshd*gsleaf(2)*Gaussw(ng)*FLAIT*sp%stom_n

         Tleaf1 = Tleaf1+fslt*Tleaf(1)*Gaussw(ng)*FLAIT
         Tleaf2 = Tleaf2+fshd*Tleaf(2)*Gaussw(ng)*FLAIT
         ! print*, fslt, Tleaf(1), Gaussw(ng), FLAIT
      enddo  ! 5 layers
      FLAIT1 = (1.0-exp(-extKb*FLAIT))/extkb
      Tleaf1 = Tleaf1/FLAIT1
      Tleaf2 = Tleaf2/(FLAIT-FLAIT1)
      ! Soil surface energy and water fluxes
      ! Radiation absorbed by soil
      sp%Rsoilab1 = fbeam*(1.-reff(1, 1))*exp(-kpr(1, 1)*FLAIT)        &
          &         + (1.-fbeam)*(1.-reff(1, 2))*exp(-kpr(1, 2)*FLAIT)          !visible
      sp%Rsoilab2 = fbeam*(1.-reff(2, 1))*exp(-kpr(2, 1)*FLAIT)        &
          &         + (1.-fbeam)*(1.-reff(2, 2))*exp(-kpr(2, 2)*FLAIT)          !NIR
      st%Rsoilab1 = st%Rsoilab1*Radabv(1)
      st%Rsoilab2 = st%Rsoilab2*Radabv(2)
      ! st%test1  = fbeam
      Tlk1     = Tleaf1 + 273.2
      Tlk2     = Tleaf2 + 273.2
      ! temp1=-extkd*FLAIT
      sp%QLair    = emair*sigma*(TairK**4)*exp(-extkd*FLAIT)
      sp%QLleaf   = emleaf*sigma*(Tlk1**4)*exp(-extkb*FLAIT)           &
                 &      + emleaf*sigma*(Tlk2**4)*(1.0 - exp(-extkb*FLAIT))
      sp%QLleaf   = sp%QLleaf*(1.0 - exp(-extkd*FLAIT))  
      if(ISNAN(sp%QLleaf)) then
         print*,"here:", emleaf, sigma, Tlk1,Tlk2, extkb, FLAIT
      endif
   end subroutine xlayers

   subroutine Radiso(st, iforcing, extkd, flai, flait, Qabs, &
      emair, Rnstar, grdn)! outputs
      implicit none
      type(site_data_type), intent(inout) :: st
      type(forcing_data_type), intent(in) :: iforcing
      real(8), intent(in)    :: extkd, flai, flait, Qabs(3,2)
      real(8), intent(inout) :: Rnstar(2), grdn
      ! Rnstar(type): type=1 for sunlit; =2 for shaded leaves (W/m2) 23 Dec 1994
      ! calculates isothermal net radiation for sunlit and shaded leaves under clear skies
      real(8) :: Tairk, rhocp, emsky, ep8z, tau8, emcloud, emair
      real(8) :: Bn0, Bnxi

      TairK   = iforcing%Tair + 273.2
      ! thermodynamic properties of air
      rhocp   = cpair*Patm*airMa/(Rconst*TairK)   !volumetric heat capacity (J/m3/K)
      ! apparent atmospheric emissivity for clear skies (Brutsaert, 1975)
      emsky   = 0.642*(st%eairP/Tairk)**(1./7)       !note eair in Pa
      ! apparent emissivity from clouds (Kimball et al 1982)
      ep8z    = 0.24+2.98e-12*st%eairP*st%eairP*exp(3000/TairK)
      tau8    = amin1(1.0,1.0-ep8z*(1.4-0.4*ep8z))            !ensure tau8<1
      emcloud = 0.36*tau8*(1.-fbeam)*(1-10./TairK)**4      !10 from Tcloud = Tair-10
      ! apparent emissivity from sky plus clouds
      ! emair=emsky+emcloud
      ! 20/06/96
      emair = emsky
      if(emair.gt.1.0) emair=1.0
      ! net isothermal outgoing longwave radiation per unit leaf area at canopy
      ! top & thin layer at flai (Note Rn* = Sn + Bn is used rather than Rn* = Sn - Bn in Leuning et al 1985)
      Bn0  = sigma*(TairK**4.)
      Bnxi = Bn0*extkd*(exp(-extkd*flai)*(emair-emleaf) + exp(-extkd*(flait-flai))*(emsoil-emleaf))
      ! isothermal net radiation per unit leaf area for thin layer of sunlit and shaded leaves
      Rnstar(1) = Qabs(1,1)+Qabs(2,1)+Bnxi
      Rnstar(2) = Qabs(1,2)+Qabs(2,2)+Bnxi
      if(isnan(Rnstar(1)))then
         print*, Qabs(1,1),Qabs(2,1),Bnxi
      endif
      if(isnan(Rnstar(2)))then
         print*, Qabs(1,2),Qabs(2,2),Bnxi
      endif
      ! radiation conductance (m/s) @ flai
      grdn = 4.*sigma*(TairK**3.)*extkd*emleaf*(exp(-extkd*flai)+exp(-extkd*(flait-flai)))/rhocp 
      if(isnan(grdn))then
         print*,"grdn",sigma,TairK,extkd,emleaf,extkd,flai,extkd,flait,flai,rhocp
      endif 
      return
   end subroutine

   subroutine agsean_day(st, sp, iforcing, windUx, Qabs, grdn, Vcmxx, eJmxx, Rnstar, &
      Tleaf, Aleaf, Eleaf, Hleaf, gbleaf, gsleaf)
      implicit none
      type(site_data_type), intent(inout) :: st
      type(spec_data_type), intent(inout) :: sp
      type(forcing_data_type), intent(in) :: iforcing
      real(8), intent(in) :: windUx, Qabs(3,2), grdn, Rnstar(2), Vcmxx, eJmxx
      real(8), intent(inout) :: Tleaf(2), Aleaf(2), Eleaf(2),Hleaf(2), gbleaf(2), gsleaf(2)
      ! local variables
      real(8) :: Tairk, rhocp, H2OLv, slope, psyc, Cmolar, weighJ
      real(8) :: gbHu, Tlk, Dleaf, co2cs, Qapar
      integer :: ileaf, kr1
      real(8) :: Gras, gbHf, gbH, rbH, rbw, rbH_L, rrdn, Y
      real(8) :: gbc, gsc0, gsw, gswv, rswv, Tlk1
      real(8) :: Aleafx, Gscx ! from Photosynsis

      ! thermodynamic parameters for air
      TairK  = iforcing%Tair + 273.2
      rhocp  = cpair*Patm*AirMa/(Rconst*TairK)
      H2OLv  = H2oLv0-2.365e3*iforcing%Tair
      slope  = (esat(iforcing%Tair+0.1) - esat(iforcing%Tair))/0.1
      psyc   = Patm*cpair*AirMa/(H2OLv*H2OMw)
      Cmolar = Patm/(Rconst*TairK)
      weighJ = 1.0
      ! boundary layer conductance for heat - single sided, forced convection
      ! (Monteith 1973, P106 & notes dated 23/12/94)
      if(windUx/wleaf>=0.0)then
         gbHu = 0.003*sqrt(windUx/wleaf)    !m/s
      else
         gbHu = 0.003         !*sqrt(-windUx/wleaf)
      endif         ! Weng 10/31/2008

      do ileaf = 1,2                   ! loop over sunlit and shaded leaves
         ! first estimate of leaf temperature - assume air temp
         Tleaf(ileaf) = iforcing%Tair
         Tlk          = Tleaf(ileaf) + 273.2    !Tleaf to deg K
         ! first estimate of deficit at leaf surface - assume Da
         Dleaf = st%Dair                !Pa
         ! first estimate for co2cs
         co2cs = co2ca               !mol/mol
         Qapar = (4.6e-6)*Qabs(1,ileaf)
         kr1   = 0                     !iteration counter for LE
         ! return point for evaporation iteration
         do               !iteration for leaf temperature
            ! single-sided boundary layer conductance - free convection (see notes 23/12/94)
            Gras = 1.595e8*ABS(Tleaf(ileaf)-iforcing%Tair)*(wleaf**3.)     !Grashof
            gbHf = 0.5*Dheat*(Gras**0.25)/wleaf
            gbH  = gbHu+gbHf                         !m/s
            rbH  = 1./gbH                            !b/l resistance to heat transfer
            rbw  = 0.93*rbH                          !b/l resistance to water vapour
            ! Y factor for leaf: stom_n = 1.0 for hypostomatous leaf;  stom_n = 2.0 for amphistomatous leaf
            rbH_L = rbH*sp%stom_n/2.                   !final b/l resistance for heat
            rrdn  = 1./grdn
            Y     = 1./(1.+ (rbH_L+raero)/rrdn)
            ! boundary layer conductance for CO2 - single side only (mol/m2/s)
            gbc    = Cmolar*gbH/1.32            !mol/m2/s
            gsc0   = gsw0/1.57                 !convert conductance for H2O to that for CO2
            ! varQc  = 0.0
            ! weighR = 1.0
            call photosyn(st, sp, Qapar, Vcmxx, eJmxx, Tlk, gsc0, Dleaf, Gbc, &
               Aleafx, Gscx)  !outputs
            ! choose smaller of Ac, Aq
            Aleaf(ileaf) = Aleafx      !0.7 Weng 3/22/2006          !mol CO2/m2/s
            ! calculate new values for gsc, cs (Lohammer model)
            co2cs        = co2ca-Aleaf(ileaf)/gbc
            ! co2Ci(ileaf) = co2cs-Aleaf(ileaf)/gscx
            gsw  = gscx*1.56       !gsw in mol/m2/s, oreginal:gsw=gscx*1.56,Weng20090226
            gswv = gsw/Cmolar                           !gsw in m/s
            rswv = 1./gswv
            ! calculate evap'n using combination equation with current estimate of gsw
            Eleaf(ileaf)=1.0*(slope*Y * Rnstar(ileaf)+rhocp*st%Dair/(rbH_L+raero))/    &   !2* Weng 0215
               &             (slope*Y + psyc*(rswv+rbw+raero)/(rbH_L+raero))

            ! calculate sensible heat flux
            Hleaf(ileaf) = Y*(Rnstar(ileaf)-Eleaf(ileaf))
            ! calculate new leaf temperature (K)
            Tlk1 = 273.2 + iforcing%Tair + Hleaf(ileaf)*(rbH/2.+raero)/rhocp
            ! calculate Dleaf use LE=(rhocp/psyc)*gsw*Ds
            Dleaf = psyc*Eleaf(ileaf)/(rhocp*gswv)
            gbleaf(ileaf) = gbc*1.32*1.075
            gsleaf(ileaf) = gsw
            ! compare current and previous leaf temperatures
            if(abs(Tlk1 - Tlk).le.0.1) exit ! original is 0.05 C Weng 10/31/2008
            ! update leaf temperature  ! leaf temperature calculation has many problems! Weng 10/31/2008
            Tlk = Tlk1
            Tleaf(ileaf) = Tlk1-273.2
            if (isnan(Tleaf(ileaf))) print*, "test: ", Tlk1, iforcing%Tair, Hleaf(ileaf), rbH, raero, rhocp
            ! print*, "test: ", Tlk1, iforcing%Tair, Hleaf(ileaf), rbH, raero, rhocp
            if(isnan(Hleaf(ileaf))) then
               print*, "Hleaf: ", Y, Rnstar(ileaf), Eleaf(ileaf)
               print*, "Eleaf: ", slope, Y, Rnstar(ileaf), rhocp, st%Dair, rbH_L, raero, psyc, rswv, rbw
               print*, "rswv: ", gswv, gsw, Cmolar, gscx
               ! stop
               return
            endif
            if(isnan(Eleaf(ileaf))) then
               print*, "Eleaf: ", slope, Y, Rnstar(ileaf), rhocp, st%Dair, rbH_L, raero, psyc, rswv, rbw
               ! stop
               return
            endif

            if(isnan(rbH)) then
               print*, "rbH: ", rbH, gbH, gbHu, gbHf, Dheat,Gras,wleaf, windUx,wleaf
               ! stop
               return
            endif
            ! if(Dleaf < -1E-60) then
            !    print*, "Dleaf: ",Dleaf, psyc, Eleaf(ileaf), rhocp, gswv
            !    stop
            ! endif

            kr1 = kr1+1
            if(kr1 > 500)then
               Tlk=TairK
               exit
            endif
            if(Tlk < 200.)then
               Tlk=TairK
               exit
            endif                     ! Weng 10/31/2008
         enddo
      enddo
      return
   end subroutine agsean_day

   subroutine agsean_ngt(st, sp, iforcing, windUx, Qabs, grdn, Vcmxx, Rnstar, &
      & Tleaf, Aleaf, Eleaf, Hleaf, gbleaf, gsleaf)
      implicit none
      type(site_data_type), intent(inout) :: st
      type(spec_data_type), intent(inout) :: sp
      type(forcing_data_type), intent(in) :: iforcing
      real(8), intent(in) :: windUx, Qabs(3,2), grdn, Vcmxx, Rnstar(2)
      real(8), intent(inout) :: Tleaf(2), Aleaf(2), Eleaf(2),Hleaf(2), gbleaf(2), gsleaf(2)
      ! local variables
      real(8) :: TairK, rhocp, H2OLv, slope, psyc, Cmolar, weighJ
      real(8) :: gbHu, Tlk, Dleaf, co2cs, Qapar
      real(8) :: Gras, gbHf, gbH, rbH, rbw, rbH_L, rrdn, Y
      real(8) :: gbc, gsc0, gsw, gswv, rswv, Tlk1
      real(8) :: Aleafx, gsc
      integer :: ileaf, kr1
      ! thermodynamic parameters for air
      TairK  = iforcing%Tair+273.2
      rhocp  = cpair*Patm*AirMa/(Rconst*TairK)
      H2OLv  = H2oLv0-2.365e3*iforcing%Tair
      slope  = (esat(iforcing%Tair+0.1)-esat(iforcing%Tair))/0.1
      psyc   = Patm*cpair*AirMa/(H2OLv*H2OMw)
      Cmolar = Patm/(Rconst*TairK)
      weighJ = 1.0
 
      ! boundary layer conductance for heat - single sided, forced convection
      ! (Monteith 1973, P106 & notes dated 23/12/94)
      gbHu = 0.003*sqrt(windUx/wleaf)    !m/s
      do ileaf = 1,2                  ! loop over sunlit and shaded leaves
         ! first estimate of leaf temperature - assume air temp
         Tleaf(ileaf) = iforcing%Tair
         Tlk   = Tleaf(ileaf)+273.2    !Tleaf to deg K
         ! first estimate of deficit at leaf surface - assume Da
         Dleaf = st%Dair                !Pa
         ! first estimate for co2cs
         co2cs = co2ca               !mol/mol
         Qapar = (4.6e-6)*Qabs(1,ileaf)
         kr1   = 0                     !iteration counter for LE
         do
 !100        continue !    return point for evaporation iteration
            ! single-sided boundary layer conductance - free convection (see notes 23/12/94)
            Gras = 1.595e8*abs(Tleaf(ileaf)-iforcing%Tair)*(wleaf**3)     !Grashof
            gbHf = 0.5*Dheat*(Gras**0.25)/wleaf
            gbH  = gbHu+gbHf                         !m/s
            rbH  = 1./gbH                            !b/l resistance to heat transfer
            rbw  = 0.93*rbH                          !b/l resistance to water vapour
            ! Y factor for leaf: stom_n = 1.0 for hypostomatous leaf;  stom_n = 2.0 for amphistomatous leaf
            rbH_L = rbH*sp%stom_n/2.                   !final b/l resistance for heat
            rrdn  = 1./grdn
            Y     = 1./(1.+ (rbH_L+raero)/rrdn)
            ! boundary layer conductance for CO2 - single side only (mol/m2/s)
            gbc    = Cmolar*gbH/1.32            !mol/m2/s
            gsc0   = gsw0/1.57                        !convert conductance for H2O to that for CO2
            ! varQc  = 0.0
            ! weighR = 1.0
            ! respiration
            Aleafx = -0.0089*Vcmxx*exp(0.069*(Tlk-293.2))
            gsc    = gsc0
            ! choose smaller of Ac, Aq
            Aleaf(ileaf) = Aleafx                     !mol CO2/m2/s
            ! calculate new values for gsc, cs (Lohammer model)
            co2cs = co2ca-Aleaf(ileaf)/gbc
            !  co2Ci(ileaf) = co2cs-Aleaf(ileaf)/gsc
            ! scale variables
            gsw  = gsc*1.56                              !gsw in mol/m2/s
            gswv = gsw/Cmolar                            !gsw in m/s
            rswv = 1./gswv
            ! calculate evap'n using combination equation with current estimate of gsw
            Eleaf(ileaf) = (slope*Y*Rnstar(ileaf)+rhocp*st%Dair/(rbH_L+raero))/   &
                    &      (slope*Y+psyc*(rswv+rbw+raero)/(rbH_L+raero))
            ! calculate sensible heat flux
            Hleaf(ileaf)=Y*(Rnstar(ileaf)-Eleaf(ileaf))
            ! calculate new leaf temperature (K)
            Tlk1=273.2+iforcing%Tair + Hleaf(ileaf)*(rbH/2.+raero)/rhocp
            ! if(ISNAN(Tlk1)) print*, "Tlk1: ",  Hleaf(ileaf), rbH, raero, rhocp
            ! calculate Dleaf use LE=(rhocp/psyc)*gsw*Ds
            Dleaf = psyc*Eleaf(ileaf)/(rhocp*gswv)
            gbleaf(ileaf) = gbc*1.32*1.075
            gsleaf(ileaf) = gsw
 
            ! compare current and previous leaf temperatures
            if(abs(Tlk1-Tlk).le.0.1)exit
            if(kr1.gt.500)exit
            ! update leaf temperature
            Tlk=Tlk1
            Tleaf(ileaf)=Tlk1-273.2
            if (isnan(Tleaf(ileaf))) then
                print*, "test: ", Tlk1, iforcing%Tair, Hleaf(ileaf), rbH, raero, rhocp
                print*, "Hleaf: ", Y, Rnstar(ileaf), Eleaf(ileaf), rbH_L,raero,rrdn
               print*, "Eleaf: ", slope, Y, Rnstar(ileaf), rhocp, st%Dair, rbH_L, raero, psyc, rswv, rbw
               print*, "rswv: ", gswv, gsw, Cmolar, gsw0

            endif
            kr1=kr1+1
          enddo                          !solution not found yet
!  10    continue
      enddo
      return
   end subroutine agsean_ngt

   subroutine photosyn(st, sp, Qapar, Vcmxx, eJmxx, Tlk, gsc0, Dleaf, Gbc, &
      Aleafx, Gscx)  ! outputs
      implicit none
      
      type(site_data_type), intent(inout) :: st
      type(spec_data_type), intent(inout) :: sp
      real(8), intent(in) :: Qapar, Vcmxx, eJmxx, Tlk, gsc0, Dleaf, Gbc
      real(8), intent(inout) :: Aleafx, Gscx
      ! local variables
      real(8) :: CO2Csx, Tlf
      ! real(8) :: TminV, TmaxV, ToptV, TminJ, TmaxJ, ToptJ
      real(8) :: VcmxT, eJmxT, eJ, weighJ, weighR
      real(8) :: conKcT, conKoT
      real(8) :: Rd, Tdiff, gammas, gamma, a1, X, Gma, Bta
      real(8) :: Acx, Aqx ! from ciandA

      ! CO2Csx = AMAX1(CO2Csx,0.6*CO2Ca)
      CO2Csx = 0.6*CO2Ca
      ! check if it is dark - if so calculate respiration and gsc0 to assign conductance
      if(Qapar.le.0.) then                            !night, umol quanta/m2/s
         Aleafx = -0.0089*Vcmxx*exp(0.069*(Tlk-293.2))   ! original: 0.0089 Weng 3/22/2006
         Gscx   = gsc0
      endif
      ! calculate  Vcmax, Jmax at leaf temp using Reed et al (1976) function J appl Ecol 13:925
      ! TminV = sp%gddonset/10.  ! original -5.        !-Jiang Jiang 2015/10/13
      ! TmaxV = 50.
      ! ToptV = 35.

      ! TminJ = TminV
      ! TmaxJ = TmaxV
      ! ToptJ = ToptV

      ! VcmxT = VJtemp(Tlf,TminV,TmaxV,ToptV,Vcmxx)
      ! eJmxT = VJtemp(Tlf,TminJ,TmaxJ,ToptJ,eJmx1)

      ! calculate Vcmax, Jmax at leaf temp (Eq 9, Harley et al 1992) turned on by Weng, 2012-03-13
      ! VcmxT = Vjmax(Tlkx,Trefk,Vcmxx,Eavm,Edvm,Rconst,Entrpy)
      ! eJmxT = Vjmax(Tlkx,Trefk,eJmx1,Eajm,Edjm,Rconst,Entrpy)

      Tlf   = Tlk-273.2
      VcmxT = Vjmax(Tlk,Vcmxx,Eavm*sp%s_vea,Edvm*sp%s_ved,sp%Entrpy)
      eJmxT = Vjmax(Tlk,eJmxx,Eajm*sp%s_jea,Edjm*sp%s_jed,sp%Entrpy)

      weighJ = 1.0
      weighR = 1.0
      
      ! calculate J, the asymptote for RuBP regeneration rate at given Q
      eJ = weighJ*fJQres(eJmxT,sp%alpha,Qapar)
      ! calculate Kc, Ko, Rd gamma*  & gamma at leaf temp
      conKcT = EnzK(Tlk,conKc0,Ekc)
      conKoT = EnzK(Tlk,conKo0,Eko)
      ! following de Pury 1994, eq 7, make light respiration a fixed proportion of
      ! Vcmax
      Rd     = 0.0089*VcmxT*weighR                              !de Pury 1994, Eq7
      Tdiff  = Tlk-Trefk
      gammas = gam0*(1.+gam1*Tdiff+gam2*Tdiff*Tdiff)       !gamma*
      gamma = (gammas+conKcT*(1.+O2ci/conKoT)*Rd/VcmxT)/(1.-Rd/VcmxT)
      ! gamma = 0.0
      ! ***********************************************************************
      ! Analytical solution for ci. This is the ci which satisfies supply and demand
      ! functions simultaneously
      ! calculate X using Lohammer model, and scale for soil moisture
      a1 = 1./(1.-0.7)
      X  = a1*st%fwsoil/((co2csx - gamma)*(1.0 + Dleaf/sp%Ds0))
      ! calculate solution for ci when Rubisco activity limits A
      Gma = VcmxT
      Bta = conKcT*(1.0+ o2ci/conKoT)
      call ciandA(Gma, Bta, gsc0, X, Rd, co2Csx, gammas, Acx)
      ! calculate +ve root for ci when RuBP regeneration limits A
      Gma = eJ/4.
      Bta = 2.*gammas
      ! calculate coefficients for quadratic equation for ci
      call ciandA(Gma, Bta, gsc0, X, Rd, co2Csx, gammas, Aqx)
      ! choose smaller of Ac, Aq
      ! sps=AMAX1(0.001,sps)                  !Weng, 3/30/2006
      Aleafx = (amin1(Acx,Aqx) - Rd) !*sps     ! Weng 4/4/2006
      ! if(Aleafx.lt.0.0) Aleafx=0.0    ! by Weng 3/21/2006
      ! calculate new values for gsc, cs (Lohammer model)
      CO2csx = co2ca-Aleafx/Gbc
      Gscx=gsc0 + X*Aleafx  ! revised by Weng
      if(isnan(gscx))then
         print*, "gscx: ", gsc0, X, Aleafx
         print*, "X: ", a1, st%fwsoil, co2csx, gamma, Dleaf, sp%Ds0, Acx,Aqx, Rd,VcmxT 
         print*, "vcmxT", Tlk,Vcmxx,Eavm,Edvm,sp%Entrpy
      endif

      return
   end subroutine photosyn

   real(8) function sinbet(doy, hour, lat)
      integer, intent(in) :: doy, hour
      real(8), intent(in) :: lat
      real(8) rad, sinlat, coslat, sindec, cosdec, A, B
      ! sin(bet), bet = elevation angle of sun
      ! calculations according to Goudriaan & van Laar 1994 P30
      rad = pi/180.
      ! sine and cosine of latitude
      sinlat = sin(rad*lat)
      coslat = cos(rad*lat)
      ! sine of maximum declination
      sindec = -sin(23.45*rad)*cos(2.0*pi*(doy + 10.0)/365.0)
      cosdec = sqrt(1.-sindec*sindec)
      ! terms A & B in Eq 3.3
      A = sinlat*sindec
      B = coslat*cosdec
      sinbet = A + B*cos(pi*(real(hour) - 12.)/12.)
      return
   end function sinbet

   subroutine ciandA(Gma,Bta,g0,X,Rd,co2Csx,gammas,Aquad)      ! Gma,Bta,g0,X,Rd,co2Cs,gammas,ciquad,Aquad
      real(8) Gma,Bta,g0,X,Rd,co2Csx,gammas,ciquad,Aquad
      real(8) b2, b1, b0, bx
      ! calculate coefficients for quadratic equation for ci
      b2 =  g0 + X*(Gma - Rd)
      b1 =  (1.-co2Csx*X)*(Gma - Rd) + g0*(Bta - co2Csx) - X*(Gma*gammas + Bta*Rd)
      b0 = -(1.-co2Csx*X)*(Gma*gammas + Bta*Rd) - g0*Bta*co2Csx

      bx = b1*b1 - 4.*b2*b0
      if (bx .gt. 0.0) then
         ! calculate larger root of quadratic
         ciquad = (-b1 + sqrt(bx))/(2.*b2)
      end if

      IF (ciquad .lt. 0 .or. bx .lt. 0.) THEN
         Aquad = 0.0
         ciquad = 0.7*co2Csx
      ELSE
         Aquad = Gma*(ciquad - gammas)/(ciquad + Bta)
      END IF
      return
   end

   ! ****************************************************************************
   ! Reed et al (1976, J appl Ecol 13:925) equation for temperature response
   ! used for Vcmax and Jmax
   real(8) function VJtemp(Tlf,TminVJ,TmaxVJ,ToptVJ,VJmax0)
      real(8) Tlf,TminVJ,TmaxVJ,ToptVJ,VJmax0
      real(8) pwr
      if (Tlf .lt. TminVJ) Tlf = TminVJ   !constrain leaf temperatures between min and max
      if (Tlf .gt. TmaxVJ) Tlf = TmaxVJ
      pwr    = (TmaxVJ - ToptVJ)/(ToptVj - TminVj)
      VJtemp = VJmax0*((Tlf - TminVJ)/(ToptVJ - TminVJ))*     &
               &       ((TmaxVJ - Tlf)/(TmaxVJ - ToptVJ))**pwr
      return
   end

   real(8) function Vjmax(Tk,Vjmax0,Eactiv,Edeact,Entrop)
      real(8) :: Tk,Vjmax0,Eactiv,Edeact,Entrop, aden, anum
      anum = Vjmax0*EXP((Eactiv/(Rconst*Trefk))*(1.-Trefk/Tk))
      aden = 1. + EXP((Entrop*Tk-Edeact)/(Rconst*Tk))
      Vjmax = anum/aden
      return
   end

   real(8) function fJQres(eJmx,alpha,Q)
      real(8) eJmx,alpha,Q
      real(8) AX, BX, CX
      AX = theta                                 !a term in J fn
      BX = alpha*Q + eJmx                          !b term in J fn
      CX = alpha*Q*eJmx                          !c term in J fn
      if ((BX*BX - 4.*AX*CX) >= 0.0) then
         fJQres = (BX - SQRT(BX*BX - 4.*AX*CX))/(2*AX)
      else
         fJQres = (BX)/(2*AX)                   !Weng 10/31/2008
      end if
      return
   end

   real(8) function EnzK(Tk,EnzK0,Eactiv)
      real(8) Tk,EnzK0,Eactiv
      real(8) temp1 
      temp1 = (Eactiv/(Rconst*Trefk))*(1.-Trefk/Tk)
      ! if (temp1<50.)then
      EnzK = EnzK0*EXP((Eactiv/(Rconst*Trefk))*(1.-Trefk/Tk))
      ! else
      ! EnzK = EnzK0*EXP(50.)                                          ! Weng 10/31/2008
      ! endif
      return
   end
end module vegetation