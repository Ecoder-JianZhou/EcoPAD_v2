program TECO
    use datatypes
    use driver
    use io_mod
#ifdef USE_MCMC
    use mcmc
    use mcmc_mod
#endif
#ifdef USE_SPINUP
    use mod_spinup
#endif

    implicit none
    integer :: count_mode, num_args, ierr
    type(site_data_type) :: st
    character(500) :: conf_nml_file

    ! get the count of command
    num_args = COMMAND_ARGUMENT_COUNT()
    if (num_args /= 1) then
        write(*,*) "Usage: ./my_program <conf_nml_file>"
        stop
    end if

    ! read the command
    call GET_COMMAND_ARGUMENT(1, conf_nml_file, ierr)
    conf_nml_file = adjustl(trim("configs/"))//adjustl(trim(conf_nml_file))

    write(*,*) "# -----------------------------------------"
    call read_configs_nml(conf_nml_file)

    ! check the three mode: do_simu; do_mcmc; do_spinup
    count_mode = 0
    if (do_simu)   count_mode = count_mode + 1
    if (do_mcmc)   count_mode = count_mode + 1
    if (do_spinup) count_mode = count_mode + 1

    if (count_mode == 0) then
        print *, "# Error: You must select one run mode!"
        stop
    elseif (count_mode > 1) then
        print *, "# Error: Only one mode can be selected!"
        stop
    endif

    write(*,*) "# Running case:", adjustl(trim(case_name))
    call createNewCase()
    call read_nml_params_initValues(defParamsFile)
    call get_forcingdata()                      ! read forcing data

    if (.not. do_snow) call get_snowdepth()
    if (do_restart)then
        ! call read_restart(restartfile)     ! this module in "writeOutput2nc.f90"
        ! call initialize_with_restart()
    endif

    

    allocate(st%sp(npft))
    ! initialize the output
    allocate(outVars_h%sp(npft))
    allocate(outVars_d%sp(npft))
    allocate(outVars_m%sp(npft))
    allocate(outVars_y%sp(npft))

    if(do_simu)then
        print *, "# Running simulation mode."
        call initialize_teco(st)!, .False.)
        call teco_simu(st, .True.)            ! run simulation

        if(do_out_hr)  call deallocate_results(outVars_h, npft)
        if(do_out_day) call deallocate_results(outVars_d, npft)
        if(do_out_mon) call deallocate_results(outVars_m, npft)
        if(do_out_yr)  call deallocate_results(outVars_y, npft)
    elseif(do_mcmc) then
        print*, "# Start to run MCMC mode."
        call initialize_teco(st)!, .False.)
#ifdef USE_MCMC
        call init_mcmc(mcmc_configfile)  ! initilize the MCMC 
        call run_mcmc(st)                    ! run MCMC
        call deallocate_mcmc()               ! deallocate the MCMC variables 
        call mcmc_vars_deallocate()
#endif
    
    elseif(do_spinup)then
        print*, "# Start to run MCMC mode."
        call initialize_teco(st)!, .False.)
#ifdef USE_SPINUP
        call init_spinup_variables()    ! initilize the spin-up variables
        call run_spinup(st)               ! run spin-up loops
        call write_spinup_res()         ! write the results of SPIN-UP
        call write_restart()            ! write the result file
        call deallo_spinup_variables()  ! deallocate the variables of SPIN-UP
#endif
    endif
    
    if(allocated(outVars_h%sp)) deallocate(outVars_h%sp)
    if(allocated(outVars_d%sp)) deallocate(outVars_d%sp)
    if(allocated(outVars_m%sp)) deallocate(outVars_m%sp)
    if(allocated(outVars_y%sp)) deallocate(outVars_y%sp)
    if(allocated(sp_names))     deallocate(sp_names)
    if(allocated(st%sp))        deallocate(st%sp)

    call deallocate_date_type()
end program TECO

! ------------------------------------------------------------------------
! ** create case **
! ------------------------------------------------------------------------
subroutine createNewCase()
    use datatypes
    implicit none

    print *, "# Update and create the output dirs"

    ! update the full path of input file
    defParamsFile   = adjustl(trim(inDir))//"/"//adjustl(trim(defParamsFile))
    climfile        = adjustl(trim(inDir))//"/"//adjustl(trim(climfile))       ! climate file name
    snowdepthfile   = adjustl(trim(inDir))//"/"//adjustl(trim(snowdepthfile))  ! snow depthfile
    restartfile     = adjustl(trim(inDir))//"/"//adjustl(trim(restartfile))    ! restartfile
    watertablefile  = adjustl(trim(inDir))//"/"//adjustl(trim(watertablefile)) ! Jian: maybe used when not run soil_physical
    ! check the inputfile
    call check_inputfile(defParamsFile, "Parameter file")
    call check_inputfile(climfile, "Climate file")
    if (.not. do_snow)    call check_inputfile(snowdepthfile,  "Snow depth file")
    if (do_restart)       call check_inputfile(restartfile,    "Restart file")
    if (.not. do_soilphy) call check_inputfile(watertablefile, "Water table file")
    ! create the outdir
    call CreateFolder(adjustl(trim(outdir)))

    ! update and create the output dir of case
    outdir_case = adjustl(trim(outdir))//"/"//adjustl(trim(case_name))
#if defined(WIN32) || defined(WIN64) || defined(_WIN32) || defined(_WIN64)
    outdir_case = adjustl(trim(outdir))//"\"//adjustl(trim(case_name))
#endif
    call CreateFolder(adjustl(trim(outdir_case)))

    ! update and create the output dir of each format outputs
#ifdef USE_NETCDF
    outDir_nc  = adjustl(trim(outdir_case))//"/"//adjustl(trim(outDir_nc))
#if defined(WIN32) || defined(WIN64) || defined(_WIN32) || defined(_WIN64)
    outDir_nc  = adjustl(trim(outdir_case))//"\"//adjustl(trim(outDir_nc))
#endif
    call CreateFolder(adjustl(trim(outDir_nc)))
#endif

    outDir_csv = adjustl(trim(outdir_case))//"/"//adjustl(trim(outDir_csv))
#if defined(WIN32) || defined(WIN64) || defined(_WIN32) || defined(_WIN64)
    outDir_csv = adjustl(trim(outdir_case))//"\"//adjustl(trim(outDir_csv))
#endif
    call CreateFolder(adjustl(trim(outDir_csv)))

    ! update and create the output for each time frequency of nc-format outputs
#ifdef USE_NETCDF
    if (do_out_hr) then
        outDir_h = adjustl(trim(outdir_nc))//"/Hourly"
        call CreateFolder(adjustl(trim(outDir_h)))
    endif

    if (do_out_day) then
        outDir_d = adjustl(trim(outdir_nc))//"/Daily"
        call CreateFolder(adjustl(trim(outDir_d)))
    endif
    if (do_out_mon) then
        outDir_m = adjustl(trim(outdir_nc))//"/Monthly"
        call CreateFolder(adjustl(trim(outDir_m)))
    endif
    if (do_out_yr) then
        outDir_y = adjustl(trim(outdir_nc))//"/Yearly"
        call CreateFolder(adjustl(trim(outDir_y)))
    endif
#endif

    if (do_spinup)then
        outDir_spinup = adjustl(trim(outdir_case))//"/"//adjustl(trim(outDir_spinup))
        call CreateFolder(adjustl(trim(outDir_spinup)))
        outfile_restart = adjustl(trim(outDir_spinup))//"/restart.nc"
    endif

    if (do_mcmc)then
        outDir_mcmc = adjustl(trim(outdir_case))//"/"//adjustl(trim(outDir_mcmc))
        call CreateFolder(adjustl(trim(outDir_mcmc)))
        ! ! if (do_mc_out_hr)then
        !     outDir_mcmc_h = adjustl(trim(outDir_mcmc))//"/results_mcmc_Hourly"
        !     call CreateFolder(adjustl(trim(outDir_mcmc_h)))
        ! ! endif
        ! ! if (do_mc_out_day) then
        !     outDir_mcmc_d = adjustl(trim(outDir_mcmc))//"/results_mcmc_Daily"
        !     call CreateFolder(adjustl(trim(outDir_mcmc_d)))
        ! ! endif
        ! ! if (do_mc_out_mon) then
        !     outDir_mcmc_m = adjustl(trim(outDir_mcmc))//"/results_mcmc_Monthly"
        !     call CreateFolder(adjustl(trim(outDir_mcmc_m)))
        ! ! endif
    endif

    if(do_restart)then
        restartfile = adjustl(trim(inDir))//adjustl(trim(restartfile))
    endif
end subroutine createNewCase

! ------------------------------------------------------------------------
! ** create folder **
! ------------------------------------------------------------------------
subroutine CreateFolder(path_new)
    implicit none
    character(len=*), INTENT(in) :: path_new
    logical :: dirExists

    inquire(file=trim(path_new), exist=dirExists )
    if (.not. dirExists) then
        call system("mkdir -p "//trim(path_new))
        print *, "# Created directory:", trim(path_new)
    endif
end subroutine CreateFolder

! ------------------------------------------------------------------------
! ** check if file exist or not **
! ------------------------------------------------------------------------
subroutine check_inputfile(filepath, whatfile)
    implicit none
    character(*), intent(in) :: filepath, whatfile
    logical :: file_exists

    inquire(file=filepath, exist=file_exists)
    if (.not. file_exists) then
        print *, "# check file: ", adjustl(trim(filepath))
        print *, "# Error: ", adjustl(trim(whatfile)), " does not exist!"
        stop
    else
        print *, "# Found: ", adjustl(trim(filepath))
    endif
end subroutine check_inputfile
