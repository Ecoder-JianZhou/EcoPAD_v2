This is the 8-matrix-models:
    "TEM","DALEC","TECO","FBDC","CASA","CENTURY","CLM","ORCHIDEE"

requirements:
    input (gpp), Tair, Rain
    soil temperature, soil water content.

functions:
    prepare: gpp_d, gpp_m, soil temperature, soil content, climate(Tair, Rain).
    calScalar: (calculate the scalars - scalars and Bscalars)
    run: run spinup and simulation