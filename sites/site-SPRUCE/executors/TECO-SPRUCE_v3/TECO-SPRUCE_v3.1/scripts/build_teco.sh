#!/bin/bash

# read config file
NML_FILE="configs/env.nml"

# **make sure clean .mod 和 .o 文件**
cleanup() {
    echo "🧹 Cleaning up .mod and .o files before exit..."
    find src -name "*.mod" -exec rm -f {} +
    find src -name "*.o" -exec rm -f {} +
    find . -maxdepth 1 -name "*.mod" -exec rm -f {} +
}
trap cleanup EXIT  # cleanup

if [[ ! -f "$NML_FILE" ]]; then
    echo "Error: Namelist file $NML_FILE not found!"
    exit 1
fi

eval $(awk -F'=' '
    /^\&build_config/ { inside=1; next }
    /^\// { inside=0; next }
    inside {
        gsub(/[",]/, "", $2);  # remove comma and quotes
        gsub(/^[ \t]+|[ \t]+$/, "", $1); # remove space
        gsub(/^[ \t]+|[ \t]+$/, "", $2);
        print "export " $1 "=" $2
    }
' "$NML_FILE")

echo $ENV_BASE_PATH
export INCLUDE_PATH="$ENV_BASE_PATH/include"
export LIB_PATH="$ENV_BASE_PATH/lib"
export LIBS="-I$INCLUDE_PATH -L$LIB_PATH"

# check gfortran
if ! command -v $FC &>/dev/null; then
    echo "Error: gfortran not found. Please install it first."
    exit 1
fi

TECO_SRC=(
    src/teco/datatypes.f90
    src/teco/io_mod.f90
    src/teco/soil.f90
    src/teco/vegetation.f90
    src/teco/transfer.f90
    src/teco/driver.f90
)


# if MCMC or not
MCMC_FLAG=""
if [[ "$USE_MCMC" == ".true." ]]; then
    MCMC_FLAG="-DUSE_MCMC"
    echo "MCMC is enabled (USE_MCMC = .true.)"
    # find driver.f90 index
    for i in "${!TECO_SRC[@]}"; do
        if [[ "${TECO_SRC[i]}" == "src/teco/driver.f90" ]]; then
            DRIVER_INDEX=$i
            break
        fi
    done
    TECO_SRC=("${TECO_SRC[@]:0:$DRIVER_INDEX}" "src/tools/mcmc_mod.f90" "${TECO_SRC[@]:$DRIVER_INDEX}")
    # TECO_SRC+=("src/tools/mcmc_mod.f90")
    # TECO_SRC+=("src/tools/mcmc_outputs.f90")
    TECO_SRC+=("src/tools/mcmc.f90")
else
    echo "MCMC is disabled (USE_MCMC = .false.)"
fi

# if SPINUP or not
SPINUP_FLAG=""
if [[ "$USE_SPINUP" == ".true." ]]; then
    SPINUP_FLAG="-DUSE_SPINUP"
    echo "SPINUP is enabled (USE_MCMC = .true.)"
    TECO_SRC+=("src/tools/spinup.f90")
else
    echo "SPINUP is disabled (USE_SPINUP = .false.)"
fi

TECO_SRC+=("src/main.f90")

FLAGS="-g -fbacktrace -Wall -fcheck=all -cpp"

# if USE_NETCDF or not
NETCDF_FLAG=""
NETCDF_LIB=""
if [[ "$USE_NETCDF" == ".true." ]]; then
    NETCDF_FLAG="-DUSE_NETCDF"
    NETCDF_LIB="-lnetcdff -lnetcdf"
    echo "NETCDF is enabled (USE_NETCDF = .true.)"
else
    echo "NETCDF is disabled (USE_NETCDF = .false.)"
fi

export FFLAGS="$FLAGS $MCMC_FLAG $SPINUP_FLAG $NETCDF_FLAG"
export LLIBS="$LIBS $NETCDF_LIB"

echo "Fortran Compiler: $FC"
echo "Base Path: $ENV_BASE_PATH"
echo "Include Path: $INCLUDE_PATH"
echo "Library Path: $LIB_PATH"
echo "Flags: $FLAGS"
echo "Libs: $LLIBS"

echo "Compiling TECO core modules..."
OBJ_FILES=() # save all ".o" files

for SRC in "${TECO_SRC[@]}"; do
    OBJ="${SRC%.f90}.o"
    echo "Compiling: $SRC -> $OBJ"
    $FC $FFLAGS -c $SRC -o "$OBJ" $LLIBS
    if [[ $? -ne 0 ]]; then
        echo "Error: Compilation failed for $SRC"
        exit 1
    fi
    OBJ_FILES+=("$OBJ")
done
echo "Linking executable teco-spruce.exe..."
$FC "${OBJ_FILES[@]}" -o teco-spruce.exe $LLIBS
if [[ $? -ne 0 ]]; then
    echo "Error: Linking all objects to executable teco-spruce.exe failed!"
    exit 1
fi

echo "Cleaning up .mod and .o files..."
find src -name "*.mod" -exec rm -f {} +
find src -name "*.o" -exec rm -f {} +
find . -maxdepth 1 -name "*.mod" -exec rm -f {} +

echo "Build completed successfully!"