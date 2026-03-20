import os

# Define file paths
parameters_nml_path = "inputs/in_treat/P06/parameters.nml"
mcmc_conf_nml_path = "configs/mcmc_conf.nml"
updated_mcmc_conf_path = "final_mcmc_conf_P06.nml"

# Read parameters.nml content
with open(parameters_nml_path, "r") as f:
    parameters_nml_content = f.readlines()

# Read mcmc_conf.nml content
with open(mcmc_conf_nml_path, "r") as f:
    mcmc_conf_nml_content = f.readlines()

print(parameters_nml_content)
print(mcmc_conf_nml_content)

# Extract site parameters and species parameters from parameters.nml
site_parameters_dict = {}
species_parameters_dict = {}

for line in parameters_nml_content:
    if "=" in line and "!" not in line:  # Ignore comments
        parts = line.split("=")
        key = parts[0].strip()
        values = parts[1].strip().split()

        try:
            if len(values) == 1:  # Single value (site parameter)
                site_parameters_dict[key] = float(values[0])
            elif len(values) == 3 and key.startswith("def_"):  # Species parameter
                species_parameters_dict[key] = [float(v) for v in values]
        except ValueError:
            continue  # Skip invalid values

# Modify mcmc_conf.nml content
updated_mcmc_conf_content = []
for line in mcmc_conf_nml_content:
    # Update general site parameters
    if "parName" in line and "parVal" in line:
        parts = line.split(",")
        par_name_part = parts[0].split("=")
        par_val_part = parts[1].split("=")

        par_name = par_name_part[1].strip().strip('"')
        if par_name in site_parameters_dict:
            new_value = site_parameters_dict[par_name]
            new_line = f'{par_name_part[0]}= "{par_name}", {par_val_part[0]}= {new_value:.5f},' + ",".join(parts[2:])
            updated_mcmc_conf_content.append(new_line)
        else:
            updated_mcmc_conf_content.append(line)

    # Update species parameters (sp(n)%var(m)%parVal)
    elif "sp(" in line and "var(" in line and "parVal" in line:
        parts = line.split("=")
        var_index = parts[0].split("var(")[-1].split(")")[0]
        sp_index = parts[0].split("sp(")[-1].split(")")[0]

        try:
            param_value = float(parts[1].strip())
            species_key = f"def_{var_index}"  # Assuming def_xxx maps to var(xxx)
            if species_key in species_parameters_dict:
                new_value = species_parameters_dict[species_key][int(sp_index) - 1]  # Adjust for 1-based index
                new_line = f'{parts[0]}= {new_value:.5f}\n'
                updated_mcmc_conf_content.append(new_line)
            else:
                updated_mcmc_conf_content.append(line)
        except ValueError:
            updated_mcmc_conf_content.append(line)

    else:
        updated_mcmc_conf_content.append(line)

# Function to align assignments in Fortran namelist format
def align_nml_assignments(lines):
    max_eq_index = max(line.find("=") for line in lines if "=" in line)
    aligned_lines = []
    
    for line in lines:
        if "=" in line:
            parts = line.split("=")
            left_part = parts[0].rstrip()
            right_part = "=" + parts[1].lstrip()
            aligned_line = f"{left_part.ljust(max_eq_index)} {right_part}"
            aligned_lines.append(aligned_line)
        else:
            aligned_lines.append(line)
    
    return aligned_lines

# Align the modified content
aligned_mcmc_conf_content = align_nml_assignments(updated_mcmc_conf_content)

# Write the aligned content back to the file
with open(updated_mcmc_conf_path, "w") as f:
    f.writelines(aligned_mcmc_conf_content)

print(f"Updated and aligned file saved at: {updated_mcmc_conf_path}")