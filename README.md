# EcoPAD
**EcoPAD (Ecological Platform for Assimilating Data)**  
is a modular platform designed for carbon cycle model, data assimilation for parameter optimization, and automated carbon cycle forecasting.  

EcoPAD integrates carbon cycle models, observational data, and data assimilation workflows into a unified system that supports:  
* model simulation  
* parameter optimization  
* automated forecasting  
* visualization of model outputs and forecasting  
  
The platform is designed to support multiple ecological sites and models through a modular site service architecture.  
***  
## System Structure  
EcoPAD is composed of three core service:    
```
EcoPAD  
│  
├── Portal  
│   User interface layer  
│  
├── Runner  
│   Task orchestration and system backend  
│  
└── Site Services  
    Model execution environments  
```
***

### Portal  
Portal is the **user-facing web interface**.  
Responsibilities:  
* user authentication  
* workflow submission 
* forecast visualization
* parameter exploration
* job monitoring
* administrative operations
    
Portal does not execute models.   
It communicates with Runner via REST API.
***  
### Runner  
Runner is the **central backend service** responsible for:  
* task management
* job scheduling
* run registry
* forecast registry
* communication with site services
* data aggregation for visualization

Runner receives requests from Portal and dispatches tasks to site containers.  
Main responsibilities include:  
* managing run records
* dispatching model tasks
* managing scheduled tasks (auto forecast)
* collecting run artifacts
* providing forecast data APIs  
***  
### Site Services  
Each ecological site runs a dedicated container service.  
Responsibilities:
* executing ecological models
* generating outputs
* exposing standardized APIs
* providing metadata about models, parameters, and outputs  

A site service **contains the model code and site-specific logic**.  
Runner communicates with site services via REST API.  
***  
## System Workflow
The execution pipeline is:  
```
User
   ↓
Portal
   ↓
Runner
   ↓
Site Service
   ↓
Model Execution
   ↓
Run Outputs
   ↓
Runner Registry
   ↓
Portal Visualization
```  
Typical workflow:  
1. User submits a workflow from Portal.  
2. Portal sends the request to Runner.
3. Runner creates a new run record.
4. Runner dispatches the job to the appropriate site service.
5. Site service executes the model.
6. Outputs are written to the run directory.
7. Runner records artifacts and updates run status.
8. Portal displays the results.  
***  
## Directory Structure
Typical project layout:  
```
EcoPAD
│
├── portal
│   ├── app
│   └── static
│
├── runner
│   ├── app
│   ├── services
│   └── config
│
├── sites
│   ├── site-template
│   └── site-SPRUCE
│
└── docker-compose.yml
```
## Quick Start
### 1 Clone the repository
```
git clone <repo_url>
cd EcoPAD
```
### 2 Start services
```
docker compose up --build
```
This will start:  
```
portal
runner
site service(s)
```
### 3 Open the portal  
``` 
http://localhost:8000  
```
### 4 Initial setup
On first launch, the system requires initialization.  
The setup process will:  
* create the database
* create a superuser account
* initialize system configuration  
***
## Key System Concepts
### Run
A run represents one execution of a model task.
Example:
```
simulate
forecast
data assimilation
MCMC optimization
```
Each run has:  
* run_id
* site
* model
* task_type
* status
* timestamps
* output artifacts  
***  
### Scheduled Tasks
EcoPAD supports **automated forecasting** through scheduled tasks.
A scheduled task defines:
* site
* model
* task type
* execution schedule
Example:
```
daily auto forecast
weekly reanalysis
```
Each scheduled task produces **multiple runs over time**.  
***  
### Forecast Registry
Runner maintains a forecast registry that tracks:  
* latest published forecast
* source run
* model
* treatment
* variable  

This registry provides the data used by the Forecast page.  
***  
## Developing a New Site Service
EcoPAD is designed to support multiple ecological sites.  
Each site is implemented as an independent **site service container**.  
### Step 1 Create a new site from the template
Copy the template:
```
sites/site-template
```
Example:
```
sites/site-MySite
```  
### Step 2 Configure site metadata
Edit the site configuration file:  
```
config/site.json
```
Example:
```
{
  "site_id": "MySite",
  "site_name": "Example Ecological Site",
  "models": ["MODEL-template_1"],
  "treatments": ["control"]
}
```  
### Step 3 Define models
Each model must provide a detail model metadata file.
Example:  
```
config/models/MODEL-template_1/model.json

{
  "id": "MODEL-template_1",
  "name": "MODEL-template_1",
  "description": "Example model No.1 for template",

  "parameter_outputs": {
    "summary": "summary.json",
    "accepted": "parameters_accepted.csv",
    "best": "parameters_best.json"
  },

  "tasks": {
    "simulate": {
      "enabled": true,
      "desc": "Run one deterministic simulation.",
      "command": ["python", "/app/executors/MODEL-template_1_simulate.py", "{run_dir}"]
    },
    "forecast": {
      "enabled": true,
      "desc": "Generate forecast outputs.",
      "command": ["python", "/app/executors/MODEL-template_1_forecast.py", "{run_dir}"]
    },
    "auto_forecast": {
      "enabled": true,
      "desc": "Run scheduled forecast generation.",
      "command": ["python", "/app/executors/MODEL-template_1_auto_forecast.py", "{run_dir}"]
    },
    "mcmc": {
      "enabled": true,
      "desc": "Run MCMC-based data assimilation.",
      "command": ["python", "/app/executors/MODEL-template_1_mcmc.py", "{run_dir}"]
    }
  },

  "default_publish_variables": ["GPP", "ER", "NEE"]
}
```
Including the parameters.csv and variables.json  
Parameter columns:  
```
id | name | unit | default | min | max | description  
```
and variables.json  
```
{
  "variables": [
    {
      "name": "GPP",
      "full_name": "Gross Primary Productivity",
      "unit": "g C m-2 d-1",
      "desc": "Gross primary productivity",
      "output_file": "GPP.json"
    }
  ]
}
```
This metadata is used by the Portal for visualization.  
### Step 4 Implement model execution (Just a explaination)
The site service must implement the /run endpoint.
Runner sends requests in the format:  
```
POST /run
```  
Example payload:
```
{
  "run_id": "abc123",
  "site": "MySite",
  "model": "MODEL-template_1",
  "task": "simulate",
  "treatments": ["control"],
  "parameters": {}
}
```
The site service should:
1. create a working directory
2. prepare model inputs
3. execute the model
4. write outputs
5. generate manifest.json
*** 
### Step 5 Generate run manifest (Just a explaination)
Each run must produce a manifest file describing outputs.
Example:
```
runs/<run_id>/manifest.json
```
Example structure: 
```
{
  "outputs": {
    "timeseries": {
      "GPP": "gpp_timeseries.csv"
    },
    "parameters": {
      "best": "best.json"
    }
  }
}
```  
Runner reads this manifest to register run artifacts.
***
### Step 6 Expose site APIs
Each site service must provide the following endpoints:
```
/meta
/run
/runs/{run_id}/manifest
/runs/{run_id}/timeseries
```
These APIs allow Runner to retrieve:  
* site metadata
* run outputs
* model time series
* parameter results
*** 
### Step 7 Register the site in Runner
Add the site to:
```
runner/config/sites.json
```
Example:
```
{
  "sites": [
    {
      "id": "MySite",
      "service_url": "http://site-mysite:8010",
      "enabled": true
    }
  ]
}
```
After restarting Runner, the new site will be available.  

## Site Development Responsibilities
Site developers are responsible for:
* implementing model execution
* defining parameter metadata
* generating model outputs
* providing the required API endpoints

The Runner platform handles:
* job orchestration
* scheduling
* data registry
* API aggregation
*** 
## Adding New Models
A site service may support multiple models.
Each model should define:
* supported tasks
* parameter metadata
* output variables
* execution command  

Model configuration can be defined in a model configuration file.
***
## Outputs and Data Structure
Recommended output structure:
```
runs/
   run_id/
      outputs/
         model/
            treatment/
               variable_timeseries.csv
      manifest.json
```
This structure allows Runner to locate outputs consistently.
***
## Development Mode
For development, Docker Compose can be used with live code changes.
Typical workflow:
```
docker compose up
```
Then modify source code and restart the relevant service.
***
## License
This project is intended for ecological research and model integration.