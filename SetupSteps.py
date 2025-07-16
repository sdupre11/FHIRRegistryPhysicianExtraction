#create the virtual environment in my working folder 
#in terminal, run... python -m venv venv 
#activate that environment using the Command Prompt inside VS Code
#click dropdown next to terminal and select "Select Default Profile" -> Command Prompt -> open a new terminal tab
#in terminal run... venv\Scripts\activate.bat 
#in terminal run... pip install pandas fhir.resources ijson tqdm orjson pyarrow dask python-dateutil
#test the installations

import pandas as pd
from fhir.resources.patient import Patient
print("Pandas version:", pd.__version__)
print("FHIR Patient resource imported successfully")

#setup github repo and push content
#test that (in terminal) git --version works
#in terminal run... git init
#create a file named .gitignore and add this without the "#":

#venv/
#__pycache__/
#*.pyc
#*.pyo
#*.pyd
#.env
#.DS_Store

#stage and commit my files
#in terminal run... git add .
#in terminal run... git commit -m "Initial project commit"
#Go to Github.com and create the new repo, do not click on "Add a README file"
#Follow the instructions to push an existing repository from the command line

#Save installed packages for environment recreation through a requirements.txt file
#in terminal run... pip freeze > requirements.txt
#in terminal run...
#git add requirements.txt
#git commit -m "Adding requirements file"
#git push