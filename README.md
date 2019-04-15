# Kaptio API Tool
## Purpose
This series of code scripts were written to enable Rocky Mountaineer access to data retrieved from Kaptio / Salesforce REST API
The code was written to serve a particulat need:
- Get the metatdata
  - Service Levels
  - Channels
  - etc.
- Get Packages
- Augment with dates of travel
- Get pricing
- Get PME content
- Build out export documents
  - XML Loader
  - Bulk Loader
  - AllSell
  
 ## Setup
 The important files are:
 - ktapi.json
   - this file stores the configuration and setup code, on first run a skeleton file will be created
     - see 
     ```
     utils_config.load_config()
     ```
 
 ## Running the code
 to run the code run the following:
 ```
 python process_main.py
 ```
