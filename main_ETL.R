rm(list = ls())
gc()

library(data.table)
library(lubridate)
library(dplyr)
library(stringr)

source('scripts/tools.R')
source('scripts/ETL_functions.R')

input_path <- 'MSF_data/'
output_path <- 'processed_data/'

merge_aportaciones <- FALSE # Do you need to merge all the aportaciones or you have already done it?
only_final_join <- TRUE # Decide whether you want to perform the whole ETL


if (!only_final_join) {
  
  # 0. Generar target (2. Altas y bajas) i clients 'base'
  createTargetVar(input_path, output_path)
  createPrimaryMembers(input_path, output_path)
  
  # 1. Información cliente
  processIndividuosTable(input_path, output_path)
  
  # 2. Temps desde alta / Canalitat d'entrada
  createChannelsAndPermanenceTime(input_path, output_path)
  
  # 3. Aumentos + 7. Permisos Comunicacion + 8. Interacciones TLMK (Carles)
  
  # 4. Interacciones
  processInteractionsTable(input_path, output_path)
  
  # 6. Axesor
  processAxesorTable(input_path, output_path)
  
  # 9. Mailings Sergi (CSV ja escrit)
  
  # 10. Aportaciones
  processAportacionesTable(input_path, output_path)
  
  
  
  
}

# Join all the data together
readAndWriteFinalDataset(output_path, merge_aportaciones, 
                         fecha_inicial = '2012-01-01', fecha_final = '2018-08-01',
                         output_name = 'final_dataset_alt.csv', exploitation = FALSE)

readAndWriteFinalDataset(output_path, merge_aportaciones, 
                         output_name = 'exploitation_dataset_alt.csv', exploitation = TRUE)

