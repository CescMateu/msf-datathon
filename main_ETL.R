rm(list = ls())
gc()

library(data.table)
library(lubridate)

source('scripts/tools.R')
source('scripts/ETL_functions.R')

input_path <- 'MSF_data/'
output_path <- 'processed_data/'

# 0. Generar target (2. Altas y bajas) i clients 'base'
createTargetVar(input_path, output_path)
createPrimaryMembers(input_path, output_path)

# 1. Información cliente
processIndividuosTable(input_path, output_path)

# 2. Temps desde alta / Canalitat d'entrada
# TODO: Codi Joan (scripts/ETL_tiempo_Desde_alta.R)

# 4. Interacciones
processInteractionsTable(input_path, output_path)

# 6. Axesor
processAxesorTable(input_path, output_path)

# 9. Aportaciones
processAportacionesTable(input_path, output_path)

# Join all the data together
readAndWriteFinalDataset(output_path)

