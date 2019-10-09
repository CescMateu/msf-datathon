rm(list = ls())
gc()

library(data.table)
library(lubridate)
source('tools.R')

setwd('~/Desktop/Datathon/')


# Define the range of IDVERSIONS
versions <- as.character(seq(ymd('1987-01-01'), ymd('2018-08-01'), by = 'months', format = "%Y-%m"))
versions <- substr(gsub("-","",versions),1,6)


# Read the target table
dt <- data.table()
for (t in versions) {
  print(paste0('Reading IDVERSION: ', t))
  dt <- rbind(dt, fread(paste0('output/target/bajas_', t, '.csv')))
}

dt[, IDVERSION := as.factor(IDVERSION)]
setkey(dt, IDVERSION)
setkey(dt, IDMIEMBRO)



# Process the non-target data
source('1_individuos.R')
source('4_interacciones.R')

data_path = 'data/'

indi_table <- processIndividuosTable(data_path)
setkey(indi_table, IDMIEMBRO)

interac_table <- processInteractionsTable(data_path)
interac_table[, IDVERSION := as.factor(IDVERSION)]
setkey(interac_table, IDMIEMBRO)
setkey(interac_table, IDVERSION)

# Join the data into a common dataset to feed the model

dt <- merge(dt, indi_table, by = c('IDMIEMBRO'), all.x = TRUE)
dt <- merge(dt, interac_table, by = c('IDMIEMBRO', 'IDVERSION'), all.x = TRUE)

# Transform NAS to 0 to the 'NUM_Interacciones' columns
nas_to_0 <- names(dt)[grep(pattern = 'NUM_INTERACCIONES_', names(dt))]
allNAsToZero(dt, cols = nas_to_0)

# Save data into a file
fwrite(x = dt, file = 'output/dataset.csv', sep = ';')



