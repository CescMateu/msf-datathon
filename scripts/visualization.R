rm(list = ls())
gc()
library(tidyr)
library(ggplot2)

train <- fread('processed_data/final_dataset_alt_cescSE_noAport.csv')
expl <- fread('processed_data/exploitation_dataset_cescSE_noAport.csv')


for (c in colnames(train)) {
  
  if (c != 'IND_BAIXA'){
    na_prop <- round(train[, sum(is.na(.SD))/nrow(train), .SDcols = c], 3)
    na_prop2 <- round(expl[, sum(is.na(.SD))/nrow(expl), .SDcols = c], 3)
    
    if (na_prop > 0 & na_prop2 > 0) {
      print(paste0('Variable: ', c))
      print(paste0('Train: ', na_prop))
      print(paste0('Expl: ', na_prop2))
      
    }
    
    
    
  }
}

