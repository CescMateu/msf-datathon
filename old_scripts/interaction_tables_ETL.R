rm(list = ls())
gc()

library(lubridate)
library(data.table)

source("tools.R")

path_data <- "data/"

miembros_filtrar <- fread(paste0(path_data, "miembros_a_filtrar.csv"))

versions <- as.character(seq(ymd('1987-01-01'), ymd('2019-02-01'), by = 'months', format = "%Y-%m"))
versions2 <- substr(gsub("-","",versions),1,6)
versions3 <- versions2[versions2 > 201112]

out <- processInteractionsTable(path_data)
out <- merge(out, miembros_filtrar, by="IDMIEMBRO")

dt_cli <- data.table("IDMIEMBRO" = unique(out[,IDMIEMBRO]))

vars <- setdiff(colnames(out),c("IDMIEMBRO","ID_VERSION"))

dt_list <- list()
i <- 0
for(v in versions3){
  print(v)
  i <- i +1 
  dt_cli_aux <- copy(dt_cli)
  dt_cli_aux[,id_version:=v,]
  vm3 <- getKPeriods(v,-2)
  vm6 <- getKPeriods(v,-5)
  vm12 <- getKPeriods(v,-11)
  
  dt_agg <-out[IDVERSION %in% vm3,.(NUM_INTERACCIONES_MES_3m  = sum(NUM_INTERACCIONES_MES)   ,
                 NUM_INTERACCIONES_MES_CANAL_0003_3m = sum(NUM_INTERACCIONES_MES_CANAL_0003),
                 NUM_INTERACCIONES_MES_CANAL_0002_3m  = sum(NUM_INTERACCIONES_MES_CANAL_0002),
                 NUM_INTERACCIONES_MES_CANAL_0009_3m = sum(NUM_INTERACCIONES_MES_CANAL_0009),
                 NUM_INTERACCIONES_MES_CANAL_0008_3m  = sum(NUM_INTERACCIONES_MES_CANAL_0008),
                 NUM_INTERACCIONES_MES_CANAL_0005_3m = sum(NUM_INTERACCIONES_MES_CANAL_0005),
                 NUM_INTERACCIONES_MES_CANAL_0001_3m  = sum(NUM_INTERACCIONES_MES_CANAL_0001),
                 NUM_INTERACCIONES_MES_CANAL_0010_3m = sum(NUM_INTERACCIONES_MES_CANAL_0010),
                 NUM_INTERACCIONES_MES_CANAL_0007_3m = sum(NUM_INTERACCIONES_MES_CANAL_0007),
                 NUM_INTERACCIONES_MES_CANAL_0004_3m = sum(NUM_INTERACCIONES_MES_CANAL_0004),
                 NUM_INTERACCIONES_MES_CANAL_0006_3m = sum(NUM_INTERACCIONES_MES_CANAL_0006),
                 NUM_INTERACCIONES_MES_CANAL_unde_3m = sum(NUM_INTERACCIONES_MES_CANAL_unde),
                 NUM_INTERACCIONES_MES_CANAL_T_3m = sum(NUM_INTERACCIONES_MES_CANAL_T)),
                by = IDMIEMBRO]
  
  dt_agg[,NUM_INTERACCIONES_TOTAL_3M := NUM_INTERACCIONES_MES_3m  +
                                     NUM_INTERACCIONES_MES_CANAL_0003_3m +
                                     NUM_INTERACCIONES_MES_CANAL_0002_3m  +
                                     NUM_INTERACCIONES_MES_CANAL_0009_3m +
                                     NUM_INTERACCIONES_MES_CANAL_0008_3m  +
                                     NUM_INTERACCIONES_MES_CANAL_0005_3m +
                                     NUM_INTERACCIONES_MES_CANAL_0001_3m  +
                                     NUM_INTERACCIONES_MES_CANAL_0010_3m +
                                     NUM_INTERACCIONES_MES_CANAL_0007_3m +
                                     NUM_INTERACCIONES_MES_CANAL_0004_3m +
                                     NUM_INTERACCIONES_MES_CANAL_0006_3m +
                                     NUM_INTERACCIONES_MES_CANAL_unde_3m +
                                     NUM_INTERACCIONES_MES_CANAL_T_3m ]
                              
  dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="IDMIEMBRO",all.x = TRUE)
  
  dt_agg <-out[IDVERSION %in% vm6,.(NUM_INTERACCIONES_MES_6m  = sum(NUM_INTERACCIONES_MES)   ,
                                    NUM_INTERACCIONES_MES_CANAL_0003_6m = sum(NUM_INTERACCIONES_MES_CANAL_0003),
                                    NUM_INTERACCIONES_MES_CANAL_0002_6m  = sum(NUM_INTERACCIONES_MES_CANAL_0002),
                                    NUM_INTERACCIONES_MES_CANAL_0009_6m = sum(NUM_INTERACCIONES_MES_CANAL_0009),
                                    NUM_INTERACCIONES_MES_CANAL_0008_6m  = sum(NUM_INTERACCIONES_MES_CANAL_0008),
                                    NUM_INTERACCIONES_MES_CANAL_0005_6m = sum(NUM_INTERACCIONES_MES_CANAL_0005),
                                    NUM_INTERACCIONES_MES_CANAL_0001_6m  = sum(NUM_INTERACCIONES_MES_CANAL_0001),
                                    NUM_INTERACCIONES_MES_CANAL_0010_6m = sum(NUM_INTERACCIONES_MES_CANAL_0010),
                                    NUM_INTERACCIONES_MES_CANAL_0007_6m = sum(NUM_INTERACCIONES_MES_CANAL_0007),
                                    NUM_INTERACCIONES_MES_CANAL_0004_6m = sum(NUM_INTERACCIONES_MES_CANAL_0004),
                                    NUM_INTERACCIONES_MES_CANAL_0006_6m = sum(NUM_INTERACCIONES_MES_CANAL_0006),
                                    NUM_INTERACCIONES_MES_CANAL_unde_6m = sum(NUM_INTERACCIONES_MES_CANAL_unde),
                                    NUM_INTERACCIONES_MES_CANAL_T_6m = sum(NUM_INTERACCIONES_MES_CANAL_T)),
               by = IDMIEMBRO]
  
  dt_agg[,NUM_INTERACCIONES_TOTAL_6m:= NUM_INTERACCIONES_MES_6m  +
           NUM_INTERACCIONES_MES_CANAL_0003_6m +
           NUM_INTERACCIONES_MES_CANAL_0002_6m  +
           NUM_INTERACCIONES_MES_CANAL_0009_6m +
           NUM_INTERACCIONES_MES_CANAL_0008_6m  +
           NUM_INTERACCIONES_MES_CANAL_0005_6m +
           NUM_INTERACCIONES_MES_CANAL_0001_6m  +
           NUM_INTERACCIONES_MES_CANAL_0010_6m +
           NUM_INTERACCIONES_MES_CANAL_0007_6m +
           NUM_INTERACCIONES_MES_CANAL_0004_6m +
           NUM_INTERACCIONES_MES_CANAL_0006_6m +
           NUM_INTERACCIONES_MES_CANAL_unde_6m +
           NUM_INTERACCIONES_MES_CANAL_T_6m ]
  
  dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="IDMIEMBRO",all.x = TRUE)


  dt_agg <-out[IDVERSION %in% vm12,.(NUM_INTERACCIONES_MES_12m  = sum(NUM_INTERACCIONES_MES)   ,
                                    NUM_INTERACCIONES_MES_CANAL_0003_12m = sum(NUM_INTERACCIONES_MES_CANAL_0003),
                                    NUM_INTERACCIONES_MES_CANAL_0002_12m  = sum(NUM_INTERACCIONES_MES_CANAL_0002),
                                    NUM_INTERACCIONES_MES_CANAL_0009_12m = sum(NUM_INTERACCIONES_MES_CANAL_0009),
                                    NUM_INTERACCIONES_MES_CANAL_0008_12m  = sum(NUM_INTERACCIONES_MES_CANAL_0008),
                                    NUM_INTERACCIONES_MES_CANAL_0005_12m = sum(NUM_INTERACCIONES_MES_CANAL_0005),
                                    NUM_INTERACCIONES_MES_CANAL_0001_12m  = sum(NUM_INTERACCIONES_MES_CANAL_0001),
                                    NUM_INTERACCIONES_MES_CANAL_0010_12m = sum(NUM_INTERACCIONES_MES_CANAL_0010),
                                    NUM_INTERACCIONES_MES_CANAL_0007_12m = sum(NUM_INTERACCIONES_MES_CANAL_0007),
                                    NUM_INTERACCIONES_MES_CANAL_0004_12m = sum(NUM_INTERACCIONES_MES_CANAL_0004),
                                    NUM_INTERACCIONES_MES_CANAL_0006_12m = sum(NUM_INTERACCIONES_MES_CANAL_0006),
                                    NUM_INTERACCIONES_MES_CANAL_unde_12m = sum(NUM_INTERACCIONES_MES_CANAL_unde),
                                    NUM_INTERACCIONES_MES_CANAL_T_12m = sum(NUM_INTERACCIONES_MES_CANAL_T)),
               by = IDMIEMBRO]
  
  dt_agg[,NUM_INTERACCIONES_TOTAL_12m:= NUM_INTERACCIONES_MES_12m  +
           NUM_INTERACCIONES_MES_CANAL_0003_12m +
           NUM_INTERACCIONES_MES_CANAL_0002_12m  +
           NUM_INTERACCIONES_MES_CANAL_0009_12m +
           NUM_INTERACCIONES_MES_CANAL_0008_12m  +
           NUM_INTERACCIONES_MES_CANAL_0005_12m +
           NUM_INTERACCIONES_MES_CANAL_0001_12m  +
           NUM_INTERACCIONES_MES_CANAL_0010_12m +
           NUM_INTERACCIONES_MES_CANAL_0007_12m +
           NUM_INTERACCIONES_MES_CANAL_0004_12m +
           NUM_INTERACCIONES_MES_CANAL_0006_12m +
           NUM_INTERACCIONES_MES_CANAL_unde_12m +
           NUM_INTERACCIONES_MES_CANAL_T_12m ]
  
  dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="IDMIEMBRO",all.x = TRUE)
  c <- colnames(dt_cli_aux)
  setnames(dt_cli_aux,c,tolower(c))
  dt_list[[i]] <- dt_cli_aux
  rm(dt_cli_aux)

} 

dt_all <- rbindlist(dt_list,use.names=TRUE)
fwrite(dt_all, paste0(path_data,"/ETL_interacciones.csv"))
