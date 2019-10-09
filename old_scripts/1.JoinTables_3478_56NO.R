rm(list = ls())

library(data.table)
library(dplyr)
library(tidyr)
library(ggplot2)

### General Path
setwd('~/Desktop/Datathon/')
path <- c('data/')

# Read data
motivobaja <- fread(file = paste0(path, "5.MOTIVOBAJA_train.txt"),sep = ";", header = T)
aumentos <- fread(file = paste0(path, '3.AUMENTOS_train.txt'), sep = ";", header = T)
interacciones <- fread(file = paste0(path, '4.INTERACCIONESCOMMITMENT_train.txt'), sep =";", header = T)



# Change interacciones colnames
new_colnames <- c("IDMIEMBRO","CODIGO","DESCRIPCION",
              "GRUPO_SEGMENTACION","FECHA", "IDCANAL","IDMEDIO","IDCAMPANYA")
setnames(interacciones, colnames(interacciones), new_colnames)

################# JUNTAR 3 y  4 ( and 5 if) ###############

DataList<- list(aumentos, interacciones, motivobaja)
Columns<-(sapply(DataList,colnames))

INTERSECT_DATAAUMENTOS_INTERACCIONES<-intersect(Columns[[1]], Columns[[2]])
INTERSECT_ALLCOLUMS<-intersect(INTERSECT_DATAAUMENTOS_INTERACCIONES, Columns[[3]])
df_34 <-aumentos %>% full_join(interacciones, by = INTERSECT_ALLCOLUMS)
#    full_join(Data_MOTIVOBAJA, by = INTERSECT_ALLCOLUMS) %>%#if wanted


################# JUNTAR 7 Y 8 ###############

INTERACCIONES1<-c("8.INTERACCIONES_TLMK_train.txt")
INTERACCIONES2<-c("8-InteraccionesTLMK_ext_train.csv")
PERMISOSCOMUNICACION<-c("7-Permisos_de_comunicacion_f.txt")

##8a
INTERACCIONES_PRIM<-read.table(paste0(path,INTERACCIONES1),";", header = T, stringsAsFactors = F)
INTERACCIONES_PRIM<- as_tibble(INTERACCIONES_PRIM)
INTERACCIONES_PRIM$VARIABLE1<-as.character(INTERACCIONES_PRIM$VARIABLE1)
##8b
INTERACCIONES_SEC<-read.table(paste0(path,INTERACCIONES2),";", header = T, stringsAsFactors = F)
INTERACCIONES_SEC<- as_tibble(INTERACCIONES_SEC)
colnames(INTERACCIONES_SEC)<-colnames(INTERACCIONES_SEC)%>%toupper()
INTERACCIONES_SEC$VARIABLE1<-as.character(INTERACCIONES_SEC$VARIABLE1)
INTERACCIONES_SEC$VARIABLE2<-as.character(INTERACCIONES_SEC$VARIABLE2)
###7
PERMISOSCOMUNICACION_df<-read.table(paste0(path,PERMISOSCOMUNICACION),";", header = T, stringsAsFactors = F)
PERMISOSCOMUNICACION_df<- as_tibble(PERMISOSCOMUNICACION_df)

datalist <- list(INTERACCIONES_PRIM, INTERACCIONES_SEC)
columns8 <- lapply(datalist, colnames)

INTERESECT_COLUMNS<-intersect(columns8[[1]], columns8[[2]])

## IDINTERACCIONMIEMBRO, OBSERVACIONES NO APORTAN

################# JUNTAR 34 y 8  ###############

dfcomplete8<-full_join(INTERACCIONES_PRIM, INTERACCIONES_SEC, INTERESECT_COLUMNS)%>%
  select(-c(IDINTERACCIONMIEMBRO, OBSERVACIONES))

Intersect34_8<-intersect(colnames(dfcomplete8), colnames(df_34))
df_34$IDCANAL<-as.character(df_34$IDCANAL)
dfcomplete8$IDCANAL<-as.character(dfcomplete8$IDCANAL)

df_348<- full_join(df_34, dfcomplete8, by= Intersect34_8) 
################# JUNTAR 348 Y 7 ###############
df_3487<- full_join(df_348, PERMISOSCOMUNICACION_df, by =c("IDMIEMBRO"))
