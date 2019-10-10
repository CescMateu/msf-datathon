# install.packages("fastDummies")
library(fastDummies)
library(data.table)
library(dplyr)
library(tidyr)
library(ggplot2)
library(lubridate)
### General Path
path<-c("file:///C:/Users/Carles/Desktop/Datatton/datathon_train/")

########   Archivos Reading
#3,4,5
Aumentos<-c("3.AUMENTOS_train.txt")
InteraccionesCOmmitment<-c("4.INTERACCIONESCOMMITMENT_train.txt")
MOTIVOBAJA_train<-c("5.MOTIVOBAJA_train.txt")

Data_AUMENTOS<-read.table(paste0(path,Aumentos),";", header = T, stringsAsFactors = F)
Data_AUMENTOS<- Data_AUMENTOS%>% as.data.table() 

Data_AUMENTOS[, FECHA_PROC := lubridate::dmy(FECHA)]

Data_AUMENTOS<-Data_AUMENTOS %>% as_tibble()


Data_iNTERACCIONES<-read.table(paste0(path,InteraccionesCOmmitment),";", header = T, stringsAsFactors = F)
colnames(Data_iNTERACCIONES)<-c("IDMIEMBRO","CODIGO","Descripción","GRUPO SEGMENTACIÓN","FECHA", "IDCANAL","IDMEDIO","IDCAMPANYA")
Data_iNTERACCIONES<- as_tibble(Data_iNTERACCIONES) %>% 
  mutate(FECHA_INTERACCION = 1) %>%# 1 = Interacción
  as.data.table()
Data_iNTERACCIONES[, FECHA_PROC := lubridate::dmy(FECHA)]

Data_iNTERACCIONES<-Data_iNTERACCIONES%>%as_tibble()

################# JUNTAR 3 y  4 ###############


DataList<- list(Data_AUMENTOS, Data_iNTERACCIONES)
Columns<-(sapply(DataList,colnames))

INTERSECT_DATAAUMENTOS_INTERACCIONES<-intersect(Columns[[1]], Columns[[2]])

df_34 <-Data_AUMENTOS %>% full_join(Data_iNTERACCIONES, by = INTERSECT_DATAAUMENTOS_INTERACCIONES)


################# JUNTAR 7 Y 8 ###############

INTERACCIONES1<-c("8.INTERACCIONES_TLMK_train.txt")
INTERACCIONES2<-c("8-InteraccionesTLMK_ext_train.csv")
PERMISOSCOMUNICACION<-c("7-Permisos_de_comunicacion_f.txt")

##8a
INTERACCIONES_PRIM<-read.table(paste0(path,INTERACCIONES1),";", header = T, stringsAsFactors = F)
INTERACCIONES_PRIM<- as_tibble(INTERACCIONES_PRIM)
INTERACCIONES_PRIM$VARIABLE1<-as.numeric(as.character(INTERACCIONES_PRIM$VARIABLE1))
INTERACCIONES_PRIM$VARIABLE2<-as.numeric(as.character(INTERACCIONES_PRIM$VARIABLE2))
INTERACCIONES_PRIM<-INTERACCIONES_PRIM%>%as.data.table()

INTERACCIONES_PRIM[, FECHA_PROC := lubridate::dmy_hms(FECHA)]
INTERACCIONES_PRIM<-INTERACCIONES_PRIM%>%as_tibble()
INTERACCIONES_PRIM$FECHA_PROC<-as.character(INTERACCIONES_PRIM$FECHA_PROC)

##8b
INTERACCIONES_SEC<-read.table(paste0(path,INTERACCIONES2),";", header = T, stringsAsFactors = F)
INTERACCIONES_SEC<- as_tibble(INTERACCIONES_SEC)
colnames(INTERACCIONES_SEC)<-colnames(INTERACCIONES_SEC)%>%toupper()
INTERACCIONES_SEC$VARIABLE1<-as.numeric(as.character(INTERACCIONES_SEC$VARIABLE1))
INTERACCIONES_SEC$VARIABLE2<-as.numeric(as.character(INTERACCIONES_SEC$VARIABLE2))

INTERACCIONES_SEC<-INTERACCIONES_SEC%>%as.data.table()

INTERACCIONES_SEC[, FECHA_PROC := lubridate::dmy(FECHA)]
INTERACCIONES_SEC<-INTERACCIONES_SEC%>%as_tibble()
INTERACCIONES_SEC$FECHA_PROC<-as.character(INTERACCIONES_SEC$FECHA_PROC)



###7
PERMISOSCOMUNICACION_df<-read.table(paste0(path,PERMISOSCOMUNICACION),";", header = T, stringsAsFactors = F)
PERMISOSCOMUNICACION_df<- as_tibble(PERMISOSCOMUNICACION_df)%>%arrange(IDMIEMBRO, IDTIPOCOMUNICACION)

PERMISOSCOMUNICACION_df1<-PERMISOSCOMUNICACION_df%>%
              group_by(IDMIEMBRO)%>%
              summarise(IDTIPOCOMUNICACION = paste(IDTIPOCOMUNICACION, collapse = "-")) 

datalist <- list(INTERACCIONES_PRIM, INTERACCIONES_SEC)
columns8 <- lapply(datalist, colnames)

INTERESECT_COLUMNS<-intersect(columns8[[1]], columns8[[2]])

## IDINTERACCIONMIEMBRO, OBSERVACIONES NO APORTAN

################# JUNTAR 34 y 8  ###############

dfcomplete8<-full_join(INTERACCIONES_PRIM, INTERACCIONES_SEC, INTERESECT_COLUMNS)%>%
  select(-c(IDINTERACCIONMIEMBRO, OBSERVACIONES))%>% 
          rename("NUEVA_APORTACION"="VARIABLE1")%>%
          rename("CAMBIOCUOTA"="VARIABLE2")%>% 
          filter((NUEVA_APORTACION >0)|!is.na(CAMBIOCUOTA)|!is.na(NUEVA_APORTACION))


Intersect34_8<-intersect(colnames(dfcomplete8), colnames(df_34))
df_34$IDCANAL<-as.character(df_34$IDCANAL)
dfcomplete8$IDCANAL<-as.character(dfcomplete8$IDCANAL)

df_34$FECHA_PROC<-as.character(df_34$FECHA_PROC)
df_348<- full_join(df_34, dfcomplete8, by= Intersect34_8) 
################# JUNTAR 348 Y 7 ###############
df_3487<- full_join(df_348, PERMISOSCOMUNICACION_df, by =c("IDMIEMBRO"))

df_34876<-df_3487


