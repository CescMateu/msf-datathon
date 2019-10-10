#################################data engineering ##################
#### ANTIGUO
# colnames(df_34876_FILTERED)
# [1] "IDMIEMBRO"   x       "FECHA"     X         "IDFAMILIA"   X       "IDREGISTRO"  X      
# [5] "IDMEDIO"     x       "IDCAMPANYA"  x       "CODIGO"      o       "Descripción" o      
# [9] "GRUPO SEGMENTACIÓN" o "IDCANAL"    o        "FECHA_INTERACCION"  x "ESTADO" x            
# [13] "NUEVA_APORTACION" ZZ  "CAMBIOCUOTA" ZZ       "IDTIPOCOMUNICACION" o "GRUPO_MEDIO" o      
# [17] "GRUPO_CAMPANYA" o


# ZZ SIGNIFCA TRATAR DESPUES

# unique(df_3487$GRUPO_MEDIO) # para mirar valores de variables
 

# VARIABLE YYYYMM 
##
rm(list=setdiff(ls(), c("df_34876_FILTERED","path","ENRIQUECIMIENTO_COLS")))
df34678<-as.data.table(df_34876_FILTERED)
df34678[, FECHA_PROC := as.Date(FECHA_PROC, "%Y-%m-%d")]
df34678[, MONTH := lubridate::month(FECHA_PROC)]
df34678[, YEAR := lubridate::year(FECHA_PROC)]

df34678[, MONTH_str := ifelse(MONTH <= 9, paste0('0', MONTH), as.character(MONTH))]
df34678[, IDVERSION := paste0(YEAR, MONTH_str)]
df34678[, MONTH := NULL]
df34678[, YEAR := NULL]
df34678[, FECHA := NULL]
df34678<-as_tibble(df34678)%>%filter(IDVERSION!="NANA")## REMOVING IDVERSION WITH NA
# VARIABLE IDREGISTRO CAMBIADA  
#     1 = AUMENTO
#     2 = DECREMENTO
df34678<- fastDummies::dummy_cols(df34678, select_columns = "IDREGISTRO")%>%
  rename(ID_REGISTRO_AUMENTO=IDREGISTRO_1)%>%
  rename(ID_REGISTRO_DECREMENTO= IDREGISTRO_2)%>%
  select(c(-IDREGISTRO_NA))
# VARIABLE IDFAMILIA
#     1 = CCMB
#     0 = NA

df34678$IDFAMILIA<-ifelse(is.na(df34678$IDFAMILIA) ,0,1)

## JOIN BY IDMIEMBRO I YYYYMM
rm(list=setdiff(ls(), c("df34678","path","ENRIQUECIMIENTO_COLS")))


####################### AQUI ES ON TARDA MOOOOLT !! ##############
##########################################################################

DF34678<-df34678%>%as.data.table()
RESULT<-DF34678[, head(.SD, 1), by = c("IDMIEMBRO","IDVERSION")]



DF347WITHDUMMIES<-RESULT%>%
  select(-c(ESTADO))

write.csv(DF347WITHDUMMIES, paste0(path,"MSF_SometablesW.csv"))


###

# res2<-df34678%>%group_by(IDVERSION,IDMIEMBRO)%>%
#   summarise_all(~first(na.omit(.)))
# 
# res2<-res2%>%
#   select(-c(ESTADO))
# 
# write.csv(res2, paste0(path,"MSF_SometablesW2.csv"))
