
library(fastDummies)
library(data.table)

#################################data engineering ##################

# VARIABLE IDTIPOCOMUNICACION EN DUMMIES
##

df3478<-df_3487 %>% fastDummies::dummy_columns(select_columns = "IDTIPOCOMUNICACION")

# VARIABLE YYYYMM 
##

df3478<-df3478 %>% mutate(YEAR= substr(FECHA,7,10))%>% 
  mutate(MONTH=substr(FECHA,4,5))%>%
  mutate(YYYYMM= paste0(YEAR, "/", MONTH))

# VARIABLE IDREGISTRO CAMBIADA  
#     1 = AUMENTO
#     2 = DECREMENTO

df3478<- fastDummies::dummy_cols(df3478, select_columns = as.character("IDREGISTRO"))%>%
  rename(ID_REGISTRO_AUMENTO=IDREGISTRO_1)%>%
  rename(ID_REGISTRO_DECREMENTO= IDREGISTRO_2)%>%
  select(c(-IDREGISTRO_NA))
# VARIABLE IDFAMILIA
#     1 = CCMB
#     0 = NA

df3478$IDFAMILIA<-ifelse(is.na(df3478$IDFAMILIA) ,0,1)

fwrite(df3478, paste0('output/X/',"MSF_3_4_7_8.csv"))


install.packages()
