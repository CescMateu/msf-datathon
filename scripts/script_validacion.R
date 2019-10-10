# Formateamos la fecha

ALTAYBAJAS_t$FECHA_1=dmy_hms(ALTAYBAJAS_t$FECHA)


# Selección del registro más reciente para cada Miembro, teniendo en cuenta que en un mismo timestamp 
# puede haber dos registros con IDREGISTRO diferente

ALTAYBAJAS_t2<-ALTAYBAJAS_t

ALTAYBAJAS_t2$CONCAT<-paste0(ALTAYBAJAS_t2$IDMIEMBRO,'_',ALTAYBAJAS_t2$FECHA_1,'_' ,ALTAYBAJAS_t2$IDREGISTRO)

ALTAYBAJAS_t2 <-  ALTAYBAJAS_t2 %>% 
  group_by(IDMIEMBRO) %>%
  summarise(registro_max = max(CONCAT))

# Selección del último registro completo para cada miembro

ALTAYBAJAS_t3<-ALTAYBAJAS_t

ALTAYBAJAS_t3$CONCAT<-paste0(ALTAYBAJAS_t3$IDMIEMBRO,'_',ALTAYBAJAS_t3$FECHA_1,'_' ,ALTAYBAJAS_t3$IDREGISTRO)

ALTAYBAJAS_t3<- filter(ALTAYBAJAS_t3,ALTAYBAJAS_t3$CONCAT %in% ALTAYBAJAS_t2$registro_max)


# Seleccionamos solamente los Activos 

ALTAYBAJAS_t3<-ALTAYBAJAS_t3[ALTAYBAJAS_t3$IDREGISTRO==0,]


# Identificamos miembros con más de un registro de alta en el mismo momento
# 61 miembros con mas de un 

miembros_duplicados <-ALTAYBAJAS_t3 %>%
  group_by(IDMIEMBRO) %>%
  count(IDMIEMBRO)


miembros_duplicados <- filter(miembros_duplicados, n>1)

#### Analizamos estos casos
#10038795
#10765517

ALTAYBAJAS_t3[ALTAYBAJAS_t3$IDMIEMBRO==10765517,]


# Seleccionamos los miembros de la validación

lista_validacion<-unique(ALTAYBAJAS_t3$IDMIEMBRO)