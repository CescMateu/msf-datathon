################################# EXCEL EXTRA TABLES JOIN ###############################
library(readxl)
path2<- "C:/Users/Carles/Desktop/Datatton/"
### Reading for posterior Joining dictionary of MEDIO with IDMEDIOS with IDMEDIO COLUMN

# Discarding c(grupomedio, descripción) for being repetitive
#### Descripción COULD BE INTERESTING FROM THE POINT OF VIEW OF EXTRACTING LOCATION IF THERE IS TIME

DICCIONARIO_DE_MEDIOS <- read_excel(paste0(path2,"DICCIONARIO DE MEDIOS.xlsx")) %>%
  rename("IDMEDIO" = "MEDIO") %>%
  rename("GRUPO_MEDIO"= grupo)%>%
  select(c(IDMEDIO, GRUPO_MEDIO)) 

# Discarding c(CAMPAÑA, DESCRIPCIÓN) for being repetitive
#### Descripción COULD BE INTERESTING FROM THE POINT OF VIEW OF EXTRACTING 
##### TYPES OF CAMPAÑAS DOING CLUSTERING (E.G. campaña de navidad, etc.)

DICCIONARIO_DE_CAMPANAS <- read_excel(paste0(path2,"DICCIONARIO DE CAMPANAS.xlsx"))%>%
  rename("IDCAMPANYA"="idccamp")%>%
  rename("GRUPO_CAMPANYA"="GRUPOCAMP")%>%
  select(IDCAMPANYA, GRUPO_CAMPANYA)

# JOINING DICTIONARIES

df_34876 <- df_34876 %>% left_join(DICCIONARIO_DE_MEDIOS, by = "IDMEDIO")%>%
  left_join(DICCIONARIO_DE_CAMPANAS, by ="IDCAMPANYA")

### READ ONLY MIEMBROS A SELECCIONAR Y filtrar data

FILTRARmiEMBROS <- unlist(read.csv(paste0(path2,"miembros_a_filtrar.csv")),use.names = F) 

df_34876_FILTERED<- df_34876 %>% 
        filter(IDMIEMBRO  %in% FILTRARmiEMBROS)


