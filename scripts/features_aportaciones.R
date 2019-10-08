library(data.table)
library(lubridate)
library(dplyr)
library(stringr)

# path_data <- "C:/Users/POR809601/Desktop/Datathon/Data/"
# path_save <- "C:/Users/POR809601/Desktop/Datathon/Data/data_intermedia/aportacions/"
# 
path_data <- "data/"
path_save <- "output/"

dt_ap <- fread(paste0(path_data,"/10.APORTACIONES_train.txt"))
c <- colnames(dt_ap)
setnames(dt_ap,c,tolower(c))


versions <- as.character(seq(ymd('1987-01-01'), ymd('2019-02-01'), by = 'months', format = "%Y-%m"))
versions2 <- substr(gsub("-","",versions),1,6)
versions3 <- versions2[versions2 > 201112]

#Volem construir variables agregades a 3m 6m i 12m a partir de les aportacions, per tipuis d'aportacio
# donatiu o aportacio periodica

# Tipo aportación:
#   D - donativo
# A - Trabajadores asociados
# S - Socio

dt_ap[,.(count=.N), by=estado]
dt_ap <- dt_ap[estado!= "M"]
#Agrupem les A a les c (Son minoritaries)
str(dt_ap)
dt_ap[tipoaportacion=="A", tipoaportacion := "S"]

dt_ap[is.na(tipoaportacion)]

dt_ap[,unique(estado)]
dt_ap[,mes_aportacion:= substr(fechaalta,4,5),]
dt_ap[,anyo_aportacion:= substr(fechaalta,7,9),]
dt_ap[,anyo_aportacion:=paste0("20",anyo_aportacion),]
dt_ap[,version_aportacion := paste0(anyo_aportacion,mes_aportacion), ]

dt_ap[,importe:=as.numeric(importe),]



dt_cli = data.table(idmiembro = unique(dt_ap[,idmiembro]))
for(v in versions3){

 
  print(paste0("--> Calculant aportacions agregades pel mes ", v, "..."))
  #Iniciem el dataset per la versio en questio
    dt_cli_aux <- copy(dt_cli)
    dt_cli_aux[,id_version:=v,]
   
   
    ### A 3 mesos #
    vm3 <- getKPeriods(v, -2)
   
    #Tipus donatiu Cobrat
    dt_agg <- dt_ap[ estado == "C" & tipoaportacion == "D" & version_aportacion %in% vm3,
                        .( impop_3m_donatiu_C = sum(importe),
                           numop_3m_donatiu_C = .N,
                           numop_3m_devol_donatiu_C = sum(numerodevoluciones)), by = idmiembro]
 
 
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
   
    #Tipus soci cobrat
    dt_agg <- dt_ap[ estado == "C"& tipoaportacion == "S" & version_aportacion %in% vm3,
                     .( impop_3m_soci_C = sum(importe),
                      numop_3m_soci_C = .N,
                      numop_3m_devol_soci_C = sum(numerodevoluciones)), by = idmiembro]
   
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
   
    dt_cli_aux[,impop_3m_total_C := impop_3m_donatiu_C + impop_3m_soci_C ,]
    dt_cli_aux[,numop_3m_total_C := numop_3m_donatiu_C + numop_3m_soci_C ,]
   ###################################################################################################
   
    #Tipus donatiu impagat
    dt_agg <- dt_ap[ estado == "I" & tipoaportacion == "D" & version_aportacion %in% vm3,
                     .( impop_3m_donatiu_I = sum(importe),
                        numop_3m_donatiu_I = .N,
                        numop_3m_devol_donatiu_I = sum(numerodevoluciones)), by = idmiembro]
    
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    #Tipus soci impagat
    dt_agg <- dt_ap[ estado == "I"& tipoaportacion == "S" & version_aportacion %in% vm3,
                     .( impop_3m_soci_I = sum(importe),
                        numop_3m_soci_I = .N,
                        numop_3m_devol_soci_I = sum(numerodevoluciones)), by = idmiembro]
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    dt_cli_aux[,impop_3m_total_I := impop_3m_donatiu_I + impop_3m_soci_I ,]
    dt_cli_aux[,numop_3m_total_I := numop_3m_donatiu_I + numop_3m_soci_I ,]
  ################################################################################################### 
    
    #Tipus donatiu cancelat
    dt_agg <- dt_ap[ estado == "L" & tipoaportacion == "D" & version_aportacion %in% vm3,
                     .( impop_3m_donatiu_L = sum(importe),
                        numop_3m_donatiu_L = .N,
                        numop_3m_devol_donatiu_L = sum(numerodevoluciones)), by = idmiembro]
    
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    #Tipus soci cancelat
    dt_agg <- dt_ap[ estado == "L"& tipoaportacion == "S" & version_aportacion %in% vm3,
                     .( impop_3m_soci_L = sum(importe),
                        numop_3m_soci_L = .N,
                        numop_3m_devol_soci_L = sum(numerodevoluciones)), by = idmiembro]
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    dt_cli_aux[,impop_3m_total_L := impop_3m_donatiu_L + impop_3m_soci_L ,]
    dt_cli_aux[,numop_3m_total_L := numop_3m_donatiu_L + numop_3m_soci_L ,]
    ################################################################################################### 
    
    ###### a 6 mesos ########
    vm6 <- getKPeriods(v, -5)
    #Tipus donatiu
    dt_agg <- dt_ap[estado == "C" & tipoaportacion == "D" & version_aportacion %in% vm6,
                     .( impop_6m_donatiu_C = sum(importe),
                        numop_6m_donatiu_C = .N,
                        numop_6m_devol_donatiu_C = sum(numerodevoluciones)), by = idmiembro]
   
   
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
   
    #Tipus soci
    dt_agg <- dt_ap[ tipoaportacion == "S" & version_aportacion %in% vm6,
                     .( impop_6m_soci_C = sum(importe),
                        numop_6m_soci_C = .N,
                        numop_6m_devol_soci_C = sum(numerodevoluciones)), by = idmiembro]
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    dt_cli_aux[,impop_6m_total_C := impop_6m_donatiu_C + impop_6m_soci_C ,]
    dt_cli_aux[,numop_6m_total_C := numop_6m_donatiu_C + numop_6m_soci_C ,]
    ###################################################################################################
    
    #Tipus donatiu impagat
    dt_agg <- dt_ap[ estado == "I" & tipoaportacion == "D" & version_aportacion %in% vm3,
                     .( impop_6m_donatiu_I = sum(importe),
                        numop_6m_donatiu_I = .N,
                        numop_6m_devol_donatiu_I = sum(numerodevoluciones)), by = idmiembro]
    
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    #Tipus soci impagat
    dt_agg <- dt_ap[ estado == "I"& tipoaportacion == "S" & version_aportacion %in% vm3,
                     .( impop_6m_soci_I = sum(importe),
                        numop_6m_soci_I = .N,
                        numop_6m_devol_soci_I = sum(numerodevoluciones)), by = idmiembro]
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    dt_cli_aux[,impop_6m_total_I := impop_6m_donatiu_I + impop_6m_soci_I ,]
    dt_cli_aux[,numop_6m_total_I := numop_6m_donatiu_I + numop_6m_soci_I ,]
    ################################################################################################### 
    
    #Tipus donatiu cancelat
    dt_agg <- dt_ap[ estado == "L" & tipoaportacion == "D" & version_aportacion %in% vm3,
                     .( impop_6m_donatiu_L = sum(importe),
                        numop_6m_donatiu_L = .N,
                        numop_6m_devol_donatiu_L = sum(numerodevoluciones)), by = idmiembro]
    
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    #Tipus soci cancelat
    dt_agg <- dt_ap[ estado == "L"& tipoaportacion == "S" & version_aportacion %in% vm3,
                     .( impop_6m_soci_L = sum(importe),
                        numop_6m_soci_L = .N,
                        numop_6m_devol_soci_L = sum(numerodevoluciones)), by = idmiembro]
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    dt_cli_aux[,impop_6m_total_L := impop_6m_donatiu_L + impop_6m_soci_L ,]
    dt_cli_aux[,numop_6m_total_L := numop_6m_donatiu_L + numop_6m_soci_L ,]
    ################################################################################################### 
    ###### a 12 mesos ########
    vm12 <- getKPeriods(v, -11)
    #Tipus donatiu
    dt_agg <- dt_ap[ estado == "C" & tipoaportacion == "D" & version_aportacion %in% vm6,
                     .( impop_12m_donatiu_C = sum(importe),
                        numop_12m_donatiu_C = .N,
                        numop_12m_devol_donatiu_C = sum(numerodevoluciones)), by = idmiembro]
   
   
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
   
    #Tipus soci
    dt_agg <- dt_ap[estado == "C" & tipoaportacion == "S" & version_aportacion %in% vm12,
                     .( impop_12m_soci_C = sum(importe),
                        numop_12m_soci_C = .N,
                        numop_12m_devol_soci_C = sum(numerodevoluciones)), by = idmiembro]
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    dt_cli_aux[,impop_12m_total_C := impop_12m_donatiu_C + impop_12m_soci_C ,]
    dt_cli_aux[,numop_12m_total_C := numop_12m_donatiu_C + numop_12m_soci_C ,]
    ###################################################################################################   
    #Tipus donatiu impagat
    dt_agg <- dt_ap[ estado == "I" & tipoaportacion == "D" & version_aportacion %in% vm3,
                     .( impop_12m_donatiu_I = sum(importe),
                        numop_12m_donatiu_I = .N,
                        numop_12m_devol_donatiu_I = sum(numerodevoluciones)), by = idmiembro]
    
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    #Tipus soci impagat
    dt_agg <- dt_ap[ estado == "I"& tipoaportacion == "S" & version_aportacion %in% vm3,
                     .( impop_12m_soci_I = sum(importe),
                        numop_12m_soci_I = .N,
                        numop_12m_devol_soci_I = sum(numerodevoluciones)), by = idmiembro]
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    dt_cli_aux[,impop_12m_total_I := impop_12m_donatiu_I + impop_12m_soci_I ,]
    dt_cli_aux[,numop_12m_total_I := numop_12m_donatiu_I + numop_12m_soci_I ,]
    ################################################################################################### 
    
    #Tipus donatiu cancelat
    dt_agg <- dt_ap[ estado == "L" & tipoaportacion == "D" & version_aportacion %in% vm3,
                     .( impop_12m_donatiu_L = sum(importe),
                        numop_12m_donatiu_L = .N,
                        numop_12m_devol_donatiu_L = sum(numerodevoluciones)), by = idmiembro]
    
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    #Tipus soci cancelat
    dt_agg <- dt_ap[ estado == "L"& tipoaportacion == "S" & version_aportacion %in% vm3,
                     .( impop_12m_soci_L = sum(importe),
                        numop_12m_soci_L = .N,
                        numop_12m_devol_soci_L = sum(numerodevoluciones)), by = idmiembro]
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    dt_cli_aux[,impop_12m_total_L := impop_12m_donatiu_L + impop_12m_soci_L ,]
    dt_cli_aux[,numop_12m_total_L := numop_12m_donatiu_L + numop_12m_soci_L ,]
   
    ###############
    # Guardem  ###
    ##############??  
    fname =  paste0(path_save, "/aportacions_",v,".csv")
    fwrite(dt_cli_aux, file = fname)
    rm(dt_agg); rm(dt_cli_aux)
}