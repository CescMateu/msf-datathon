
path_data <- input_path


#POSAR silently =FALSE si volem que el resultat es
#carregui a l'enviorment

#IDEM per save, pero en aqeust cas guardant a csv

load_days_from_alta <- function(path_data,
                                save =FALSE,
                                silently=FALSE,
                                path_save =NULL){
  
  require(lubridate)
  
  DifferenceInDays <- function(date1,date2){
    
    dif <- abs(as.numeric(date1 - date2))
    return(dif) 
    
  }
  
  SuitableDate <- function(x){
    x <- gsub(pattern="-", replacement="", x = substr(x,1,10))
    x <- as.Date(x, format="%Y%m%d")
    return(x)
  }
  
  clientes_filtrar <- fread(paste0(path_data,"/miembros_a_filtrar.csv"))$IDMIEMBRO
  dt <- fread(paste0(path_data,"/2.ALTASYBAJAS_train.txt"))
  dt_filt <- dt[IDMIEMBRO %in% clientes_filtrar & IDREGISTRO==0,.(IDMIEMBRO,FECHA,IDMEDIO)]
  dt_filt[,medio_entrada:=as.numeric(as.factor(IDMEDIO))]
  major_cats <- dt_filt[,.(count=.N),by =IDMEDIO][order(-count)][1:20][,IDMEDIO]  
  dt_filt[,canal_entrada:=ifelse(IDMEDIO %in% major_cats,IDMEDIO,"otro"),]
  dt_filt[,canal_entrada_num:=as.numeric(as.factor(canal_entrada))]
  dt_agg <- dt_filt[,.(count=.N), by =list(canal_entrada, canal_entrada_num)]
  #fwrite(dt_agg,paste0(path_save,"/TD_canales_entrada.csv"))
  dt_filt[,canal_entrada:=NULL,]
  
  dt_filt[, FECHA2 := lubridate::dmy_hms(FECHA)]
  dt_filt[, mes_alta := lubridate::month(FECHA2)]
  dt_filt[, anyo_alta := lubridate::year(FECHA2)]
  dt_filt[, dia_alta := lubridate::day(FECHA2)]
  dt_filt[,dia_alta:=as.character(dia_alta)]
  dt_filt[, mes_alta := as.character(mes_alta)]
  dt_filt[,FECHA:=NULL]
  dt_filt[,FECHA2:=NULL]
  
  dt_filt[dia_alta %in% as.character(1:9), dia_alta := paste0("0",dia_alta),]
  dt_filt[mes_alta %in% as.character(1:9), mes_alta := paste0("0",mes_alta),]
  
  
  dt_filt[,version_alta := paste0(anyo_alta,mes_alta,dia_alta),]
  dt_filt <- dt_filt[,.(IDMIEMBRO,version_alta, canal_entrada_num),]
  dt_filt[,fc_alta:=SuitableDate(version_alta)]
  dt_filt[,version_alta:=NULL,]
  
  #Loop sobre cada id version
  versions <- as.character(seq(ymd('2012-01-01'), ymd('2019-02-01'), by = 'months', format = "%Y-%m"))
  versions2 <- substr(gsub("-","",versions),1,6)
  
  dt_list <- list()
  i <- 0
  for(v in versions2){
    i <- i+1
    print(v)
    dt_aux <- copy(dt_filt)
    dt_aux[,id_version_date:=paste0(v,"01")]
    dt_aux[,id_version_date:=SuitableDate(id_version_date),]
    dt_aux[,num_dias_desde_alta := ifelse(fc_alta >id_version_date, NA, DifferenceInDays(fc_alta,id_version_date)  ),]
    dt_aux[,num_meses_desde_alta:=num_dias_desde_alta/30]
    dt_aux[,num_anyos_desde_alta:=num_dias_desde_alta/365]
    dt_aux[,id_version := v]
    dt_aux[,id_version_date :=NULL,]
    dt_aux[,fc_alta:=NULL,]
    dt_list[[i]] <- dt_aux
  }
  
  
  dt_all <- rbindlist(dt_list, use.names = TRUE)
  
  if(save){
    fwrite(paste0(path_save,"/data_dias_desde_alta.csv"))
  }
  
  if(silently == FALSE){
    return(dt_all)
  }
}

dt_altas <- load_days_from_alta(path_data =path_data,
                                save = FALSE,
                                silently=FALSE)





