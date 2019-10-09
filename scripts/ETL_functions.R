createTargetVar <- function(input_path, output_path) {
  
  print('ETL: Creating target variable')
  
  #################################
  # ------ DATA PREPROCESSING --- #
  #################################
  
  
  # READ DATA
  dt_bajas <- fread(paste0(input_path, '2.ALTASYBAJAS_train.txt'))
  
  # Choose which vars do we want to keep
  factor_vars <- c('IDFAMILIA', 'IDREGISTRO', 'IDMEDIO', 'IDCAMPANYA')
  dt_bajas$IDFAMILIA <- dt_bajas[, as.factor(IDFAMILIA)]
  
  
  # Preprocess the dates to create an IDVERSION with format YYYYMM
  fecha_inicial <- dt_bajas[, min(FECHA)]
  dt_bajas[, FECHA_PROC := lubridate::dmy_hms(FECHA)]
  dt_bajas[, MONTH := lubridate::month(FECHA_PROC)]
  dt_bajas[, YEAR := lubridate::year(FECHA_PROC)]
  
  dt_bajas[, MONTH_str := ifelse(MONTH <= 9, paste0('0', MONTH), as.character(MONTH))]
  dt_bajas[, IDVERSION := paste0(YEAR, MONTH_str)]
  dt_bajas[, MONTH := NULL]
  dt_bajas[, YEAR := NULL]
  dt_bajas[, FECHA := NULL]
  
  # Which are the maximum an minimum dates of the dataset?
  dt_bajas[, min(FECHA_PROC)]
  dt_bajas[, max(FECHA_PROC)]
  
  # Create a sequence of IDVERSIONS between the min date and the max date
  versions <- as.character(seq(ymd('2012-01-01'), ymd('2019-02-01'), by = 'months', format = "%Y-%m"))
  versions2 <- substr(gsub("-","",versions),1,6)
  
  #################################################
  # ------ CREATE THE "ACTIVE CLIENTS" MATRIX --- #
  #################################################
  
  # Create a table which indicates at each t, which customers where active
  socis_actius <- data.table('IDMIEMBRO' = dt_bajas[, unique(IDMIEMBRO)])
  socis_actius <- merge(socis_actius, dt_bajas[IDREGISTRO == 0, .(IDMIEMBRO, IDVERSION)], by = 'IDMIEMBRO', all.x = TRUE)
  socis_actius <- socis_actius[!is.na(IDVERSION)] # Descartamos socios que nunca han tenido un registro de alta (1200 solo)
  setnames(socis_actius, 'IDVERSION', 'IDVERSION_ALTA')
  
  socis_actius <- merge(socis_actius, dt_bajas[IDREGISTRO == 99, .(IDMIEMBRO, IDVERSION)], by = 'IDMIEMBRO', all.x = TRUE)
  setnames(socis_actius, 'IDVERSION', 'IDVERSION_BAJA')
  
  socis_remove <- socis_actius[, .(count = .N), by = IDMIEMBRO][count>1][, IDMIEMBRO] # Socis amb més d'una alta o baixa
  socis_actius <- socis_actius[!IDMIEMBRO %in% socis_remove]
  
  # Ens quedem amb 720k socies
  
  # TEST 1 - OK!
  if (nrow(socis_actius[IDVERSION_BAJA < IDVERSION_ALTA]) != 0) {
    stop('Something is wrong!!!')
  }
  
  
  # Definim matriu de clients actius
  for (t in versions2) {
    # Condicions perquè un client sigui actiu a temps t
    #     1) S'hagi donat d'alta abans de t (data d'alta mes petit que t)
    #     2) No s'hagi donat de baixa a temps t (data de baixa nula o major que t)
    #print(t)
    socis_actius[, eval(paste0('ACTIU_', t)) := ifelse(IDVERSION_ALTA < t & (IDVERSION_BAJA > t | is.na(IDVERSION_BAJA)), 1, 0) ]
  }
  
  #######################################
  # ------ CREATE THE TARGET MATRIX --- #
  #######################################
  
  count <- c()
  i <- 0
  count_baixes_4_6 <- c()
  for (t in versions2) {
    
    if (t > '201808') {
      
      print('201808 has been reached!')
      
    } else {
      
      print(t)
      
      # Generem els IDVERSIONS per t+4, t+5, t+6
      vs <- getKPeriods(t, 6)
      v_4  <- vs[5]
      v_5  <- vs[6]
      v_6  <- vs[7]
      
      # Generem taules separades per cada t
      keep_base <- c('IDMIEMBRO', 'IDVERSION_ALTA', 'IDVERSION_BAJA')
      keep_var <-  paste0('ACTIU_', c(v_4, v_5, v_6))
      keep <- c(keep_base, keep_var)
      
      # CONDICIONS PER DEFINIR LA TARGET VARIABLE
      # 1) El client ha de ser actiu a temps t
      dt_aux <- socis_actius[get(paste0('ACTIU_', t)) == 1, keep, with = F]
      
      # Guardem dades pel plot
      i = i +1
      count[i] <- nrow(dt_aux)
      
      # 2) La possible baixa del client s'ha de produir com a mínim en el 4t mes que estem intentant predir
      dt_aux <- dt_aux[IDVERSION_BAJA >= v_4 | is.na(IDVERSION_BAJA)]
      
      # 3) S'ha de donar de baixa en t+4, t+5 o t+6
      dt_aux[, IND_BAIXA := ifelse(get(keep_var[1]) == 0 | get(keep_var[2]) == 0 | get(keep_var[3]) == 0, 1, 0)]
      # Guardem dades pel plot
      count_baixes_4_6[i] <- ifelse(nrow(dt_aux[IND_BAIXA == 1]) > 0, 
                                    dt_aux[, .N, by = IND_BAIXA][IND_BAIXA == 1, N], 
                                    0)
      
      dt_aux <- dt_aux[, .(IDMIEMBRO, IND_BAIXA)]
      dt_aux[, IDVERSION := t]
      
      N_zeros = nrow(dt_aux[IND_BAIXA == 0])
      
      # We erase 50% of the 0's that we have in each month
      dt_aux_1 <- dt_aux[IND_BAIXA == 1]
      dt_aux_0 <- dt_aux[IND_BAIXA == 0][sample(1:N_zeros, size = floor(nrow(dt_aux_1) * 15), replace = FALSE)]
      dt_aux <- rbind(dt_aux_0, dt_aux_1)
      
      # Write the data in separated data files
      fwrite(x = dt_aux, file = paste0(output_path, 'target/bajas_', t, '.csv'), sep = ';')
    }
    
    
  }
  
  print('ETL 1: Process finished!')
  
}






createPrimaryMembers <- function(input_path, output_path) {
  
  print('ETL: Creating the base of our members')
  
  versions <- as.character(seq(ymd('2012-01-01'), ymd('2018-08-01'), by = 'months', format = "%Y-%m"))
  versions <- substr(gsub("-","",versions),1,6)
  
  dt <- data.table()
  for (t in versions) {
    print(paste0('Reading IDVERSION: ', t))
    dt <- rbind(dt, fread(paste0(output_path, 'target/bajas_', t, '.csv')))
  }
  
  miembros_filtrar = dt[, .(IDMIEMBRO = unique(IDMIEMBRO))]
  fwrite(x = miembros_filtrar, file = paste0(output_path, 'miembros_a_filtrar.csv'), sep = ';')
  
}



processIndividuosTable <- function(input_path, output_path) {
  
  print('ETL: Processing Individuos table')
  
  # Read data
  indi <- fread(file = paste0(input_path, '1.INDIVIDUOS_SOCIOS_O_BAJAS_train.txt'), sep =";", header = T)
  
  indi[, TELEFONOFIJO := as.factor(TELEFONOFIJO)]
  levels(indi$TELEFONOFIJO) <- c(1, 0)
  
  indi[, TELEFONOMOVIL := as.factor(TELEFONOMOVIL)]
  levels(indi$TELEFONOMOVIL) <- c(1, 0)
  
  
  indi[, EMAIL := as.factor(EMAIL)]
  levels(indi$EMAIL) <- c(1, 0)
  
  indi[, DIRECCION := as.factor(DIRECCION)]
  levels(indi$DIRECCION) <- c(1, 0)
  
  indi[, IDIDIOMA := as.factor(IDIDIOMA)]
  
  indi[, SEXO := as.factor(SEXO)]
  
  indi[, ONETOONE := as.factor(ONETOONE)]
  indi[, CODPOSTAL := as.factor(CODPOSTAL)]
  
  indi[, FECHANAC_PROC := lubridate::dmy(FECHANACIMIENTO)]
  indi[, AGE := as.integer(floor((lubridate::today() - FECHANAC_PROC) / 365))]
  
  indi[, FECHANACIMIENTO := NULL]
  indi[, MONTH_NACIM := lubridate::month(FECHANAC_PROC)]
  indi[, DAY_NACIM := lubridate::day(FECHANAC_PROC)]
  indi[, YEAR_NACIM := lubridate::year(FECHANAC_PROC)]
  indi[, FECHANAC_PROC := NULL]
  
  fwrite(x = indi, file = paste0(output_path, '1_individuos_ETL.csv'), sep = ';')
  
}


processInteractionsTable <- function(input_path, output_path) {
  
  print('ETL: Processing Interacciones table')
  # Read data
  interacciones <- fread(file = paste0(input_path, '4.INTERACCIONESCOMMITMENT_train.txt'), sep =";", header = T)
  new_colnames <- c("IDMIEMBRO","CODIGO","DESCRIPCION",
                    "GRUPO_SEGMENTACION","FECHA", "IDCANAL","IDMEDIO","IDCAMPANYA")
  setnames(interacciones, colnames(interacciones), new_colnames)
  
  
  # Explore data
  interacciones[, .N, by = c('CODIGO', 'DESCRIPCION')][order(-N)][1:20]
  
  interacciones[, FECHA_PROC := lubridate::dmy_hms(FECHA)]
  interacciones[, MES := ifelse(lubridate::month(FECHA_PROC) < 10,
                                paste0('0', lubridate::month(FECHA_PROC)),
                                lubridate::month(FECHA_PROC))]
  interacciones[, IDVERSION := paste0(lubridate::year(FECHA_PROC), MES)]
  interacciones[, FECHA := NULL]
  interacciones[, MES := NULL]
  
  # Número de interacciones a cada cliente por mes
  N_INTER_MES <- interacciones[, .(NUM_INTERACCIONES_MES =.N), by = c('IDMIEMBRO', 'IDVERSION')]
  
  
  # Número de interacciones a cada cliente por canal
  for (idcanal in interacciones[, unique(IDCANAL)]) {
    
    # Retrieve information by channel
    int_canal <- interacciones[IDCANAL == idcanal, .(n_int_aux =.N), by = c('IDMIEMBRO', 'IDVERSION')]  
    setnames(int_canal, 'n_int_aux', paste0('NUM_INTERACCIONES_MES_CANAL_', idcanal))
    
    # Join information to the total interactions matrix
    N_INTER_MES <- merge(N_INTER_MES, int_canal, by = c('IDMIEMBRO', 'IDVERSION'), all.x = TRUE)
    
  }
  
  # Transform NAs to 0 ()
  allNAsToZero(N_INTER_MES)
  
  
  # Part joan
  
  miembros_filtrar <- fread(paste0(output_path, "miembros_a_filtrar.csv"))
  
  versions <- as.character(seq(ymd('1987-01-01'), ymd('2019-02-01'), by = 'months', format = "%Y-%m"))
  versions2 <- substr(gsub("-","",versions),1,6)
  versions3 <- versions2[versions2 > 201112]
  
  out <- merge(N_INTER_MES, miembros_filtrar, by="IDMIEMBRO")
  
  dt_cli <- data.table("IDMIEMBRO" = unique(out[,IDMIEMBRO]))
  
  vars <- setdiff(colnames(out),c("IDMIEMBRO","IDVERSION"))
  
  dt_list <- list()
  i <- 0
  for(v in versions3){
    print(v)
    i <- i +1 
    dt_cli_aux <- copy(dt_cli)
    dt_cli_aux[,IDVERSION:=v,]
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
    setnames(dt_cli_aux,c,toupper(c))
    dt_list[[i]] <- dt_cli_aux
    rm(dt_cli_aux)
    
  } 
  
  dt_all <- rbindlist(dt_list,use.names=TRUE)
  setnames(dt_all, colnames(dt_all), toupper(colnames(dt_all)))
  fwrite(dt_all, paste0(output_path, "4_interacciones_ETL.csv"), sep = ';')
  
}







processAxesorTable <- function(input_path, output_path) {
  
  print('ETL: Processing Axesor table')
  
  axesor <- fread(file = paste0(input_path, '6-enriquecimiento_20170201.txt'))
  
  cols_factor <- c('ComunidadAutonoma', 'Provincia', 'Municipio', 'NivelEstudios', 'TipoFamilia', 'Nucleo',
                   'PerfilNucleo', 'T_Renta', 'T_TasaParo', 'MiembrosFamilia', 'Menores18', 'Entre18_45', 
                   'Entre46_65', 'Jubilados', 'NHijos', 'PerfilHijos')
  
  axesor <- axesor[, .SD, .SDcols = c('IDMIEMBRO', cols_factor)]
  axesor[, (cols_factor):= lapply(.SD, as.factor), .SDcols=cols_factor]
  axesor[, (cols_factor):= lapply(.SD, as.numeric), .SDcols=cols_factor]
  
  fwrite(axesor, file = paste0(output_path, '6_axesor_ETL.csv'), sep = ';')
  
}




processAportacionesTable <- function(input_path, output_path) {
  
  print('ETL: Processing Aportaciones table')
  
  #big_output <- data.table()
   
  path_data <- input_path
  path_save <- output_path
  
  dt_miembros <- fread(paste0(path_save,"miembros_a_filtrar.csv"))
  dt_ap <- fread(paste0(path_data,"/10.APORTACIONES_train.txt"))
  #nrow(dt_ap) # 27104806
  
  dt_ap <- merge(dt_miembros, dt_ap, by = 'IDMIEMBRO', all.x = TRUE)
  #nrow(dt_ap) # 22989435
  
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
    
    print(paste0("Computing aggregated aportaciones for month: ", v))
    
    #Iniciem el dataset per la versio en questio
    dt_cli_aux <- copy(dt_cli)
    dt_cli_aux[,id_version:=v,]
    
    
    ### A 3 mesos #
    vm3 <- getKPeriods(v, -2)
    
    #Tipus donatiu Cobrat
    # dt_agg <- dt_ap[ estado == "C" & tipoaportacion == "D" & version_aportacion %in% vm3,
    #                  .( impop_3m_donatiu_C = sum(importe),
    #                     numop_3m_devol_donatiu_C = sum(numerodevoluciones)), by = idmiembro]
    # 
    # 
    # dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    #Tipus soci cobrat
    dt_agg <- dt_ap[ estado == "C"& tipoaportacion == "S" & version_aportacion %in% vm3,
                     .( impop_3m_soci_C = sum(importe),
                        numop_3m_devol_soci_C = sum(numerodevoluciones)), by = idmiembro]
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
  
    ###################################################################################################
    
    #Tipus donatiu impagat
    # dt_agg <- dt_ap[ estado == "I" & tipoaportacion == "D" & version_aportacion %in% vm3,
    #                  .( impop_3m_donatiu_I = sum(importe),
    #                     numop_3m_devol_donatiu_I = sum(numerodevoluciones)), by = idmiembro]
    # 
    # 
    # dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    #Tipus soci impagat
    dt_agg <- dt_ap[ estado == "I"& tipoaportacion == "S" & version_aportacion %in% vm3,
                     .( impop_3m_soci_I = sum(importe),
                        numop_3m_devol_soci_I = sum(numerodevoluciones)), by = idmiembro]
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    

    ################################################################################################### 
    
    # #Tipus donatiu cancelat
    # dt_agg <- dt_ap[ estado == "L" & tipoaportacion == "D" & version_aportacion %in% vm3,
    #                  .( impop_3m_donatiu_L = sum(importe),
    #                     numop_3m_devol_donatiu_L = sum(numerodevoluciones)), by = idmiembro]
    # 
    # 
    # dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    #Tipus soci cancelat
    dt_agg <- dt_ap[ estado == "L"& tipoaportacion == "S" & version_aportacion %in% vm3,
                     .( impop_3m_soci_L = sum(importe),
                        numop_3m_devol_soci_L = sum(numerodevoluciones)), by = idmiembro]
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    

    ################################################################################################### 
    
    ###### a 6 mesos ########
    vm6 <- getKPeriods(v, -5)
    #Tipus donatiu
    # dt_agg <- dt_ap[estado == "C" & tipoaportacion == "D" & version_aportacion %in% vm6,
    #                 .( impop_6m_donatiu_C = sum(importe),
    #                    numop_6m_devol_donatiu_C = sum(numerodevoluciones)), by = idmiembro]
    # 
    # 
    # dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    #Tipus soci
    dt_agg <- dt_ap[ tipoaportacion == "S" & version_aportacion %in% vm6,
                     .( impop_6m_soci_C = sum(importe),
                        numop_6m_devol_soci_C = sum(numerodevoluciones)), by = idmiembro]
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    ###################################################################################################
    
    #Tipus donatiu impagat
    # dt_agg <- dt_ap[ estado == "I" & tipoaportacion == "D" & version_aportacion %in% vm3,
    #                  .( impop_6m_donatiu_I = sum(importe),
    #                     numop_6m_devol_donatiu_I = sum(numerodevoluciones)), by = idmiembro]
    # 
    # 
    # dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    #Tipus soci impagat
    dt_agg <- dt_ap[ estado == "I"& tipoaportacion == "S" & version_aportacion %in% vm3,
                     .( impop_6m_soci_I = sum(importe),
                        numop_6m_devol_soci_I = sum(numerodevoluciones)), by = idmiembro]
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    ################################################################################################### 
    
    #Tipus donatiu cancelat
    # dt_agg <- dt_ap[ estado == "L" & tipoaportacion == "D" & version_aportacion %in% vm3,
    #                  .( impop_6m_donatiu_L = sum(importe),
    #                     numop_6m_devol_donatiu_L = sum(numerodevoluciones)), by = idmiembro]
    # 
    # 
    # dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    #Tipus soci cancelat
    dt_agg <- dt_ap[ estado == "L"& tipoaportacion == "S" & version_aportacion %in% vm3,
                     .( impop_6m_soci_L = sum(importe),
                        numop_6m_devol_soci_L = sum(numerodevoluciones)), by = idmiembro]
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    
    ################################################################################################### 
    ###### a 12 mesos ########
    vm12 <- getKPeriods(v, -11)
    #Tipus donatiu
    # dt_agg <- dt_ap[ estado == "C" & tipoaportacion == "D" & version_aportacion %in% vm6,
    #                  .( impop_12m_donatiu_C = sum(importe),
    #                     numop_12m_donatiu_C = .N,
    #                     numop_12m_devol_donatiu_C = sum(numerodevoluciones)), by = idmiembro]
    # 
    # 
    # dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )

    #Tipus soci
    dt_agg <- dt_ap[estado == "C" & tipoaportacion == "S" & version_aportacion %in% vm12,
                    .( impop_12m_soci_C = sum(importe),
                       numop_12m_devol_soci_C = sum(numerodevoluciones)), by = idmiembro]
    
    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )
    

    ###################################################################################################
    #Tipus donatiu impagat
    # dt_agg <- dt_ap[ estado == "I" & tipoaportacion == "D" & version_aportacion %in% vm3,
    #                  .( impop_12m_donatiu_I = sum(importe),
    #                     numop_12m_donatiu_I = .N,
    #                     numop_12m_devol_donatiu_I = sum(numerodevoluciones)), by = idmiembro]
    # 
    # 
    # dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )

    #Tipus soci impagat
    dt_agg <- dt_ap[ estado == "I"& tipoaportacion == "S" & version_aportacion %in% vm3,
                     .( impop_12m_soci_I = sum(importe),
                        numop_12m_devol_soci_I = sum(numerodevoluciones)), by = idmiembro]

    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )

    ###################################################################################################

    #Tipus donatiu cancelat
    # dt_agg <- dt_ap[ estado == "L" & tipoaportacion == "D" & version_aportacion %in% vm3,
    #                  .( impop_12m_donatiu_L = sum(importe),
    #                     numop_12m_donatiu_L = .N,
    #                     numop_12m_devol_donatiu_L = sum(numerodevoluciones)), by = idmiembro]
    # 
    # 
    # dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )

    #Tipus soci cancelat
    dt_agg <- dt_ap[ estado == "L"& tipoaportacion == "S" & version_aportacion %in% vm3,
                     .( impop_12m_soci_L = sum(importe),
                        numop_12m_devol_soci_L = sum(numerodevoluciones)), by = idmiembro]

    dt_cli_aux <- merge(dt_cli_aux,dt_agg, by="idmiembro", all.x=TRUE )

    
    #####
    
    allNAsToZero(DT = dt_cli_aux, cols = 'all')
    
    fwrite(x = dt_cli_aux, file = paste0(path_save, 'aportaciones/aportaciones_chunk_', v, '.csv'), sep = ';')
    
    ###############################################
    #Concatenar en el dataset final
    #big_output <- rbind(big_output, dt_cli_aux)
    
    # Alliberar memòria
    rm(dt_agg); rm(dt_cli_aux)
  }
  
  #fwrite(big_output, file = paste0(path_save, "9_aportaciones_ETL.csv"), sep = ';')
  
}



createChannelsAndPermanenceTime <- function(input_data, output_data){
  
  print('ETL: Processing Permanence time and entrance channels')
  
  # Read data
  clientes_filtrar <- fread(paste0(output_data,"/miembros_a_filtrar.csv"))$IDMIEMBRO
  dt <- fread(paste0(input_data,"/2.ALTASYBAJAS_train.txt"))
  
  # Filter data with the selected members
  dt_filt <- dt[IDMIEMBRO %in% clientes_filtrar & IDREGISTRO==0,.(IDMIEMBRO,FECHA,IDMEDIO)]
  
  # Which are the major entrance channels?
  dt_filt[,medio_entrada:=as.numeric(as.factor(IDMEDIO))]
  major_cats <- dt_filt[,.(count=.N),by =IDMEDIO][order(-count)][1:20][,IDMEDIO]  
  dt_filt[,canal_entrada:=ifelse(IDMEDIO %in% major_cats,IDMEDIO,"otro"),]
  dt_filt[,canal_entrada_num:=as.numeric(as.factor(canal_entrada))]
  dt_agg <- dt_filt[,.(count=.N), by =list(canal_entrada, canal_entrada_num)]
  
  # Write the channels dataset
  fwrite(dt_agg,paste0(output_data,"/canales_entrada_ETL.csv"), sep = ';')
  
  # Continue with the permanence time
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
    dt_aux[,IDVERSION := v]
    dt_aux[,id_version_date :=NULL,]
    dt_aux[,fc_alta:=NULL,]
    dt_list[[i]] <- dt_aux
  }
  
  dt_all <- rbindlist(dt_list, use.names = TRUE)
  setnames(dt_all, colnames(dt_all), toupper(colnames(dt_all)))
  
  # Write the result
  fwrite(x = dt_all, file = paste0(output_data,"2_time_from_entrance_ETL.csv"), sep = ';')
  
  
}







readAndWriteFinalDataset <- function(output_path, merge_aportaciones) {
  
  # Define the range of IDVERSIONS
  versions <- as.character(seq(ymd('2012-01-01'), ymd('2018-08-01'), by = 'months', format = "%Y-%m"))
  versions <- substr(gsub("-","",versions),1,6)
  
  # TARGET DATA
  print('Reading target data...')
  dt <- data.table()
  for (t in versions) {
    dt <- rbind(dt, fread(paste0(output_path, 'target/bajas_', t, '.csv')))
  }
  setkey(dt, IDMIEMBRO)
  
  # INDIVIDUOS
  print('Performing a left join with the Individuos table')
  dt <- performLeftJoinFromFile(base_dt = dt, 
                                filename = paste0(output_path, '1_individuos_ETL.csv'), 
                                keys = 'IDMIEMBRO')
  # AXESOR
  print('Performing a left join with the Axesor table')
  dt <- performLeftJoinFromFile(base_dt = dt, 
                                filename = paste0(output_path, '6_axesor_ETL.csv'), 
                                keys = 'IDMIEMBRO')
  
  # APORTACIONES
  if (merge_aportaciones) {
    print('Reading aportaciones data...')
    dt <- data.table()
    for (t in versions) {
      print(t)
      dt <- rbind(dt, fread(paste0(output_path, 'aportaciones/aportaciones_chunk_', t, '.csv'), sep = ';'))
    }
    setnames(dt, c('idmiembro', 'id_version'), c('IDMIEMBRO', 'IDVERSION'))
    setkey(dt, IDMIEMBRO)
    
    fwrite(dt, file = paste0(output_path, '9_aportaciones_ETL.csv'), sep = ';')
    rm('dt'); gc()
  }
  
  print('Performing a left join with the Aportaciones table')
  dt <- performLeftJoinFromFile(base_dt = dt, 
                                filename = paste0(output_path, '9_aportaciones_ETL.csv'), 
                                keys = c('IDVERSION', 'IDMIEMBRO'))
  
  
  # TIEMPO ALTA + CANALIDAD ENTRADA
  print('Performing a left join with the TiempoAlta/Canalidad table')
  dt <- performLeftJoinFromFile(base_dt = dt, 
                                filename = paste0(output_path, '2_time_from_entrance_ETL.csv'), 
                                keys = c('IDVERSION', 'IDMIEMBRO'))
  
  
  
  # INTERACCIONES
  print('Performing a left join with the Interacciones table')
  dt <- performLeftJoinFromFile(base_dt = dt, 
                                filename = paste0(output_path, '4_interacciones_ETL.csv'), 
                                keys = c('IDVERSION', 'IDMIEMBRO'))
  
  
  # Save final dataset into a file
  print('Writing final dataset...')
  fwrite(x = dt, file = paste0(output_path, 'final_dataset.csv'), sep = ';')
  
  
  print('Process finished!')
  
}












