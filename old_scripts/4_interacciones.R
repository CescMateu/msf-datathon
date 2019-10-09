
# Interactions Table

require(data.table)

processInteractionsTable <- function(data_path) {
  
  print('Processing table: 4.Interacciones')
  # Read data
  interacciones <- fread(file = paste0(data_path, '4.INTERACCIONESCOMMITMENT_train.txt'), sep =";", header = T)
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
  
  
  return(N_INTER_MES)

}
