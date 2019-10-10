rm(list = ls())
gc()

a <- fread('processed_data/MSF_SometablesW.csv')
setnames(a, 'Descripci\xf3n', 'Descripcion')
setnames(a, 'GRUPO SEGMENTACI\xd3N', 'GrupoSegmentacion')

a[, Descripcion := NULL]
a[, FECHA_INTERACCION := NULL]
a[, GrupoSegmentacion := NULL]
a[, V1 := NULL]
a[, MONTH_str := NULL]
a[, FECHA_PROC := NULL]

cols_factor <- c('IDMEDIO', 'IDCAMPANYA', 'CODIGO', 'IDCANAL', 'NUEVA_APORTACION', 'CAMBIOCUOTA',
                 'IDTIPOCOMUNICACION', 'GRUPO_MEDIO', 'GRUPO_CAMPANYA', 'ID_REGISTRO_AUMENTO',
                 'ID_REGISTRO_DECREMENTO')

a[, (cols_factor):= lapply(.SD, as.factor), .SDcols=cols_factor]
a[, (cols_factor):= lapply(.SD, as.numeric), .SDcols=cols_factor]


for (c in colnames(a)) {
  na_prop <- a[, sum(is.na(.SD))/nrow(a), .SDcols = c]  
  print(paste0(c, ':', na_prop))
}


a <- fread('processed_data/mailings.csv')
setnames(a, colnames(a), c('IDMIEMBRO', 'V1_VARIABLE_MAILING_SERGI', 'IDVERSION', 'IND_IMPACTE_POSTAL', 
                           'IND_IMPACTE_EMAIL', 'OFERTA1', 'OFERTA2', 'OFERTA3'))
a <- fwrite(x = a, file = 'processed_data/mailings.csv', sep = ';')

for (c in colnames(a)) {
  na_prop <- a[, sum(is.na(.SD))/nrow(a), .SDcols = c]  
  print(paste0(c, ':', na_prop))
}



a <- fread('processed_data/final_dataset.csv')

