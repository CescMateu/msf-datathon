
# Individuos Table


processIndividuosTable <- function(data_path) {
  
  print('Processing table: 1.Individuos')
  
  # Read data
  indi <- fread(file = paste0(data_path, '1.INDIVIDUOS_SOCIOS_O_BAJAS_train.txt'), sep =";", header = T)
  
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
  
  return(indi)
  
}


