


getPreviousNextPeriod <- function(PERIODO, Next_or_Prev){
  year <- substr(PERIODO, 1, 4) %>% as.numeric
  month <- substr(PERIODO, 5, 6) %>% as.numeric
  if (Next_or_Prev == -1){
    nMonth <- ifelse(month == 1, 12, month - 1)
    nMonth <- str_pad(nMonth, 2, side = "left", pad = "0")
    nYear <- ifelse(month == 1, year - 1, year)
    nPERIODO <- paste0(nYear, nMonth)
  } else {
    nMonth <- ifelse(month == 12, 1, month + 1)
    nMonth <- str_pad(nMonth, 2, side = "left", pad = "0")
    nYear <- ifelse(month == 12, year + 1, year)
    nPERIODO <- paste0(nYear, nMonth)
  }
  return(nPERIODO)
}


# @param k: k previous or next periods to obtain
# @param PERIODO: Reference period
#
# @return: Vector of k previous or next periods (including reference period)
getKPeriods <- function(PERIODO, k){
  PERIODOS <- PERIODO
  next_or_prev <- ifelse(k > 0, 1, -1)
  if (k == 0){
    return(PERIODO)
  }
  for(index in 1:max((abs(k)), 1)){
    PERIODOS <- c(getPreviousNextPeriod(PERIODOS[1], next_or_prev), PERIODOS)
  }
  return(sort(PERIODOS))
}




allNAsToZero <-  function(DT, cols = 'all') {
  
  if (length(cols) == 1 & cols == 'all') {
    
    for (j in names(DT))
      set(DT,which(is.na(DT[[j]])),j,0)
    
  } else {
    
    for (j in cols)
      set(DT,which(is.na(DT[[j]])),j,0)
  }
}





performLeftJoinFromFile <- function(base_dt, filename, keys) {
  
  if(!file.exists(filename)) {
    
    warning(paste0('Filename does not exist, returning original dataset. (', filename, ')'))
    return(base_dt)
    
  } else {
    
    test <- fread(filename, sep = ';', nrows = 5)
    
    if (!all(keys %in% colnames(test))) {
      stop(paste0('Keys not found in colnames: ', paste0(colnames(test), collapse = ', ')))
    }
    
    # Load data
    new_dt <- fread(filename, sep = ';')
    
    # Left join
    base_dt <- merge(base_dt, new_dt, by = keys, all.x = TRUE)
    
    return(base_dt)
    }
  }


DifferenceInDays <- function(date1,date2){
  
  dif <- abs(as.numeric(date1 - date2))
  return(dif) 
  
}

SuitableDate <- function(x){
  x <- gsub(pattern="-", replacement="", x = substr(x,1,10))
  x <- as.Date(x, format="%Y%m%d")
  return(x)
}
