rm(list = ls())


library(data.table)
library(lubridate)

source('tools.R')


setwd('~/Desktop/Datathon/')

#################################
# ------ DATA PREPROCESSING --- #
#################################

# READ DATA
dt_bajas <- fread('data/2.ALTASYBAJAS_train.txt')

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
versions <- as.character(seq(ymd('1987-01-01'), ymd('2019-02-01'), by = 'months', format = "%Y-%m"))
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
  print(t)
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
    
    # Write the data in separated data files
    fwrite(x = dt_aux, file = paste0('output/target/bajas_', t, '.csv'), sep = ';')
    }
  
}

# Plot evolució clients actius 
q = 200
plot(x = versions2[1:(length(versions2) - 6)], y = count, type = 'l')
plot(x = versions2[q:(length(versions2) - 6)], 
     y = count_baixes_4_6[q:length(count_baixes_4_6)], type = 'l')


baixes = data.table(idversion = versions2[1:(length(versions2) - 6)], count_baixes = count_baixes_4_6)

baixes[, month := substr(idversion, start = 5, stop = 6)]
baixes[, year := substr(idversion, start = 1, stop = 4)]
baixes[, date := lubridate::ymd(paste0(as.character(year), '-',
                                       as.character(month), '-01'))]

plot(x = baixes$date, y = baixes$count_baixes, 'l', 
     main = 'Baixes registrades entre 4 i 6 mesos vista')






