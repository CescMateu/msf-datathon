a <- fread('processed_data/exploitation_dataset.csv')

duplic_members <- a[,.(count=.N),by=IDMIEMBRO][count>1][, IDMIEMBRO]
non_duplic_members <- a[,.(count=.N),by=IDMIEMBRO][count==1][, IDMIEMBRO]


dupl_data <- a[IDMIEMBRO %in% duplic_members]
non_duplc_data <- a[IDMIEMBRO %in% non_duplic_members]

dupl_data_solved <- dupl_data[dupl_data[, .I[which.min(NUM_DIAS_DESDE_ALTA)], by = IDMIEMBRO]$V1]


all_expl <- rbind(dupl_data_solved, non_duplc_data)

fwrite(all_expl, file = 'processed_data/exploitation_dataset.csv')
