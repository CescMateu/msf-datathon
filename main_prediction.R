rm(list = ls())

library(xgboost)
library(data.table)


labelEncoder <- function(x){
  if(!class(x) %in% c("numeric","integer") ){
    out <- as.numeric(as.factor(x))
  }else{
    out <- x
  }
  return(out)
}
#Path  where the model is located

path_model <- "models/xgb_model_argentina_winner"


#Path for the data
expl_filename <- 'processed_data/exploitation_dataset_cescSE_noAport.csv'

#Path save

path_save <-"output/"


#Load the model
bst <-  xgb.load(paste0(path_model))


dt_ex <- fread(expl_filename)
dt_ex[,names(dt_ex) := lapply(.SD, FUN = labelEncoder ), .SDcols = names(dt_ex)]

#Convert to D-Matrix
dt_2 <- copy(dt_ex)
dt_2[,`:=`(IDMIEMBRO = NULL, IDVERSION =NULL),]


#Convert into an XGDB Matrix for model execution
dt_2_m <- as.matrix(dt_2); gc()
dex <-  xgb.DMatrix(dt_2_m ,
                    missing = NA)
pred <- predict(bst, dex)

#Was the model trained for predicting 0's?
pred_table <- data.table(IDMIEMBRO = dt_ex[,IDMIEMBRO],
                         probability = pred)

# Choose the 2% with more probability
n_churn <- ceiling(nrow(pred_table) * 0.02)

IDMIEMBRO_churn <- pred_table[order(-probability)][1:n_churn, IDMIEMBRO]

pred_table[, pred_cat := 0]
pred_table[IDMIEMBRO %in% IDMIEMBRO_churn, pred_cat := 1]

# #Load the optimal cutoff
# metrics <- fread("output/out_of_sample_results/metrics.csv")
# pred_table[,pred_cat:=ifelse(probability >=metrics[,optimal_cutoff],1,0 ),]
# 
# pred_table <- pred_table[,.(IDMIEMBRO, probability, pred_cat)]
pred_table[, .N, by = pred_cat]





setnames(pred_table, "pred_cat","prediction")

fwrite(pred_table, paste0(path_save,"/prediction_201902.csv"))
