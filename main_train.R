rm(list = ls())
gc()

########################################################################################################
###################################  INPUTS  ###########################################################
########################################################################################################

# These are the inputs that have to be modified according to the user
####
run_id <- gsub(as.character(lubridate::now()), pattern = '-', replacement = '')
run_id <- gsub(run_id, pattern = ' ', replacement = '_')
run_id <- gsub(run_id, pattern = ':', replacement = '')

#Path were the data is located & file_name
file_train_name <-"processed_data/final_dataset.csv" 
path_date <- ""


# Path were the tools_xgb.R are located.
path_scripts <- "scripts/"


# Path where the training results will be saved
path_save_trainings <-  "output/"


# We set these two hyperparameters free for tuning
# Learning_rate
LR <- 0.05
# Number of rounds
NROUNDS <- 500


# Wheter to take a random sample of the training dataset and the proportion to keep
take_a_sample <- TRUE
prop_sample <- 0.1 #this would  only keep 10% of the full dataset regardless of the target

########################################################################################################
########################################################################################################


# From this point on evrything should go smooth: the steps are defined as follows

# 0 Set working enviornment: folders, libraries, tools, etc.

# 1. Load the data, split intro train and validation. The train will be subsequently
#    split into train-validation (aka development), obtaining thus a train-validation-test scenario.

# 2. Train and XGBoost using training set while validating over validation set (using early stopping). 
#    Performance metrics are saved in the out-of-sample folders.  

# 3. Once the model has been trained, evaluate and predict over the validation set (out of time)
#    performance metrics are computed and saved in the out-of-time folder.




########################################################################################################
################################ 0. Set Working Enviorment #############################################
########################################################################################################

# Source required user-defined tools
source(paste0(path_scripts,"/tools_xgb.R"))

# Efficiently load-or-install packages
pckgs_to_load <- c("data.table","xgboost","grid","xlsx","pROC","dplyr","ggplot2")
loadPackages(pckgs_to_load)

# Create the directories to save results
if(!file.exists(path_save_trainings)) dir.create(path_save_trainings)

# Create a directory for oos and oot results
path_save_oos <- paste0(path_save_trainings,"out_of_sample_results/")
path_save_oot <- paste0(path_save_trainings,"out_of_time_results/")
if(!file.exists(path_save_oos)) dir.create(path_save_oos)
if(!file.exists(path_save_oot)) dir.create(path_save_oot)

########################################################################################################


########################################################################################################
################################ 1. Load and split data ################################################
########################################################################################################

#Load full dataset

#Uncomment!!!!
dt <- fread(file_train_name, sep = ';')

# Split the full dataset into train-test following a version criteria
all_versions <- unique(dt[,IDVERSION])
versions_train <-  all_versions[all_versions <= 201802]
version_execution_test <- 201808
dt_train <- dt[IDVERSION %in% versions_train ,]


#Take a sample if necessary in order to speed-up training
if(take_a_sample){
  n <- nrow(dt_train)
  n_keep <- sample(1:n, size= floor(prop_sample*n), replace=FALSE)
  dt_train <- dt_train[n_keep,,]
}

#Define the out-of-time dataset for testing
dt_test <- dt[IDVERSION == version_execution_test ]
target_test <- dt_test[,.(IDMIEMBRO, IND_BAIXA)]
dt_test[,IND_BAIXA := NULL,]


########################################################################################################
############################## 2. Model Training & oos evaluation ######################################
########################################################################################################

#Convert all to numeric to avoid one-hot-encoding
#One-hot-encoding would slow down training, numerical convertion has been prooven right in
# most cases.

dt_train[,names(dt_train) := lapply(.SD, FUN = labelEncoder ), .SDcols = names(dt_train)]

#######################################################################
#split intro train and validation (aka development) and generate d-Matrices for feeding to the XGBoost
target <- "IND_BAIXA"
s <-split_train_test(data = dt_train , perc.train = 0.8) 
dt_train_target <- s$train
dt_dev_target <- s$test

keep <- colnames(dt_train)
keep2 <- setdiff(keep, c(target, "IDMIEMBRO", 'IDVERSION'))

dev_target <- as.matrix(dt_dev_target[, get(target)])
dt_dev_target_2 <- dt_dev_target[,keep2, with = FALSE]
dt_dev_target_matrix <- as.matrix(dt_dev_target_2); gc()
ddev <-  xgb.DMatrix(dt_dev_target_matrix, 
                     label = dev_target, 
                     missing = NA)

train_target <- as.matrix(dt_train_target[, get(target)])

dt_train_target_2 <- dt_train_target[,keep2, with = FALSE]
dt_train_target_matrix <- as.matrix(dt_train_target_2); gc()
dtrain <-  xgb.DMatrix(dt_train_target_matrix, 
                       label = train_target, 
                       missing = NA)

#We have the data ready

#set XGBoost hyperparameters
params <- list(objective = "binary:logistic", # binary classification
               subsample =  0.8,
               colsample_bytree = 0.8,
               max_depth = 5,                # maximum depth of tree
               nthread = 12,                 # number of threads to be used
               eta = LR,                    # Learning rate
               eval_metric = "auc",          # Evaluation metric to define early stopping
               scale_pos_weight = NA)  
params["scale_pos_weight"] = dt_train[IND_BAIXA == 0, .N]/dt_train[, sum(IND_BAIXA)]


#Clean stuff to free memory and define the watchlist for early_stopping
rm(dt_train_target_matrix)
rm(dt_dev_target_matrix)
watchlist = list(train = dtrain, eval = ddev)


#Model training. This might take a while.
bst <- xgb.train(params = params,
                 data = dtrain,
                 watchlist = watchlist,
                 nrounds = NROUNDS, 
                 maximize = TRUE,
                 verbose = 1,
                 early_stopping_rounds = 15)

#For fuck's sake, save the model please
xgb.save(bst, fname = paste0("models/xgb_model_", run_id))



#####
#Plot performance metrics and other stuff

## Variable importance
#############################################################################
writeLines("     ---> Computing variable importances....")
dt_imp <- xgb.importance(feature_names = colnames(dt_train_target_2), model = bst)
dt_imp[,cumgain := cumsum(Gain)]
dt_imp[,ind_varimp := ifelse(cumgain < 0.99,1,0)]
imp_vars <- data.table(Feature =  dt_imp[ind_varimp == 1,]$Feature)
fwrite(imp_vars, file = paste0(path_save_oos, "/imp_vars.csv"))



ggplot(data = dt_imp) +
  geom_bar(mapping = aes(x = Feature, y = Gain), stat = 'Identity')
# plot.varimp  <- xgb_plot_varimp(var_importance = dt_imp,
#                                 nvars = 15,
#                                 save = FALSE,
#                                 save_path = path_save_oos,
#                                 fill="brown2")
# 
# var_imp_plot <- plot.varimp$plot
# var_imp_plot <- var_imp_plot + the + theme(axis.text.x = element_text(size=30, color="black"),
#                                            axis.text.y = element_text(size=30, color="black"))
# png(paste0(path_save_oos, "/varimp.png"), width = 800, height = 800)
# print(var_imp_plot)
# dev.off()

pred_train <- predict(bst, dtrain)
dt_train_target[,probability := pred_train,]

####################################################################
########## Propension vs top variables  ############################
####################################################################

plot_top_varimp_pred(dataset = dt_train_target, 
                     varimp_table = dt_imp,
                     field_importance = "Gain",
                     silently = TRUE,
                     field_variable ="Feature",
                     save_plots = TRUE,
                     distrByVar = "IDMIEMBRO",
                     path.save = path_save_oos ,
                     n_top_vars = 10)


####################################################################
## Roc curve
####################################################################

pred_train <- predict(bst, dtrain)
label_train <- dt_train_target[,get(target)]
roc_train <- roc(label_train, pred_train, algorithm = 2)
auc_train <- paste0("AUCtrain = " , round(as.numeric(auc(roc_train )),3) )

pred_test <- predict(bst, ddev)
label_test <- dt_dev_target[,get(target)]
roc_test <- roc(label_test, pred_test, algorithm = 2)
auc_test <- auc(roc_test)
auc_test_s <- paste0("AUCtest = " , round(as.numeric(auc(roc_test )),3) )

#Figure out the optimal cut-off, maximizin both specificity and sensitivity
best <- coords(roc_test, "best")

png(paste0(path_save_oos, "/roc.png"), width = 800, height = 800)
plot(roc_test, main="", col = "brown2",
     cex.axis=2) 
lines(roc_train, main="", col="black") 
text(x= 0.2, y=0.05,auc_test_s,cex=2)
text(x= 0.2, y=0.1,auc_train,cex=2)
dev.off()


#############################################################################
## Precision, uplift & recall
#############################################################################

result_test <- data.table("probability" = pred_test, "target" = label_test)
result_test_id_cliente <- data.table("IDMIEMBRO" = dt_dev_target[,IDMIEMBRO],
                                     "probability" = pred_test, 
                                     "target" = label_test)
fwrite(result_test_id_cliente, paste0(path_save_oos,"/results_over_test.csv"))
g <- Gain.Table(data = result_test)
auc_gain_test <-data.table("AUC" = g$AUC)
gains <- g$GainTable

fwrite(gains, file= paste0(path_save_oos, "/gains_table.csv"), sep = ';')
png(paste0(path_save_oos, "/recall.png"), width = 800, height = 800)
print(g$Plot.Recall)
dev.off()

png(paste0(path_save_oos, "/precision.png"), width = 800, height = 800)
print(g$Plot.Precision)
dev.off()

png(paste0(path_save_oos, "/uplift.png"), width = 800, height = 800)
print(g$Plot.Uplift)
dev.off()


PR_plot <- ggplot( data =gains ,aes(x = acc.precision, y=acc.recall)) + 
  geom_line(col='brown2', size =1.5) +
  xlab("Precision") + ylab("Recall") + the

png(paste0(path_save_oos, "/PR.png"), width = 800, height = 800)
print(PR_plot)
dev.off()


###################################
#Other stuff
###################################

#Accuracy
result_test[,pred_cat:=ifelse(probability >=best["threshold"],1,0 ),]
result_test[,correct_prediction := ifelse(target ==pred_cat,1,0 ),]
accur <- result_test[,mean(correct_prediction)]

cutofs <- data.table("optimal_cutoff" =  best["threshold"],
                     "specificity" = best["specificity"],
                     "sensitivity" =  best["sensitivity"],
                     "auc_over_test" = auc_test,
                     "accuracy" = accur)

fwrite(cutofs, paste0(path_save_oos, "/metrics.csv"))


up10 <- gains[percentile == 0.1, acc.uplift]
x <- data.table(auc_roc_test = auc_test, 
                auc_gain_test = auc_gain_test,
                uplift10 = up10)
fwrite(x, paste0(path_save_oos,"/auc_roc_test.csv"))
#rm(dtrain)
#rm(ddev)
#rm(dataset_train)


########################################################################################################
############################## 3. Out-of-time evaluation ###############################################
########################################################################################################


#Now that the model has been trained, evaluated and tuned over the out-of-time (ie next months) test set
if(!exists("bst")) bst <-  xgb.load(paste0(path_save_oos,"/xgb_model_", run_id))

dt_test[,names(dt_test) := lapply(.SD, FUN = labelEncoder ), .SDcols = names(dt_test)]


#Convert dt_test into an xgdb_matrix and predict

dt_2 <- copy(dt_test)
dt_2[,`:=`(IDMIEMBRO = NULL, IDVERSION =NULL),]

#Convert into an XGDB Matrix for model execution
dt_2_m <- as.matrix(dt_2); gc()
dex <-  xgb.DMatrix(dt_2_m ,
                    missing = NA)
pred <- predict(bst, dex)

#Was the model trained for predicting 0's?
pred_table <- data.table(IDMIEMBRO = dt_test[,IDMIEMBRO],
                         probability = pred)

#calculate metrics over test set
pred_table <- merge(pred_table, target_test, by="IDMIEMBRO")
setnames(pred_table, "IND_BAIXA","target")
g <- Gain.Table(data = pred_table)
auc_gain_test <-data.table("AUC" = g$AUC)
gains <- g$GainTable

# write.xlsx( gains,row.names = FALSE, col.names = TRUE,
#             file= paste0(path_save_oot, "/gains_table.xlsx"),
#             sheetName = "gainTable" )

fwrite(gains, file = paste0(path_save_oot, 'gains_table.csv'), sep = ';')
png(paste0(path_save_oot, "/recall.png"), width = 800, height = 800)
print(g$Plot.Recall)
dev.off()

png(paste0(path_save_oot, "/precision.png"), width = 800, height = 800)
print(g$Plot.Precision)
dev.off()

png(paste0(path_save_oot, "/uplift.png"), width = 800, height = 800)
print(g$Plot.Uplift)
dev.off()


PR_plot <- ggplot( data =gains ,aes(x = acc.precision, y=acc.recall)) + 
  geom_line(col="brown2", size =1.5) +
  xlab("Precision") + ylab("Recall") + the

png(paste0(PR_plot, "/PR.png"), width = 800, height = 800)
print(PR_plot)
dev.off()


##Accuracy
metrics <- fread(paste0(path_save_oos, "/metrics.csv"))
pred_table[,pred_cat:=ifelse(probability >=best["threshold"],1,0 ),]

roc_test_oot <- roc(response =pred_table[,target], 
                    predictor = pred_table[,probability],
                    algorithm = 2)
auc_test_oot <- auc(roc_test_oot)
auc_test_s_oot <- paste0("AUCtest = " , round(as.numeric(auc(roc_test_oot )),3) )

png(paste0(path_save_oot, "/roc.png"), width = 800, height = 800)
plot(roc_test_oot, main="", col = "brown2",
     cex.axis=2, xlim=c(1,0), ylim=c(0,1)) 
text(x= 0.2, y=0.05,auc_test_s_oot,cex=2)
dev.off()


