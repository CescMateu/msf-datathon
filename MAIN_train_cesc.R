
# Initialisation
rm(list = ls())
gc()

library(data.table)
library(xgboost)

# Read data
dt <- fread('processed_data/final_dataset.csv')

# Specify the types of variables (numeric, factor...)
target <- 'IND_BAIXA'
id_vars <- c('IDMIEMBRO', 'IDVERSION')
numeric_vars <- c('AGE', 'DAY_NACIM', grep(pattern = 'NUM_INTERACCIONES_', 
                              x = colnames(dt), 
                              value = T))
factor_vars <- setdiff(c(target, colnames(dt)),  c(numeric_vars, id_vars))
dt <- dt[, lapply(.SD, as.factor), .SDcols = factor_vars]
dt <- dt[, -id_vars, with = FALSE]

# Split data in train and test
n <- nrow(dt)
ntrain <- floor(n * 0.8)
ntest <- n - ntrain

train_idx <- sample(x = 1:n, size = ntrain)

X_train <- dt[train_idx, -target, with = FALSE]
Y_train <- dt[train_idx, target, with = FALSE]
X_test <- dt[-train_idx, -target, with = FALSE]
Y_test <- dt[-train_idx, target, with = FALSE]


# Create the xgb native matrices
dtrain <- xgb.DMatrix(data = data.matrix(X_train), label = data.matrix(Y_train$IND_BAIXA))
dtest <- xgb.DMatrix(data = data.matrix(X_test), label = data.matrix(Y_test$IND_BAIXA))
watchlist <- list(train=dtrain, test=dtest)


# Train our model!
bst <- xgb.train(data=dtrain, max.depth=2, eta=1, nthread = 2, nrounds=4, 
                 watchlist=watchlist, eval.metric = "error", 
                 eval.metric = "logloss", objective = "binary:logistic")



label = getinfo(dtest, "label")
pred <- predict(bst, dtest)
err <- as.numeric(sum(as.integer(pred > 0.5) != label))/length(label)
print(paste("test-error=", err))


p1 <- hist(pred[label == 1])
p2 <- hist(pred[label == 0])
plot( p1, col=rgb(0,0,1,1/4), xlim=c(0,0.6))  # first histogram
plot( p2, col=rgb(1,0,0,1/4), xlim=c(0,0.6), add=T)  # second

