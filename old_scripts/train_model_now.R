rm(list = ls())
gc()

dt <- fread(file = 'output/dataset.csv', sep = ';')

# Train-Test split
## 75% of the sample size
smp_size <- floor(0.75 * nrow(dt))

## set the seed to make your partition reproducible
set.seed(123)
train_ind <- sample(seq_len(nrow(dt)), size = smp_size)

train <- dt[train_ind, ]
test <- dt[-train_ind, ]




# Train RF
library(randomForest)

y_name = 'IND_BAIXA'
X_names = names(dt)[!names(dt) %in% y_name]

y <- train[, y_name, with = FALSE]
train <- train[, X_names, with = FALSE]
randomForest(x = train, y = y)


!names(dt) %in% target



