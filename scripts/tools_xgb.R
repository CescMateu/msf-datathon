loadPackages <- function(packages) {
  for (x in packages) {
    if (!require(x, character.only = TRUE)) {
      install.packages(x, dep = TRUE)
      if (!require(x, character.only = TRUE)) {
        stop("Package not found")
      }
    }
  }
}


split_train_test <- function (data, perc.train) {
  if (perc.train > 1 | perc.train <= 0) 
    stop("perc.train must be between 0 and 1.")
  if (!is.data.table(data)) 
    data <- as.data.table(data)
  n <- nrow(data)
  n.train <- floor(perc.train * n)
  rows.train <- sample(1:n, size = n.train, replace = FALSE)
  data.train <- data[rows.train, ]
  data.test <- data[-rows.train]
  out <- list(train = data.train, test = data.test)
  return(out)
}


labelEncoder <- function(x){
  if(!class(x) %in% c("numeric","integer") ){
    out <- as.numeric(as.factor(x))
  }else{
    out <- x
  }
  return(out)
}


plot_top_varimp_pred <- function (dataset, prediction.dataset = NULL, prediction_var = "probability", 
                                  distrByVar = "id_cliente", varimp_table, field_variable = "Variable", 
                                  field_importance = "PercentInfluence", n_top_vars = 5, save_plots = FALSE, 
                                  silently = FALSE, path.save = NULL, groups = 10) {
  
  pckgs <- c("data.table", "ggplot2", "dplyr", "grid", "reshape2")
  for (p in pckgs) {
    require(p, character.only = TRUE)
  }
  n_top_vars <- min(n_top_vars, nrow(varimp_table))
  top.vars <- varimp_table[1:n_top_vars, eval(parse(text = field_variable))]
  keep <- c(top.vars, distrByVar, prediction_var)
  keep <- intersect(keep, colnames(dataset))
  dataset <- dataset[, keep, with = FALSE]
  if (!prediction_var %in% colnames(dataset)) {
    if (is.null(prediction.dataset)) {
      stop("Prediction dataset not found nor prediction column in data.")
    }
    else {
      if (!(distrByVar %in% colnames(dataset)) | !(distrByVar %in% 
                                                   colnames(prediction.dataset))) {
        stop(paste0("Columns ", distrByVar, " must be in both dataset and prediction"))
      }
      else {
        dataset <- merge(dataset, prediction.dataset[, 
                                                     c(prediction_var, distrByVar), with = FALSE], 
                         by = distrByVar, all.x = TRUE)
      }
    }
  }
  types <- sapply(as.data.frame(dataset), class)
  metadata <- data.table(var = names(types), class = types)
  metadata[var %like% "^ind_" | var %like% "^id_" | var %like% 
             "^sw_" | var %like% "^numcto_", `:=`(class, "factor")]
  metadata[var %like% "^sio_" | var %like% "^numcto_", `:=`(class, 
                                                            "numeric")]
  setnames(dataset, prediction_var, "probability")
  dataset[, `:=`(percentile, as.factor(ntile(-probability, 
                                             groups))), ]
  colors <- c("brown2", "black", 'red', 
              'grey', 'orange', 'blue', 
              'green', "darkgoldenrod1", 
              "darkorchid1")
  if (groups == 10) {
    brks <- 1:10
  }
  else if (groups == 50) {
    brks <- c(1, 10, 20, 30, 40, 50)
  }
  else if (groups == 100) {
    brks <- c(1, round(seq(0, 100, length = 10), -1)[-1])
  }
  else {
    brks <- round(seq(0, groups, length = 10), 0)
  }
  list_plots <- list()
  for (i in seq_along(top.vars)) {
    x <- top.vars[i]
    i <- i - 1
    message(paste0("--> Plot for variable ", x))
    type <- as.character((metadata[var == x, ]$class))
    text1 <- paste0(field_variable, "=='", x, "'")
    rel.inf <- varimp_table[eval(parse(text = text1)), eval(parse(text = field_importance))]
    if (rel.inf <= 1) {
      rel.inf <- 100 * rel.inf
    }
    tit <- paste0("Relative influence = ", round(rel.inf, 
                                                 2), "%")
    i <- i + 1
    if (type %in% c("numeric", "integer")) {
      s <- dataset[, .(mean = mean(get(x), na.rm = TRUE)), 
                   by = percentile]
      plot <- ggplot() + geom_line(data = s, aes(x = as.numeric(percentile), 
                                                 y = mean, group = 1), color = "brown2", 
                                   size = 1.5) + ggtitle(paste0("Mean of ", x, 
                                                                " by percentile of probability  \n (", tit, 
                                                                ")")) + xlab("percentile of probability") + 
        scale_x_continuous(breaks = brks) + ylab(x) + 
        theme(axis.text.x = element_text(size = 12, 
                                         color = "black"), axis.text.y = element_text(size = 12, 
                                                                                      color = "black"), legend.text = element_text(size = 12, 
                                                                                                                                   color = "black"), axis.title = element_text(size = 12, 
                                                                                                                                                                               face = "bold"), title = element_text(size = 10, 
                                                                                                                                                                                                                    face = "bold"), axis.line = element_line(colour = "black"), 
              legend.key.size = unit(1, "cm"), legend.key.height = unit(1, 
                                                                        "cm"), panel.background = element_blank())
    }
    else if (type %in% c("factor", "character")) {
      if (length(levels(as.factor(dataset[[x]]))) > 10) {
        l <- length(levels(as.factor(dataset[[x]])))
        warning(paste0("Variable ", x, " is as a factor with ", 
                       l, " levels. The maximum number of levels supported is 10 and thus the plot will be skipped"))
        next
      }
      else {
        t <- table(percentile = dataset[["percentile"]], 
                   x = dataset[[x]])
        t <- prop.table(t, margin = 1)
        t2 <- as.data.table(melt(t))
        t2[, `:=`(percentile, as.factor(percentile)), 
           ]
        t2[, `:=`(value, 100 * value), ]
        t2[, `:=`(x, as.factor(x)), ]
        plot <- ggplot() + geom_bar(aes(y = value, x = as.numeric(percentile), 
                                        fill = x), data = t2, stat = "identity") + 
          ylab("Percentage(%)") + scale_y_continuous(breaks = seq(0, 
                                                                  100, by = 25), limits = c(0, 100)) + scale_fill_manual(values = colors) + 
          guides(fill = guide_legend(title = x)) + scale_x_continuous(breaks = brks) + 
          ggtitle(paste0("Distr. of ", x, " by percentile of prob.  \n (", 
                         tit, ")")) + theme(axis.text.x = element_text(size = 12, 
                                                                       color = "black"), axis.text.y = element_text(size = 12, 
                                                                                                                    color = "black"), legend.text = element_text(size = 12, 
                                                                                                                                                                 color = "black"), axis.title = element_text(size = 12, 
                                                                                                                                                                                                             face = "bold"), title = element_text(size = 10, 
                                                                                                                                                                                                                                                  face = "bold"), axis.line = element_line(colour = "black"), 
                                            legend.key.size = unit(1, "cm"), legend.key.height = unit(1, 
                                                                                                      "cm"), panel.background = element_blank())
      }
    }
    list_plots[[i]] <- plot
    if (save_plots) {
      if (is.null(path.save)) 
        path.save <- getwd()
      fname <- paste0(path.save, "/varimp_", i, ".png")
      ggsave(plot, filename = fname, width = 200, height = 100, 
             units = "mm")
    }
  }
  if (silently == FALSE) {
    return(list_plots)
  }
}

labelEncoder <- function(x){
  if(!class(x) %in% c("numeric","integer") ){
    out <- as.numeric(as.factor(x))
  }else{
    out <- x
  }
  return(out)
}


xgb_plot_varimp <- function (var_importance, nvars = 30, save = F, save_path = "~", 
                             name.graph = "var.importance.png", width.png = 12, height.png = 10, 
                             range_vars = NULL, binwidth = 0.05, fill = rgb(0/255, 150/255,200/255, 0.5),
                             color = rgb(0, 0, 0, 0.8), horizontal = TRUE, 
                             reverseOrder = FALSE, title = "Model", ylab = "Percent. Influence", 
                             xlab = NULL, angle = 65, hjust = 1) {
  
  var_importance <- var_importance[var_importance$Gain !=  0, ]
  var_importance$Feature <- factor(var_importance$Feature, 
                                   levels = var_importance$Feature, ordered = T)
  if (is.null(range_vars)) {
    var_importance <- var_importance[1:min(nvars, nrow(var_importance)), 
                                     ]
  }
  else {
    var_importance <- var_importance[range_vars, ]
  }
  if ((horizontal && !reverseOrder) || (!horizontal && reverseOrder)) {
    var_importance$Feature <- factor(var_importance$Feature, 
                                     levels = rev(var_importance$Feature), ordered = TRUE)
  }
  p <- ggplot()
  if (is.null(color)) {
    p <- p + geom_bar(data = var_importance, aes(x = Feature, 
                                                 y = Gain), binwidth = binwidth, fill = fill, color = Feature, 
                      stat = "identity")
  }
  else {
    p <- p + geom_bar(data = var_importance, aes(x = Feature, 
                                                 y = Gain), binwidth = binwidth, fill = fill, color = color, 
                      stat = "identity")
  }
  p <- p + ggtitle(title) + ylab(ylab) + xlab(xlab) + theme(axis.text.x = element_text(angle = angle, 
                                                                                       hjust = hjust))
  if (horizontal) 
    p <- p + coord_flip()
  if (save == T) {
    ggsave(filename = name.graph, path = save_path, width = width.png, 
           height = height.png)
  }
  return(print(p))
}
###########################################################################################################################
###########################################################################################################################

#Theme for ggplots
the <- theme(axis.text.x = element_text(size=15, color="black"),
             axis.text.y = element_text(size=15, color="black"),
             legend.text = element_text(size=15, color="black"),
             axis.title = element_text(size=14,face="bold"),
             axis.line = element_line(colour = "black"),
             legend.key.size = unit(1, "cm"),
             legend.key.height	=unit(1,"cm"),
             panel.background = element_blank())


Gain.Table <- function(data, groups =100, return.plots = TRUE){
  
  if( !"probability" %in% colnames(data) ) stop("Probability column not found in data.")
  if( !"target" %in% colnames(data) ) stop("Target column not found in data.")
  
  if(!is.data.table(data)) data <- as.data.table(data)
  total.N <- nrow(data)
  total.target <-sum(data[,target]==1)# nrow(data[target==1])
  
  data[, percentile:=ntile(-probability,groups)/100]
  setorder(data,percentile)
  
  prop.target <- total.target/total.N
  gain <- data[,.(N=.N, N.target=sum(target)), by=percentile]
  gain[, cum.N:=cumsum(N),]
  gain[,cum.target :=cumsum(N.target),]
  gain[, precision:=round(N.target/N,4), ]
  gain[,recall:= round(N.target/total.target,4),]
  gain[,acc.precision:=round(cum.target/cum.N,4)]
  gain[,acc.recall:= round(cum.target/total.target,4),]
  gain[,uplift:=round(precision/prop.target,2),]
  gain[,acc.uplift:=round(acc.precision/prop.target,2),]
  
  if(max(gain$precision) < 0.01){
    gain[,precision := 100*precision,]
    gain[,precision := 100*precision,]
    gain[,recall := 100*recall,]
    gain[,acc.precision := 100*acc.precision,]
    gain[,acc.recall := 100*acc.recall,]
  }
  #   
  
  x = gain$percentile
  y = gain$acc.recall
  
  id <- order(x)
  AUC <- (sum(diff(x[id]) * zoo::rollmean(y[id], 2)))
  
  
  ####################################################
  #plots
  if(return.plots){
    text_high <- textGrob("+", gp=gpar(fontsize=19, fontface="bold"))
    text_low <- textGrob("-", gp=gpar(fontsize=20, fontface="bold"))
    max.precision <- min(100,100*(max(gain$acc.precision) +  max(gain$acc.precision)/6))
    text.position.prec <- min(max.precision, 100*prop.target + 100*prop.target*2)
    max.uplift <- (max(gain$acc.uplift) +  max(gain$acc.uplift)/6)
    
    p.prec <- ggplot(data=gain, aes(x=100*percentile, y=100*acc.precision)) +
      geom_bar(stat="identity", width=0.8, fill="brown2") + 
      xlab("Percentile")+
      ylab("Accumulated precision (%)")+
      geom_hline(yintercept=100*prop.target, size=1.5, linetype="dashed")+
      annotate("text", label = paste0("Base target rate \n", round(100*prop.target,2), "%"), x = 85, 
               y = text.position.prec, color = "black", hjust=0.5,size=8)+
      ylim(min = 0 , max = max.precision)+
      annotation_custom(text_high,xmin=0,xmax=3,ymin=-2,ymax=-2)+
      annotation_custom(text_low,xmin=100,xmax=100,ymin=-2,ymax=-2)+
      the
    
    
    p.uplift <- ggplot(data=gain, aes(x=100*percentile, y = acc.uplift)) +
      geom_bar(stat="identity", width=0.8, fill="brown2") + 
      xlab("Percentile")+
      ylab("Accumulated uplfit")+
      ylim(min = 0 , max = max.uplift)+
      geom_hline(yintercept=1, size=1.5, linetype="dashed")+
      annotation_custom(text_high,xmin=0,xmax=3,ymin=-2,ymax=-2)+
      annotation_custom(text_low,xmin=100,xmax=100,ymin=-2,ymax=-2)+
      the 
    
    
    p.recall <- ggplot(data=gain, aes(x=100*percentile, y=100*acc.recall)) +
      geom_bar(stat="identity", width=0.8, fill="brown2") + 
      xlab("Percentile")+
      ylab("Accumualted Recall (%)")+
      annotation_custom(text_high,xmin=0,xmax=3,ymin=-2,ymax=-2)+
      annotate("text", label = paste0("AUC=", round(100*AUC,2)), x = 10, 
               y =90 , color = "black", hjust=0.5,size=9)+
      annotation_custom(text_low,xmin=100,xmax=100,ymin=-2,ymax=-2)+
      the 
    
    return(list("GainTable" = gain, "AUC" =AUC,
                "Plot.Precision" = p.prec,
                "Plot.Uplift" = p.uplift,
                "Plot.Recall" =p.recall))
  }else{
    
    return(list("GainTable" = gain, "AUC" =AUC))
    
  } 
}

