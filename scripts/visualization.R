
library(tidyr)
library(ggplot2)

mtcars <- fread('Documents/sabadell/msf-datathon/processed_data/final_dataset_alt2.csv')
mtcars2 <- fread('Documents/sabadell/msf-datathon/processed_data/exploitation_dataset_alt2.csv')
# or `library(tidyverse)`

mtcars %>% gather() %>% head()

ggplot(gather(mtcars), aes(value)) + 
  geom_histogram(bins = 10) + 
  facet_wrap(~key, scales = 'free_x')