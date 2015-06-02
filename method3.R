Directory ="/Users/Bruce/desktop/hw5" 
setwd(Directory)
library(data.table)
library(parallel)
library(doParallel)
library(plyr)

registerDoParallel(cores=4)

############################################################################################################
############################ summary part: get all the results #############################################

####### this part calls functions from part1,part2 and part3 to get deciles and regression result

# get paths
paths_trip = list.files(,pattern = "trip_data_[0-9]")
paths_fare = list.files(,pattern = "*_fare_")
path_all = cbind(paths_trip,paths_fare)

start = proc.time()
##### get all 12 pairs of summary statistics and frequency tables
data_all = foreach(i=1:12) %dopar% get_one_pair_result(path_all[i,])

####calculate deciles
deciles_all = get_deciles(data_all)

##### calculate regression #######
regression_all = regression_result(data_all)

time = proc.time() - start
################## part1: process one pair's result  ##############################
####### 1. read in the columns we want from one pair of data and transfer these columns(total - tolls, transfer time,etc.)

ReadOneData = function(filename){ 
  fare = fread(filename[2], sep = ",", header = TRUE, select = c(7,10,11))
  colnames(fare) = c("surcharge","tolls_amount","total_amount")
  trip = fread(filename[1], sep = ",", header = TRUE, select = c(6,7))
  colnames(trip) = c("pickup_datetime","dropoff_datetime")
  amount = fare$total_amount - fare$tolls_amount
  pickup = strptime(trip$pickup_datetime, "%Y-%m-%d %H:%M:%S")
  dropoff =strptime(trip$dropoff_datetime, "%Y-%m-%d %H:%M:%S")
  triptime = as.numeric(difftime(dropoff, pickup, unit = "secs"))
  data_out = data.frame(amount = amount,time = triptime, surcharge = fare$surcharge)
  ### filter data
  data_out = subset(data_out,data_out$amount > 0 & data_out$amount < 100 & data_out$time >0)
  data_out
}

###### 2. use one pair's data from ReadOneData to calculate one frequency table
get_one_frequency = function(data) {
  
  freq.table = as.data.frame(table(data$amount))
  names(freq.table) = c("amount", "freq")
  freq.table$amount = as.numeric(levels(freq.table$amount))
  freq.table = freq.table[order(freq.table[ ,1]), ]
  freq.table
}

###### 3. use one pair's data from ReadOneData to get summary statistics for each pair ########
get_summary_statistics = function(data){
  response = data$amount
  predictor = data$time
  N = length(predictor)
  mean.response = mean(response,na.rm = TRUE)
  mean.predictor = mean(predictor,na.rm = TRUE)
  Var.response = var(response,na.rm = TRUE)
  Var.predictor = var(predictor,na.rm = TRUE)
  cov = cov(predictor,response)
  c(Obs = N, MEAN.X = mean.predictor, MEAN.Y = mean.response, VAR.X = Var.predictor,VAR.Y =Var.response, COV = cov)
    
}

######## 4. combine the previous three step together ############
get_one_pair_result = function(filename){
  data.in = ReadOneData(filename)
  data.in.freq = get_one_frequency(data.in)
  data.in.summary = get_summary_statistics(data.in)
  list(frequenct_table = data.in.freq,summary_table = data.in.summary)
}


##############################################################################################
##############################################################################################


#################### part2: calculate deciles #################
############################################################################

get_deciles = function(data_all){
  ## save all the frequency into a big list
  freq_list = lapply(1:12,function(i){
    data_all[[i]][[1]]
  })
  ## combine all the frequency table into one table(sorted)
  freq_combine = combine_freq(freq_list)
  deciles = deciles_calc(freq_combine,cut_point = 10)
  ## show deciles for all data
  deciles
  
}
####### combine all the frequency tables #########

combine_freq = function(freq_list) {
  freq.name = names(freq_list[[1]])[1]
  for (i in 1:length(freq_list)) {
    # rename the variable names before join the dataframes
    names(freq_list[[i]])[2] = as.character(i)
  }
  
  freq.all = join_all(dfs = freq_list, by = freq.name, type = "full")
  freq.all$freq = rowSums(freq.all[ , -1], na.rm = TRUE)
  freq.all = freq.all[ , c(freq.name, "freq")]
  
  freq.all = freq.all[order(freq.all[ ,1]), ]
  names(freq.all) = c("amount", "freq")
  freq.all
}

########### calculate deciles from the combined frequency table ##############

deciles_calc =  function (freq_all,cut_point = 10) {
  # The default of breaks is 10, which gives deciles.  
  cut = seq(0.1,1,length.out = cut_point)
  freq_all$cumsum = cumsum(freq_all$freq)
  ## how many observations 
  N = sum(freq_all$freq)
  ### the ith decilies' position
  position = cut*N
  ### calculate deciles
  ## prespecify a vector to store deciles
  deciles_value = vector(length = 10 , mode = "double")
  
  deciles_value = sapply(position,function(i){
    head(x = freq_all$amount[freq_all$cumsum >= i], n = 1)    
  })
  
  cbind(deciles = cut,value = deciles_value)
}

#######################################################################
#######################################################################


##################### part3: regression parts ##########################
####################################################################
### function to calculate regression result based on all 12 summary statistics vectors
regression_result = function(data_all){
  
  summary_list = lapply(1:12,function(i){
    data_all[[i]][[2]]
  })
  summary_all = get_full_statistics(summary_list)
  regression_all = get_regression(summary_all)
  regression_all
}

### function to update summary statistics based on previous one
get_full_statistics = function(summary_list){
  ## inital value of two tables' summary statistics
  summary_old_ini = summary_list[[1]]
  summary_new_ini = summary_list[[2]]
  ## initial value of combining two tables
  summary_combine_ini = update_summary_stat(summary_old_ini,summary_new_ini)
  for(i in 3:12){
  summary_new = summary_list[[i]]
  ## update summary statistics based on new set of summary statistics
  summary_combine = update_summary_stat(summary_combine_ini,summary_new)
  summary_combine_ini = summary_combine  
  }
  summary_combine
}

### for simple linear regression

get_regression = function(summary_all){
  beta1 = summary_all["COV"]/summary_all["VAR.X"]
  beta0 = summary_all["MEAN.Y"] - beta1*summary_all["MEAN.X"]
  rsquare = (summary_all["COV"])^2/(summary_all["VAR.X"]*summary_all["VAR.Y"])
  
  summary = c(beta0,beta1,rsquare)
  names(summary) = c("intercept","slope","R-square")
  summary
}


##### function to combine two summary tables and get a new summary table for next step of calculation

update_summary_stat = function(summary_old,summary_new){
  
  new_N = summary_old["Obs"] + summary_new["Obs"]
  new_mean_x = weighted.mean(x = c(summary_old["MEAN.X"], summary_new["MEAN.X"]),
                               w = c(summary_old["Obs"], summary_new["Obs"]))

  new_mean_y = weighted.mean(x = c(summary_old["MEAN.Y"], summary_new["MEAN.Y"]),
                                            w = c(summary_old["Obs"], summary_new["Obs"]))
  
 
  new_var_x = combine_var(summary_old,summary_new)[1]
  new_var_y = combine_var(summary_old,summary_new)[2]
  new_cov = combine_cov(summary_old,summary_new)
  
  summary = c(new_N,new_mean_x,new_mean_y,new_var_x,new_var_y,new_cov)
  names(summary) = c("Obs","MEAN.X","MEAN.Y","VAR.X","VAR.Y","COV")
  summary
    
}

########### sub functions to update variances and covariances ####

#### function to update covariance
combine_cov = function(summary_old,summary_new){
  
 N.total = summary_old["Obs"] + summary_new["Obs"]
  cov.total = summary_old["COV"]*((summary_old)["Obs"]-1)/(N.total-1) + 
              summary_new["COV"]*((summary_new)["Obs"]-1)/(N.total-1) +
              (summary_old["MEAN.X"] - summary_new["MEAN.X"])*(summary_old["MEAN.Y"] - summary_new["MEAN.Y"])*(summary_old["Obs"]*summary_new["Obs"]/N.total/(N.total-1))
}

#### function to update variance
combine_var = function(summary_old,summary_new){
  
  N.total = summary_old["Obs"] + summary_new["Obs"]
  
  Mean.x.total = weighted.mean(x = c(summary_old["MEAN.X"], summary_new["MEAN.X"]),
                         w = c(summary_old["Obs"], summary_new["Obs"]))
  
  Mean.y.total = weighted.mean(x = c(summary_old["MEAN.Y"], summary_new["MEAN.Y"]),
                         w = c(summary_old["Obs"], summary_new["Obs"]))
  delta_x = summary_old["MEAN.X"] - summary_new["MEAN.X"]
  delta_y = summary_old["MEAN.Y"] - summary_new["MEAN.Y"]
  var.x.total = summary_old["VAR.X"]*(summary_old["Obs"]-1)/(N.total-1) + 
              summary_new["VAR.X"]*(summary_new["Obs"]-1)/(N.total-1) +
              delta_x^2*summary_old["Obs"]*summary_new["Obs"]/N.total/(N.total-1)
  
  var.y.total = summary_old["VAR.Y"]*(summary_old["Obs"]-1)/(N.total-1) + 
    summary_new["VAR.Y"]*(summary_new["Obs"]-1)/(N.total-1) +
    delta_y^2*summary_old["Obs"]*summary_new["Obs"]/N.total/(N.total-1)
  
  c(var.x.total,var.y.total)
  
}

#####################################################################################
#####################################################################################





