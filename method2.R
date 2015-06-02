#### Method2: MySQL + R
start = proc.time()
dir = "/Users/Bruce/desktop/hw5"
setwd(dir)
library(DBI)
library(RMySQL)
con =dbConnect(MySQL(), user='root', password='1234', dbname='NYCTaxi', host="localhost")

## create trip_table
table_trip_name = sapply(1:12,function(i){
  paste0("CREATE TABLE trip",i,"( pickup_datetime DATETIME,",
  "dropoff_datetime DATETIME)ENGINE = MYISAM;")  
  })

## create fare_table
table_fare_name = sapply(1:12,function(i){
  paste0("CREATE TABLE fare",i,"( surcharge DOUBLE,",
        "tolls_amount DOUBLE,total_amount DOUBLE)ENGINE = MYISAM;")  
})
## create all the trip tables
sapply(1:12,function(i)dbSendQuery(con,table_trip_name[i]))       
## create all the fare tables
sapply(1:12,function(i)dbSendQuery(con,table_fare_name[i]))

## load trip data
#start = proc.time()
load_trip_table = sapply(1:12,function(i){
  paste0("LOAD DATA LOCAL INFILE",
         " '/Users/Bruce/desktop/hw5/trip_data_",i,".csv'", 
         " INTO TABLE trip",i,
         " FIELDS TERMINATED BY ','",      
         " LINES TERMINATED BY '\n'",
         " IGNORE 1 ROWS",
         " (@dummy,@dummy,@dummy,@dummy,@dummy,pickup_datetime,",
         " dropoff_datetime,@dummy,@dumy,@dummy,@dummy,@dummy,@dummy,@dummy)")  
})
## load fare data
load_fare_table = sapply(1:12,function(i){
  paste0("LOAD DATA LOCAL INFILE",
         " '/Users/Bruce/desktop/hw5/trip_fare_",i,".csv'", 
         " INTO TABLE fare",i,
         " FIELDS TERMINATED BY ','",      
         " LINES TERMINATED BY '\n'",
         " IGNORE 1 ROWS",
         " (@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,",
         "surcharge,@dummy,@dummy,tolls_amount,total_amount)")  
})
#load in all trip tables
sapply(1:12,function(i)dbSendQuery(con,load_trip_table[i]))
#load in all fare tables
sapply(1:12,function(i)dbSendQuery(con,load_fare_table[i]))

##create columns to store difference between total_amount and tolls_amount
create_total_tolls = sapply(1:12,function(i){
  paste0("ALTER TABLE fare",i," ADD diff DOUBLE")  
})

sapply(1:12,function(i) dbSendQuery(con,create_total_tolls[i]))

## add new columns of total_amount - tolls_amount
add_total_tolls = sapply(1:12,function(i){
  paste0("UPDATE fare",i," set diff = total_amount - tolls_amount")  
})

sapply(1:12,function(i) dbSendQuery(con,add_total_tolls[i]))

##create columns of time(dropoff_time - pickup_time)
create_transfer_time = sapply(1:12,function(i){  
  paste0("ALTER TABLE trip",i," ADD time_sec DOUBLE") 
})
sapply(1:12,function(i) dbSendQuery(con,create_transfer_time[i]))

## add new columns of time_sec using dropoff.time - pickup.time
add_transfer_time = sapply(1:12,function(i){
  paste0("UPDATE trip",i," set time_sec = TIMESTAMPDIFF(SECOND,pickup_datetime,dropoff_datetime)")  
})

sapply(1:12,function(i) dbSendQuery(con,add_transfer_time[i]))

### create table to store total_amount - tolls_amount from all files
dbSendQuery(con,"CREATE TABLE amount (total_tolls DOUBLE,id INT NOT NULL AUTO_INCREMENT PRIMARY KEY)")
## store all the total less tolls into a new table
add_amount = sapply(1:12,function(i){
  paste0("INSERT INTO amount(total_tolls) SELECT diff FROM fare",i)
})

for(i in 1:12){
  dbSendQuery(con,add_amount[i])
}

### create table to store time in seconds from all files
dbSendQuery(con,"CREATE TABLE time (time_sec_all DOUBLE,id INT NOT NULL AUTO_INCREMENT PRIMARY KEY)")

## store time in sec in table
add_time = sapply(1:12,function(i){
  paste0("INSERT INTO time(time_sec_all) SELECT time_sec FROM trip",i)
  
})

for(i in 1:12){
  dbSendQuery(con,add_time[i])
}
### create table to store surcharge from all files
dbSendQuery(con,"CREATE TABLE sur_charge (surcharge_all DOUBLE,id INT NOT NULL AUTO_INCREMENT PRIMARY KEY)")

## store surcharge 
add_surcharge = sapply(1:12,function(i){
  paste0("INSERT INTO sur_charge(surcharge_all) SELECT surcharge FROM fare",i)
})

for(i in 1:12){
  dbSendQuery(con,add_surcharge[i])
}

######## calculate quantiles for total less tolls #############
deci = seq(0.1,1,length.out =10)
len_data = dbGetQuery(con,"select count(*) from amount")
len_data = len_data[1,1]
position = round(deci*len_data-1)
## create table to store sorted amount table
dbSendQuery(con,"CREATE TABLE sort_amount as SELECT total_tolls FROM amount ORDER BY total_tolls")

quantile_position = sapply(1:10,function(i){
  paste0("select total_tolls from sort_amount limit ",position[i],",1")
})
quantile = vector(length = 10,mode = "numeric")
for(i in 1:10){
  quantile[i] = dbGetQuery(con, quantile_position[i])
}
## inner join of THREE table -- FOR REGRESSION
dbSendQuery(con,"CREATE TABLE amount_time AS SELECT total_tolls,time.id, time_sec_all from time INNER JOIN amount WHERE amount.id = time.id")
dbSendQuery(con,"CREATE TABLE amount_time_surcharge AS SELECT total_tolls,time_sec_all,surcharge_all from amount_time INNER JOIN sur_charge WHERE amount_time.id = sur_charge.id")
# drop total_tolls larger than $100 or smaller than $0 and time_in_secs < 0
dbSendQuery(con,"CREATE TABLE filter_amount_time AS SELECT * FROM amount_time_surcharge WHERE total_tolls < 100 && total_tolls > 0 && time_sec_all>0")

## CALCULATE regression result
dbSendQuery(con,"SELECT
@sumXY := SUM(total_tolls*time_sec_all),
@sumXX := SUM(time_sec_all*time_sec_all),
@sumYY := SUM(total_tolls*total_tolls),
@n := count(*),
@meanX := AVG(time_sec_all),
@sumX :=SUM(time_sec_all),
@meanY := AVG(total_tolls),
@sumY :=SUM(total_tolls)
FROM filter_amount_time")


## calculate correaltion coefficient
dbClearResult(dbListResults(con)[[1]])
dbGetQuery(con,"SELECT  (@n*@sumXY - @sumX*@sumY)  / SQRT((@n*@sumXX - @sumX*@sumX) * (@n*@sumYY - @sumY*@sumY))")
## calculate slope
dbGetQuery(con,"SELECT
@b := (@n*@sumXY - @sumX*@sumY) / (@n*@sumXX - @sumX*@sumX) AS slope");
## calculate intercept
dbGetQuery(con,"SELECT 
@a := (@meanY - @b*@meanX) AS intercept");

time = proc.time() - start



















