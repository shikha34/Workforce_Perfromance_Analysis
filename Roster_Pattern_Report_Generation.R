

Data_Cleaning_Pipeline2<-function(x){
#########################################PATTERN ANALYSIS###########################################
library("lubridate")
library(stringr)
library("sqldf")

setwd("C:/D Drive Data/RosterMatrix/Jan2020")


# long2<-unique(long1,by=c("EmployeeCode","Date"))
# long1<-subset(long1,!(Month=="Apr" & Year=="2018"))

##the code uses long1 data from Step1 ##############################################################
names(long1)
pattern<-long1[,c(1,6,7,8,9,15)]
pattern$AttendanceStatus<-ifelse(pattern$AttendanceStatus=="WEEKOFF","WEEKOFF","PRESENT")

###################Enter the pattern period start and end date#####################################

start <- mdy("11-01-2019")
end <- mdy("01-31-2020")

# pattern$Day<-day(pattern$Date)
pattern$EmployeeCode<-as.character(pattern$EmployeeCode)
pattern<-pattern[order(pattern$EmployeeCode,pattern$Date),]
no_emp<-NROW(unique(pattern$EmployeeCode))
day_<-as.numeric(difftime(end,start,units = "days"))+1
Exp_time<-rep(start:end,times=no_emp)
Day<-rep(1:38,length.out=NROW(Exp_time))
EmployeeCode<-rep(unique(pattern$EmployeeCode),each=day_)
# NROW(Day)
# NROW(Exp_time)
# NROW(EmployeeCode)
stand<-data.frame(EmployeeCode,Exp_time,Day)
names(stand)[2]<-"Date"
stand$Date<-as.Date(stand$Date,origin="1970-01-01")

#merging with pattern data
patternn<-merge(stand,pattern,by=c("EmployeeCode","Date"),all=TRUE)
rm(pattern)
names(patternn)
#patternn<-patternn[-c(2)]
patternn$Date<-as.Date(patternn$Date,origin="1970-01-01")
patternn$Month<-format(patternn$Date,"%b")
patternn<-patternn[order(patternn$EmployeeCode,patternn$Date),]
patternn$Date<-as.numeric(patternn$Date)

######################change the year for future pattern analysis#########################################

patternn$Jun<-ifelse(patternn$Month=="Jun" |( as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                                                as.numeric(as.Date("2019-06-01",origin="1970-01-01")-days(6)) : as.numeric(as.Date("2019-06-01",origin="1970-01-01"))) |
                       (as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                          as.numeric(as.Date("2019-07-01",origin="1970-01-01")):as.numeric(as.Date("2019-07-01",origin="1970-01-01")+days(5))),1,0)


patternn$Jul<-ifelse(patternn$Month=="Jul" |( as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                                                as.numeric(as.Date("2019-07-01",origin="1970-01-01")-days(6)) : as.numeric(as.Date("2019-07-01",origin="1970-01-01"))) |
                       (as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                          as.numeric(as.Date("2019-08-01",origin="1970-01-01")):as.numeric(as.Date("2019-08-01",origin="1970-01-01")+days(5))),1,0)


patternn$Aug<-ifelse(patternn$Month=="Aug" |( as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                                                as.numeric(as.Date("2019-08-01",origin="1970-01-01")-days(6)) : as.numeric(as.Date("2019-08-01",origin="1970-01-01"))) |
                       (as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                          as.numeric(as.Date("2019-09-01",origin="1970-01-01")):as.numeric(as.Date("2019-09-01",origin="1970-01-01")+days(5))),1,0)


patternn$Sep<-ifelse(patternn$Month=="Sep" |( as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                                                as.numeric(as.Date("2019-09-01",origin="1970-01-01")-days(6)) : as.numeric(as.Date("2019-09-01",origin="1970-01-01"))) |
                       (as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                          as.numeric(as.Date("2019-10-01",origin="1970-01-01")):as.numeric(as.Date("2019-10-01",origin="1970-01-01")+days(5))),1,0)

patternn$Oct<-ifelse(patternn$Month=="Oct" |( as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                                                as.numeric(as.Date("2019-10-01",origin="1970-01-01")-days(6)) : as.numeric(as.Date("2019-10-01",origin="1970-01-01"))) |
                       (as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                          as.numeric(as.Date("2019-12-01",origin="1970-01-01")):as.numeric(as.Date("2019-12-01",origin="1970-01-01")+days(5))),1,0)


patternn$Nov<-ifelse(patternn$Month=="Nov" |( as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                                                as.numeric(as.Date("2019-11-01",origin="1970-01-01")-days(6)) : as.numeric(as.Date("2019-11-01",origin="1970-01-01"))) |
                       (as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                          as.numeric(as.Date("2019-12-01",origin="1970-01-01")):as.numeric(as.Date("2019-12-01",origin="1970-01-01")+days(5))),1,0)

patternn$Dec<-ifelse(patternn$Month=="Dec" |( as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                                                as.numeric(as.Date("2019-12-01",origin="1970-01-01")-days(6)) : as.numeric(as.Date("2019-12-01",origin="1970-01-01"))) |
                       (as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                          as.numeric(as.Date("2019-01-01",origin="1970-01-01")):as.numeric(as.Date("2019-01-01",origin="1970-01-01")+days(5))),1,0)

patternn$Jan<-ifelse(patternn$Month=="Jan" |( as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                                                as.numeric(as.Date("2020-01-01",origin="1970-01-01")-days(6)) : as.numeric(as.Date("2020-01-01",origin="1970-01-01"))) |
                       (as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                          as.numeric(as.Date("2020-02-01",origin="1970-01-01")):as.numeric(as.Date("2020-02-01",origin="1970-01-01")+days(5))),1,0)

patternn$Feb<-ifelse(patternn$Month=="Feb" |( as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                                                as.numeric(as.Date("2019-02-01",origin="1970-01-01")-days(6)) : as.numeric(as.Date("2019-02-01",origin="1970-01-01"))) |
                       (as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                          as.numeric(as.Date("2019-03-01",origin="1970-01-01")):as.numeric(as.Date("2019-03-01",origin="1970-01-01")+days(5))),1,0)

patternn$Mar<-ifelse(patternn$Month=="Mar" |( as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                                                as.numeric(as.Date("2019-03-01",origin="1970-01-01")-days(6)) : as.numeric(as.Date("2019-03-01",origin="1970-01-01"))) |
                       (as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                          as.numeric(as.Date("2019-04-01",origin="1970-01-01")):as.numeric(as.Date("2019-04-01",origin="1970-01-01")+days(5))),1,0)


patternn$Apr<-ifelse(patternn$Month=="Apr" |( as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                                                as.numeric(as.Date("2019-04-01",origin="1970-01-01")-days(6)) : as.numeric(as.Date("2019-04-01",origin="1970-01-01"))) |
                       (as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                          as.numeric(as.Date("2019-05-01",origin="1970-01-01")):as.numeric(as.Date("2019-05-01",origin="1970-01-01")+days(5))),1,0)


patternn$May<-ifelse(patternn$Month=="May" |( as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                                                as.numeric(as.Date("2019-05-01",origin="1970-01-01")-days(6)) : as.numeric(as.Date("2019-05-01",origin="1970-01-01"))) |
                       (as.numeric(as.Date(patternn$Date,origin="1970-01-01")) %in%
                          as.numeric(as.Date("2019-06-01",origin="1970-01-01")):as.numeric(as.Date("2019-06-01",origin="1970-01-01")+days(5))),1,0)

names(patternn)
patternn$Date<-as.Date(patternn$Date,origin="1970-01-01",format="%Y-%m-%d")

patternn[,9:NCOL(patternn)]<-lapply(patternn[,9:NCOL(patternn)],function(x){
  x<-ifelse(x==1,patternn$AttendanceStatus,0)
})

# Days<-list()
# for( i in 1:38)
# {
#   for(j in 1:NROW(patternn))
#   Days<-ifelse(patternn$Day[j]==i,1,0)
# 
# }

Apr<-subset(patternn,Apr!=0)
May<-subset(patternn,May!=0)
Jun<-subset(patternn,Jun!=0)
Jul<-subset(patternn,Jul!=0)
Aug<-subset(patternn,Aug!=0)
Sep<-subset(patternn,Sep!=0)
Oct<-subset(patternn,Oct!=0)

Nov<-subset(patternn,Nov!=0)
Dec<-subset(patternn,Dec!=0)
Jan<-subset(patternn,Jan!=0)
Feb<-subset(patternn,Feb!=0)
Mar<-subset(patternn,Mar!=0)

Apr<-Apr[c("EmployeeCode","Date" ,"WorkingCity","SubDept","EmployeeLevelText","Apr")]
May<-May[c("EmployeeCode","Date" ,"WorkingCity","SubDept","EmployeeLevelText","May")]
Jun<-Jun[c("EmployeeCode","Date" ,"WorkingCity","SubDept","EmployeeLevelText","Jun")]
Jul<-Jul[c("EmployeeCode","Date" ,"WorkingCity","SubDept","EmployeeLevelText","Jul")]
Aug<-Aug[c("EmployeeCode","Date" ,"WorkingCity","SubDept","EmployeeLevelText","Aug")]
Sep<-Sep[c("EmployeeCode","Date" ,"WorkingCity","SubDept","EmployeeLevelText","Sep")]
Oct<-Oct[c("EmployeeCode","Date" ,"WorkingCity","SubDept","EmployeeLevelText","Oct")]
Nov<-Nov[c("EmployeeCode","Date" ,"WorkingCity","SubDept","EmployeeLevelText","Nov")]
Dec<-Dec[c("EmployeeCode","Date" ,"WorkingCity","SubDept","EmployeeLevelText","Dec")]
Jan<-Jan[c("EmployeeCode","Date" ,"WorkingCity","SubDept","EmployeeLevelText","Jan")]
Feb<-Feb[c("EmployeeCode","Date" ,"WorkingCity","SubDept","EmployeeLevelText","Feb")]
Mar<-Mar[c("EmployeeCode","Date" ,"WorkingCity","SubDept","EmployeeLevelText","Mar")]

D_Apr<-reshape(Apr, direction = "wide",  
               idvar = c("EmployeeCode" ,"WorkingCity","SubDept","EmployeeLevelText"),
               timevar = c("Date"))
D_Apr$Month<-"Apr"
names(D_Apr)[5:(NCOL(D_Apr)-1)]<-c("Day_1","Day_2","Day_3","Day_4","Day_5","Day_6","Day_7","Day_8","Day_9","Day_10"
                                   ,"Day_11","Day_12","Day_13","Day_14","Day_15","Day_16","Day_17","Day_18","Day_19","Day_20"
                                   ,"Day_21","Day_22","Day_23","Day_24","Day_25","Day_26","Day_27","Day_28",
                                   "Day_29","Day_30","Day_31","Day_32","Day_33","Day_34","Day_35","Day_36",
                                   "Day_37",
                                   "Day_38","Day_39","Day_40","Day_41","Day_42","Day_43","Day_44","Day_45","Day_46","Day_47")
D_May<-reshape(May, direction = "wide",  
               idvar = c("EmployeeCode" ,"WorkingCity","SubDept","EmployeeLevelText"),
               timevar = c("Date"))
D_May$Month<-"May"
names(D_May)[5:(NCOL(D_May)-1)]<-c("Day_1","Day_2","Day_3","Day_4","Day_5","Day_6","Day_7","Day_8","Day_9","Day_10"
                                   ,"Day_11","Day_12","Day_13","Day_14","Day_15","Day_16","Day_17","Day_18","Day_19","Day_20"
                                   ,"Day_21","Day_22","Day_23","Day_24","Day_25","Day_26","Day_27","Day_28",
                                   "Day_29","Day_30","Day_31","Day_32","Day_33","Day_34","Day_35","Day_36","Day_37",
                                   "Day_38","Day_39","Day_40","Day_41","Day_42","Day_43","Day_44","Day_45","Day_46","Day_47")


D_Jun<-reshape(Jun, direction = "wide",  
               idvar = c("EmployeeCode" ,"WorkingCity","SubDept","EmployeeLevelText"),
               timevar = c("Date"))
D_Jun$Month<-"Jun"
names(D_Jun)[5:(NCOL(D_Jun)-1)]<-c("Day_1","Day_2","Day_3","Day_4","Day_5","Day_6","Day_7","Day_8","Day_9","Day_10"
                                   ,"Day_11","Day_12","Day_13","Day_14","Day_15","Day_16","Day_17","Day_18","Day_19","Day_20"
                                   ,"Day_21","Day_22","Day_23","Day_24","Day_25","Day_26","Day_27","Day_28",
                                   "Day_29","Day_30","Day_31","Day_32","Day_33","Day_34","Day_35","Day_36","Day_37",
                                   "Day_38","Day_39","Day_40","Day_41","Day_42","Day_43","Day_44","Day_45","Day_46","Day_47")


D_Jul<-reshape(Jul, direction = "wide",  
               idvar = c("EmployeeCode" ,"WorkingCity","SubDept","EmployeeLevelText"),
               timevar = c("Date"))
D_Jul$Month<-"Jul"
names(D_Jul)[5:(NCOL(D_Jul)-1)]<-c("Day_1","Day_2","Day_3","Day_4","Day_5","Day_6","Day_7","Day_8","Day_9","Day_10"
                                   ,"Day_11","Day_12","Day_13","Day_14","Day_15","Day_16","Day_17","Day_18","Day_19","Day_20"
                                   ,"Day_21","Day_22","Day_23","Day_24","Day_25","Day_26","Day_27","Day_28",
                                   "Day_29","Day_30","Day_31","Day_32","Day_33","Day_34","Day_35","Day_36","Day_37",
                                   "Day_38","Day_39","Day_40","Day_41","Day_42","Day_43","Day_44","Day_45","Day_46","Day_47","Day_48")


D_Aug<-reshape(Aug, direction = "wide",  
               idvar = c("EmployeeCode" ,"WorkingCity","SubDept","EmployeeLevelText"),
               timevar = c("Date"))
D_Aug$Month<-"Aug"
names(D_Aug)[5:(NCOL(D_Aug)-1)]<-c("Day_1","Day_2","Day_3","Day_4","Day_5","Day_6","Day_7","Day_8","Day_9","Day_10"
                                   ,"Day_11","Day_12","Day_13","Day_14","Day_15","Day_16","Day_17","Day_18","Day_19","Day_20"
                                   ,"Day_21","Day_22","Day_23","Day_24","Day_25","Day_26","Day_27","Day_28",
                                   "Day_29","Day_30","Day_31","Day_32","Day_33","Day_34","Day_35","Day_36","Day_37",
                                   "Day_38","Day_39","Day_40","Day_41","Day_42","Day_43","Day_44","Day_45","Day_46","Day_47","Day_48")


D_Sep<-reshape(Sep, direction = "wide",  
               idvar = c("EmployeeCode" ,"WorkingCity","SubDept","EmployeeLevelText"),
               timevar = c("Date"))
D_Sep$Month<-"Sep"
names(D_Sep)[5:(NCOL(D_Sep)-1)]<-c("Day_1","Day_2","Day_3","Day_4","Day_5","Day_6","Day_7","Day_8","Day_9","Day_10"
                                   ,"Day_11","Day_12","Day_13","Day_14","Day_15","Day_16","Day_17","Day_18","Day_19","Day_20"
                                   ,"Day_21","Day_22","Day_23","Day_24","Day_25","Day_26","Day_27","Day_28",
                                   "Day_29","Day_30","Day_31","Day_32","Day_33","Day_34","Day_35","Day_36","Day_37",
                                   "Day_38","Day_39","Day_40","Day_41","Day_42","Day_43","Day_44","Day_45",
                                   "Day_46","Day_47")


D_Oct<-reshape(Oct, direction = "wide",  
               idvar = c("EmployeeCode" ,"WorkingCity","SubDept","EmployeeLevelText"),
               timevar = c("Date"))
D_Oct$Month<-"Oct"
names(D_Oct)[5:(NCOL(D_Oct)-1)]<-c("Day_1","Day_2","Day_3","Day_4","Day_5","Day_6","Day_7","Day_8","Day_9","Day_10"
                                   ,"Day_11","Day_12","Day_13","Day_14","Day_15","Day_16","Day_17","Day_18","Day_19","Day_20"
                                   ,"Day_21","Day_22","Day_23","Day_24","Day_25","Day_26","Day_27","Day_28",
                                   "Day_29","Day_30","Day_31","Day_32","Day_33","Day_34","Day_35","Day_36","Day_37",
                                   "Day_38","Day_39","Day_40","Day_41","Day_42","Day_43","Day_44","Day_45","Day_46","Day_47","Day_48")

D_Nov<-reshape(Nov, direction = "wide",  
               idvar = c("EmployeeCode" ,"WorkingCity","SubDept","EmployeeLevelText"),
               timevar = c("Date"))
D_Nov$Month<-"Nov"
names(D_Nov)[5:(NCOL(D_Nov)-1)]<-c("Day_1","Day_2","Day_3","Day_4","Day_5","Day_6","Day_7","Day_8","Day_9","Day_10"
                                   ,"Day_11","Day_12","Day_13","Day_14","Day_15","Day_16","Day_17","Day_18","Day_19","Day_20"
                                   ,"Day_21","Day_22","Day_23","Day_24","Day_25","Day_26","Day_27","Day_28",
                                   "Day_29","Day_30","Day_31","Day_32","Day_33","Day_34","Day_35","Day_36","Day_37",
                                   "Day_38","Day_39","Day_40","Day_41","Day_42","Day_43","Day_44","Day_45",
                                   "Day_46","Day_47")


D_Dec<-reshape(Dec, direction = "wide",  
               idvar = c("EmployeeCode" ,"WorkingCity","SubDept","EmployeeLevelText"),
               timevar = c("Date"))
D_Dec$Month<-"Dec"

names(D_Dec)[5:(NCOL(D_Dec)-1)]<-c("Day_1","Day_2","Day_3","Day_4","Day_5","Day_6","Day_7","Day_8","Day_9","Day_10"
                                   ,"Day_11","Day_12","Day_13","Day_14","Day_15","Day_16","Day_17","Day_18","Day_19","Day_20"
                                   ,"Day_21","Day_22","Day_23","Day_24","Day_25","Day_26","Day_27","Day_28",
                                   "Day_29","Day_30","Day_31","Day_32","Day_33","Day_34","Day_35","Day_36","Day_37",
                                   "Day_38","Day_39","Day_40","Day_41","Day_42","Day_43","Day_44","Day_45","Day_46","Day_47","Day_48"
)


D_Jan<-reshape(Jan, direction = "wide",  
               idvar = c("EmployeeCode" ,"WorkingCity","SubDept","EmployeeLevelText"),
               timevar = c("Date"))
D_Jan$Month<-"Jan"
names(D_Jan)[5:(NCOL(D_Jan)-1)]<-c("Day_1","Day_2","Day_3","Day_4","Day_5","Day_6","Day_7","Day_8","Day_9","Day_10"
                                   ,"Day_11","Day_12","Day_13","Day_14","Day_15","Day_16","Day_17","Day_18","Day_19","Day_20"
                                   ,"Day_21","Day_22","Day_23","Day_24","Day_25","Day_26","Day_27","Day_28",
                                   "Day_29","Day_30","Day_31","Day_32","Day_33","Day_34","Day_35","Day_36","Day_37",
                                   "Day_38","Day_39","Day_40","Day_41","Day_42","Day_43","Day_44","Day_45","Day_46","Day_47","Day_48"
)

D_Feb<-reshape(Feb, direction = "wide",  
               idvar = c("EmployeeCode" ,"WorkingCity","SubDept","EmployeeLevelText"),
               timevar = c("Date"))
D_Feb$Month<-"Feb"
names(D_Feb)[5:(NCOL(D_Feb)-1)]<-c("Day_1","Day_2","Day_3","Day_4","Day_5","Day_6","Day_7","Day_8","Day_9","Day_10"
                                   ,"Day_11","Day_12","Day_13","Day_14","Day_15","Day_16","Day_17","Day_18","Day_19","Day_20"
                                   ,"Day_21","Day_22","Day_23","Day_24","Day_25","Day_26","Day_27","Day_28",
                                   "Day_29","Day_30","Day_31","Day_32","Day_33","Day_34","Day_35","Day_36","Day_37",
                                   "Day_38","Day_39","Day_40","Day_41","Day_42","Day_43","Day_44","Day_45","Day_46","Day_47","Day_48"
)
D_Mar<-reshape(Mar, direction = "wide",  
               idvar = c("EmployeeCode" ,"WorkingCity","SubDept","EmployeeLevelText"),
               timevar = c("Date"))
D_Mar$Month<-"Mar"
names(D_Mar)[5:(NCOL(D_Mar)-1)]<-c("Day_1","Day_2","Day_3","Day_4","Day_5","Day_6","Day_7","Day_8","Day_9","Day_10"
                                   ,"Day_11","Day_12","Day_13","Day_14","Day_15","Day_16","Day_17","Day_18","Day_19","Day_20"
                                   ,"Day_21","Day_22","Day_23","Day_24","Day_25","Day_26","Day_27","Day_28",
                                   "Day_29","Day_30","Day_31","Day_32","Day_33","Day_34","Day_35","Day_36","Day_37",
                                   "Day_38","Day_39","Day_40","Day_41","Day_42","Day_43","Day_44","Day_45","Day_46","Day_47","Day_48"
)


library(plyr)
library(dplyr)
library(data.table)

raw_data<-rbindlist(list(D_Jun,D_Jul,D_Aug,D_Sep,
                         D_Oct,D_Nov,D_Dec,D_Jan,D_Feb,D_Mar,D_Apr, D_May), fill = TRUE)

#raw_data<-rbindlist(list(D_Nov,D_Dec,D_Jan), fill = TRUE)

raw_data<-data.frame(raw_data)
names(raw_data)
raw_data<-raw_data[,c(1:4,47,5:46,48:NCOL(raw_data))]

#metros
metros_pattern<-subset(raw_data,WorkingCity=="DEL" | WorkingCity=="CCU" |
                         WorkingCity=="BLR" | WorkingCity=="HYD" | WorkingCity=="BOM" | WorkingCity=="MAA"| WorkingCity=="PNQ")

nonmetros_pattern<-subset(raw_data,WorkingCity!="DEL" & WorkingCity!="CCU"&
                            WorkingCity!="BLR" & WorkingCity!="HYD" & WorkingCity!="BOM" & WorkingCity!="MAA" & WorkingCity!="PNQ")

#3 days present 1 weekoff 2 presnet 1 weekoff= metros

names(metros_pattern)

metros_pattern$Workpattern<-do.call(paste0,metros_pattern[c(6:NCOL(metros_pattern))])
metros_pattern$count_p<-str_count(metros_pattern$Workpattern,"PRESENTPRESENTPRESENTWEEKOFFPRESENTPRESENTWEEKOFF")

# 6 present 1 weekoff= non metros
nonmetros_pattern$Workpattern<-do.call(paste0,nonmetros_pattern[c(6:NCOL(nonmetros_pattern))])
nonmetros_pattern$count_p<-str_count(nonmetros_pattern$Workpattern,"PRESENTPRESENTPRESENTPRESENTPRESENTPRESENTWEEKOFF")

metros_pattern<-subset(metros_pattern,select = -c(Workpattern))
nonmetros_pattern<-subset(nonmetros_pattern,select = -c(Workpattern))
pattern_c<-rbind(metros_pattern,nonmetros_pattern)

######################Employee Wise Pattern Raw Data###################################################

write.csv(pattern_c,"Patternraw_Data.csv")

######################################################################################################

#removing designation and days columns to count frequency per base
pattern_c2<-pattern_c[c("EmployeeCode","WorkingCity","SubDept", "Month","count_p" )]

pattern_c2$ctr<-1

df<-sqldf("Select Month,WorkingCity,  SubDept, count_p, sum(ctr) as Sum, count(EmployeeCode) as
          emp_cnt from pattern_c2 group by
          Month,WorkingCity,  SubDept,count_p")

###################Base wise pattern Data##############################################################
write.csv(df,"C:/D Drive Data/RosterMatrix/DEL/new_pattern.csv")

#######################################################################################################

#########Metro, Non Metro 1 Non Metro 2 Bifurcation####################################################

pattern_c1<-reshape(pattern_c2[-6], direction = "wide",  
                    idvar = c("EmployeeCode" ,"WorkingCity","SubDept"),
                    timevar = c("Month"))
names(pattern_c1)<-gsub("^count_p.","",names(pattern_c1))
pattern_c1[,c(4:NCOL(pattern_c1))]<-sapply(pattern_c1[,c(4:NCOL(pattern_c1))], function(x){x<-ifelse(is.na(x),0,x)})


##metro
metro_ptn<-subset(pattern_c1,WorkingCity=="BLR"|WorkingCity=="BOM"|WorkingCity=="CCU"|
                    WorkingCity=="DEL"|WorkingCity=="HYD"|WorkingCity=="MAA")

##non metro i

nonmetro1_ptn<-subset(pattern_c1,WorkingCity=="BBI"|WorkingCity=="GOI"|WorkingCity=="JAI"|
                        WorkingCity=="CJB"|WorkingCity=="NAG"|WorkingCity=="AMD"|
                        WorkingCity=="LKO"|WorkingCity=="PNQ"|WorkingCity=="TRV"|
                        WorkingCity=="PAT"|WorkingCity=="IDR")

##non metro ii

nonmetro2_ptn<-subset(pattern_c1,WorkingCity!="BBI"&WorkingCity!="GOI"&WorkingCity!="JAI"&
                        WorkingCity!="CJB"&WorkingCity!="NAG"&WorkingCity!="AMD"&
                        WorkingCity!="LKO"&WorkingCity!="PNQ"&WorkingCity!="TRV"&
                        WorkingCity!="PAT"&WorkingCity!="IDR"&WorkingCity!="BLR"&WorkingCity!="BOM"&WorkingCity!="CCU"&
                        WorkingCity!="DEL"&WorkingCity!="HYD"&WorkingCity!="MAA")



write.csv(metro_ptn,"Patternraw_metro.csv")
write.csv(nonmetro1_ptn,"Patternraw_nonmetro1.csv")
write.csv(nonmetro2_ptn,"Patternraw_nonmetro2.csv")
}

Data_Cleaning_Pipeline2()
