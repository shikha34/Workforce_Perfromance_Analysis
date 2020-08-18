####PLANNED AND UNPLANNED LEAVES###################
###################################################
Data_Cleaning_Pipeline4<-function(x)
{
  setwd("C:/D Drive Data/Planned_Shift/plannedleaves")
  myDir <- "C:/D Drive Data/Planned_Shift/plannedleaves"
  
  library(readxl)
  library(dummies)
  library(stringr)
  library("ParamHelpers")
  library(fastDummies)
  library("lubridate")
  library("mlr")
  library("stringr")
  library("dplyr")
  library(data.table) 
  
  ##the leave data keeps on updating daily as leaves which are not approved gets approved on daily basis and gets rejected also.
  
  data<-list()
  # d1<-read_excel("d1_6feb_18.xlsx")
  #d1<-read_excel("d1_26June.xlsx")
  
  ###
  d1<-read_excel("d1_Apr19Dec20.xlsx")
  table(d1$RosterNonRosterType)
  table(d1$EmployeeLevelText)
  
  d1<-subset(d1,RosterNonRosterType=="Roster Employees")
  str(d1$AppliedDate)
  
  # d1<-subset(d1,EmployeeLevelText=="Assistant Manager"| EmployeeLevelText=="Senior Manager"|
  #              EmployeeLevelText=="Manager")
  
  
  d1<-subset(d1,select = c(EmployeeCode,Gender,DOJ,EmployeeLevelText,
                           WorkingCity,SubDept,LeaveType,Status,AppliedDate,FromDate,ToDate))
  d1$AppliedDate<-as.Date(d1$AppliedDate,format = "%d-%b-%Y %H:%M:%S")
  d1$FromDate<-as.Date(d1$FromDate,format = "%d-%b-%Y")
  d1$ToDate<-as.Date(d1$ToDate,format = "%d-%b-%Y")
  
  cleaning<-function(x)
  {
    x=sapply(x,toupper)
    x=gsub("$","",x,fixed=TRUE)
    x=gsub("=","",x,fixed=TRUE)
    x=gsub("+","",x,fixed=TRUE)
    x=gsub("-","",x,fixed=TRUE)
    x=gsub("&","",x,fixed=TRUE)
    x=gsub("[[:punct:]]","",x)
    x=gsub("\\","",x,fixed=TRUE)
    x=gsub("~","",x,fixed=TRUE)
    x=gsub("^","",x,fixed=TRUE)
    x=gsub("\\s+"," ",x)
    x=gsub("  ","",x,fixed=TRUE)
    x=gsub(" ","",x,fixed=TRUE)
    x=gsub("_","",x,fixed=TRUE)
    
    
  }
  
  d1[c("EmployeeLevelText","WorkingCity","SubDept","LeaveType","Status")]<-
    
    sapply(d1[c("EmployeeLevelText","WorkingCity","SubDept","LeaveType","Status")],cleaning)
  
  pleave_data<-d1
  pleave_data1<-subset(pleave_data,Status=="APPROVED")
  # table(pleave_data$WorkingCity)
  # pleave_data$FromDate<-as.Date(pleave_data$FromDate,format = "%d-%b-%Y")
  # pleave_data$ToDate<-as.Date(pleave_data$ToDate,format = "%d-%b-%Y")
  
  # pleave_data1<-subset(pleave_data,as.Date(FromDate,"%Y-%m-%d")<as.Date("2019-02-01","%Y-%m-%d"))
  
  # check<-subset(pleave_data,pleave_data$FromDate>as.Date("2017-10-01",format="%Y-%m-%d"))
  
  ######################MANUAL STEP of putting in the dates#############################################
  #FromDate : Start date of leave period
  #EndDate : End date of the Leave period
  #ensure that the from date is not greater than the to date
  
  pleave_data1$FromDate<-ifelse(pleave_data1$FromDate<=as.Date("2019-04-01",format="%Y-%m-%d"),
                                as.Date("2019-04-01",format="%Y-%m-%d"),pleave_data1$FromDate)
  
  pleave_data1$FromDate<-ifelse(pleave_data1$FromDate>=as.Date("2019-12-31",format="%Y-%m-%d"),
                                as.Date("2019-12-31",format="%Y-%m-%d"),pleave_data1$FromDate)
  
  
  pleave_data1$FromDate<-as.Date(pleave_data1$FromDate,format="%Y-%m-%d",origin = "1970-01-01")
  
  pleave_data1$ToDate<-ifelse(pleave_data1$ToDate>=as.Date("2019-12-31",format="%Y-%m-%d"),as.Date("2019-12-31",format="%Y-%m-%d"),pleave_data1$ToDate)
  
  pleave_data1$ToDate<-as.Date(pleave_data1$ToDate,format="%Y-%m-%d",origin = "1970-01-01")
  
  
  
  ##PLEASE CHECK FOR FROM DATE IS NOT GREATER THAN TO DATE ELSE BELOW CODES WILL NOT RUN
  
  
  
  # library(data.table)
  # pleave_data1<-data.table(pleave_data1)
  # setDT(pleave_data1)[, .(Dte = seq(FromDate, ToDate, by = "1 day")), by = EmployeeCode]
  
  lst <- Map(function(x, y) seq(x,y, by = "1 day"), pleave_data1$FromDate, pleave_data1$ToDate)
  # lst <- mapply(function(x,y) seq(x,y, by = "1 day"), pleave_data1$FromDate, pleave_data1$ToDate)
  # lst <- Map(seq, pleave_data1$FromDate, pleave_data1$ToDate,by="month")
  
  i1 <- rep(1:nrow(pleave_data1), lengths(lst)) 
  v<-data.frame(pleave_data1[i1,-3], dates = do.call("c", lst))
  f<-v
  library("lubridate")
  f<-f[order(f$EmployeeCode,f$dates,f$AppliedDate),]
  f$latest<-c()
  
  f$latest<-lapply(1:NROW(f),function(i)
  {ifelse(f$EmployeeCode[i]==f$EmployeeCode[i+1]& f$dates[i]==f$dates[i+1]& f$AppliedDate[i]<f$AppliedDate[i+1],1,0)})
  f$latest<-as.numeric(f$latest)
  ff<-subset(f,latest!=1)
  ff$Month<-format(ff$dates,"%m")
  ff$Year<-format(ff$dates,"%Y")
  ff$Day<-25
  ff$Roster_Time<-paste(ff$Year,ff$Month,ff$Day,sep="-")
  ff$Roster_Time<-as.Date(ff$Roster_Time,format = "%Y-%m-%d")
  ff$Roster_Time1<-ff$Roster_Time-months(1)
  
  ff$Planned<-ifelse(ff$AppliedDate<ff$Roster_Time1,1,0)
  
  ff$Planned<-ifelse(ff$Planned==1,"PLANNED","UNPLANNED")
  ff$dates<-as.Date(ff$dates,format="%Y-%m-%d")
  ff$Month<-format(ff$dates,"%b")
  
  ff<-subset(ff,WorkingCity!="CORP")
  
  table(ff$SubDept)
  ff<-subset(ff,SubDept=="CUSTOMERSERVICES"|SubDept=="RAMP"|SubDept=="SECURITY"|
               SubDept=="RAMPSAFETY"|SubDept=="SAFETYMARSHAL"|SubDept=="RSA"|SubDept=="AIRPORTOPERATIONSCUSTOMERSERVICES")
  ff<-subset(ff,EmployeeLevelText=="EXECUTIVE"|EmployeeLevelText=="OFFICER"|EmployeeLevelText=="RSA"|
               EmployeeLevelText=="SENIOREXECUTIVE")
  ff$SubDept[ff$SubDept=="RSA"]<-"RAMP"
  ff$SubDept[ff$SubDept=="SAFETYMARSHAL"]<-"RAMP"
  ff$SubDept[ff$SubDept=="RAMPSAFETY"]<-"RAMP"
  ff$SubDept[ff$SubDept=="AIRPORTOPERATIONSCUSTOMERSERVICES"]<-"CUSTOMERSERVICES"
  
  # ff$LeaveType[ff$LeaveType=="COMPENSATORYOFF"]<-"WEEKOFF"
  ff$LeaveType[ff$LeaveType=="PRIVILEGELEAVE"]<-"PL"
  ff$LeaveType[ff$LeaveType=="LEAVEWITHOUTPAY"]<-"LWP"
  
  # ff$LeaveType[ff$LeaveType=="ABSENT"|
  #                          ff$LeaveType=="UNAPPROVEDABSENCE"]<-"ABSENT"
  
  ff$LeaveType[ff$LeaveType=="HALFDAY"|
                 ff$LeaveType=="MISSPUNCH"]<-"PRESENT"
  ff$LeaveType[ff$LeaveType=="CASUALLEAVE"|
                 ff$LeaveType=="SICKLEAVE"]<-"SL_CL"
  
  ff$LeaveType[ff$LeaveType=="ADOPTIONLEAVE"|ff$LeaveType=="ACCIDENTLEAVE"|
                 ff$LeaveType=="MATERNITYLEAVE"|ff$LeaveType=="EMERGENCYLEAVE"|
                 ff$LeaveType=="OTHERLEAVE"|ff$LeaveType=="PATERNITYLEAVE"|
                 ff$LeaveType=="RELOCATIONLEAVE"]<-"LONG_NAS"
  ff$LeaveType[ff$LeaveType=="HOLIDAYLEAVE"|
                 ff$LeaveType=="HOLIDAY"]<-"HOL"
  ff$LeaveType[ff$LeaveType=="UNAPPROVEDABSENCE"]<-"ABSENT"
  ff$LeaveType[ff$LeaveType=="COMPENSATORYOFF"]<-"COMPOFF"
  
  ff$Week<-weekdays(ff$dates)
  ff<-ff[order(ff$EmployeeCode,ff$dates),]
  ff1<-unique(ff,by=c("EmployeeCode","dates"))
  
  write.csv(ff1,"LeaveStatusReport.csv")
  
  ###########LEAVE UTILISATION MONTHLY#################################
  
  ff1$ctr<-1
  leav_utilz<-sqldf("select Year,Month,WorkingCity,SubDept,LeaveType,sum(ctr) as Leaves ,count(DISTINCT EmployeeCode) 
                    as unique_employees from ff1 group by Year,Month,WorkingCity,SubDept,LeaveType")
  
  
  metros_leav_utliz<-subset(leav_utilz,WorkingCity=="BLR"|WorkingCity=="BOM"|WorkingCity=="CCU"|
                              WorkingCity=="DEL"|WorkingCity=="HYD"|WorkingCity=="MAA")
  
  
  ##non metro i
  
  nonmetro1_leav_utliz<-subset(leav_utilz,WorkingCity=="BBI"|WorkingCity=="GOI"|WorkingCity=="JAI"|
                                 WorkingCity=="CJB"|WorkingCity=="NAG"|WorkingCity=="AMD"|
                                 WorkingCity=="LKO"|WorkingCity=="PNQ"|WorkingCity=="TRV"|
                                 WorkingCity=="PAT"|WorkingCity=="IDR")
  
  ##non metro ii
  
  nonmetro2_leav_utliz<-subset(leav_utilz,WorkingCity!="BBI"&WorkingCity!="GOI"&WorkingCity!="JAI"&
                                 WorkingCity!="CJB"&WorkingCity!="NAG"&WorkingCity!="AMD"&
                                 WorkingCity!="LKO"&WorkingCity!="PNQ"&WorkingCity!="TRV"&
                                 WorkingCity!="PAT"&WorkingCity!="IDR"&WorkingCity!="BLR"&WorkingCity!="BOM"&WorkingCity!="CCU"&
                                 WorkingCity!="DEL"&WorkingCity!="HYD"&WorkingCity!="MAA")
  
  
  write.csv(leav_utilz,"LU_RawReport.csv")
  write.csv(metros_leav_utliz,"LU_metro.csv")
  write.csv(nonmetro1_leav_utliz,"LU_nonmetro1.csv")
  write.csv(nonmetro2_leav_utliz,"LU_nonmetro2.csv")
}

Data_Cleaning_Pipeline4()