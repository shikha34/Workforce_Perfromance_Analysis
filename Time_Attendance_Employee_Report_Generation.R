
Data_Cleaning_Pipeline<-function(x){
  
  
  rm(list=ls())
  library(readxl)
  library(dummies)
  library(stringr)
  library(reshape)
  library(data.table)
  library(sqldf)
  library("tidyr")
  library("sqldf")
  library("dplyr")
  
  setwd("C:/D Drive Data/activity_split/rawdata")
  myDir <- "C:/D Drive Data/activity_split/rawdata" 
  
  
  dep<-c("apr2019.xlsx","may2019.xlsx","jun2019.xlsx","jul2019.xlsx","aug2019.xlsx","sep2019.xlsx","oct2019.xlsx","nov2019.xlsx","dec2019.xlsx",
         "jan2020.xlsx","feb2020.xlsx")
  
  
  
  dep1 <- gsub("[.]xlsx", "", dep) 
  
  
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
  
  cleaning1<-function(x)
  {
    x=sapply(x,toupper)
    x=gsub("$","",x,fixed=TRUE)
    x=gsub("=","",x,fixed=TRUE)
    x=gsub("+","",x,fixed=TRUE)
    x=gsub("-","",x,fixed=TRUE)
    x=gsub("&","",x,fixed=TRUE)
    x=gsub("\\","",x,fixed=TRUE)
    x=gsub("~","",x,fixed=TRUE)
    x=gsub("^","",x,fixed=TRUE)
    x=gsub("\\s+"," ",x)
    x=gsub("  ","",x,fixed=TRUE)
    x=gsub(" ","",x,fixed=TRUE)
    x=gsub("_","",x,fixed=TRUE)
    
    
  }
  
  data<-list()
  dat1<-list()
  
  for( i in 1:NROW(dep))
  {
    
    data[i]<-list( read_excel(file.path(myDir, dep[i])) )
    data[[i]]<-subset(data[[i]],select = c(EmployeeCode,EmploymentStatus,AttendanceType,Gender,DOJ,EmployeeLevelText,
                                           WorkingCity,SubDept,Date, Shift,ShiftStartTime,ShiftEndTime,ActualInTime,ActualOutTime,AttendanceStatus, 
                                           LeaveType,LeaveStatusYN,RequestStatus,RegularizedYN,LeaveStatusYN,ApprovedRejectedOn,RosterNonRosterType
    ))
    data[[i]]<-data[[i]][!duplicated(data[[i]][,c(1,9,10)]),]
    
    data[[i]]$DOJ<-as.Date(data[[i]]$DOJ,format="%d-%b-%Y")
    
    data[[i]]$ShiftStartTime<-strptime(data[[i]]$ShiftStartTime,format="%H:%M")
    
    data[[i]]$ShiftStartTime<-strptime(data[[i]]$ShiftStartTime,format="%H:%M")
    data[[i]]$ShiftEndTime<-strptime(data[[i]]$ShiftEndTime,format="%H:%M")
    data[[i]]$ActualInTime<-strptime(data[[i]]$ActualInTime,format="%H:%M")
    data[[i]]$ActualOutTime<-strptime(data[[i]]$ActualOutTime,format="%H:%M")
    
    
    data[[i]]$Working_Hours<-ifelse(data[[i]]$ActualInTime>data[[i]]$ActualOutTime,(24-abs(difftime(data[[i]]$ActualOutTime,data[[i]]$ActualInTime,units="hours"))),abs(difftime(data[[i]]$ActualOutTime,data[[i]]$ActualInTime,units="hours")))
    
    data[[i]]$Working_Hours1<-data[[i]]$Working_Hours
    
    data[[i]][,c("AttendanceStatus")]<-sapply(data[[i]][,c("AttendanceStatus")],cleaning)
    
    ###PRESENT
    
    ##taking mispunch cases
    data[[i]]$Working_Hours<-ifelse(data[[i]]$AttendanceStatus=="MISSPUNCH",substring(data[[i]]$Shift,1,3),data[[i]]$Working_Hours)
    data[[i]]$Working_Hours<-ifelse(data[[i]]$AttendanceStatus=="MISSPUNCH" & 
                                      grepl("WOFF|HOL|ABSENT",data[[i]]$Shift) &
                                      (data[[i]]$WorkingCity=="DEL"|data[[i]]$WorkingCity=="BOM"|data[[i]]$WorkingCity=="HYD"|data[[i]]$WorkingCity=="MAA"|data[[i]]$WorkingCity=="CCU"|
                                         data[[i]]$WorkingCity=="BLR"),10,data[[i]]$Working_Hours)
    data[[i]]$Working_Hours<-ifelse(data[[i]]$AttendanceStatus=="MISSPUNCH" & 
                                      grepl("WOFF|HOL|ABSENT",data[[i]]$Shift) &
                                      (data[[i]]$WorkingCity!="DEL"&data[[i]]$WorkingCity!="BOM"&data[[i]]$WorkingCity!="HYD"&data[[i]]$WorkingCity!="MAA"&data[[i]]$WorkingCity!="CCU"&
                                         data[[i]]$WorkingCity!="BLR"),8.5,data[[i]]$Working_Hours)
    
    data[[i]]$Working_Hours<-ifelse(data[[i]]$Working_Hours==0 & 
                                      data[[i]]$AttendanceStatus=="PRESENT",substring(data[[i]]$Shift,1,3),data[[i]]$Working_Hours)
    
    data[[i]]$Working_Hours<-ifelse(data[[i]]$Working_Hours==0 &  grepl("WOFF|HOL|ABSENT",data[[i]]$Shift) &
                                      (data[[i]]$WorkingCity=="DEL"|data[[i]]$WorkingCity=="BOM"|data[[i]]$WorkingCity=="HYD"|data[[i]]$WorkingCity=="MAA"|data[[i]]$WorkingCity=="CCU"|
                                         data[[i]]$WorkingCity=="BLR") &
                                      data[[i]]$AttendanceStatus=="PRESENT",10,data[[i]]$Working_Hours)
    
    data[[i]]$Working_Hours<-ifelse(data[[i]]$Working_Hours==0 &  grepl("WOFF|HOL|ABSENT",data[[i]]$Shift) &
                                      (data[[i]]$WorkingCity!="DEL"&data[[i]]$WorkingCity!="BOM"&data[[i]]$WorkingCity!="HYD"&data[[i]]$WorkingCity!="MAA"& data[[i]]$WorkingCity!="CCU"&
                                         data[[i]]$WorkingCity!="BLR") &
                                      data[[i]]$AttendanceStatus=="PRESENT",8.5,data[[i]]$Working_Hours)
    
    data[[i]]$Working_Hours<-as.numeric(data[[i]]$Working_Hours)
    data[[i]]$Working_Hours<-ifelse(is.na(data[[i]]$Working_Hours),0,data[[i]]$Working_Hours)
    data[[i]]$Working_Hours1<-ifelse(data[[i]]$Working_Hours>0 & (data[[i]]$AttendanceStatus=="PRESENT"|
                                                                    data[[i]]$AttendanceStatus=="MISSPUNCH"|data[[i]]$AttendanceStatus=="HALFDAY" ),data[[i]]$Working_Hours,0)
    
    
    ##ABSENT
    
    data[[i]]$Working_Hours1<-ifelse(data[[i]]$Working_Hours==0 & data[[i]]$AttendanceStatus=="ABSENT","ABSENT",data[[i]]$Working_Hours1)
    
    ##LEAVE
    data[[i]]$Working_Hours1<-ifelse(data[[i]]$Working_Hours==0 & data[[i]]$AttendanceStatus=="LEAVE" & (data[[i]]$RequestStatus=="APPROVED"
                                                                                                         |data[[i]]$RequestStatus=="PARTIALAPPROVED"),data[[i]]$LeaveType,data[[i]]$Working_Hours1)
    
    #HOLIDAY
    
    data[[i]]$Working_Hours1<-ifelse(data[[i]]$Working_Hours==0 & 
                                       (data[[i]]$AttendanceStatus=="HOLIDAYLEAVE"|data[[i]]$AttendanceStatus=="HOLIDAY"),"HOLIDAY",data[[i]]$Working_Hours1)
    
    
    ##WEEKOFF
    
    data[[i]]$Working_Hours1<-ifelse(data[[i]]$Working_Hours==0 & 
                                       data[[i]]$AttendanceStatus=="WEEKOFF","WEEKOFF",data[[i]]$Working_Hours1)
    
    
    data[[i]]$Date<-as.Date(data[[i]]$Date,format="%d-%b-%Y")
    data[[i]]<-data[[i]][order(data[[i]]$Date),]
    
    # data[[i]]$repeated<-lapply(1:NROW(data[[i]]),function(x){ifelse(identical(data[[i]][x,1],data[[i]][x+1,1]),0,1)})
    
    x1<-as.data.frame( data[[i]])
    
    
    x1[c( "Gender","EmployeeLevelText","WorkingCity","SubDept","RosterNonRosterType","RequestStatus","RegularizedYN","AttendanceStatus")]<-sapply(x1[c( "Gender","EmployeeLevelText","WorkingCity","SubDept","RosterNonRosterType","RequestStatus","RegularizedYN",
                                                                                                                                                        "AttendanceStatus")],cleaning)
    
    x1[c( "Shift")]<-sapply(x1[c( "Shift")],cleaning1)
    
    x1<-subset(x1,RosterNonRosterType=="ROSTEREMPLOYEES")
    x1<-subset(x1,WorkingCity!="CORP")
    
    x1$SubDept[x1$SubDept=="RSA"]<-"RAMP"
    x1$SubDept[x1$SubDept=="SAFETYMARSHAL"]<-"RAMP"
    x1$SubDept[x1$SubDept=="RAMPSAFETY"]<-"RAMP"
    
    x1$SubDept[x1$SubDept=="AIRPORTOPERATIONSCUSTOMERSERVICES"]<-"CUSTOMERSERVICES"
    
    
    x1<-subset(x1,SubDept=="RAMP"|SubDept=="SECURITY"|SubDept=="CUSTOMERSERVICES")
    
    x1<-subset(x1,EmployeeLevelText=="EXECUTIVE"|EmployeeLevelText=="OFFICER"|EmployeeLevelText=="RSA"
               |EmployeeLevelText=="SENIOREXECUTIVE"|EmployeeLevelText=="EXECUTIVE")
    
    # x1<-subset(x1,EmployeeLevelText=="ASSISTANTMANAGER"|EmployeeLevelText=="MANAGER"|EmployeeLevelText=="SENIORMANAGER")
    # 
    
    x1$Metro<-ifelse(x1$WorkingCity=="DEL" | x1$WorkingCity=="CCU" |
                       x1$WorkingCity=="BLR" | x1$WorkingCity=="HYD" | x1$WorkingCity=="BOM" | x1$WorkingCity=="MAA"|x1$WorkingCity=="PNQ",1,0)
    
    x1$NonMetro<-ifelse(x1$WorkingCity!="DEL" & x1$WorkingCity!="CCU" &
                          x1$WorkingCity!="BLR" & x1$WorkingCity!="HYD" & x1$WorkingCity!="BOM" & x1$WorkingCity!="MAA"|x1$WorkingCity!="PNQ",1,0)
    
    x1$Shift_hours<-substr(x1$Shift,start=1,stop=3)
    # x1$Shift_hours<-ifelse( grepl("WOFF|HOL|ABSENT",x1$Shift) &
    #                           (x1$WorkingCity=="DEL"|x1$WorkingCity=="BOM"|x1$WorkingCity=="HYD"|x1$WorkingCity=="MAA"|x1$WorkingCity=="CCU"|
    #                              x1$WorkingCity=="BLR"),10,x1$Shift_hours)
    # 
    # x1$Shift_hours<-ifelse( grepl("WOFF|HOL|ABSENT",x1$Shift) &
    #                           (x1$WorkingCity=="DEL"&x1$WorkingCity=="BOM"&x1$WorkingCity=="HYD"&x1$WorkingCity=="MAA"& x1$WorkingCity=="CCU"&
    #                              x1$WorkingCity=="BLR"),8.5,x1$Shift_hours)
    # 
    x1$Shift_hours<-as.numeric(x1$Shift_hours)
    
    x1$WEEKOFF_shift<-ifelse(x1$Shift_hours=="WOF"|x1$Shift_hours=="WOFf",1,0)
    x1$WEEKOFF_actual<-ifelse(x1$AttendanceStatus=="WEEKOFF",1,0)
    
    x1$HOLIDAY_shift<-ifelse(x1$Shift_hours=="HOL",1,0)
    x1$ABSENT<-ifelse(x1$AttendanceStatus=="ABSENT",1,0)
    x1$HALFDAY<-ifelse(x1$AttendanceStatus=="HALFDAY",1,0)
    x1$HOLIDAY_actual<-ifelse(x1$AttendanceStatus=="HOLIDAY"|x1$AttendanceStatus=="HOLIDAYLEAVE"|x1$AttendanceStatus=="LEAVE",1,0)
    x1$MISSPUNCH<-ifelse(x1$AttendanceStatus=="MISSPUNCH",1,0)
    
    x1$BR_freq<-str_count(x1$Shift,"BR")
    
    x1$Shift1<-ifelse(grepl("^10|^12|^11|^10.5|^11.5|^12.5", x1$Shift),  
                      substring(x1$Shift,5),substring(x1$Shift,4))
    
    x1$Shift1<-gsub("^G|^F|^BR|ENT","",x1$Shift1)
    
    x1$Shift1<-ifelse(x1$Shift1=="0000TO0000",NA,x1$Shift1)
    x1$ActualInTime<-as.POSIXct(x1$ActualInTime,format="%Y-%m-%d %H:%M:%S")
    x1$ActualOutTime<-as.POSIXct(x1$ActualOutTime,format="%Y-%m-%d %H:%M:%S")
    
    
    #making start and end shift columns
    x1<-setDT(x1)[,c("Shift_Start_Time","Shift_End_Time"):=tstrsplit(Shift1,"TO")]
    x1$Shift_Start_Time1<-format(strptime(x1$Shift_Start_Time,"%H%M"),"%H:%M")
    x1$Shift_End_Time1<-format(strptime(x1$Shift_End_Time,"%H%M"),"%H:%M")
    
    x1$Shift_Start_Time1<-ifelse(is.na(x1$Shift_Start_Time1),0,x1$Shift_Start_Time1)
    x1$Shift_End_Time1<-ifelse(is.na(x1$Shift_End_Time1),0,x1$Shift_End_Time1)
    
    x1$Shift_Start_Time1<-ifelse(grepl("NA",x1$Shift_Start_Time1),0,x1$Shift_Start_Time1)
    x1$Shift_End_Time1<-ifelse(grepl("NA",x1$Shift_End_Time1),0,x1$Shift_End_Time1)
    
    x1$Shift_Start_Time1<-as.POSIXct(x1$Shift_Start_Time1,"%H:%M",tz = "GMT")
    x1$Shift_End_Time1<-as.POSIXct(x1$Shift_End_Time1,"%H:%M",tz = "GMT")
    
    x1$Overtime_hrs<-x1$Shift_hours-x1$Working_Hours 
    
    # x1$EarlyTime<-difftime(x1$Shift_Start_Time1,x1$ActualInTime,units = "mins",format ="%Y-%m-%d %H:%M:%S")
    # x1$LateTime<-difftime(x1$Shift_End_Time1,x1$ActualOutTime,units = "mins",format ="%Y-%m-%d %H:%M:%S")
    # 
    x1$EarlyTime<-difftime(strptime(format(x1$Shift_Start_Time1,"%H:%M:%S"),"%H:%M:%S"),strptime(format(x1$ActualInTime,"%H:%M:%S"),"%H:%M:%S"),units="mins")
    x1$LateTime<-difftime(strptime(format(x1$Shift_End_Time1,"%H:%M:%S"),"%H:%M:%S"),strptime(format(x1$ActualOutTime,"%H:%M:%S"),"%H:%M:%S"),units="mins")
    
    x1$miss<-ifelse((strptime(format(x1$ActualInTime,"%H:%M:%S"),"%H:%M:%S")==strptime("00:00:00","%H:%M:%S") |
                       strptime(format(x1$ActualOutTime,"%H:%M:%S"),"%H:%M:%S")==strptime("00:00:00","%H:%M:%S") )
                    &
                      !grepl("WOF|HOL|ABSENT|LEAVE",x1$Shift) & !grepl("WEEKOFF|HOLIDAY|ABSENT|LEAVE|HOLIDAYLEAVE",x1$AttendanceStatus) ,1,0)
    
    #remove 1 in miss as they are misspunch cases
    #remove attendance with holiday ,leave, absent, weekoff , but if there is login greater than 0 then they should not be removed.
    x1$removed<-ifelse((x1$HOLIDAY_actual==1 |x1$MISSPUNCH==1|x1$ABSENT==1|x1$WEEKOFF_actual==1|
                          x1$HALFDAY==1)& x1$Working_Hours1==0,1,0)
    
    ##having above scenarios and then removing cases which have been approved and reguarise for being present
    
    x1$take<-ifelse((x1$HOLIDAY_actual!=1 & x1$MISSPUNCH!=1& x1$ABSENT!=1& x1$WEEKOFF_actual!=1
    )& x1$Working_Hours1>0 & x1$RegularizedYN=="Y" & x1$LeaveStatusYN=="N" & (x1$RequestStatus=="APPROVED"|
                                                                                x1$RequestStatus=="PARTIALAPPROVED"),1,0)
    
    
    # x1$EarlyTime<-difftime(x1$Shift_Start_Time1,x1$ActualInTime,units = "mins")
    # x1$LateTime<-difftime(x1$Shift_End_Time1,x1$ActualOutTime,units = "mins")
    # 
    x1$Date<-as.character(x1$Date)
    x1$ActualInTime<-as.character(x1$ActualInTime)
    x1$ActualOutTime<-as.character(x1$ActualOutTime)
    x1$Working_Hours<-as.character(x1$Working_Hours)
    x1$Working_Hours1<-as.character(x1$Working_Hours1)
    x1$ShiftStartTime<-as.character(x1$ShiftStartTime)
    x1$ShiftEndTime<-as.character(x1$ShiftEndTime)
    x1$DOJ<-as.character(x1$DOJ)
    
    dat1[i]<-list(x1) 
    
  }
  
  
  final<-do.call(rbind,dat1)
  final$Date<-as.Date(final$Date,"%Y-%m-%d")
  final$Month<-format(final$Date,"%b")
  final$Year<-format(final$Date,"%Y")
  
  final<-data.frame(final)
  final$ActualInTime<-as.POSIXct(final$ActualInTime,format="%Y-%m-%d %H:%M:%S",tz="GMT")
  final$ActualOutTime<-as.POSIXct(final$ActualOutTime,format="%Y-%m-%d %H:%M:%S",tz="GMT")
  # final<-subset(final,select = -c(repeated))
  
  #################late login aand early logout parameters################
  final$Start_early_late<-NULL
  final$End_early_late<-NULL
  
  final$EarlyTime1<-NULL
  final$LateTime1<-NULL
  
  final$Start_early_late<- ifelse(
    
    (
      (strptime(format(final$Shift_Start_Time1,"%H:%M:%S"),"%H:%M:%S")>=strptime("18:00:0","%H:%M:%S")
       & (strptime(format(final$Shift_Start_Time1,"%H:%M:%S"),"%H:%M:%S")<=strptime("23:59:00","%H:%M:%S"))
       
      )
      &  ( strptime(format(final$ActualInTime,"%H:%M:%S"),"%H:%M:%S")>=strptime("00:00:00","%H:%M:%S")
           &
             strptime(format(final$ActualInTime,"%H:%M:%S"),"%H:%M:%S")<=strptime("12:00:00","%H:%M:%S")
           
      )
    )
    |
      ((strptime(format(final$Shift_Start_Time1,"%H:%M:%S"),"%H:%M:%S")>=strptime("00:00:0","%H:%M:%S")
        & (strptime(format(final$Shift_Start_Time1,"%H:%M:%S"),"%H:%M:%S")<=strptime("02:00:00","%H:%M:%S"))
        
      )
      &
        
        ( strptime(format(final$ActualInTime,"%H:%M:%S"),"%H:%M:%S")>=strptime("18:00:00","%H:%M:%S")
          &
            strptime(format(final$ActualInTime,"%H:%M:%S"),"%H:%M:%S")<=strptime("23:59:00","%H:%M:%S")
          
        )
      )
    
    |
      ((strptime(format(final$Shift_Start_Time1,"%H:%M:%S"),"%H:%M:%S")>=strptime("23:00:00","%H:%M:%S")
        & (strptime(format(final$Shift_Start_Time1,"%H:%M:%S"),"%H:%M:%S")<=strptime("23:59:00","%H:%M:%S"))
        
      )
      &
        
        ( strptime(format(final$ActualInTime,"%H:%M:%S"),"%H:%M:%S")>=strptime("00:00:00","%H:%M:%S")
          &
            strptime(format(final$ActualInTime,"%H:%M:%S"),"%H:%M:%S")<=strptime("12:00:00","%H:%M:%S")
        ) )
    |
      
      ((strptime(format(final$Shift_Start_Time1,"%H:%M:%S"),"%H:%M:%S")>=strptime("00:00:0","%H:%M:%S")
        & (strptime(format(final$Shift_Start_Time1,"%H:%M:%S"),"%H:%M:%S")<=strptime("12:00:00","%H:%M:%S"))
        
      )      &
        
        ( strptime(format(final$ActualInTime,"%H:%M:%S"),"%H:%M:%S")>=strptime("18:00:00","%H:%M:%S")
          &
            strptime(format(final$ActualInTime,"%H:%M:%S"),"%H:%M:%S")<=strptime("23:59:00","%H:%M:%S")
          
        )
      )
    
    ,
    1,0)
  
  
  
  
  final$End_early_late<- ifelse(
    
    (
      (strptime(format(final$Shift_End_Time1,"%H:%M:%S"),"%H:%M:%S")>=strptime("18:00:0","%H:%M:%S")
       & (strptime(format(final$Shift_End_Time1,"%H:%M:%S"),"%H:%M:%S")<=strptime("23:59:00","%H:%M:%S"))
       
      )
      &  ( strptime(format(final$ActualOutTime,"%H:%M:%S"),"%H:%M:%S")>=strptime("00:00:00","%H:%M:%S")
           &
             strptime(format(final$ActualOutTime,"%H:%M:%S"),"%H:%M:%S")<=strptime("12:00:00","%H:%M:%S")
           
      )
    )
    |
      ((strptime(format(final$Shift_End_Time1,"%H:%M:%S"),"%H:%M:%S")>=strptime("00:00:0","%H:%M:%S")
        & (strptime(format(final$Shift_End_Time1,"%H:%M:%S"),"%H:%M:%S")<=strptime("02:00:00","%H:%M:%S"))
        
      )
      &
        
        ( strptime(format(final$ActualOutTime,"%H:%M:%S"),"%H:%M:%S")>=strptime("18:00:00","%H:%M:%S")
          &
            strptime(format(final$ActualOutTime,"%H:%M:%S"),"%H:%M:%S")<=strptime("23:59:00","%H:%M:%S")
          
        )
      )
    
    |
      ((strptime(format(final$Shift_End_Time1,"%H:%M:%S"),"%H:%M:%S")>=strptime("23:00:00","%H:%M:%S")
        & (strptime(format(final$Shift_End_Time1,"%H:%M:%S"),"%H:%M:%S")<=strptime("23:59:00","%H:%M:%S"))
        
      )
      &
        
        ( strptime(format(final$ActualOutTime,"%H:%M:%S"),"%H:%M:%S")>=strptime("00:00:00","%H:%M:%S")
          &
            strptime(format(final$ActualOutTime,"%H:%M:%S"),"%H:%M:%S")<=strptime("12:00:00","%H:%M:%S")
        ) )
    |
      
      ((strptime(format(final$Shift_End_Time1,"%H:%M:%S"),"%H:%M:%S")>=strptime("00:00:0","%H:%M:%S")
        & (strptime(format(final$Shift_End_Time1,"%H:%M:%S"),"%H:%M:%S")<=strptime("12:00:00","%H:%M:%S"))
        
      )      &
        
        ( strptime(format(final$ActualOutTime,"%H:%M:%S"),"%H:%M:%S")>=strptime("18:00:00","%H:%M:%S")
          &
            strptime(format(final$ActualOutTime,"%H:%M:%S"),"%H:%M:%S")<=strptime("23:59:00","%H:%M:%S")
          
        )
      )
    
    ,
    1,0)
  
  ##early time is for shift start and actual start
  ## late time is for shift end and actual end
  final$LateTime1<-ifelse(final$End_early_late==1,(1440-abs(final$LateTime)),final$LateTime)
  
  final$EarlyTime1<-ifelse(final$Start_early_late==1,(1440-abs(final$EarlyTime)),final$EarlyTime)
  
  longg<-final
  rm(final)
  longg$Date<-as.Date(longg$Date,"%Y-%m-%d")
  longg$Year<-format(longg$Date,"%Y")
  longg$Month<-format(longg$Date,"%b")
  longg$Day<-format(longg$Date,"%d")
  longg$Week<-weekdays(longg$Date)
  longg$Quarters<-quarters(longg$Date)
  # longg<-subset(longg,select = -c(repeated))
  longg<-data.frame(longg)
  longg[c("EmployeeLevelText","WorkingCity","SubDept")]<-sapply(longg[c("EmployeeLevelText","WorkingCity","SubDept")],cleaning)
  
  longg<-subset(longg,WorkingCity!="CORP")
  longg<-subset(longg,EmployeeLevelText!="ASSOCIATEVP")
  
  longg$Metro<-ifelse(longg$WorkingCity=="DEL" | longg$WorkingCity=="CCU" |
                        longg$WorkingCity=="BLR" | longg$WorkingCity=="HYD" | longg$WorkingCity=="BOM" | longg$WorkingCity=="MAA",1,0)
  
  longg$NonMetro<-ifelse(longg$WorkingCity!="DEL" & longg$WorkingCity!="CCU" &
                           longg$WorkingCity!="BLR" & longg$WorkingCity!="HYD" & longg$WorkingCity!="BOM" & longg$WorkingCity!="MAA",1,0)
  
  longg$LeaveType<-sapply(longg$LeaveType,cleaning)
  longg$AttendanceStatus<-ifelse(longg$AttendanceStatus=="LEAVE",longg$LeaveType,longg$AttendanceStatus)
  
  longg$WEEKOFF<-ifelse(longg$AttendanceStatus=="WEEKOFF",1,0)
  longg$ABSENT<-ifelse(longg$AttendanceStatus=="ABSENT",1,0)
  longg$ACCIDENTLEAVE<-ifelse(longg$AttendanceStatus=="ACCIDENTLEAVE",1,0)
  longg$CASUALLEAVE<-ifelse(longg$AttendanceStatus=="CASUALLEAVE",1,0)
  longg$COMPENSATORYOFF<-ifelse(longg$AttendanceStatus=="COMPENSATORYOFF",1,0)
  longg$EMERGENCYLEAVE<-ifelse(longg$AttendanceStatus=="EMERGENCYLEAVE",1,0)
  longg$HOLIDAY<-ifelse(longg$AttendanceStatus=="HOLIDAY"|longg$AttendanceStatus=="HOLIDAYLEAVE",1,0)
  longg$LEAVEWITHOUTPAY<-ifelse(longg$AttendanceStatus=="LEAVEWITHOUTPAY",1,0)
  longg$MATERNITYLEAVE<-ifelse(longg$AttendanceStatus=="MATERNITYLEAVE",1,0)
  longg$OTHERLEAVE<-ifelse(longg$AttendanceStatus=="OTHERLEAVE",1,0)
  longg$PATERNITYLEAVE<-ifelse(longg$AttendanceStatus=="PATERNITYLEAVE",1,0)
  longg$PRESENT<-ifelse(longg$AttendanceStatus=="PRESENT",1,0)
  longg$PRIVILEGELEAVE<-ifelse(longg$AttendanceStatus=="PRIVILEGELEAVE",1,0)
  longg$RELOCATIONLEAVE<-ifelse(longg$AttendanceStatus=="RELOCATIONLEAVE",1,0)
  longg$SICKLEAVE<-ifelse(longg$AttendanceStatus=="SICKLEAVE",1,0)
  longg$UNAPPROVEDABSENCE<-ifelse(longg$AttendanceStatus=="UNAPPROVEDABSENCE",1,0)
  longg$ADOPTIONLEAVE<-ifelse(longg$AttendanceStatus=="ADOPTIONLEAVE",1,0)
  longg$HALFDAY<-ifelse(longg$AttendanceStatus=="HALFDAY",1,0)
  longg$MISSPUNCH<-ifelse(longg$AttendanceStatus=="MISSPUNCH",1,0)
  
  longg$SL_CL<-ifelse(longg$SICKLEAVE==1 | longg$CASUALLEAVE ==1,1,0)
  longg$LONG_NAS<-ifelse(longg$MATERNITYLEAVE==1 | longg$ACCIDENTLEAVE==1|longg$OTHERLEAVE ==1| longg$PATERNITYLEAVE ==1|
                           longg$RELOCATIONLEAVE==1|longg$ADOPTIONLEAVE==1 | longg$EMERGENCYLEAVE==1,1,0)
  
  longg$WEEKOFF1<-ifelse(longg$WEEKOFF==1 | longg$COMPENSATORYOFF ==1,1,0)
  longg$ABSENT1<-ifelse(longg$UNAPPROVEDABSENCE==1 | longg$ABSENT ==1,1,0)
  longg$PRESENT1<-ifelse(longg$PRESENT==1 | longg$HALFDAY==1|longg$MISSPUNCH==1,1,0)
  
  longg$ctr<-1
  
  longg<-subset(longg,!is.na(AttendanceStatus))
  
  longg$Present_WHA<-ifelse((longg$AttendanceStatus=="PRESENT"|longg$AttendanceStatus=="MISSPUNCH"
                             |longg$AttendanceStatus=="HALFDAY") &
                              (longg$Shift=="WOFF0000TO0000"|longg$Shift=="ABSENT0000TO0000"|
                                 longg$Shift=="HOL0000TO0000"),1,0)
  
  longg$Date<-as.character(longg$Date)
  longg$EmployeeCode<-as.character(longg$EmployeeCode)
  longg<-data.table(longg)
  
  
  ##repaceing attendance staus with leaves if its approved or partial approved as partial approved cases wont fall in 
  ### leave status with "Y" it will have leave status as "N
  longg$AttendanceStatus<-ifelse(longg$LeaveStatusYN=="Y"&(longg$RequestStatus=="APPROVED"|
                                                             longg$RequestStatus=="PARTIALAPPROVED"),longg$LeaveType,longg$AttendanceStatus)
  
  long1<-unique(longg,by=c("EmployeeCode","Date"))
  library(reshape)
  table(long1$AttendanceStatus)
  long1$AttendanceStatus[long1$AttendanceStatus=="COMPENSATORYOFF"]<-"WEEKOFF"
  long1$AttendanceStatus[long1$AttendanceStatus=="PRIVILEGELEAVE"]<-"PL"
  long1$AttendanceStatus[long1$AttendanceStatus=="LEAVEWITHOUTPAY"]<-"LWP"
  
  long1$AttendanceStatus[long1$AttendanceStatus=="ABSENT"|
                           long1$AttendanceStatus=="UNAPPROVEDABSENCE"]<-"ABSENT"
  
  long1$AttendanceStatus[long1$AttendanceStatus=="HALFDAY"|
                           long1$AttendanceStatus=="MISSPUNCH"]<-"PRESENT"
  
  long1$AttendanceStatus[long1$AttendanceStatus=="CASUALLEAVE"|
                           long1$AttendanceStatus=="SICKLEAVE"]<-"SL_CL"
  
  long1$AttendanceStatus[long1$AttendanceStatus=="ADOPTIONLEAVE"|long1$AttendanceStatus=="ACCIDENTLEAVE"|
                           long1$AttendanceStatus=="MATERNITYLEAVE"|long1$AttendanceStatus=="EMERGENCYLEAVE"|
                           long1$AttendanceStatus=="OTHERLEAVE"|long1$AttendanceStatus=="PATERNITYLEAVE"|
                           long1$AttendanceStatus=="RELOCATIONLEAVE"]<-"LONG_NAS"
  long1$AttendanceStatus[long1$AttendanceStatus=="HOLIDAYLEAVE"|
                           long1$AttendanceStatus=="HOLIDAY"]<-"HOL"
  
  
  
  #monhtly weekly overall
  dd1<-dummy(long1$Week) 
  long1<-cbind(long1,dd1)
  gg11<-long1%>% filter(AttendanceStatus %in% c("PRESENT" ,"WEEKOFF","ABSENT","SL_CL"
                                                ,"PL" ,"LWP","HOL" , "LONG_NAS"))  %>%
    select(c("AttendanceStatus","ctr", "WeekMonday","WeekTuesday" , "WeekWednesday" ,"WeekThursday","WeekFriday","WeekSaturday", "WeekSunday",
             "Year","Month","WorkingCity","SubDept","EmployeeLevelText"))
  
  gg11<-aggregate(gg11[,c("WeekFriday", "WeekMonday","WeekSaturday", "WeekSunday" , "WeekThursday",     
                          "WeekTuesday" , "WeekWednesday")],by=list(gg11$AttendanceStatus,gg11$Year,gg11$Month,
                                                                    gg11$WorkingCity, gg11$SubDept, gg11$EmployeeLevelText),sum)
  
  
  names(gg11)[1]<-"Leave_Type"
  names(gg11)[2]<-"Year"
  names(gg11)[3]<-"Month"
  names(gg11)[4]<-"Base_Code"
  names(gg11)[5]<-"Department"
  names(gg11)[6]<-"Designation"
  
  names(gg11)
  gg11<-gg11[c(1:6,8,12,13,11,7,9,10)]
  
  write.csv(gg11,"C:/D Drive Data/RosterMatrix/Jan2020/Raw1.csv")
  d1<-subset(longg)
  
  d1$Shift_Hrs<-substr(d1$Shift,1,3)
  brakshift<-d1
  brakshift<-brakshift[order(brakshift$EmployeeCode,brakshift$Date),]
  brakshift$Shift_Hrs<-lapply(1:NROW(brakshift),function(x){
    ifelse(brakshift$EmployeeCode[x]==brakshift$EmployeeCode[x+1] &
             brakshift$Date[x]==brakshift$Date[x+1] &
             brakshift$BR_freq[x]==1 ,8.5, brakshift$Shift_Hrs[x])})
  
  brakshift$Shift_Hrs<-as.numeric(brakshift$Shift_Hrs)                             
  
  brakshift$Date<-as.character(brakshift$Date)
  
  ## will consider two rows for breakshift cases needs to be taken for login analysis and deficit hours.
  
  brakshift1<-d1
  brakshift1<-brakshift1 %>% distinct(EmployeeCode,Date,Shift, .keep_all = TRUE)
  
  ##for shifts compliance need to take one row for each of the breakshift cases.
  brakshift2<-brakshift %>% distinct(EmployeeCode,Date, .keep_all = TRUE)
  
  brakshift1<-data.frame(brakshift1)
  
  brakshift1$ActualInTime<-as.character(brakshift1$ActualInTime)
  brakshift1$ActualOutTime<-as.character(brakshift1$ActualOutTime)
  
  brakshift2<-data.frame(brakshift2)
  
  brakshift2$ActualInTime<-as.character(brakshift2$ActualInTime)
  
  brakshift2$ActualOutTime<-as.character(brakshift2$ActualOutTime)
  
  brakshift2$Planned_weekoff<-ifelse(grepl("^WOFF",brakshift2$Shift),1,0)
  
  brakshift2$Actual_weekoff<-ifelse(brakshift2$AttendanceStatus=="WEEKOFF"|
                                      brakshift2$AttendanceStatus=="COMPENSATORYOFF",1,0)
  
  brakshift2$Shift_Hrs<-ifelse(brakshift2$Actual_weekoff==1,"Actual_weekoff",brakshift2$Shift_Hrs)
  
  
  ###################Shift Complinace Raw Data#& Report########################################################
  
  d3<-sqldf("select EmployeeCode,WorkingCity,SubDept,EmployeeLevelText,Month,Date,Shift, Shift_Hrs,
            count(Shift_Hrs) as Freq from brakshift2 group by EmployeeCode,WorkingCity,SubDept,EmployeeLevelText,Month,Date,Shift, Shift_Hrs
            ")
  
  
  write.csv(d3,"Shifts_comp_raw.csv")
  
  d3_<-sqldf("select EmployeeCode,WorkingCity,SubDept,EmployeeLevelText,Month,Shift, Shift_Hrs,
             count(Shift_Hrs) as Freq
             from brakshift2 group by EmployeeCode,WorkingCity,SubDept,EmployeeLevelText,Month,Shift, Shift_Hrs
             ")
  write.csv(d3_,"Shifts_comp_summary.csv")
  
  ###################################Deficit hours###################################################### 
  
  d1<-brakshift1[,c(1,7,8:15,27,24,51,52,47,34,39,40,74)]
  
  d1$Nologin<-ifelse(as.character(d1$ActualInTime)=="2020-04-01 00:00:00" & as.character(d1$ActualOutTime)=="2020-04-01 00:00:00",
                     1,0)
  #d1<-subset(d1,AttendanceStatus=="PRESENT" & MISSPUNCH==0)
  #taking all data
  
  d1$Date<-as.Date(d1$Date,"%Y-%m-%d")
  d1$Year<-format(d1$Date,"%Y")
  d1$Month<-format(d1$Date,"%b")
  
  dd<-d1
  dd$StartShift<-ifelse(dd$EarlyTime1<0,"Late",ifelse(dd$EarlyTime1>0,"Early","Ontime"))
  dd$EndShift<-ifelse(dd$LateTime1<0,"Late",ifelse(dd$LateTime1>0,"Early","Ontime"))
  dd$Start_Shift_Change<-ifelse(dd$EarlyTime1>=180 |dd$EarlyTime1<=-180,"Shift_Change","NoShift_Change" )
  dd$End_Shift_Change<-ifelse(dd$LateTime1>=180 |dd$LateTime1<=-180,"Shift_Change","NoShift_Change" )
  
  ###calcuting deficit hours column based on following conditions:
  
  dd$Shift_hours<-ifelse(dd$AttendanceStatus=="PRIVILEGELEAVE"|dd$AttendanceStatus=="COMPENSATORYOFF"|
                           dd$AttendanceStatus=="CASUALLEAVE"|dd$AttendanceStatus=="HOLIDAYLEAVE"|dd$AttendanceStatus=="RELOCATIONLEAVE"|
                           dd$AttendanceStatus=="MATERNITYLEAVE"|dd$AttendanceStatus=="HOLIDAY",0,dd$Shift_hours)
  
  dd$Working_Hours1<-ifelse(dd$AttendanceStatus=="WEEKOFF"|dd$AttendanceStatus=="PRIVILEGELEAVE"|dd$AttendanceStatus=="COMPENSATORYOFF"|
                              dd$AttendanceStatus=="CASUALLEAVE"|dd$AttendanceStatus=="HOLIDAYLEAVE"|dd$AttendanceStatus=="RELOCATIONLEAVE"|
                              dd$AttendanceStatus=="MATERNITYLEAVE"|dd$AttendanceStatus=="HOLIDAY",0,dd$Working_Hours1)
  
  df_hrs2<-sqldf("select Month,EmployeeCode,WorkingCity, SubDept,AttendanceStatus,sum(Shift_hours) as 
                 Planned_Hrs, sum(Working_Hours1) as Actual_Hours,sum(WEEKOFF1) as WEEKOFF from dd group by
                 Month,EmployeeCode,WorkingCity, SubDept,AttendanceStatus")
  
  # names(dd)
  
  ##removing misspunch and leaves and taking only present cases.
  dd1<-subset(dd,AttendanceStatus=="PRESENT")
  
  df_hrs4<-sqldf("select Month,EmployeeCode,WorkingCity, SubDept,StartShift,sum(EarlyTime1) as
                 Mins_S from dd1 group by
                 Month,EmployeeCode,WorkingCity, SubDept,StartShift")
  
  df_hrs4$Mins_S<-abs(df_hrs4$Mins_S)
  
  df_hrs4_<-sqldf("select Month,EmployeeCode,WorkingCity, SubDept,EndShift,sum(LateTime1) as
                  Mins_E from dd1 group by
                  Month,EmployeeCode,WorkingCity, SubDept,EndShift")
  
  df_hrs4_$Mins_E<-abs(df_hrs4_$Mins_E)
  
  ######daily Deficit Report#############################################################################
  
  df_hrs4_daily_s<-sqldf("select Month,Date,Shift,ActualInTime,ActualOutTime,EmployeeCode,WorkingCity, SubDept,AttendanceStatus,
                         Start_Shift_Change,StartShift,MISSPUNCH,sum(EarlyTime1) as
                         Mins_S from dd1 group by
                         Month,Date,Shift,ActualInTime,ActualOutTime,EmployeeCode,WorkingCity, SubDept,AttendanceStatus,Start_Shift_Change,StartShift,MISSPUNCH")
  
  df_hrs4_daily_s$Mins_S<-abs(df_hrs4_daily_s$Mins_S)
  
  df_hrs4_daily_e<-sqldf("select  Month,Date,Shift,ActualInTime,ActualOutTime,EmployeeCode,WorkingCity, SubDept,End_Shift_Change,EndShift,MISSPUNCH,sum(LateTime1) as
                         Mins_E from dd1 group by
                         Month,Date,Shift,ActualInTime,ActualOutTime,EmployeeCode,WorkingCity, SubDept,End_Shift_Change,EndShift,MISSPUNCH")
  df_hrs4_daily_e$Mins_E<-abs(df_hrs4_daily_e$Mins_E)
  
  write.csv(df_hrs2,"Deficit_HOurs.csv")
  write.csv(df_hrs4,"LoginAnalysis_Start.csv")
  write.csv(df_hrs4_,"LoginAnalysis_End.csv")
  
  write.csv(df_hrs4_daily_s,"LoginAnalysis_Start_raw.csv")
  write.csv(df_hrs4_daily_e,"LoginAnalysis_End_raw.csv")
  
  
  dd1$LateTime1<-abs(dd1$LateTime1)
  dd1$EarlyTime1<-abs(dd1$EarlyTime1)
  
  
  #########################Late Login Early Logout#############################################
  
  #d2<-subset(dd,dd$LateTime1_afterbuffer>=10|dd$EarlyTime1_afterbuffer>=10)
  d2<-dd1
  d3<-subset(d2,d2$StartShift=="Late"&d2$EndShift=="Early")
  d4<-subset(d2,d2$StartShift=="Early"&d2$EndShift=="Late")
  
  ##summary of d3
  
  d3_summary<-sqldf("select WorkingCity,EmployeeCode,Date,Shift,ActualInTime,ActualOutTime,EmployeeCode,WorkingCity, SubDept,Start_Shift_Change,StartShift,MISSPUNCH,sum(EarlyTime1) as
                    Mins_S from dd1 group by
                    Month,Date,Shift,ActualInTime,ActualOutTime,EmployeeCode,WorkingCity, SubDept,Start_Shift_Change,StartShift,MISSPUNCH")
  
  d3_summary$Total_LostHours<-d3_summa
  
  
  write.csv(d3,"LateLogin_EarlyLogout.csv")
  write.csv(d4,"EarlyLogin_LateLogout.csv")
  
  
  ## Raw Attendance data Report###########################################################################
  
  ##daily
  attendance<-sqldf("select Month,Date,Shift,ActualInTime,ActualOutTime,EmployeeCode,WorkingCity, SubDept,AttendanceStatus
                    from brakshift1 group by
                    Month,Date,Shift,ActualInTime,ActualOutTime,EmployeeCode,WorkingCity, SubDept,AttendanceStatus")
  
  write.csv(attendance,"Raw_AttendanceData.csv")
  
}
Data_Cleaning_Pipeline()
