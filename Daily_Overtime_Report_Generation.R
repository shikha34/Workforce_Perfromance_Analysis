
Data_Cleaning_Pipeline3<-function(x)
{
setwd("D:/overtime")

library(readxl)
library(stringr)
library(reshape)
library(data.table)
library(sqldf)
library(plyr)
library(dplyr)

##Taking Data of Step1


long2<-unique(longg,by=c("EmployeeCode","Date","Shift" ))

long2<-subset(long2,(EmployeeLevelText=="EXECUTIVE"|EmployeeLevelText=="OFFICER"|
                       EmployeeLevelText=="RSA"|EmployeeLevelText=="SENIOREXECUTIVE"|EmployeeLevelText=="RAMPSAFETY")& 
                (SubDept=="CUSTOMERSERVICES"|SubDept=="RAMP"|SubDept=="SECURITY"| SubDept=="RSA"|SubDept=="RAMPSAFETY"|
                   SubDept=="SAFETYMARSHAL"|SubDept=="AIRPORTOPERATIONSCUSTOMERSERVICES") )

names(long2)
#long2<-long2[,-c(77:85)]
long2$SubDept[long2$SubDept=="RSA"]<-"RAMP"
long2$SubDept[long2$SubDept=="SAFETYMARSHAL"]<-"RAMP"
long2$SubDept[long2$SubDept=="RAMPSAFETY"]<-"RAMP"

long2$SubDept[long2$SubDept=="AIRPORTOPERATIONSCUSTOMERSERVICES"]<-"CUSTOMERSERVICES"
table(long2$SubDept)

final3<-long2
#rm(long1)

final3$Overtime_hrs<-(-final3$Overtime_hrs)

##
final3$Overtime_hrs1<-ifelse(final3$Overtime_hrs>=0.5,final3$Overtime_hrs,0)

##flag of overtime


##LINKING WITH OVERTIME report DETAILS DATA

ot_report<-read_excel("C:/D Drive Data/overtime/overtime_jan2020.xlsx")

table(ot_report$EmployeeSubGroupText)
##there is no department column in ot report ###################
ot_report<-subset(ot_report,EmployeeSubGroupText=="Executive"|EmployeeSubGroupText=="Officer"|
                    EmployeeSubGroupText=="RSA"| EmployeeSubGroupText=="Senior Executive")

names(ot_report)
ot_report$DutyDate<-as.Date(ot_report$DutyDate,"%d-%b-%Y")
final3$Date1<-final3$Date
final3$Date1<-as.character(final3$Date1)
final3$EmployeeCode<-as.character(final3$EmployeeCode)
ot_report$EmployeeCode<-as.character(ot_report$EmployeeCode)
ot_report$DutyDate<-as.character(ot_report$DutyDate)

#filter approved hours only
#ot_report<-subset(ot_report,FinalOTHours>0)
#left join
final4<-merge(final3,ot_report,by.x = c("Date1","EmployeeCode"),by.y = c("DutyDate","EmployeeCode"),all.x = TRUE)

rm(final3)
table(final4$OTType)

##this filter needs to be taken when we are only doing overtime anlaysis
#as we are calculating deficit surplus we need to take both.

#final4<-subset(final4,FinalOTHours>0)

###taking working city from ot report as most of the employees have been transferred to some other bases
## and hence taking the base in which the employee did overtime.
# final4$WorkingCity<-as.character(final4$WorkingCity)

final4$WorkingCityCode<-ifelse(is.na(final4$WorkingCityCode),final4$WorkingCity,final4$WorkingCityCode)

final4$Company_Hol<-ifelse(final4$OTType=="Company Holiday",1,0)
final4$Public_Hol<-ifelse(final4$OTType=="Public Holiday",1,0)
final4$National_Hol<-ifelse(final4$OTType=="National Holiday",1,0)
final4$Operational<-ifelse(final4$OTType=="Operational",1,0)

final4<-subset(final4,!is.na(Date1))
final4$Date1<-as.Date(final4$Date1,"%Y-%m-%d")
final4$Month<-ifelse(is.na(final4$Month),format(final4$Date1,"%b"),final4$Month)
##taking only working greater than 0 and approved ot_hours greater than 0
rm(final3)
#final4<-subset(final4,AttendanceStatus=="PRESENT"|FinalOTHours>0)
final4<-data.frame(final4)

final4[c("AppliedOTHours","ApprovedOTHours" ,"AppliedDate"           
         , "ApprovedOn","ActionedByEmployeeCode" ,"ActionedByEmployeeName", "OTType")]<-
  sapply(final4[c("AppliedOTHours","ApprovedOTHours" ,"AppliedDate"           
                  , "ApprovedOn","ActionedByEmployeeCode" ,"ActionedByEmployeeName", "OTType")],function(x){
                    x<-ifelse(is.na(x),0,x)
                  })
final4$Overtime_flag<-ifelse(final4$Overtime_hrs>0,1,0)
final4$Underutilised_flag<-ifelse(final4$Overtime_hrs<0,1,0)
final4$Ontime_flag<-ifelse(final4$Overtime_hrs==0,1,0)
final4$Approved_OT_flag<-ifelse(final4$ApprovedOTHours>0,1,0)
final4$Shift_hours<-substr(final4$Shift,start=1,stop=3)
final4$Shift_hours<-as.numeric(final4$Shift_hours)

####calculating shift hours.

##planned shift hours monthly
final4$Working_Hours<-as.numeric(final4$Working_Hours)
final4$ctr<-1
final4$Quarter<-quarters(final4$Date1)
final4$Shift_hours<-ifelse(is.na(final4$Shift_hours),0,final4$Shift_hours)

sum1<-sqldf("select OTType, sum(ctr) as ctr from final4 group by OTType")

#cases with shift hours as NA indicates the shift as weekoff,absent,hol hence replacing them with 

pl_shft_hrs<-sqldf("select EmployeeCode,WorkingCityCode,SubDept,EmployeeLevelText,Month,Year,sum(Shift_hours) as Shift_hours 
                   ,sum(Working_Hours) as  Working_Hours, sum(ApprovedOTHours) as Approved_OT_hrs ,
                   sum(Approved_OT_flag) as Nos_Approved_OT,sum(Company_Hol) as Company_Hol, sum(Public_Hol) as Public_Hol,
                   sum(National_Hol) as National_Hol, sum(Operational) as Operational from
                   final4 group by EmployeeCode,WorkingCityCode,SubDept,EmployeeLevelText,Month,Year")

#s1$Working_Hours<-as.numeric(s1$Working_Hours)
pl_shft_hrs$Delta<-pl_shft_hrs$Working_Hours-pl_shft_hrs$Shift_hours
#negative indicate overtime
#pl_shft_hrs$Appr_OT_Whrs<-pl_shft_hrs$Approved_OT_hrs+pl_shft_hrs$Working_Hours
pl_shft_hrs$Old_OT_flag<-ifelse(pl_shft_hrs$Approved_OT_hrs>0,1,0)
pl_shft_hrs$New_OT_flag<-ifelse(pl_shft_hrs$Delta>0,1,0)
pl_shft_hrs$New_OT_hrs<-ifelse(pl_shft_hrs$Delta>0 
                               ,pl_shft_hrs$Delta,0)
# pl_shft_hrs$New_OT_hrs<-ifelse(pl_shft_hrs$New_OT_hrs<=0,0,pl_shft_hrs$New_OT_hrs)
names(pl_shft_hrs)
# pl_shft_hrs<-pl_shft_hrs[-c(15)]
pl_shft_hrs$Flag<-ifelse(pl_shft_hrs$Delta>0,"Overutilised","Underutilised")
pl_shft_hrs$Flag<-ifelse(pl_shft_hrs$Delta==0,"Ontime",pl_shft_hrs$Flag)


###################################Overtime Report merged with Attendance Report Raw Data#######

write.csv(pl_shft_hrs,"Overtime_data_Jan2020.csv")

##################################################Metro##############################################
metro_ptn<-subset(pl_shft_hrs,WorkingCityCode=="BLR"|WorkingCityCode=="BOM"|WorkingCityCode=="CCU"|
                    WorkingCityCode=="DEL"|WorkingCityCode=="HYD"|WorkingCityCode=="MAA")

####################################non metro i #################################################

nonmetro1_ptn<-subset(pl_shft_hrs,WorkingCityCode=="BBI"|WorkingCityCode=="GOI"|WorkingCityCode=="JAI"|
                        WorkingCityCode=="CJB"|WorkingCityCode=="NAG"|WorkingCityCode=="AMD"|
                        WorkingCityCode=="LKO"|WorkingCityCode=="PNQ"|WorkingCityCode=="TRV"|
                        WorkingCityCode=="PAT"|WorkingCityCode=="IDR")

###########################non metro ii ###########################################################

nonmetro2_ptn<-subset(pl_shft_hrs,WorkingCityCode!="BBI"&WorkingCityCode!="GOI"&WorkingCityCode!="JAI"&
                        WorkingCityCode!="CJB"&WorkingCityCode!="NAG"&WorkingCityCode!="AMD"&
                        WorkingCityCode!="LKO"&WorkingCityCode!="PNQ"&WorkingCityCode!="TRV"&
                        WorkingCityCode!="PAT"&WorkingCityCode!="IDR"&WorkingCityCode!="BLR"&WorkingCityCode!="BOM"&WorkingCityCode!="CCU"&
                        WorkingCityCode!="DEL"&WorkingCityCode!="HYD"&WorkingCityCode!="MAA")



write.csv(metro_ptn,"OTraw_metro.csv")
write.csv(nonmetro1_ptn,"OTraw_nonmetro1.csv")
write.csv(nonmetro2_ptn,"OTraw_nonmetro2.csv")

######################weeekly overtime manadays###################################################################

names(final4)

weeklydata<-sqldf("select Week,Month,WorkingCity,SubDept, sum(ApprovedOTHours) as ApprovedOT,
                  sum(Approved_OT_flag) as Approved_OT_flag from
                  final4 group by Week,Month,WorkingCity,SubDept")

####metro
##metro
metro_ptn<-subset(weeklydata,WorkingCity=="BLR"|WorkingCity=="BOM"|WorkingCity=="CCU"|
                    WorkingCity=="DEL"|WorkingCity=="HYD"|WorkingCity=="MAA")

##non metro i

nonmetro1_ptn<-subset(weeklydata,WorkingCity=="BBI"|WorkingCity=="GOI"|WorkingCity=="JAI"|
                        WorkingCity=="CJB"|WorkingCity=="NAG"|WorkingCity=="AMD"|
                        WorkingCity=="LKO"|WorkingCity=="PNQ"|WorkingCity=="TRV"|
                        WorkingCity=="PAT"|WorkingCity=="IDR")

##non metro ii

nonmetro2_ptn<-subset(weeklydata,WorkingCity!="BBI"&WorkingCity!="GOI"&WorkingCity!="JAI"&
                        WorkingCity!="CJB"&WorkingCity!="NAG"&WorkingCity!="AMD"&
                        WorkingCity!="LKO"&WorkingCity!="PNQ"&WorkingCity!="TRV"&
                        WorkingCity!="PAT"&WorkingCity!="IDR"&WorkingCity!="BLR"&WorkingCity!="BOM"&WorkingCity!="CCU"&
                        WorkingCity!="DEL"&WorkingCity!="HYD"&WorkingCity!="MAA")



write.csv(metro_ptn,"weeklyOTraw_metro.csv")
write.csv(nonmetro1_ptn,"weeklyOTraw_nonmetro1.csv")
write.csv(nonmetro2_ptn,"weeklyOTraw_nonmetro2.csv")

}
Data_Cleaning_Pipeline3()
