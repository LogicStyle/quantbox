buildSMB <- function(){
  con <- db.local()
  qr <- paste("select * from SecuMain where ID='EI000985'")
  re <- dbGetQuery(con,qr)
  dbDisconnect(con)
  if(nrow(re)==0) add.index.lcdb(indexID="EI000985")

  RebDates <- getRebDates(as.Date('2005-01-31'),as.Date('2016-06-30'),'month')
  TS <- getTS(RebDates,'EI000985')
  TSF <- gf_lcfs(TS,'F000002')
  TSF <- plyr::ddply(TSF,"date",transform,factorscore=ifelse(is.na(factorscore),median(factorscore,na.rm=TRUE),factorscore))
  TSF <- plyr::ddply(TSF, ~ date, plyr::mutate, group = as.numeric(ggplot2::cut_number(factorscore,3)))
  tmp.TS1 <- TSF[TSF$group==1,c("date","stockID")]
  tmp.TS1 <- plyr::ddply(tmp.TS1, ~ date, plyr::mutate, recnum = seq(1,length(date)))
  tmp.TS3 <- TSF[TSF$group==3,c("date","stockID")]
  tmp.TS3 <- plyr::ddply(tmp.TS3, ~ date, plyr::mutate, recnum = seq(1,length(date)))

  tmp <- plyr::ddply(tmp.TS1,~date,plyr::summarise,nstock=length(date))
  tmp.date <- data.frame(date=getRebDates(min(TS$date),max(TS$date),rebFreq = 'day'))
  tmp.date$nstock <- tmp$nstock[findInterval(tmp.date$date,tmp$date)]
  tmp.date$datecor <- tmp$date[findInterval(tmp.date$date,tmp$date)]
  TS1 <- data.frame(datenew=rep(tmp.date$date,tmp.date$nstock),
                    date=rep(tmp.date$datecor,tmp.date$nstock))
  TS1 <- plyr::ddply(TS1, ~ datenew, plyr::mutate, recnum = seq(1,length(datenew)))
  TS1 <- merge.x(TS1,tmp.TS1)
  TS1 <- TS1[,c("datenew","stockID")]
  colnames(TS1) <- c("date","stockID")

  tmp <- plyr::ddply(tmp.TS3,~date,plyr::summarise,nstock=length(date))
  tmp.date <- data.frame(date=getRebDates(min(TS$date),max(TS$date),rebFreq = 'day'))
  tmp.date$nstock <- tmp$nstock[findInterval(tmp.date$date,tmp$date)]
  tmp.date$datecor <- tmp$date[findInterval(tmp.date$date,tmp$date)]
  TS3 <- data.frame(datenew=rep(tmp.date$date,tmp.date$nstock),
                    date=rep(tmp.date$datecor,tmp.date$nstock))
  TS3 <- plyr::ddply(TS3, ~ datenew, plyr::mutate, recnum = seq(1,length(datenew)))
  TS3 <- merge.x(TS3,tmp.TS3)
  TS3 <- TS3[,c("datenew","stockID")]
  colnames(TS3) <- c("date","stockID")

  qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.DailyReturn 'stockRtn'
              from QT_DailyQuote t where t.TradingDay>=",rdate2int(min(TS$date)),
              " and t.TradingDay<=",rdate2int(max(TS$date)))
  con <- db.quant()
  re <- sqlQuery(con,qr)
  close(con)
  re$date <- intdate2r(re$date)

  TSR1 <- merge.x(TS1,re)
  TSR3 <- merge.x(TS3,re)
  R1 <- TSR1[,c("date","stockRtn")]
  R1 <- R1[!is.na(R1$stockRtn),]
  R1 <- plyr::ddply(R1,~date,plyr::summarise,dailtRtn=mean(stockRtn,na.rm = T))
  R3 <- TSR3[,c("date","stockRtn")]
  R3 <- R3[!is.na(R3$stockRtn),]
  R3 <- plyr::ddply(R3,~date,plyr::summarise,dailtRtn=mean(stockRtn,na.rm = T))

  rtn <- merge(R1,R3,by='date')
  colnames(rtn) <- c('date','S','B')
  rtn$stockID <- c('EI000985')
  rtn$factorName <- c('SMB')
  rtn$factorScore <- rtn$S-rtn$B
  rtn$date <- rdate2int(rtn$date)
  rtn <- rtn[,c('date','stockID','factorName','factorScore')]

  con <- db.local()
  if(dbExistsTable(con,'QT_FactorScore_amtao')){
    dbWriteTable(con,'QT_FactorScore_amtao',rtn,overwrite=F,append=T)
  }else{
    dbWriteTable(con,'QT_FactorScore_amtao',rtn)
  }
  dbDisconnect(con)
  return('Done!')
}



buildHML <- function(){
  con <- db.local()
  qr <- paste("select * from SecuMain where ID='EI000985'")
  re <- dbGetQuery(con,qr)
  dbDisconnect(con)
  if(nrow(re)==0) add.index.lcdb(indexID="EI000985")

  RebDates <- getRebDates(as.Date('2005-01-31'),as.Date('2016-06-30'),'month')
  TS <- getTS(RebDates,'EI000985')
  TSF <- gf_lcfs(TS,'F000006')
  TSF <- plyr::ddply(TSF,"date",transform,factorscore=ifelse(is.na(factorscore),median(factorscore,na.rm=TRUE),factorscore))
  TSF <- plyr::ddply(TSF, ~ date, plyr::mutate, group = as.numeric(ggplot2::cut_number(factorscore,3)))
  tmp.TS1 <- TSF[TSF$group==1,c("date","stockID")]
  tmp.TS1 <- plyr::ddply(tmp.TS1, ~ date, plyr::mutate, recnum = seq(1,length(date)))
  tmp.TS3 <- TSF[TSF$group==3,c("date","stockID")]
  tmp.TS3 <- plyr::ddply(tmp.TS3, ~ date, plyr::mutate, recnum = seq(1,length(date)))

  tmp <- plyr::ddply(tmp.TS1,~date,plyr::summarise,nstock=length(date))
  tmp.date <- data.frame(date=getRebDates(min(TS$date),max(TS$date),rebFreq = 'day'))
  tmp.date$nstock <- tmp$nstock[findInterval(tmp.date$date,tmp$date)]
  tmp.date$datecor <- tmp$date[findInterval(tmp.date$date,tmp$date)]
  TS1 <- data.frame(datenew=rep(tmp.date$date,tmp.date$nstock),
                    date=rep(tmp.date$datecor,tmp.date$nstock))
  TS1 <- plyr::ddply(TS1, ~ datenew, plyr::mutate, recnum = seq(1,length(datenew)))
  TS1 <- merge.x(TS1,tmp.TS1)
  TS1 <- TS1[,c("datenew","stockID")]
  colnames(TS1) <- c("date","stockID")

  tmp <- plyr::ddply(tmp.TS3,~date,plyr::summarise,nstock=length(date))
  tmp.date <- data.frame(date=getRebDates(min(TS$date),max(TS$date),rebFreq = 'day'))
  tmp.date$nstock <- tmp$nstock[findInterval(tmp.date$date,tmp$date)]
  tmp.date$datecor <- tmp$date[findInterval(tmp.date$date,tmp$date)]
  TS3 <- data.frame(datenew=rep(tmp.date$date,tmp.date$nstock),
                    date=rep(tmp.date$datecor,tmp.date$nstock))
  TS3 <- plyr::ddply(TS3, ~ datenew, plyr::mutate, recnum = seq(1,length(datenew)))
  TS3 <- merge.x(TS3,tmp.TS3)
  TS3 <- TS3[,c("datenew","stockID")]
  colnames(TS3) <- c("date","stockID")

  qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.DailyReturn 'stockRtn'
              from QT_DailyQuote t where t.TradingDay>=",rdate2int(min(TS$date)),
              " and t.TradingDay<=",rdate2int(max(TS$date)))
  con <- db.quant()
  re <- sqlQuery(con,qr)
  close(con)
  re$date <- intdate2r(re$date)

  TSR1 <- merge.x(TS1,re)
  TSR3 <- merge.x(TS3,re)
  R1 <- TSR1[,c("date","stockRtn")]
  R1 <- R1[!is.na(R1$stockRtn),]
  R1 <- plyr::ddply(R1,~date,plyr::summarise,dailtRtn=mean(stockRtn,na.rm = T))
  R3 <- TSR3[,c("date","stockRtn")]
  R3 <- R3[!is.na(R3$stockRtn),]
  R3 <- plyr::ddply(R3,~date,plyr::summarise,dailtRtn=mean(stockRtn,na.rm = T))

  rtn <- merge(R1,R3,by='date')
  colnames(rtn) <- c('date','H','L')
  rtn$stockID <- c('EI000985')
  rtn$factorName <- c('HML')
  rtn$factorScore <- rtn$H-rtn$L
  rtn$date <- rdate2int(rtn$date)
  rtn <- rtn[,c('date','stockID','factorName','factorScore')]


  con <- db.local()
  if(dbExistsTable(con,'QT_FactorScore_amtao')){
    dbWriteTable(con,'QT_FactorScore_amtao',rtn,overwrite=F,append=T)
  }else{
    dbWriteTable(con,'QT_FactorScore_amtao',rtn)
  }
  dbDisconnect(con)
  return('Done!')
}



lcdb.update.FF3 <- function(){

  update.SMB <- function(){
    con <- db.local()
    begT <- dbGetQuery(con,"select max(date) 'endDate' from QT_FactorScore_amtao where factorName='SMB'")[[1]]
    dbDisconnect(con)
    begT <- trday.nearby(intdate2r(begT),by = 1)
    endT <- Sys.Date()-1
    if(begT>endT){
      return()
    }
    tmp.begT <- begT - lubridate::days(lubridate::day(begT))
    tmp.endT <- endT - lubridate::days(lubridate::day(endT))
    if(tmp.begT==tmp.endT) tmp.begT <- trday.nearby(tmp.begT,by = 0)
    RebDates <- getRebDates(tmp.begT,tmp.endT,'month')
    TS <- getTS(RebDates,'EI000985')
    TSF <- gf_lcfs(TS,'F000002')
    TSF <- plyr::ddply(TSF,"date",transform,factorscore=ifelse(is.na(factorscore),median(factorscore,na.rm=TRUE),factorscore))
    TSF <- plyr::ddply(TSF, ~ date, plyr::mutate, group = as.numeric(ggplot2::cut_number(factorscore,3)))
    tmp.TS1 <- TSF[TSF$group==1,c("date","stockID")]
    tmp.TS1 <- plyr::ddply(tmp.TS1, ~ date, plyr::mutate, recnum = seq(1,length(date)))
    tmp.TS3 <- TSF[TSF$group==3,c("date","stockID")]
    tmp.TS3 <- plyr::ddply(tmp.TS3, ~ date, plyr::mutate, recnum = seq(1,length(date)))

    tmp <- plyr::ddply(tmp.TS1,~date,plyr::summarise,nstock=length(date))
    tmp.date <- data.frame(date=getRebDates(begT,endT,rebFreq = 'day'))
    tmp.date$nstock <- tmp$nstock[findInterval(tmp.date$date,tmp$date)]
    tmp.date$datecor <- tmp$date[findInterval(tmp.date$date,tmp$date)]
    TS1 <- data.frame(datenew=rep(tmp.date$date,tmp.date$nstock),
                      date=rep(tmp.date$datecor,tmp.date$nstock))
    TS1 <- plyr::ddply(TS1, ~ datenew, plyr::mutate, recnum = seq(1,length(datenew)))
    TS1 <- merge.x(TS1,tmp.TS1)
    TS1 <- TS1[,c("datenew","stockID")]
    colnames(TS1) <- c("date","stockID")

    tmp <- plyr::ddply(tmp.TS3,~date,plyr::summarise,nstock=length(date))
    tmp.date <- data.frame(date=getRebDates(begT,endT,rebFreq = 'day'))
    tmp.date$nstock <- tmp$nstock[findInterval(tmp.date$date,tmp$date)]
    tmp.date$datecor <- tmp$date[findInterval(tmp.date$date,tmp$date)]
    TS3 <- data.frame(datenew=rep(tmp.date$date,tmp.date$nstock),
                      date=rep(tmp.date$datecor,tmp.date$nstock))
    TS3 <- plyr::ddply(TS3, ~ datenew, plyr::mutate, recnum = seq(1,length(datenew)))
    TS3 <- merge.x(TS3,tmp.TS3)
    TS3 <- TS3[,c("datenew","stockID")]
    colnames(TS3) <- c("date","stockID")

    qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.DailyReturn 'stockRtn'
                from QT_DailyQuote t where t.TradingDay>=",rdate2int(begT),
                " and t.TradingDay<=",rdate2int(endT))
    con <- db.quant()
    re <- sqlQuery(con,qr)
    odbcCloseAll()
    re$date <- intdate2r(re$date)

    TSR1 <- merge.x(TS1,re)
    TSR3 <- merge.x(TS3,re)
    R1 <- TSR1[,c("date","stockRtn")]
    R1 <- R1[!is.na(R1$stockRtn),]
    R1 <- plyr::ddply(R1,~date,plyr::summarise,dailtRtn=mean(stockRtn,na.rm = T))
    R3 <- TSR3[,c("date","stockRtn")]
    R3 <- R3[!is.na(R3$stockRtn),]
    R3 <- plyr::ddply(R3,~date,plyr::summarise,dailtRtn=mean(stockRtn,na.rm = T))

    rtn <- merge(R1,R3,by='date')
    colnames(rtn) <- c('date','S','B')
    rtn$stockID <- c('EI000985')
    rtn$factorName <- c('SMB')
    rtn$factorScore <- rtn$S-rtn$B
    rtn$date <- rdate2int(rtn$date)
    rtn <- rtn[,c('date','stockID','factorName','factorScore')]

    con <- db.local()
    dbWriteTable(con,'QT_FactorScore_amtao',rtn,overwrite=F,append=T)
    dbDisconnect(con)
  }

  update.HML <- function(){
    con <- db.local()
    begT <- dbGetQuery(con,"select max(date) 'endDate' from QT_FactorScore_amtao where factorName='HML'")[[1]]
    dbDisconnect(con)
    begT <- trday.nearby(intdate2r(begT),by = 1)
    endT <- Sys.Date()-1
    if(begT>endT){
      return()
    }
    tmp.begT <- begT - lubridate::days(lubridate::day(begT))
    tmp.endT <- endT - lubridate::days(lubridate::day(endT))
    if(tmp.begT==tmp.endT) tmp.begT <- trday.nearby(tmp.begT,by = 0)
    RebDates <- getRebDates(tmp.begT,tmp.endT,'month')
    TS <- getTS(RebDates,'EI000985')
    TSF <- gf_lcfs(TS,'F000006')
    TSF <- plyr::ddply(TSF,"date",transform,factorscore=ifelse(is.na(factorscore),median(factorscore,na.rm=TRUE),factorscore))
    TSF <- plyr::ddply(TSF,"date", plyr::mutate, group = as.numeric(ggplot2::cut_number(factorscore,3)))
    tmp.TS1 <- TSF[TSF$group==1,c("date","stockID")]
    tmp.TS1 <- plyr::ddply(tmp.TS1,"date", plyr::mutate, recnum = seq(1,length(date)))
    tmp.TS3 <- TSF[TSF$group==3,c("date","stockID")]
    tmp.TS3 <- plyr::ddply(tmp.TS3,"date", plyr::mutate, recnum = seq(1,length(date)))

    tmp <- plyr::ddply(tmp.TS1,~date,plyr::summarise,nstock=length(date))
    tmp.date <- data.frame(date=getRebDates(begT,endT,rebFreq = 'day'))
    tmp.date$nstock <- tmp$nstock[findInterval(tmp.date$date,tmp$date)]
    tmp.date$datecor <- tmp$date[findInterval(tmp.date$date,tmp$date)]
    TS1 <- data.frame(datenew=rep(tmp.date$date,tmp.date$nstock),
                      date=rep(tmp.date$datecor,tmp.date$nstock))
    TS1 <- plyr::ddply(TS1, ~ datenew, plyr::mutate, recnum = seq(1,length(datenew)))
    TS1 <- merge.x(TS1,tmp.TS1)
    TS1 <- TS1[,c("datenew","stockID")]
    colnames(TS1) <- c("date","stockID")

    tmp <- plyr::ddply(tmp.TS3,~date,plyr::summarise,nstock=length(date))
    tmp.date <- data.frame(date=getRebDates(begT,endT,rebFreq = 'day'))
    tmp.date$nstock <- tmp$nstock[findInterval(tmp.date$date,tmp$date)]
    tmp.date$datecor <- tmp$date[findInterval(tmp.date$date,tmp$date)]
    TS3 <- data.frame(datenew=rep(tmp.date$date,tmp.date$nstock),
                      date=rep(tmp.date$datecor,tmp.date$nstock))
    TS3 <- plyr::ddply(TS3, ~ datenew, plyr::mutate, recnum = seq(1,length(datenew)))
    TS3 <- merge.x(TS3,tmp.TS3)
    TS3 <- TS3[,c("datenew","stockID")]
    colnames(TS3) <- c("date","stockID")

    qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.DailyReturn 'stockRtn'
                from QT_DailyQuote t where t.TradingDay>=",rdate2int(begT),
                " and t.TradingDay<=",rdate2int(endT))
    con <- db.quant()
    re <- sqlQuery(con,qr)
    odbcCloseAll()
    re$date <- intdate2r(re$date)

    TSR1 <- merge.x(TS1,re)
    TSR3 <- merge.x(TS3,re)
    R1 <- TSR1[,c("date","stockRtn")]
    R1 <- R1[!is.na(R1$stockRtn),]
    R1 <- plyr::ddply(R1,~date,plyr::summarise,dailtRtn=mean(stockRtn,na.rm = T))
    R3 <- TSR3[,c("date","stockRtn")]
    R3 <- R3[!is.na(R3$stockRtn),]
    R3 <- plyr::ddply(R3,~date,plyr::summarise,dailtRtn=mean(stockRtn,na.rm = T))

    rtn <- merge(R1,R3,by='date')
    colnames(rtn) <- c('date','H','L')
    rtn$stockID <- c('EI000985')
    rtn$factorName <- c('HML')
    rtn$factorScore <- rtn$H-rtn$L
    rtn$date <- rdate2int(rtn$date)
    rtn <- rtn[,c('date','stockID','factorName','factorScore')]


    con <- db.local()
    dbWriteTable(con,'QT_FactorScore_amtao',rtn,overwrite=F,append=T)
    dbDisconnect(con)
  }

  con <- db.local()
  qr <- paste("select * from SecuMain where ID='EI000985'")
  re <- dbGetQuery(con,qr)
  dbDisconnect(con)
  if(nrow(re)==0) add.index.lcdb(indexID="EI000985")

  update.SMB()
  update.HML()
  return('Done!')
}


gf.F_ROE_new <- function(TS){

  tmp <- brkQT(substr(unique(TS$stockID),3,8))
  qr <- paste("SELECT convert(varchar,CON_DATE,112) 'date',
              'EQ'+STOCK_CODE 'stockID',C12 'F_ROE_1'
              FROM CON_FORECAST_STK
              where CON_TYPE=1 and RPT_TYPE=4 and STOCK_TYPE=1
              and RPT_DATE=year(CON_DATE)
              and STOCK_CODE in",tmp,
              " and CON_DATE>=",QT(min(TS$date)),
              " and CON_DATE<=",QT(max(TS$date)))
  con <- db.cs()
  re <- sqlQuery(con,qr,stringsAsFactors=F)
  odbcClose(con)
  re$date <- intdate2r(re$date)
  re <- plyr::arrange(re,date,stockID)
  re <- reshape2::dcast(re,date~stockID,value.var = 'F_ROE_1')
  re <- zoo::na.locf(re)
  re <- reshape2::melt(re,id='date',variable.name='stockID',value.name = "F_ROE_1",na.rm=T)
  re$date <- as.Date(re$date)
  re$F_ROE_1 <- as.numeric(re$F_ROE_1)
  TSF <- merge.x(TS,re)
  TSF$stockID <- as.character(TSF$stockID)
  return(TSF)
}



rmSuspend.nextday <- function(TS){

  con <- db.local()
  TS$tmpdate <- trday.nearby(TS$date,by=1)
  TS$tmpdate <- rdate2int(TS$tmpdate)
  dbWriteTable(con,'yrf_tmp',TS[,c('tmpdate','stockID')],overwrite=T,append=F,row.names=F)
  qr <- "SELECT * FROM yrf_tmp y
  LEFT JOIN QT_UnTradingDay u
  ON y.tmpdate=u.TradingDay and y.stockID=u.ID"
  re <- dbGetQuery(con,qr)
  re <- re[is.na(re$ID),c("tmpdate","stockID")]
  re$flag <- 1
  re <- dplyr::left_join(TS,re,by=c('tmpdate','stockID'))
  re <- re[!is.na(re$flag),c('date','stockID')]

  dbDisconnect(con)
  return(re)
}



rmSuspend.today <- function(TS){

  con <- db.local()
  TS$date <- rdate2int(TS$date)
  dbWriteTable(con,'yrf_tmp',TS,overwrite=T,append=F,row.names=F)
  qr <- "SELECT * FROM yrf_tmp y
  LEFT JOIN QT_UnTradingDay u
  ON y.date=u.TradingDay and y.stockID=u.ID"
  re <- dbGetQuery(con,qr)
  re <- re[is.na(re$ID),c("date","stockID")]
  re$date <- intdate2r(re$date)

  dbDisconnect(con)
  return(re)
}



rmNegativeEvents.AnalystDown <- function(TS){
  TSF <- gf.F_NP_chg(TS,span='w4')
  TSF <- dplyr::filter(TSF,is.na(factorscore) | factorscore>(-1))
  TS <- TSF[,c('date','stockID')]
  return(TS)
}


rmNegativeEvents.PPUnFrozen <- function(TS,bar=5){
  TS$date_end <- trday.nearby(TS$date,5)
  begT <- min(TS$date)
  endT <- max(TS$date_end)
  con <- db.jy()
  qr <- paste("SELECT CONVERT(VARCHAR,[StartDateForFloating],112) 'date',
  'EQ'+s.SecuCode 'stockID'
  FROM LC_SharesFloatingSchedule lc,SecuMain s
  where lc.InnerCode=s.InnerCode
  and lc.SourceType in (24,25) and Proportion1>=",bar,
              " and StartDateForFloating>=",QT(begT), " and StartDateForFloating<=",QT(endT),
              "order by lc.StartDateForFloating,s.SecuCode")
  re <- sqlQuery(con,qr)
  odbcClose(con)
  re$date <- intdate2r(re$date)
  re$date_from <- trday.nearby(re$date,-4)
  re <- re %>% rowwise() %>%
    do(data.frame(date=getRebDates(.$date_from, .$date,'day'),
                  stockID=rep(.$stockID,5)))
  suppressWarnings(re <- dplyr::setdiff(TS[,c('date','stockID')],re))
  return(re)
}


#' lcfs.update.amtao
#'
#' @examples
#'
#' factorFun="gf.ILLIQ"
#' factorPar=""
#' factorDir=1
#' factorID="F000018"
#' begT = as.Date("2000-01-01")
#' endT = Sys.Date()-1
#' splitNbin = "month"
#' lcfs.update.amtao()
lcfs.update.amtao <- function(factorFun,factorPar,factorDir,factorID,begT,endT,splitNbin) {

  con <- db.local()

  loopT <- dbGetQuery(con,"select distinct tradingday from QT_FactorScore order by tradingday")[[1]]
  loopT <- loopT[loopT>=rdate2int(begT) & loopT<=rdate2int(endT)]
  loopT.L <- split(loopT,cut(intdate2r(loopT),splitNbin))

  subfun <- function(Ti){
    cat(paste(" ",min(Ti),"to",max(Ti)," ...\n"))
    dates <- paste(Ti,collapse=",")
    TS <- dbGetQuery(con,paste("select TradingDay as date, ID as stockID from QT_FactorScore where TradingDay in (",dates,")"))
    TS$date <- intdate2r(TS$date)
    TS <- plyr::arrange(TS,date,stockID)
    TSF <- getRawFactor(TS,factorFun,factorPar)
    TSF$date <- rdate2int(TSF$date)
    TSF <- renameCol(TSF,src="factorscore",tgt=factorID)

    for(Tij in Ti){ # update the factorscore day by day.
      #     Tij <- Ti[1]
      # cat(paste(" ",Tij))
      dbWriteTable(con,"yrf_tmp",TSF[TSF$date==Tij,],overwrite=TRUE,append=FALSE,row.names=FALSE)
      qr <- paste("UPDATE QT_FactorScore
                SET ",factorID,"= (SELECT ",factorID," FROM yrf_tmp WHERE yrf_tmp.stockID =QT_FactorScore.ID)
                WHERE QT_FactorScore.ID = (SELECT stockID FROM yrf_tmp WHERE yrf_tmp.stockID =QT_FactorScore.ID)
                and QT_FactorScore.TradingDay =",Tij)
      res <- dbSendQuery(con,qr)
      dbClearResult(res)
    }
    gc()
  }

  cat(paste("Function lcfs.add: updateing factor score of",factorID,".... \n"))
  plyr::l_ply(loopT.L, subfun, .progress = plyr::progress_text(style=3))
  dbDisconnect(con)
}






