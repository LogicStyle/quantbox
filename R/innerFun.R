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





lcdb.build.QT_IndexTiming<- function(){

  subfun <- function(indexDate){
    tmp <- brkQT(substr(indexDate$indexID,3,8))

    #get index component
    qr <- paste("select 'EI'+s1.SecuCode 'indexID','EQ'+s2.SecuCode 'stockID',
                convert(varchar(8),l.InDate,112) 'InDate',
                convert(varchar(8),l.OutDate,112) 'OutDate'
                from LC_IndexComponent l
                LEFT join SecuMain s1 on l.IndexInnerCode=s1.InnerCode
                LEFT join SecuMain s2 on l.SecuInnerCode=s2.InnerCode
                where s1.SecuCode in",tmp,
                " order by s1.SecuCode,l.InDate")
    con <- db.jy()
    indexComp <- sqlQuery(con,qr)
    odbcClose(con)
    con <- db.local()
    dbWriteTable(con, name="amtao_tmp", value=indexComp, row.names = FALSE, overwrite = TRUE)
    dbDisconnect(con)

    #correct begT
    tmpdate <- plyr::ddply(indexComp,'indexID',plyr::summarise,mindate=intdate2r(min(InDate)))
    indexDate <- merge(indexDate,tmpdate,by='indexID')
    indexDate$begT <- as.Date(ifelse(indexDate$begT<indexDate$mindate,indexDate$mindate,indexDate$begT),origin = "1970-01-01")
    indexDate <- indexDate[,c("indexID","indexName","begT","endT")]

    for(i in 1:nrow(indexDate)){
      ptm <- proc.time()
      cat(i,':',as.character(indexDate$indexID[i]),rdate2int(indexDate$begT[i]),'\n')

      tmpdate <- rdate2int(getRebDates(indexDate$begT[i],indexDate$endT[i],'day'))
      tmpdate <- data.frame(date=tmpdate)
      con <- db.local()
      dbWriteTable(con, name="yrf_tmp", value=tmpdate, row.names = FALSE, overwrite = TRUE)
      dbDisconnect(con)
      qr <- paste("SELECT a.date as date, b.stockID from yrf_tmp a, amtao_tmp b
                  where b.IndexID=", QT(indexDate$indexID[i]),
                  "and b.InDate<=a.date and (b.OutDate>a.date or b.OutDate IS NULL)")
      TS <- dbGetQuery(db.local(),qr)
      TS$date <- intdate2r(TS$date)
      if(i==1){
        tmp <- getRebDates(min(indexDate$begT),max(indexDate$endT),'day')
        tmp <- getTS(tmp,indexID = 'EI801003')
        alldata <- gf.PE_ttm_raw(tmp)
        alldata <- dplyr::rename(alldata,pettm=factorscore)
        tmp <- gf.PB_mrq_raw(tmp)
        tmp <- dplyr::rename(tmp,pbmrq=factorscore)
        alldata <- merge(alldata,tmp,by=c('date','stockID'))
      }

      TSF <- merge.x(TS,alldata,by=c('date','stockID'))
      TSF <- TSF[!is.na(TSF$pettm),]
      TSF <- TSF[!is.na(TSF$pbmrq),]

      #pe median
      indexvalue <- plyr::ddply(TSF,'date',plyr::summarise,value=median(pettm))
      indexvalue <- cbind(indexID=indexDate$indexID[i],indexName=indexDate$indexName[i],indexvalue,valtype='PE',caltype='median')

      #pe mean
      tmp <- TSF[,c('date','stockID','pettm')]
      tmp <- tmp[tmp$pettm>0 & tmp$pettm<1000,]
      colnames(tmp) <- c('date','stockID','factorscore')
      tmp <- RFactorModel:::factor.outlier(tmp,3)
      tmp <- plyr::ddply(tmp,'date',plyr::summarise,value=mean(factorscore))
      tmp <- cbind(indexID=indexDate$indexID[i],indexName=indexDate$indexName[i],tmp,valtype='PE',caltype='mean')
      indexvalue <- rbind(indexvalue,tmp)

      #pb median
      tmp <- plyr::ddply(TSF,'date',plyr::summarise,value=median(pbmrq))
      tmp <- cbind(indexID=indexDate$indexID[i],indexName=indexDate$indexName[i],tmp,valtype='PB',caltype='median')
      indexvalue <- rbind(indexvalue,tmp)

      #pe mean
      tmp <- TSF[,c('date','stockID','pbmrq')]
      tmp <- tmp[tmp$pbmrq>0,]
      colnames(tmp) <- c('date','stockID','factorscore')
      tmp <- RFactorModel:::factor.outlier(tmp,3)
      tmp <- plyr::ddply(tmp,'date',plyr::summarise,value=mean(factorscore))
      tmp <- cbind(indexID=indexDate$indexID[i],indexName=indexDate$indexName[i],tmp,valtype='PB',caltype='mean')
      indexvalue <- rbind(indexvalue,tmp)


      if(i==1){
        re <- indexvalue
      }else{
        re <- rbind(re,indexvalue)
      }
    }
    return(re)
  }#subfun finished


  indexset <- c('EI000016','EI399005','EI399006','EI000300','EI000905',
                'EI000805','EI000979','EI000808','EI000819','EI000998',
                'EI000934','EI399959','EI000933','EI000932','EI801120')

  tmp <- brkQT(substr(indexset,3,8))
  qr <- paste("select 'EI'+s.SecuCode 'indexID',s.SecuAbbr 'indexName',
              convert(varchar(8),i.PubDate,112) 'begT'
              from LC_IndexBasicInfo i,SecuMain s
              where i.IndexCode=s.InnerCode and s.SecuCode in",tmp)
  con <- db.jy()
  indexDate <- sqlQuery(con,qr)
  odbcClose(con)
  indexDate$begT <- intdate2r(indexDate$begT)
  indexDate[indexDate$begT<as.Date('2005-01-04'),'begT'] <- as.Date('2005-01-04')
  indexDate$endT <- Sys.Date()-1

  re <- subfun(indexDate)
  re$date <- rdate2int(re$date)
  con <- db.local()
  dbWriteTable(con,'QT_IndexTiming',re,row.names=FALSE)
  dbDisconnect(con)
  return('Done!')
}


lcdb.addindex.QT_IndexTiming<- function(indexset){

  subfun <- function(indexDate){
    tmp <- QT(substr(indexDate$indexID,3,8))

    #get index component
    qr <- paste("select 'EI'+s1.SecuCode 'indexID','EQ'+s2.SecuCode 'stockID',
                convert(varchar(8),l.InDate,112) 'InDate',
                convert(varchar(8),l.OutDate,112) 'OutDate'
                from LC_IndexComponent l
                LEFT join SecuMain s1 on l.IndexInnerCode=s1.InnerCode
                LEFT join SecuMain s2 on l.SecuInnerCode=s2.InnerCode
                where s1.SecuCode=",tmp,
                " order by s1.SecuCode,l.InDate")
    con <- db.jy()
    indexComp <- sqlQuery(con,qr)
    odbcClose(con)
    con <- db.local()
    dbWriteTable(con, name="amtao_tmp", value=indexComp, row.names = FALSE, overwrite = TRUE)
    dbDisconnect(con)

    #correct begT
    tmpdate <- plyr::ddply(indexComp,'indexID',plyr::summarise,mindate=intdate2r(min(InDate)))
    indexDate <- merge(indexDate,tmpdate,by='indexID')
    indexDate$begT <- as.Date(ifelse(indexDate$begT<indexDate$mindate,indexDate$mindate,indexDate$begT),origin = "1970-01-01")
    indexDate <- indexDate[,c("indexID","indexName","begT","endT")]

    tmpdate <- rdate2int(getRebDates(indexDate$begT,indexDate$endT,'day'))
    tmpdate <- data.frame(date=tmpdate)
    con <- db.local()
    dbWriteTable(con, name="yrf_tmp", value=tmpdate, row.names = FALSE, overwrite = TRUE)

    qr <- paste("SELECT a.date as date, b.stockID from yrf_tmp a, amtao_tmp b
                where b.IndexID=", QT(indexDate$indexID),
                "and b.InDate<=a.date and (b.OutDate>a.date or b.OutDate IS NULL)")
    TS <- dbGetQuery(con,qr)
    TS$date <- intdate2r(TS$date)

    alldata <- gf.PE_ttm_raw(TS)
    alldata <- dplyr::rename(alldata,pettm=factorscore)
    tmp <- gf.PB_mrq_raw(TS)
    tmp <- dplyr::rename(tmp,pbmrq=factorscore)
    alldata <- merge(alldata,tmp,by=c('date','stockID'))

    TSF <- merge.x(TS,alldata,by=c('date','stockID'))
    TSF <- TSF[!is.na(TSF$pettm),]
    TSF <- TSF[!is.na(TSF$pbmrq),]

    #pe median
    indexvalue <- plyr::ddply(TSF,'date',plyr::summarise,value=median(pettm))
    indexvalue <- cbind(indexID=indexDate$indexID,indexName=indexDate$indexName,indexvalue,valtype='PE',caltype='median')

    #pe mean
    tmp <- TSF[,c('date','stockID','pettm')]
    tmp <- tmp[tmp$pettm>0 & tmp$pettm<1000,]
    colnames(tmp) <- c('date','stockID','factorscore')
    tmp <- RFactorModel:::factor.outlier(tmp,3)
    tmp <- plyr::ddply(tmp,'date',plyr::summarise,value=mean(factorscore))
    tmp <- cbind(indexID=indexDate$indexID,indexName=indexDate$indexName,tmp,valtype='PE',caltype='mean')
    indexvalue <- rbind(indexvalue,tmp)

    #pb median
    tmp <- plyr::ddply(TSF,'date',plyr::summarise,value=median(pbmrq))
    tmp <- cbind(indexID=indexDate$indexID,indexName=indexDate$indexName,tmp,valtype='PB',caltype='median')
    indexvalue <- rbind(indexvalue,tmp)

    #pe mean
    tmp <- TSF[,c('date','stockID','pbmrq')]
    tmp <- tmp[tmp$pbmrq>0,]
    colnames(tmp) <- c('date','stockID','factorscore')
    tmp <- RFactorModel:::factor.outlier(tmp,3)
    tmp <- plyr::ddply(tmp,'date',plyr::summarise,value=mean(factorscore))
    tmp <- cbind(indexID=indexDate$indexID,indexName=indexDate$indexName,tmp,valtype='PB',caltype='mean')
    indexvalue <- rbind(indexvalue,tmp)
    dbDisconnect(con)

    return(indexvalue)
  }#subfun finished

  con1 <- db.local()
  old <- dbGetQuery(con1,"select distinct indexID from QT_IndexTiming")
  indexset <- setdiff(indexset,c(old$indexID))
  if(length(indexset)==0) return('Already in database!')

  qr <- paste("select 'EI'+s.SecuCode 'indexID',s.SecuAbbr 'indexName',
              convert(varchar(8),i.PubDate,112) 'begT'
              from LC_IndexBasicInfo i,SecuMain s
              where i.IndexCode=s.InnerCode and s.SecuCode=",QT(substr(indexset,3,8)))
  con2 <- db.jy()
  indexDate <- sqlQuery(con2,qr)
  odbcClose(con2)
  indexDate$begT <- intdate2r(indexDate$begT)
  indexDate[indexDate$begT<as.Date('2005-01-04'),'begT'] <- as.Date('2005-01-04')
  endT <- dbGetQuery(con1,"select max(date) 'date' from QT_IndexTiming")
  indexDate$endT <- intdate2r(endT$date)

  re <- subfun(indexDate)
  re$date <- rdate2int(re$date)
  dbWriteTable(con1,'QT_IndexTiming',re,overwrite=F,append=T,row.names=FALSE)
  dbDisconnect(con1)
  return('Done!')

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






