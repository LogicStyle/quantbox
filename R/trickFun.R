#' alpha portfolio demo data
#'
#' part of index EI000905's alpha portfolio data set.
#' @format A data frame with 1149 rows and 3 variables.
"portdemo"


#' assets return demo dataset.
#'
#' A dataset containing stock index(000985.CSI), bond index(037.CS) and commodity(GC00.CMX) daily return data since 2009.
#'
#' @format A data frame with 2865 rows and 4 variables:
#' \describe{
#'   \item{date}{date type}
#'   \item{stock}{stock index return}
#'   \item{bond}{bond index return}
#'   \item{commodity}{commodity index return}
#' }
"rtndemo"



# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ======================
# ===================== series of quant report functions  ===========================
# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ======================




#' bank rotation
#'
#' This is a bank stocks rotation strategy.Its idea comes from
#' \url{https://www.jisilu.cn/question/50176}.
#'
#' @author Andrew Dow
#' @param begT is strategy's begin date
#' @param endT is strategy's end date
#' @param chgBar is rotation's bar
#' @param fee is trading cost
#' @return a list of bank factorscore and strategy's historical return.
#' @examples
#' begT <- as.Date('2014-01-03')
#' endT <- Sys.Date()-1
#' chgBar <- 0.2
#' bankport <- bank.rotation(begT,endT,chgBar)
#' @export
bank.rotation <- function(begT,endT=Sys.Date()-1,chgBar=0.2,fee=0.003,
                             pbfun=c('gf.PB_lyr','gf.PB_mrq'),
                             roefun=c('gf.ROE_ttm','gf.ROE','gf.F_ROE')){
  pbfun <- match.arg(pbfun)
  roefun <- match.arg(roefun)

  rebDates <- getRebDates(begT,endT,rebFreq = 'day')
  TS <- getTS(rebDates,indexID = 'ES33480000')
  qr <- paste("select ID 'stockID',ListedDate from SecuMain where ID in",brkQT(unique(TS$stockID)))
  ipo <- queryAndClose.dbi(db.local(),qr)
  ipo$ListedDate <- intdate2r(ipo$ListedDate)
  TS <- left_join(TS,ipo,by='stockID')
  TS <- na.omit(TS)
  TS <- TS[TS$date>TS$ListedDate,c('date','stockID'),]
  TS <- rmSuspend(TS)

  TSF <- getTSF(TS,pbfun)
  tmp <- getTSF(TS,roefun)
  tmp <- tmp[,c('date','stockID','factorscore')]
  if(substr(roefun,4,4)!='F'){
    tmp$factorscore <- tmp$factorscore/100
  }
  TSF <- left_join(TSF,tmp,by=c('date','stockID'))
  colnames(TSF) <- c("date","stockID","PB","ROE")
  TSF <- na.omit(TSF)
  TSF$factorscore <- log(TSF$PB*2,base=(1+TSF$ROE))
  TSF <- dplyr::arrange(TSF,date,factorscore)

  #get bank
  dates <- unique(TSF$date)
  bankPort <- TSF[1,c('date','stockID')]
  for(i in 2:length(dates)){
    oldstock <- tail(bankPort$stockID,1)
    tmp <- TSF[TSF$date==dates[i],c('date','stockID',"factorscore")]
    if(oldstock %in% tmp$stockID){
      newscore <- tmp[1,'factorscore']
      oldscore <- tmp[tmp$stockID==oldstock,'factorscore']*(1-chgBar)
      if(newscore<oldscore){
        bankPort <- rbind(bankPort,tmp[1,c("date","stockID")])
        next
      }
    }
    bankPort <- rbind(bankPort,data.frame(date=dates[i],stockID=oldstock))
  }

  #get bank daily return
  qr <- paste("SELECT TradingDay 'date',ID 'stockID'
              ,PrevClosePrice 'pre_close',OpenPrice 'open'
              ,ClosePrice 'close',DailyReturn 'pct_chg'
              FROM QT_DailyQuote
              where ID in",brkQT(unique(bankPort$stockID)),
              " and TradingDay>=",rdate2int(min(bankPort$date)),
              " and TradingDay<=",rdate2int(max(bankPort$date)),
              " order by TradingDay,ID")
  re <- queryAndClose.odbc(db.quant(),qr,stringsAsFactors=F)
  re <- transform(re,date=intdate2r(date))

  TSR <- left_join(bankPort,re,by = c("date", "stockID"))

  for(i in 1:nrow(TSR)){
    if(i==1){
      TSR$pct_chg[i] <- 0
      next
    }
    if(i==2){
      TSR$pct_chg[i] <- TSR$close[i]/TSR$open[i]-1-fee
      next
    }

    if(TSR$stockID[i]!=TSR$stockID[i-1]){
      TSR$pct_chg[i] <- re[re$stockID==TSR$stockID[i-1] & re$date==TSR$date[i],'pct_chg']
    }else if(TSR$stockID[i]==TSR$stockID[i-1] & TSR$stockID[i-1]!=TSR$stockID[i-2]){
      tmp.open <- re[re$stockID==TSR$stockID[i-2] & re$date==TSR$date[i],'open']
      tmp.close <- re[re$stockID==TSR$stockID[i-2] & re$date==TSR$date[i-1],'close']
      tmp.rtn <- tmp.open/tmp.close-1-fee
      TSR$pct_chg[i] <- tmp.rtn+TSR$close[i]/TSR$open[i]-1-fee
    }
  }

  #get bench mark return
  bench <- getIndexQuote('EI801780',min(TSR$date),max(TSR$date),variables = c('pre_close','close'),datasrc = 'jy')
  bench$indexRtn <- bench$close/bench$pre_close-1
  bench <- bench[,c("date","indexRtn")]

  rtn <- left_join(bench,TSR[,c('date','pct_chg')],by='date')
  colnames(rtn) <- c("date","indexRtn","bankRtn")
  rtn <- na.omit(rtn)
  rtn <- xts::xts(rtn[,-1],order.by = rtn[,1])

  bankPort$mark <- 'hold'
  TSF <- left_join(TSF,bankPort,by=c('date','stockID'))
  TSF$stockName <- stockID2name(TSF$stockID)
  TSF <- TSF[,c("date", "stockID",'stockName',"PB","ROE","factorscore",'mark')]
  return(list(TSF=TSF,rtn=rtn))
}





# ===================== ~ index valuation  ====================
#' lcdb.build.QT_IndexValuation
#'
#' @name index_valuation
#' @rdname index_valuation
#' @export
#' @examples
#' lcdb.build.QT_IndexValuation()
lcdb.build.QT_IndexValuation<- function(){

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
    indexComp <- queryAndClose.odbc(db.jy(),qr)
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
        alldata <- gf.PE_ttm(tmp,fillna = FALSE)
        alldata <- dplyr::rename(alldata,pettm=factorscore)
        tmp <- gf.PB_mrq(tmp,fillna = FALSE)
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
      tmp <- RFactorModel:::factor_outlier(tmp,method='sd',par = 3,sectorAttr = NULL)
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
      tmp <- RFactorModel:::factor_outlier(tmp,method='sd',par = 3,sectorAttr = NULL)
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
  indexDate <- queryAndClose.odbc(db.jy(),qr)
  indexDate$begT <- intdate2r(indexDate$begT)
  indexDate[indexDate$begT<as.Date('2005-01-04'),'begT'] <- as.Date('2005-01-04')
  indexDate$endT <- Sys.Date()-1

  re <- subfun(indexDate)
  re$date <- rdate2int(re$date)
  con <- db.local()
  dbWriteTable(con,'QT_IndexValuation',re,row.names=FALSE)
  dbDisconnect(con)
  return('Done!')
}

#' lcdb.addindex.QT_IndexValuation
#'
#' @rdname index_valuation
#' @export
#' @examples
#' lcdb.addindex.QT_IndexValuation()
lcdb.addindex.QT_IndexValuation<- function(indexset){

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
    indexComp <- queryAndClose.odbc(db.jy(),qr)
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

    alldata <- gf.PE_ttm(TS,fillna = FALSE)
    alldata <- dplyr::rename(alldata,pettm=factorscore)
    tmp <- gf.PB_mrq(TS,fillna = FALSE)
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
    tmp <- RFactorModel:::factor_outlier(tmp,method='sd',par = 3,sectorAttr = NULL)
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
    tmp <- RFactorModel:::factor_outlier(tmp,method='sd',par = 3,sectorAttr = NULL)
    tmp <- plyr::ddply(tmp,'date',plyr::summarise,value=mean(factorscore))
    tmp <- cbind(indexID=indexDate$indexID,indexName=indexDate$indexName,tmp,valtype='PB',caltype='mean')
    indexvalue <- rbind(indexvalue,tmp)
    dbDisconnect(con)

    return(indexvalue)
  }#subfun finished

  con1 <- db.local()
  old <- dbGetQuery(con1,"select distinct indexID from QT_IndexValuation")
  indexset <- setdiff(indexset,c(old$indexID))
  if(length(indexset)==0) return('Already in database!')

  qr <- paste("select 'EI'+s.SecuCode 'indexID',s.SecuAbbr 'indexName',
              convert(varchar(8),i.PubDate,112) 'begT'
              from LC_IndexBasicInfo i,SecuMain s
              where i.IndexCode=s.InnerCode and s.SecuCode=",QT(substr(indexset,3,8)))
  indexDate <- queryAndClose.odbc(db.jy(),qr)
  indexDate$begT <- intdate2r(indexDate$begT)
  indexDate[indexDate$begT<as.Date('2005-01-04'),'begT'] <- as.Date('2005-01-04')
  endT <- dbGetQuery(con1,"select max(date) 'date' from QT_IndexValuation")
  indexDate$endT <- intdate2r(endT$date)

  re <- subfun(indexDate)
  re$date <- rdate2int(re$date)
  dbWriteTable(con1,'QT_IndexValuation',re,overwrite=F,append=T,row.names=FALSE)
  dbDisconnect(con1)
  return('Done!')

}


#' lcdb.update.QT_IndexValuation
#'
#' @rdname index_valuation
#' @author Andrew Dow
#' @examples
#' lcdb.update.QT_IndexValuation()
#' @export
lcdb.update.QT_IndexValuation<- function(){

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

    for(i in 1:nrow(indexDate)){
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
        add.index.lcdb('EI801003')
        tmp <- getTS(tmp,indexID = 'EI801003')
        alldata <- gf.PE_ttm(tmp,fillna = FALSE)
        alldata <- dplyr::rename(alldata,pettm=factorscore)
        tmp <- gf.PB_mrq(tmp,fillna = FALSE)
        tmp <- dplyr::rename(tmp,pbmrq=factorscore)
        alldata <- merge(alldata,tmp,by=c('date','stockID'))
      }

      TSF <- merge.x(TS,alldata,by=c('date','stockID'))
      TSF <- dplyr::filter(TSF,!is.na(pettm),!is.na(pbmrq))

      #pe median
      indexvalue <- plyr::ddply(TSF,'date',plyr::summarise,value=median(pettm))
      indexvalue <- cbind(indexID=indexDate$indexID[i],indexName=indexDate$indexName[i],indexvalue,valtype='PE',caltype='median')

      #pe mean
      tmp <- TSF[,c('date','stockID','pettm')]
      tmp <- dplyr::filter(tmp,pettm>0,pettm<1000)
      colnames(tmp) <- c('date','stockID','factorscore')
      tmp <- RFactorModel:::factor_outlier(tmp,method='sd',par = 3,sectorAttr = NULL)
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
      tmp <- RFactorModel:::factor_outlier(tmp,method='sd',par = 3,sectorAttr = NULL)
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

  con <- db.local()
  begT <- dbGetQuery(con,"select max(date) 'date' from QT_IndexValuation")
  begT <- trday.nearby(intdate2r(begT$date),by=1)
  endT <- trday.nearby(Sys.Date(),by=-1)
  if(begT>endT){
    return('Done!')
  }else{
    indexDate <- dbGetQuery(con,"select distinct indexID,indexName from QT_IndexValuation")
    indexDate$begT <- begT
    indexDate$endT <- endT

    re <- subfun(indexDate)
    re$date <- rdate2int(re$date)
    dbWriteTable(con,'QT_IndexValuation',re,overwrite=F,append=T,row.names=FALSE)
    dbDisconnect(con)
    return('Done!')
  }
}


#' getIndexValuation
#'
#' @rdname index_valuation
#' @author Andrew Dow
#' @examples
#' re <- getIV()
#' @export
getIV <- function(valtype=c('PE','PB'),caltype=c('median','mean'),
                  begT=as.Date('2005-01-04'),endT=Sys.Date()-1){
  valtype <- match.arg(valtype)
  caltype <- match.arg(caltype)

  qr <- paste("select indexID,indexName,date,value from QT_IndexValuation
              where date>=",rdate2int(begT),
              " and date<=",rdate2int(endT),
              "and valtype=",QT(valtype)," and caltype=",QT(caltype))
  con <- db.local()
  re <- dbGetQuery(con,qr)
  dbDisconnect(con)
  re$date <- intdate2r(re$date)
  name <- paste(valtype,caltype,sep='_')
  colnames(re) <- c("indexID","indexName","date",name)
  re <- arrange(re,indexID,date)

  Nindex <- unique(re[,c('indexID','indexName')])
  result <- data.frame()
  for(i in 1:nrow(Nindex)){
    Data <- re[re$indexID==Nindex$indexID[i],c('date',name)]
    Datats <- xts::xts(Data[,-1],order.by = Data[,1])
    Datats <- TTR::runPercentRank(Datats, n = 250, cumulative = T, exact.multiplier = 0.5)
    Datats <-  data.frame(date=zoo::index(Datats), value=zoo::coredata(Datats))
    Datats <- Datats[251:nrow(Datats),]
    Datats <- cbind(Nindex$indexID[i],Nindex$indexName[i],Datats)
    colnames(Datats) <- c("indexID","indexName","date","percentRank")
    result <- rbind(result,Datats)
  }
  result <- merge.x(result,re,by = c("indexID","indexName","date"))
  result <- arrange(result,date,indexID)
  return(result)
}


# ===================== ~ index timing  ====================


#' LLT timing
#'
#'
#' @author Andrew Dow
#' @examples
#' re <- LLT()
#' @export
LLT <- function(indexID='EI000300',begT=as.Date('2005-01-04'),d=60,trancost=0.001,type=c('LLT','SMA')){
  type <- match.arg(type)

  endT <- Sys.Date()-1
  variables <- c("open","close","pct_chg")
  indexQuote <- getIndexQuote(indexID,begT,endT,variables,datasrc="jy")
  indexQuote$stockID <- NULL

  if(type=='LLT'){
    alpha <- 2/(1+d)

    indexQuote$LLT <- c(indexQuote$close[1:2],rep(0,nrow(indexQuote)-2))
    for(i in 3:nrow(indexQuote)){
      indexQuote$LLT[i] <- (alpha-alpha^2/4)* indexQuote$close[i]+
        (alpha^2/2)*indexQuote$close[i-1]-(alpha-3*alpha^2/4)*indexQuote$close[i-2]+
        2*(1-alpha)*indexQuote$LLT[i-1]-(1-alpha)^2*indexQuote$LLT[i-2]
    }
    indexQuote <- indexQuote[d:nrow(indexQuote),]
    rownames(indexQuote) <- seq(1,nrow(indexQuote))

    indexQuote$pos <- c(0)
    indexQuote$signal <- c('')
    indexQuote$tmp <- indexQuote$pct_chg
    for(i in 3:nrow(indexQuote)){
      if(indexQuote$LLT[i-1]>indexQuote$LLT[i-2] && indexQuote$pos[i-1]==0){
        indexQuote$pos[i] <- 1
        indexQuote$tmp[i] <- indexQuote$close[i]/indexQuote$open[i]-1-trancost
      }else if(indexQuote$LLT[i-1]>indexQuote$LLT[i-2] && indexQuote$pos[i-1]==1){
        indexQuote$pos[i] <- 1
      }else if(indexQuote$LLT[i-1]<indexQuote$LLT[i-2] && indexQuote$pos[i-1]==1){
        indexQuote$pos[i] <- 0
        indexQuote$tmp[i] <- indexQuote$open[i]/indexQuote$close[i-1]-1-trancost
      }

      if(indexQuote$LLT[i]>indexQuote$LLT[i-1] && indexQuote$pos[i]==0){
        indexQuote$signal[i] <- 'buy'
      }else if(indexQuote$LLT[i]>indexQuote$LLT[i-1] && indexQuote$pos[i]==1){
        indexQuote$signal[i] <- 'hold'
      }else if(indexQuote$LLT[i]<indexQuote$LLT[i-1] && indexQuote$pos[i]==1){
        indexQuote$signal[i] <- 'sell'
      }

    }

  }else{
    indexQuote$MA <- TTR::SMA(indexQuote$close,d)
    indexQuote <- indexQuote[d:nrow(indexQuote),]
    rownames(indexQuote) <- seq(1,nrow(indexQuote))

    indexQuote$pos <- c(0)
    indexQuote$signal <- c('')
    indexQuote$tmp <- indexQuote$pct_chg
    for(i in 2:nrow(indexQuote)){
      if(indexQuote$close[i-1]>indexQuote$MA[i-1] && indexQuote$pos[i-1]==0){
        indexQuote$pos[i] <- 1
        indexQuote$tmp[i] <- indexQuote$close[i]/indexQuote$open[i]-1-trancost
      }else if(indexQuote$close[i-1]>indexQuote$MA[i-1] && indexQuote$pos[i-1]==1){
        indexQuote$pos[i] <- 1
      }else if(indexQuote$close[i-1]<indexQuote$MA[i-1] && indexQuote$pos[i-1]==1){
        indexQuote$pos[i] <- 0
        indexQuote$tmp[i] <- indexQuote$open[i]/indexQuote$close[i-1]-1-trancost
      }

      if(indexQuote$close[i]>indexQuote$MA[i] && indexQuote$pos[i]==0){
        indexQuote$signal[i] <- 'buy'
      }else if(indexQuote$close[i]>indexQuote$MA[i] && indexQuote$pos[i]==1){
        indexQuote$signal[i] <- 'hold'
      }else if(indexQuote$close[i]<indexQuote$MA[i] && indexQuote$pos[i]==1){
        indexQuote$signal[i] <- 'sell'
      }
    }
  }

  indexQuote$strRtn <- indexQuote$tmp*indexQuote$pos
  indexQuote <- subset(indexQuote,select=-c(pos,tmp))
  return(indexQuote)

}



#' getIndustryMA
#'
#'
#' @author Andrew Dow
#' @examples
#' re <- getIndustryMA(begT=as.Date('2014-01-04'))
#' @export
getIndustryMA <- function(begT=as.Date('2005-01-04'),endT=Sys.Date()-1){
  con <- db.jy()
  qr <- "select 'EI'+s.SecuCode 'stockID',s.SecuAbbr 'stockName',
  c.DM 'industryCode',c.MS 'industryName'
  from LC_CorrIndexIndustry l,SecuMain s,CT_SystemConst c
  where l.IndustryStandard=24 and s.SecuMarket=83
  and l.IndexCode=s.InnerCode and l.IndustryCode=c.DM
  and c.LB=1804 and c.IVALUE=1"
  indexInd <- sqlQuery(con,qr,stringsAsFactors=F)

  indexQuote <- getIndexQuote(indexInd$stockID,begT,endT,variables='close',datasrc="jy")
  indexQuote <- arrange(indexQuote,stockID,date)

  indexScore <- data.frame()
  for(i in 1:nrow(indexInd)){
    tmp <- indexQuote[indexQuote$stockID==indexInd$stockID[i],]
    tmp <- transform(tmp,MA1=TTR::SMA(close,8),MA2=TTR::SMA(close,13),
                     MA3=TTR::SMA(close,21),MA4=TTR::SMA(close,34),
                     MA5=TTR::SMA(close,55),MA6=TTR::SMA(close,89),
                     MA7=TTR::SMA(close,144),MA8=TTR::SMA(close,233))
    tmp <- na.omit(tmp)
    tmp$score <- (tmp$close>tmp$MA1)+(tmp$close>tmp$MA2)+(tmp$close>tmp$MA3)+(tmp$close>tmp$MA4)+
      (tmp$close>tmp$MA5)+(tmp$close>tmp$MA6)+(tmp$close>tmp$MA7)+(tmp$close>tmp$MA8)
    tmp <- tmp[,c('date','stockID','score')]
    tmp$industryName <- indexInd$industryName[i]
    tmp <- tmp[,c( "date","stockID","industryName","score")]
    indexScore <- rbind(indexScore,tmp)
  }
  indexScore <- arrange(indexScore,date,stockID)
  odbcClose(con)
  return(indexScore)
}


# ===================== ~ get data  ====================

#' private offering fund
#'
#'
#' get private offering fund daily nav and premium and discount ration info
#' @author Andrew Dow
#' @examples
#' re <- POFund(fundID,begT,endT)
#' @export
POFund <- function(fundID,begT,endT){
  tmp <- brkQT(fundID)
  con <- db.jy()
  qr <- paste("SELECT s.SecuCode+'.OF' 'fundID',s.SecuAbbr 'fundName',
              convert(varchar(8),mf.EndDate,112) 'date',mf.UnitNV 'NAV',q.ClosePrice 'close'
              FROM MF_NetValue mf,SecuMain s,QT_DailyQuote q
              where mf.InnerCode=s.InnerCode and mf.InnerCode=q.InnerCode and mf.EndDate=q.TradingDay
              and mf.EndDate>=",QT(begT)," and mf.EndDate<=",QT(endT),
              " and s.SecuCode in ",tmp," order by s.SecuCode,mf.EndDate")
  fund <- sqlQuery(con,qr)
  fund$pre <- fund$close/fund$NAV-1
  fund$date <- intdate2r(fund$date)
  odbcClose(con)
  return(fund)
}







#' getIndexFuturesSpread
#'
#'
#' @author Andrew Dow
#' @examples
#' re <- getIFSpread()
#' @export
getIFSpread <- function(begT=as.Date('2010-04-16'),endT=Sys.Date()-1){
  qr <- paste("select convert(varchar,t.TradingDay,112) 'date',
              t.ContractCode 'stockID',t.ClosePrice 'close',t.BasisValue 'spread',
              convert(varchar,f.EffectiveDate,112) 'effectiveDate',
              convert(varchar,f.LastTradingDate,112) 'lastTradingDate'
              from Fut_TradingQuote t,Fut_ContractMain f
              where t.ContractInnerCode=f.ContractInnerCode and t.ContractCode like 'I%'
              and t.TradingDay>=",QT(begT),
              " and t.TradingDay<=",QT(endT),
              "ORDER by t.TradingDay,t.ContractCode")
  con <- db.jy()
  IFData <- sqlQuery(con,qr)
  odbcClose(con)
  IFData <- transform(IFData,date=intdate2r(date),
                      effectiveDate=intdate2r(effectiveDate),
                      lastTradingDate=intdate2r(lastTradingDate))
  tmp1 <- IFData[substr(IFData$stockID,3,4)=='0Y',c("date","stockID","close","spread")]
  colnames(tmp1) <- c("dateCon","stockIDCon","closeCon","spreadCon")
  tmp2 <- IFData[substr(IFData$stockID,3,4)!='0Y',c("date","stockID","close","spread","effectiveDate","lastTradingDate")]
  IFData <- cbind(tmp1,tmp2)
  if(sum(IFData$dateCon!=IFData$date)>0 | sum(IFData$closeCon- IFData$close)>1 |
     sum(IFData$spreadCon-IFData$spread)>1) stop('cbind fail!')
  IFData <- IFData[,c("date","stockIDCon","stockID","effectiveDate","lastTradingDate","close","spread")]

  IFData$spreadPct <- IFData$spread/(IFData$close-IFData$spread)
  IFData$spreadPctAna <- sign(IFData$spreadPct)*((1+abs(IFData$spreadPct))^(365/as.numeric(IFData$lastTradingDate- IFData$date))-1)
  IFData[IFData$date==IFData$lastTradingDate,'spreadPctAna'] <- 0
  IFData <- IFData[,c("date","stockIDCon","stockID","effectiveDate","lastTradingDate",
                      "close","spread","spreadPct","spreadPctAna")]
  return(IFData)

}


# ===================== ~ grid trading  ====================

#' grid trading with index futures
#'
#'
#' @author Andrew Dow
#' @examples
#' indexID <- 'EI000905'
#' begT <- as.Date('2015-09-01')
#' endT <- Sys.Date()-1
#' para <- list(total=5e6,initPos=2,posChg=1,bar=0.1,tradeCost=1/1000)
#' re <- gridTrade.IF(indexID,begT,endT,para)
#' @export
gridTrade.IF <- function(indexID,begT,endT=Sys.Date()-1,para){

  getData <- function(indexID,begT,endT){
    if(indexID=='EI000300'){
      tmp <- 'IF1%'
    }else if(indexID=='EI000905'){
      tmp <- 'IC1%'
    }else if(indexID=='EI000016'){
      tmp <- 'IH1%'
    }

    #get index future quote
    qr <- paste("select convert(varchar(10),t.TradingDay,112) 'date',
                t.ContractCode 'stockID',
                convert(varchar(10),c.EffectiveDate,112) 'effectiveDate',
                convert(varchar(10),c.LastTradingDate,112) 'lastTradingDate',
                t.ClosePrice 'close'
                from Fut_TradingQuote t,Fut_ContractMain c
                where t.ContractInnerCode=c.ContractInnerCode and
                t.ContractCode like ",QT(tmp),
                " and t.TradingDay>=",QT(begT)," and t.TradingDay<=",QT(endT),
                " order by t.TradingDay,t.ContractCode")
    con <- db.jy()
    indexData <- sqlQuery(con,qr,stringsAsFactors =F)
    odbcClose(con)
    indexData <- transform(indexData,date=intdate2r(date),effectiveDate=intdate2r(effectiveDate),
                           lastTradingDate=intdate2r(lastTradingDate))
    indexData$lastTradingDate <- trday.nearby(indexData$lastTradingDate,by=-1)

    # keep the next quarter contract
    indexData$tmp <- c(0)
    shiftData <- data.frame()
    for(i in 1:nrow(indexData)){
      if(i==1){
        IF.ID <- indexData$stockID[4]
        IF.lastDay <- indexData$lastTradingDate[4]
      }
      if(indexData$stockID[i]==IF.ID && indexData$date[i]<IF.lastDay){
        indexData$tmp[i] <- 1
      }else if(indexData$stockID[i]==IF.ID && indexData$date[i]==IF.lastDay){
        shiftData <- rbind(shiftData,indexData[i,c("date","stockID","close")])
        tmp <- indexData[indexData$date==indexData$date[i],]
        IF.ID <- tmp$stockID[4]
        IF.lastDay <- tmp$lastTradingDate[4]
      }
    }
    indexData <- indexData[indexData$tmp==1,c("date","stockID","close")]

    #get index quote
    tmp <- getIndexQuote(indexID,begT,endT,variables='close',datasrc="jy")
    tmp$stockID <- NULL
    colnames(tmp) <- c("date","benchClose")

    indexData <- merge(indexData,tmp,by='date',all.x=T)
    alldata <- list(indexData=indexData,shiftData=shiftData)
    return(alldata)
  }


  calcData <- function(indexData,shiftData,para){
    if(substr(indexData$stockID[1],1,2) %in% c('IF','IH')){
      multiplier <- 300
    }else if(substr(indexData$stockID[1],1,2)=='IC'){
      multiplier <- 200
    }

    indexData <- transform(indexData,benchPct=benchClose/indexData$benchClose[1]-1,
                           pos=c(0),mv=c(0),cost=c(0),cash=c(0),rtn=c(0),totalasset=c(0),remark=NA)
    for(i in 1:nrow(indexData)){
      #initial
      if(i==1){
        indexData$pos[i] <-para$initPos
        indexData$mv[i] <-indexData$pos[i]*indexData$close[i]*multiplier
        indexData$cost[i] <-para$tradeCost*indexData$mv[i]
        indexData$cash[i] <-para$total-indexData$mv[i]-indexData$cost[i]
        indexData$totalasset[i] <-indexData$mv[i]+indexData$cash[i]
        indexData$rtn[i] <- indexData$totalasset[i]/para$total-1
        indexData$remark[i] <-'initial'
        next
      }

      # shift positions
      if(indexData$stockID[i]!=indexData$stockID[i-1]){
        tmp <- subset(shiftData,stockID==indexData$stockID[i-1] & date==indexData$date[i],select=close)
        indexData$pos[i] <-indexData$pos[i-1]
        indexData$mv[i] <-indexData$pos[i]*indexData$close[i]*multiplier
        indexData$cost[i] <-para$tradeCost*(indexData$mv[i]+indexData$pos[i-1]*tmp$close*multiplier)
        indexData$cash[i] <-indexData$cash[i-1]-indexData$cost[i]+indexData$pos[i-1]*tmp$close*multiplier-indexData$mv[i]
        indexData$totalasset[i] <-indexData$mv[i]+indexData$cash[i]
        indexData$rtn[i] <- indexData$totalasset[i]/indexData$totalasset[i-1]-1
        indexData$remark[i] <-'shift'
      }

      #position change
      if(indexData$benchPct[i]>para$bar){
        todayPos <- para$initPos-floor(indexData$benchPct[i]/para$bar)*para$posChg
      }else if(indexData$benchPct[i]<(-1*para$bar)){
        todayPos <- para$initPos+floor(abs(indexData$benchPct[i]/para$bar))*para$posChg
      }else{
        todayPos <- para$initPos
      }

      if(todayPos<indexData$pos[i-1] & indexData$pos[i-1]>0){
        posChg <- min(indexData$pos[i-1]-todayPos,indexData$pos[i-1])
        #subtract position
        if(is.na(indexData$remark[i])){
          indexData$pos[i] <-indexData$pos[i-1]-posChg
          indexData$mv[i] <-indexData$pos[i]*indexData$close[i]*multiplier
          indexData$cost[i] <-para$tradeCost*posChg*multiplier*indexData$close[i]
          indexData$cash[i] <-indexData$cash[i-1]-indexData$cost[i]+posChg*multiplier*indexData$close[i]
          indexData$totalasset[i] <-indexData$mv[i]+indexData$cash[i]
          indexData$rtn[i] <- indexData$totalasset[i]/indexData$totalasset[i-1]-1
          indexData$remark[i] <-'subtract'
        }else{
          #shift position + subtract position
          indexData$pos[i] <-indexData$pos[i]-posChg
          indexData$mv[i] <-indexData$pos[i]*indexData$close[i]*multiplier
          indexData$cost[i] <-indexData$cost[i]+para$tradeCost*posChg*multiplier*indexData$close[i]
          indexData$cash[i] <-indexData$cash[i-1]-indexData$cost[i]+posChg*multiplier*indexData$close[i]
          indexData$totalasset[i] <-indexData$mv[i]+indexData$cash[i]
          indexData$rtn[i] <- indexData$totalasset[i]/indexData$totalasset[i-1]-1
        }
      }else if(todayPos>indexData$pos[i-1] & indexData$cash[i-1]>=indexData$close[i]*multiplier){
        #add position
        posChg <- min(todayPos-indexData$pos[i-1],floor(indexData$cash[i-1]/indexData$close[i]*multiplier))
        if(is.na(indexData$remark[i])){
          indexData$pos[i] <-indexData$pos[i-1]+posChg
          indexData$mv[i] <-indexData$pos[i]*indexData$close[i]*multiplier
          indexData$cost[i] <-para$tradeCost*posChg*multiplier*indexData$close[i]
          indexData$cash[i] <-indexData$cash[i-1]-indexData$cost[i]-posChg*multiplier*indexData$close[i]
          indexData$totalasset[i] <-indexData$mv[i]+indexData$cash[i]
          indexData$rtn[i] <- indexData$totalasset[i]/indexData$totalasset[i-1]-1
          indexData$remark[i] <-'add'
        }else{
          #shift position + add position
          indexData$pos[i] <-indexData$pos[i]+posChg
          indexData$mv[i] <-indexData$pos[i]*indexData$close[i]*multiplier
          indexData$cost[i] <-indexData$cost[i]+para$tradeCost*posChg*multiplier*indexData$close[i]
          indexData$cash[i] <-indexData$cash[i-1]-indexData$cost[i]-posChg*multiplier*indexData$close[i]
          indexData$totalasset[i] <-indexData$mv[i]+indexData$cash[i]
          indexData$rtn[i] <- indexData$totalasset[i]/indexData$totalasset[i-1]-1
        }

      }else{
        #hold position
        if(is.na(indexData$remark[i])){
          indexData$pos[i] <-indexData$pos[i-1]
          indexData$mv[i] <-indexData$pos[i]*indexData$close[i]*multiplier
          indexData$cash[i] <-indexData$cash[i-1]
          indexData$totalasset[i] <-indexData$mv[i]+indexData$cash[i]
          indexData$rtn[i] <- indexData$totalasset[i]/indexData$totalasset[i-1]-1
        }else next
      }
    }

    return(indexData)
  }

  allData <- getData(indexID,begT,endT)
  indexData <- allData$indexData
  shiftData <- allData$shiftData

  indexData <- calcData(indexData,shiftData,para)
  indexData <- transform(indexData,benchPct=round(benchPct,digits = 4),
                         rtn=round(rtn,digits = 4))
  indexData <- indexData[,c("date","stockID","close","benchClose","benchPct","pos","mv","rtn","totalasset","remark" )]
  return(indexData)
}




#' grid trading with index fund
#'
#'
#' @author Andrew Dow
#' @examples
#' indexID <- 'EI000905'
#' begT <- as.Date('2015-09-01')
#' endT <- Sys.Date()-1
#' para <- list(total=5e6,initmv=2e6,bar=0.1,mvChg=1e6,tradeCost=1/1000)
#' re <- gridTrade.index(indexID,begT,endT,para)
#' @export
gridTrade.index <- function(indexID,begT,endT=Sys.Date()-1,para){
  getData <- function(indexID,begT,endT){
    #get index quote
    indexData <- getIndexQuote(indexID,begT,endT,'close',datasrc="jy")
    indexData$benchClose <- indexData$close
    indexData$close <- indexData$close/indexData$close[1]
    indexData <- indexData[,c('date','stockID','close','benchClose')]
    return(indexData)
  }

  calcData <- function(indexData,para){
    indexData <- transform(indexData,benchPct=benchClose/indexData$benchClose[1]-1,
                           pos=c(0),mv=c(0),invest=c(0),cost=c(0),cash=c(0),rtn=c(0),totalasset=c(0),remark=NA)
    for(i in 1:nrow(indexData)){
      #initial
      if(i==1){
        indexData$invest[i] <- para$initmv
        indexData$pos[i] <- floor(para$initmv/(indexData$close[i]*100))*100
        indexData$mv[i] <-indexData$pos[i]*indexData$close[i]
        indexData$cost[i] <-indexData$mv[i]*para$tradeCost
        indexData$cash[i] <-para$total-indexData$mv[i]-indexData$cost[i]
        indexData$totalasset[i] <-indexData$mv[i]+indexData$cash[i]
        indexData$remark[i] <-'initial'
        indexData$rtn[i] <- indexData$totalasset[i]/para$total-1
        next
      }

      #position change
      if(indexData$benchPct[i]>para$bar){
        todayInvest <- para$initmv-floor(indexData$benchPct[i]/para$bar)*para$mvChg
      }else if(indexData$benchPct[i]<(-1*para$bar)){
        todayInvest <- para$initmv+floor(abs(indexData$benchPct[i]/para$bar))*para$mvChg
      }else{
        todayInvest <- para$initmv
      }

      if(todayInvest<indexData$invest[i-1] & indexData$mv[i-1]>0){
        #subtract position
        investChg <- min(indexData$invest[i-1]-todayInvest,indexData$mv[i-1])
        indexData$invest[i] <- max(todayInvest,0)
        chgPos <- floor(investChg/(indexData$close[i]*100))*100
        indexData$pos[i] <-indexData$pos[i-1]-chgPos
        indexData$mv[i] <-indexData$pos[i]*indexData$close[i]
        indexData$cost[i] <-chgPos*indexData$close[i]*para$tradeCost
        indexData$cash[i] <-indexData$cash[i-1]-indexData$cost[i]+chgPos*indexData$close[i]
        indexData$totalasset[i] <-indexData$mv[i]+indexData$cash[i]
        indexData$rtn[i] <- indexData$totalasset[i]/indexData$totalasset[i-1]-1
        indexData$remark[i] <-'subtract'

      }else if(todayInvest>indexData$invest[i-1] & indexData$cash[i-1]>0){
        #add position
        investChg <- min(todayInvest-indexData$invest[i-1],indexData$cash[i-1])
        indexData$invest[i] <- min(todayInvest,para$total)
        chgPos <- floor(investChg/(indexData$close[i]*100))*100
        indexData$pos[i] <-indexData$pos[i-1]+chgPos
        indexData$mv[i] <-indexData$pos[i]*indexData$close[i]
        indexData$cost[i] <-chgPos*indexData$close[i]*para$tradeCost
        indexData$cash[i] <-indexData$cash[i-1]-chgPos*indexData$close[i]-indexData$cost[i]
        indexData$totalasset[i] <-indexData$mv[i]+indexData$cash[i]
        indexData$rtn[i] <- indexData$totalasset[i]/indexData$totalasset[i-1]-1
        indexData$remark[i] <-'add'

      }else{
        #hold position
        indexData$invest[i] <- indexData$invest[i-1]
        indexData$pos[i] <-indexData$pos[i-1]
        indexData$mv[i] <-indexData$pos[i]*indexData$close[i]
        indexData$cash[i] <-indexData$cash[i-1]
        indexData$totalasset[i] <-indexData$mv[i]+indexData$cash[i]
        indexData$rtn[i] <- indexData$totalasset[i]/indexData$totalasset[i-1]-1
      }
    }
    return(indexData)
  }

  indexData <- getData(indexID,begT,endT)
  indexData <- calcData(indexData,para)
  indexData <- transform(indexData,benchPct=round(benchPct,digits = 4),
                         rtn=round(rtn,digits = 4))
  indexData <- indexData[,c("date","stockID","close","benchClose","benchPct","pos","mv","rtn","totalasset","remark")]
  return(indexData)
}




#' resumption stock arbitrage
#'
#'
#' @author Andrew Dow
#' @examples
#' begT <- Sys.Date()-1
#' endT <- Sys.Date()
#' re <- resumeArbitrage(begT,endT)
#' @export
resumeArbitrage <- function(begT,endT){
  get.resume.stock <- function(begT,endT,dayinterval=20,datasource=c('jy','tpan')){
    datasource <- match.arg(datasource)
    if(datasource=='jy'){
      qr <- paste("SELECT 'EQ'+ss.SecuCode 'stockID',ss.SecuAbbr 'stockName',
                  CONVERT(varchar(20),s.SuspendDate,112) 'suspendDate',
                  CONVERT(varchar(20),s.ResumptionDate,112) 'resumeDate'
                  from LC_SuspendResumption s,SecuMain ss
                  where s.ResumptionDate>=",QT(begT),
                  " and s.ResumptionDate<=",QT(endT),
                  " and s.InnerCode=ss.InnerCode and ss.SecuCategory=1")
      con <- db.jy()
      resume.stock <- sqlQuery(con,qr,stringsAsFactors=F)
      odbcClose(con)

      resume.stock$suspendDate <- intdate2r(resume.stock$suspendDate)
      resume.stock$resumeDate <- intdate2r(resume.stock$resumeDate)
      resume.stock$lastSuspendDay <- trday.nearby(resume.stock$resumeDate, by = -1)
      resume.stock <- resume.stock[(resume.stock$resumeDate-resume.stock$suspendDate)>dayinterval,]
    }else{
      tmp.begT <- trday.nearby(begT,by=0)
      dates <- trday.get(begT =tmp.begT, endT = endT)
      dates <- rdate2int(dates)
      txtname <- c(paste("T:/Input/ZS/index/csitfp4fund",dates,"001.txt",sep = ""),
                   paste("T:/Input/ZS/index/csitfp4fund",dates,"002.txt",sep = ""))
      suspendstock <- plyr::ldply(txtname,read.csv, header=FALSE, sep="|",skip = 1, stringsAsFactors=FALSE)
      suspendstock <- subset(suspendstock,V3 %in% c(T,"T"),select=c(V1,V2))
      suspendstock <- suspendstock[substr(suspendstock$V1, 1, 1) %in% c("6","0","3"),]
      colnames(suspendstock) <- c("stockID","suspendDate")
      suspendstock$stockID <- str_c('EQ',suspendstock$stockID)
      suspendstock <- plyr::arrange(suspendstock, suspendDate, stockID)
      result <- data.frame()
      for(i in length(dates):2){
        x <- suspendstock[suspendstock$suspendDate==dates[i],"stockID"]
        y <- suspendstock[suspendstock$suspendDate==dates[i-1],"stockID"]
        stock <- setdiff(y,x)
        if(length(stock)>0){
          tmp <- data.frame(resumeDate=rep(intdate2r(dates[i]),length(stock)),stockID=stock)
          result <- rbind(result,tmp)
        }else next
      }
      tmp <- str_c(str_sub(result$stockID,3,8),collapse = "','")
      tmp <- str_c("('",tmp,"')")

      qr <- paste("SELECT 'EQ'+ss.SecuCode 'stockID',ss.SecuAbbr 'stockName',
                  CONVERT(varchar(20),s.SuspendDate,112) 'suspendDate'
                  from LC_SuspendResumption s,SecuMain ss
                  where  s.InnerCode=ss.InnerCode and
                  (s.ResumptionDate>=",str_c("'",as.character(begT),"'"),
                  " or s.ResumptionDate='1900-01-01')
                  and ss.SecuCategory=1 and ss.SecuCode in",tmp)
      con <- db.jy()
      resume.stock <- sqlQuery(con,qr,stringsAsFactors=F)
      odbcClose(con)
      if(nrow(resume.stock)>0){
        resume.stock$suspendDate <- intdate2r(resume.stock$suspendDate)
        resume.stock <- merge(resume.stock,result,by="stockID")
        resume.stock$lastSuspendDay <- trday.nearby(resume.stock$resumeDate, by = -1)
        resume.stock <- resume.stock[(resume.stock$resumeDate-resume.stock$suspendDate)>dayinterval,]
      }

    }

    return(resume.stock)
  }

  get.fund.info <- function(){
    qr <- "select s.SecuCode 'fundCode',s.SecuAbbr 'fundName','EI'+s1.SecuCode 'indexCode',s1.SecuAbbr 'indexName'
    from MF_InvestTargetCriterion i
    inner join SecuMain s on s.InnerCode=i.InnerCode
    inner join SecuMain s1 on s1.InnerCode=i.TracedIndexCode
    where i.InvestTarget=90 and i.IfExecuted=1 and i.MinimumInvestRatio>=0.9
    and i.InnerCode in(
    select f.InnerCode
    from MF_FundArchives f,SecuMain s
    where f.Type=3 and f.InnerCode=s.InnerCode
    and f.ListedDate is not NULL and f.ExpireDate is NULL
    and f.FundTypeCode='1101' and f.InvestmentType=7 and s.SecuCode not like '%J'
    )"
    con <- db.jy()
    lof.info <- sqlQuery(con,qr)
    odbcClose(con)
    lof.info$type <- c("LOF")

    sf.info <- dbReadTable(db.local(), "SF_Info")

    fund.info <- sf.info[,c("MCode","MName","IndexCode","IndexName")]
    fund.info$MCode <- substr(fund.info$MCode,1,6)
    fund.info$IndexCode <- substr(fund.info$IndexCode,1,6)
    fund.info$IndexCode <- paste('EI',fund.info$IndexCode,sep="")
    fund.info$type <- c("SF")
    colnames(fund.info) <- colnames(lof.info)
    fund.info <- rbind(fund.info,lof.info)

    return(fund.info)
  }

  get.index.component <- function(stock,index,date){
    stock <- brkQT(substr(stock,3,8))
    index <- brkQT(substr(index,3,8))
    qr <- paste("select  'EI'+s1.SecuCode 'indexID',s1.SecuAbbr 'indexName',
                'EQ'+s2.SecuCode 'stockID',s2.SecuAbbr 'stockName',
                CONVERT(varchar(20),i.EndDate,112) 'enddate',
                i.Weight,CONVERT(varchar(20),i.UpdateTime,112) 'update'
                from LC_IndexComponentsWeight i
                left join SecuMain s1 on i.IndexCode=s1.InnerCode
                left join SecuMain s2 on i.InnerCode=s2.InnerCode
                where i.InnerCode in (select InnerCode from SecuMain where SecuCode in ",stock," and SecuCategory=1)
                and i.IndexCode in (SELECT InnerCode from SecuMain where SecuCode in ",index,"and SecuCategory=4)",
                " and i.EndDate>=",QT(date))
    con <- db.jy()
    index.component <- sqlQuery(con,qr)
    odbcClose(con)
    if(nrow(index.component)>0){
      index.component$enddate <- intdate2r(index.component$enddate)
      return(index.component)
    }else{
      print('No qualified stock in these index!')
    }
  }

  get.stock.industry <- function(stock){
    stock <- brkQT(substr(stock,3,8))
    qr <- paste("(select 'EQ'+b.SecuCode 'stockID',e.SecuCode 'sectorID',e.SecuAbbr 'sectorName'
                from LC_ExgIndustry as a
                inner join SecuMain as b on a.CompanyCode = b.CompanyCode
                inner join CT_SystemConst as c on a.SecondIndustryCode = c.CVALUE
                inner join LC_CorrIndexIndustry as d on c.DM = d.IndustryCode
                INNER join SecuMain as e on d.IndexCode = e.InnerCode
                where a.Standard = 23 and a.IfPerformed = 1 and b.SecuCode in ",stock,
                " and b.SecuCategory = 1 and c.LB = 1755 and d.IndustryStandard = 23)
                union
                (select 'EQ'+b.SecuCode 'stockID',e.SecuCode 'sectorID',e.SecuAbbr 'sectorName'
                from LC_ExgIndustry as a
                inner join SecuMain as b on a.CompanyCode = b.CompanyCode
                inner join CT_SystemConst as c on a.FirstIndustryCode = c.CVALUE
                inner join LC_CorrIndexIndustry as d on c.DM = d.IndustryCode
                INNER join SecuMain as e on d.IndexCode = e.InnerCode
                where a.Standard = 23 and a.IfPerformed = 1 and b.SecuCode in ",stock,
                " and b.SecuCategory = 1 and c.LB = 1755 and d.IndustryStandard = 23)")
    con <- db.jy()
    stock.industry <- sqlQuery(con,qr)
    odbcClose(con)
    return(stock.industry)
  }

  get.industry.quote <- function(industry,begday){
    industry <- brkQT(industry)
    qr <- paste("SELECT s.SecuCode 'sectorID',CONVERT(varchar(8),q.TradingDay,112) 'date',
                q.ClosePrice 'close'
                FROM QT_IndexQuote q,SecuMain s
                where q.InnerCode=s.InnerCode and q.TradingDay>=",QT(begday),
                " and s.SecuCategory=4 and s.SecuCode in ",industry,
                " order by s.SecuCode,q.TradingDay")
    con <- db.jy()
    index.quote <- sqlQuery(con,qr)
    odbcClose(con)
    index.quote$date <- intdate2r(index.quote$date)
    return(index.quote)
  }

  calc.match.industrypct <- function(resume.stock,index.quote){
    colnames(index.quote) <- c("sectorID","suspendDate","close1" )
    resume.stock <- merge(resume.stock,index.quote,by=c('sectorID','suspendDate'),all.x = T)
    colnames(index.quote) <- c("sectorID","lastSuspendDay","close2" )
    resume.stock <- merge(resume.stock,index.quote,by=c('sectorID','lastSuspendDay'),all.x = T)
    resume.stock$IndustryPct <- resume.stock$close2/resume.stock$close1-1
    resume.stock <- resume.stock[abs(resume.stock$IndustryPct)>=0.1,]

    if(nrow(resume.stock)>0){
      resume.stock <- resume.stock[,c("stockID","stockName","suspendDate", "resumeDate",
                                      "lastSuspendDay","sectorID","sectorName","IndustryPct")]
      return(resume.stock)
    }else{
      print('No valuation adjustment!')
    }


  }

  calc.match.indexcomponent <- function(resume.stock,index.component,bar=2){
    result <- data.frame()
    for(i in 1:nrow(resume.stock)){
      tmp.result <- resume.stock[i,]
      tmp.index.component <- index.component[index.component$stockID==resume.stock$stockID[i],]
      if(length(unique(tmp.index.component$indexID))>1){
        for(j in unique(tmp.index.component$indexID)){
          tmp <- tmp.index.component[tmp.index.component$indexID==j,]
          tmp <- arrange(tmp,enddate)
          ind <- findInterval(resume.stock[i,"suspendDate"],tmp$enddate)
          if(ind==0 || max(tmp$enddate)<resume.stock[i,"suspendDate"]) next
          tmp.result$inindex <- tmp$indexID[ind]
          tmp.result$inindexname <- tmp$indexName[ind]
          tmp.result$wgtinindex <- tmp$Weight[ind]
          result <- rbind(result,tmp.result)
        }
      }else{
        tmp <- tmp.index.component
        tmp <- arrange(tmp,enddate)
        ind <- findInterval(resume.stock[i,"suspendDate"],tmp$enddate)
        if(ind==0 || max(tmp$enddate)<resume.stock[i,"suspendDate"]) next
        tmp.result$inindex <- tmp$indexID[ind]
        tmp.result$inindexname <- tmp$indexName[ind]
        tmp.result$wgtinindex <- tmp$Weight[ind]
        result <- rbind(result,tmp.result)
      }
    }
    result <- result[result$wgtinindex>=bar,]
    if(nrow(result)>0) return(result)
    else print("No qualified index!")
  }

  calc.match.fundunit <- function(fund.result){
    tmp.sfcode <- unique(fund.result$fundCode[fund.result$type=='SF'])
    tmp.sfcode <- paste(tmp.sfcode,'.OF',sep='')
    tmp.lofcode <- unique(fund.result$fundCode[fund.result$type=='LOF'])
    tmp.begT <- min(fund.result$suspendDate)
    fund.size <- data.frame()
    if(length(tmp.sfcode)>0){
      tmp <- brkQT(tmp.sfcode)
      qr <- paste("select t.MCode,i.MName,t.Date,
                  (t.MUnit*t.MNav+ t.AUnit*t.ANav + t.BUnit*t.BNav) 'Unit'
                  from SF_TimeSeries t,SF_Info i
                  where t.MCode=i.MCode and t.MCode in",tmp,
                  "and t.Date>=",rdate2int(tmp.begT))
      con <- db.local()
      sf.size <- dbGetQuery(con,qr)
      dbDisconnect(con)
      colnames(sf.size) <- c("Code","Name","Date","Unit")
      sf.size$Code <- substr(sf.size$Code,1,6)
      fund.size <-rbind(fund.size,sf.size)
    }

    if(length(tmp.lofcode)>0){
      tmp <- brkQT(tmp.lofcode)
      qr <- paste("select s.SecuCode,s.SecuAbbr,CONVERT(varchar(8),m.EndDate,112) 'EndDate',m.FloatShares/100000000 'Unit'
                  from MF_SharesChange m,SecuMain s
                  where m.InnerCode=s.InnerCode and s.SecuCode in",tmp,
                  " and m.StatPeriod='996' and m.EndDate>=",
                  QT(tmp.begT),
                  " order by s.SecuCode,m.EndDate")
      con <- db.jy()
      lof.size <- sqlQuery(con,qr)
      odbcClose(con)
      colnames(lof.size) <- c("Code","Name","Date","Unit")
      fund.size <-rbind(fund.size,lof.size)
    }
    fund.size$Date <- intdate2r(fund.size$Date)

    fund.result$OldUnit <- c(0)
    fund.result$NewUnit <- c(0)
    for(i in 1:nrow(fund.result)){
      tmp <- fund.size[fund.size$Code==fund.result$fundCode[i] & !is.na(fund.size$Unit),]
      tmp <- arrange(tmp,Date)
      if(tmp$Date[1]>fund.result$suspendDate[i]) fund.result$OldUnit[i] <- tmp$Unit[1]
      else fund.result$OldUnit[i] <- tmp$Unit[tmp$Date==fund.result$suspendDate[i]]
      fund.result$NewUnit[i] <- tmp$Unit[nrow(tmp)]
    }

    fund.result$UnitPct <- as.numeric(fund.result$NewUnit)/as.numeric(fund.result$OldUnit)-1
    fund.result$newWeight <- fund.result$wgtinindex*fund.result$OldUnit/fund.result$NewUnit
    fund.result <- fund.result[fund.result$newWeight>=5 & fund.result$NewUnit>0.5,]
    if(nrow(fund.result)>0){
      fund.result <- fund.result[,c("fundCode","fundName","type",
                                    "stockName","wgtinindex","suspendDate","resumeDate",
                                    "sectorName","IndustryPct","OldUnit","NewUnit",'newWeight')]
      fund.result <- arrange(fund.result,desc(newWeight))
      return(fund.result)
    }else{
      print("No qualified stock!")
    }

  }


  resume.stock <- get.resume.stock(begT,endT) #get qualified resumption stock
  if(nrow(resume.stock)==0) return("None!")

  fund.info <- get.fund.info() #get all lof and structure fund basic info

  #get resumption stock in the traced index of these lof and sf
  tmp.index <- toupper(unique(fund.info$indexCode))
  tmp.date <- trday.offset(min(resume.stock$suspendDate), by = months(1))
  index.component <- get.index.component(resume.stock$stockID,tmp.index,tmp.date)
  if(is.character(index.component)) return("None!")

  #get stock's amac industy and the industry's corresponding index quote
  resume.stock <- resume.stock[resume.stock$stockID %in% index.component$stockID,]
  stock.industry <- get.stock.industry(resume.stock$stockID)
  resume.stock <- merge(resume.stock,stock.industry,by='stockID')
  index.quote <- get.industry.quote(industry = resume.stock$sectorID,begday = min(resume.stock$suspendDate))

  #calculate stock's industry change pct during suspend time
  resume.stock <- calc.match.industrypct(resume.stock,index.quote)
  if(is.character(resume.stock)) return("None!")
  #
  resume.stock <- calc.match.indexcomponent(resume.stock,index.component)
  if(is.character(resume.stock)) return("None!")


  # get the final result
  fund.info <- fund.info[fund.info$indexCode %in% resume.stock$inindex,]
  fund.result <- merge(fund.info,resume.stock,by.x='indexCode',by.y = 'inindex',all.x =T)
  fund.result <- arrange(fund.result,fundCode)
  fund.result <- fund.result[,c("fundCode","fundName","indexCode","indexName","type","stockID","stockName",
                                "wgtinindex","suspendDate","resumeDate","lastSuspendDay",
                                "sectorID","sectorName","IndustryPct")]
  fund.result <- calc.match.fundunit(fund.result)

  return(fund.result)

}







# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ======================
# ===================== series of ultility functions  ===========================
# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ======================


#' write.clipboard
#'
#' write data frame to clipboard
#' @export
write.clipboard <- function(x,row.names=FALSE,col.names=TRUE,...) {
  write.table(x,"clipboard-16384",sep="\t",row.names=row.names,col.names=col.names,...)
}

#' xts2df
#'
#' turn xts to dataframe
#' @export
xts2df <- function(x) {
  df <- data.frame(date=zoo::index(x),zoo::coredata(x))
  return(df)
}


#' connect tinysoft database
#'
#'
#' @author Andrew Dow
#' @return a tinysoft conn.
#' @examples
#' qr <- "setsysparam(pn_stock(),'SZ000002');
#' setsysparam(pn_date(), today()); return nday(30,'date'
#' ,datetimetostr(sp_time()), 'open',open(), 'close',close());"
#' re <- sqlQuery(db.ts(), qr);
#' @export
db.ts <- function(){
  odbcConnect("tinysoftdb")
}



#' fund_exrtn_terr
#'
#' calculate fund's excess return and tracking error
#' @param fundID is fund ID.
#' @param begT is begin date.
#' @param endT is end date, default value is \bold{today}.
#' @param scale is number of periods in a year,default value is 250.
#' @examples
#' te <- fund_exrtn_terr(fundID='162411.OF',begT=as.Date('2013-06-28'))
#' te <- fund_exrtn_terr(fundID=c('162411.OF','501018.OF'),begT=as.Date('2016-06-04'))
#' @export
fund_exrtn_terr <- function(fundID,begT,endT=Sys.Date(),freq=NULL,scale=250){
  require(WindR)
  WindR::w.start(showmenu = F)

  setupdate<-w.wss(fundID,'fund_setupdate')[[2]]
  setupdate <- w.asDateTime(setupdate$FUND_SETUPDATE,asdate = T)

  if(missing(begT)){
    df <- data.frame(fundID=fundID,begT=setupdate,endT=endT)
  }else{
    df <- data.frame(fundID=fundID,begT=begT,endT=endT)
    df[,'begT'] <- ifelse(df$begT<setupdate,setupdate,df$begT)
    df$begT <- as.Date(df$begT,origin='1970-01-01')
  }
  result <- data.frame()

  innerte <- function(tsobj,scale){
    TrackingError <-  sqrt(sum(tsobj^2) / (length(tsobj) - 1)) * sqrt(scale)
    return(TrackingError)
  }

  for(i in 1:nrow(df)){
    fundts <-w.wsd(df$fundID[i],"NAV_adj_return1",df$begT[i],df$endT[i],"Fill=Previous")[[2]]
    tmp <- w.wss(df$fundID[i],'fund_benchindexcode')[[2]]
    benchts <-w.wsd(tmp$FUND_BENCHINDEXCODE,"pct_chg",df$begT[i],df$endT[i],"Fill=Previous")[[2]]
    allts <- merge(fundts,benchts,by='DATETIME')
    allts <- na.omit(allts)
    allts <- transform(allts,NAV_ADJ_RETURN1=NAV_ADJ_RETURN1/100,PCT_CHG=PCT_CHG/100)
    allts <- xts::xts(allts[,-1],order.by = allts[,1])


    if(is.null(freq)){
      rtn <- rtn.summary(allts)
      result <- rbind(result,data.frame(fundID=df$fundID[i],
                                        begT=df$begT[i],
                                        endT=df$endT[i],
                                        fundrtn=rtn[1,1],
                                        benchrtn=rtn[1,2],
                                        exrtn=rtn[1,1]-rtn[1,2],
                                        te=innerte(allts[,1]-allts[,2],scale = scale)))

    }else{
      from <- unique(cut.Date2(zoo::index(allts),freq,lab.side="begin"))
      to <- unique(cut.Date2(zoo::index(allts),freq,lab.side="end"))
      rtn <- zoo::as.zoo(allts)
      # ---- periods cumulative rtn
      table.periods <- timeSeries::fapply(timeSeries::as.timeSeries(rtn),from,to,FUN=PerformanceAnalytics::Return.annualized)
      table.periods <- as.data.frame(table.periods)
      table.periods <- cbind(table.periods,exrtn=table.periods[,1]-table.periods[,2])


      rtn <- rtn[,1]-rtn[,2]
      te.periods <- timeSeries::fapply(timeSeries::as.timeSeries(rtn),from,to,FUN=innerte,scale=scale)
      te.periods <- as.data.frame(te.periods)
      table.periods <- cbind(table.periods,te.periods)
      colnames(table.periods) <- c('fundrtn','benchrtn','exrtn','te')
      result <- rbind(result,data.frame(fundID=df$fundID[i],
                                        begT=from,
                                        endT=to,
                                        table.periods))

    }

  }
  return(result)
}





#' lcdb.update.CorpStockPool
#'
#' @param filenames a vector of filename with path.
#' @examples
#' filenames <- c('D:/sqlitedb/core.csv','D:/sqlitedb/preclose.csv')
#' lcdb.update.CorpStockPool(filenames)
#' @export
lcdb.update.CorpStockPool <- function(filenames){
  all <- data.frame()
  for(i in 1:length(filenames)){
    tmp <- read.csv(filenames[i])
    all <- rbind(all,tmp)
  }
  colnames(all) <- c('stockID','stockName',"MiscellaneousItem",'SecuMarket','FundBelong','CorpStockPool',
                     'InvestAdviceNum','DimensionID','DimensionName','Remark','Operator','AddDate','AddTime',
                     'CheckOperator','HavePosition','ValidBeginDate','ValidEndDate','SecurityCate')
  all$stockID <- stringr::str_pad(all$stockID,6,pad = '0')
  all$stockID <- stringr::str_c('EQ',all$stockID,sep = '')
  con <- db.local()
  dbWriteTable(con,'CT_CorpStockPool',all,overwrite=T,row.names=F)
  dbDisconnect(con)
}


#' combine rtn.periods and rtn.summary
#'
#' @param rtn an xts, vector, matrix, data frame, timeSeries or zoo object of asset returns
#' @param freq An interval specification, one of "day", "week", "month", "quarter" and "year", optionally preceded by an integer and a space, or followed by "s".See \code{\link{cut.Date}} for detail.
#' @param Rf risk free rate, in same period as your returns
#' @return a matrix, giving the summary infomation of the rtn series,including Annualized Return,Annualized Std Dev,Annualized Sharpe,HitRatio,Worst Drawdown
#' @seealso \code{\link[QUtility]{rtn.periods}}
#' @seealso \code{\link[QUtility]{rtn.summary}}
#' @examples
#' rtn <- rtndemo
#' rtn <- xts::xts(rtn[,-1],rtn[,1])
#' rtn.persum(rtn)
#' @export
rtn.persum <- function(rtn,freq="year",Rf=0,showPer=T){

  from <- unique(cut.Date2(zoo::index(rtn),freq,lab.side="begin"))
  to <- unique(cut.Date2(zoo::index(rtn),freq,lab.side="end"))

  rtn <- zoo::as.zoo(rtn)
  # ---- periods cumulative rtn
  table.periods <- timeSeries::fapply(timeSeries::as.timeSeries(rtn),from,to,FUN=PerformanceAnalytics::Return.cumulative)
  table.periods <- as.matrix(table.periods)
  rownames(table.periods) <- paste(from,to,sep=" ~ ")
  # ---- overall cumulative rtn and annnualized rtn
  table.overall <- PerformanceAnalytics::Return.cumulative(rtn)

  rtn <- xts::as.xts(rtn)
  annual <- as.matrix(Table.Annualized(rtn,Rf=Rf))

  maxDD <- PerformanceAnalytics::maxDrawdown(rtn)
  dim(maxDD) <- c(1, NCOL(rtn))
  colnames(maxDD) <- colnames(rtn)
  rownames(maxDD) <- "Worst Drawdown"
  result <- rbind(table.periods,table.overall,annual,maxDD)
  result <- as.data.frame(result)
  if(showPer==T){
    for(i in 1:ncol(result)){
      result[,i] <- paste(round(result[,i],3)*100,'%',sep='')
    }
  }

  return(result)

}


#' percent
#'
#' Percent formatter: multiply by one hundred and display percent sign.
#' @param x a numeric vector to format
#' @return a function with single parameter x, a numeric vector, that returns a character vector
#' @seealso \code{\link[scales]{percent}}
#' @examples
#' x <- c(-1, 0, 0.1, 0.555555, 1, 100)
#' percent(x)
#' @export
percent <- function(x, digits = 2, format = "f", ...) {
  paste0(formatC(100 * x, format = format, digits = digits, ...), "%")
}


#' getIndexBasicInfo
#'
#'
#' @param indexID is set of index ID.
#' @examples
#' index <- getIndexBasicInfo('EI000300')
#' index <- getIndexBasicInfo(c('EI000300','EI000905'))
#' @export
getIndexBasicInfo <- function(indexID) {
  tmp <- brkQT(substr(indexID,3,8))
  qr <- paste("SELECT 'EI'+s.SecuCode 'SecuCode'
              ,s.SecuAbbr
              ,ct1.MS 'IndexType'
              ,ct2.MS 'IndustryStandard'
              ,[PubOrgName]
              ,CONVERT(VARCHAR,PubDate,112) 'PubDate'
              ,CONVERT(VARCHAR,BaseDate,112) 'BaseDate'
              ,BasePoint
              ,ct3.MS 'WAMethod'
              ,ComponentSum
              ,ct3.MS 'ComponentAdPeriod'
              ,EndDate
              ,CONVERT(VARCHAR,l.XGRQ,112) 'XGRQ'
              FROM LC_IndexBasicInfo l
              left join SecuMain s on l.IndexCode=s.InnerCode
              left join CT_SystemConst ct1 on ct1.LB=1266 and l.IndexType=ct1.DM
              left join CT_SystemConst ct2 on ct2.LB=1081 and l.IndustryStandard=ct2.DM
              left join CT_SystemConst ct3 on ct3.LB=1265 and l.WAMethod=ct3.DM
              left join CT_SystemConst ct4 on ct4.LB=1264 and l.ComponentAdPeriod=ct4.DM
              where s.SecuCode in",tmp)
  re <- queryAndClose.odbc(db.jy(),qr)
  return(re)
}

