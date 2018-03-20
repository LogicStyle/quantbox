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
#' @name bankrotation
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
#' FactorLists <- list(
#'   buildFactorList(factorFun='gf.PB_mrq'),
#'   buildFactorList(factorFun='gf.ROE_ttm'))
#' re <- bank.rotationV2(begT,endT,FactorLists=FactorLists)
#' @export
bank.rotation <- function(begT,endT=Sys.Date()-1,chgBar=0.2,fee=0.003,
                             pbfun=c('gf.PB_lyr','gf.PB_mrq'),
                             roefun=c('gf.ROE_ttm','gf.ROE','gf.F_ROE')){
  pbfun <- match.arg(pbfun)
  roefun <- match.arg(roefun)

  rebDates <- getRebDates(begT,endT,rebFreq = 'day')
  TS <- getTS(rebDates,indexID = 'ES33480000')
  qr <- paste("select ID 'stockID',ListedDate from SecuMain where ID in",brkQT(unique(TS$stockID)))
  ipo <- queryAndClose.dbi(db.local('main'),qr)
  ipo$ListedDate <- intdate2r(ipo$ListedDate)
  TS <- left_join(TS,ipo,by='stockID')
  TS <- na.omit(TS)
  TS <- TS[TS$date>TS$ListedDate,c('date','stockID'),]
  TS <- rm_suspend(TS,0)

  TSF <- getRawFactor(TS,pbfun)
  tmp <- getRawFactor(TS,roefun)
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


#' @rdname bankrotation
#' @export
bank.rotationV2 <- function(begT,endT,FactorLists,chgBar,fee=0.003,
                             sectorID='ES33480000',prefer=FALSE){

  #get factorscore
  rebDates <- getRebDates(begT,endT,rebFreq = 'day')
  TS <- getTS(rebDates,indexID = sectorID)
  ipo <- data.frame(stockID=unique(TS$stockID),stringsAsFactors = FALSE)
  ipo <- transform(ipo,ListedDate=trday.IPO(stockID))

  TS <- TS %>% left_join(ipo,by='stockID') %>% filter(!is.na(ListedDate)) %>%
    filter(date>ListedDate+365) %>% select(date,stockID)

  TSF <- getRawMultiFactor(TS,FactorLists)
  colnames(TSF) <- c("date","stockID","PB","ROE")
  if(median(TSF$ROE,na.rm = TRUE)>1){
    TSF <- transform(TSF,ROE=ROE/100)
  }
  TSF <- TSF %>% mutate(factorscore=log(PB*2,base=(1+ROE))) %>%
   filter(!is.na(factorscore)) %>% arrange(date,factorscore)


  #get bench mark return
  bench <- getIndexQuote('EI801780',as.Date('2005-01-04'),max(TS$date),variables = c('pre_close','close'),datasrc = 'jy')
  bench <- bench %>% mutate(indexRtn=close/pre_close-1) %>% select(date,indexRtn)

  #if missing change bar, set change bar by historical vol.
  if(missing(chgBar)){
    indexVol <- xts::xts(bench[,-1],order.by = bench[,1])
    indexVol <- zoo::rollapply(indexVol, 22, sd)
    indexVol <- na.omit(indexVol)
    indexVol <- TTR::runPercentRank(indexVol,250,cumulative = TRUE)
    indexVol <- data.frame(date=zoo::index(indexVol),vol=zoo::coredata(indexVol))
    indexVol <- indexVol[250:nrow(indexVol),]
    chgBardf <- indexVol %>% mutate(bar=ifelse(vol<1/3,0.1,ifelse(vol<2/3,0.15,0.2))) %>%
      select(-vol)
  }

  #get bank
  dates <- unique(TSF$date)
  bankPort <- data.frame()
  for(i in 1:length(dates)){
    TSF_ <- TSF[TSF$date==dates[i],]
    TS_ <- TSF_[,c('date','stockID')]
    TS_ <- rm_suspend(TS_,0)
    if(nrow(TS_)<nrow(TSF_)){
      TSF_ <- TS_ %>% left_join(TSF_,by=c('date','stockID'))
    }

    if(prefer){
      TSF_ <- TSF_ %>% arrange(desc(ROE)) %>% slice(1:round(n()/3*2)) %>% arrange(factorscore)
    }

    if(i==1){
      bankPort_ <- TSF_[1,c('date','stockID')]

    }else{
      if(missing(chgBar)){
        chgBar <- chgBardf[chgBardf$date==dates[i],'bar']
      }

      oldstock <- tail(bankPort$stockID,1)
      bankPort_ <- data.frame(date=dates[i],stockID=oldstock)
      if(oldstock %in% TSF_$stockID && oldstock !=TSF_$stockID[1]){
        newscore <- TSF_[1,'factorscore']
        oldscore <- TSF_[TSF_$stockID==oldstock,'factorscore']*(1-chgBar)
        if(newscore<oldscore){
          bankPort_ <- TSF_[1,c("date","stockID")]
        }
      }
    }
    bankPort <- rbind(bankPort,bankPort_)
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



  rtn <- left_join(TSR[,c('date','pct_chg')],bench,by='date')
  colnames(rtn) <- c("date","bankRtn","indexRtn")
  rtn <- na.omit(rtn)
  rtn <- as.data.frame(rtn)
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
lcdb.build.QT_IndexValuation<- function(indexID=c('EI399006','EI000933'),addIndex=FALSE){
  qr <- paste("select 'EI'+s.SecuCode 'indexID',s.SecuAbbr 'indexName',
              convert(varchar(8),i.PubDate,112) 'begT'
              from LC_IndexBasicInfo i,SecuMain s
              where i.IndexCode=s.InnerCode and s.SecuCode in",brkQT(substr(indexID,3,8)))
  indexDate <- queryAndClose.odbc(db.jy(),qr,stringsAsFactors=FALSE)
  indexDate <- transform(indexDate,begT=intdate2r(ifelse(begT<20050104,20050104,begT)),
                              endT=Sys.Date()-1)
  con <- db.local('main')
  if(addIndex){
    old <- dbGetQuery(con,"select distinct indexID from QT_IndexValuation")
    indexID <- setdiff(indexID,c(old$indexID))
    if(length(indexID)==0){
      return('Already in database!')
    }
    endT <- dbGetQuery(con,"select max(date) 'date' from QT_IndexValuation")
    indexDate <- indexDate[indexDate$indexID %in% indexID,]
    indexDate$endT <- intdate2r(endT$date)
  }

  re <- QT_IndexValuation_subfun(indexDate)
  if(addIndex){
    dbWriteTable(con,'QT_IndexValuation',re,overwrite=FALSE,append=TRUE,row.names=FALSE)
  }else{
    dbWriteTable(con,'QT_IndexValuation',re,row.names=FALSE)
  }

  dbDisconnect(con)
  return('Done!')
}

#inner function
QT_IndexValuation_subfun <- function(indexDate){

  #get index component
  qr <- paste("select 'EI'+s1.SecuCode 'indexID','EQ'+s2.SecuCode 'stockID',
              convert(varchar(8),l.InDate,112) 'InDate',
              convert(varchar(8),l.OutDate,112) 'OutDate'
              from LC_IndexComponent l
              LEFT join SecuMain s1 on l.IndexInnerCode=s1.InnerCode
              LEFT join SecuMain s2 on l.SecuInnerCode=s2.InnerCode
              where s1.SecuCode in",brkQT(substr(indexDate$indexID,3,8)),
              " order by s1.SecuCode,l.InDate")
  indexComp <- queryAndClose.odbc(db.jy(),qr,stringsAsFactors=FALSE)
  con <- db.local('main')
  dbWriteTable(con, name="amtao_tmp", value=indexComp, row.names = FALSE, overwrite = TRUE)

  #correct begT
  tmpdate <- plyr::ddply(indexComp,'indexID',plyr::summarise,mindate=intdate2r(min(InDate)))
  indexDate <- indexDate %>% dplyr::left_join(tmpdate,by='indexID') %>%
    dplyr::mutate(begT=as.Date(ifelse(begT<mindate,mindate,begT),origin='1970-01-01')) %>% dplyr::select(-mindate)

  #get bigTS
  bigTS <- data.frame()
  tmpdate <- data.frame(date=rdate2int(getRebDates(min(indexDate$begT),max(indexDate$endT),'day')))
  dbWriteTable(con, name="yrf_tmp", value=tmpdate, row.names = FALSE, overwrite = TRUE)
  for(i in 1:nrow(indexDate)){
    qr <- paste("SELECT a.date as date, b.stockID from yrf_tmp a, amtao_tmp b
                where b.IndexID=", QT(indexDate$indexID[i]),
                "and b.InDate<=a.date and (b.OutDate>a.date or b.OutDate IS NULL) and a.date>=",rdate2int(indexDate$begT[i]))
    TS_ <- dbGetQuery(con,qr)
    TS_ <- transform(TS_,date=intdate2r(date),indexID=indexDate$indexID[i])
    bigTS <- rbind(bigTS,TS_)
  }

  #get pe pb data
  TS <- dplyr::distinct(bigTS,date,stockID)
  pedata <- getTSF(TS,factorFun = 'gf.PE_ttm',factorPar = list(fillna = FALSE))
  pbdata <- getTSF(TS,factorFun = 'gf.PB_mrq',factorPar = list(fillna = FALSE))
  pedata <- dplyr::rename(pedata,pettm=factorscore)
  pbdata <- dplyr::rename(pbdata,pbmrq=factorscore)
  alldata <- dplyr::left_join(bigTS,pedata,by=c('date','stockID'))
  alldata <- dplyr::left_join(alldata,pbdata,by=c('date','stockID'))

  pemeanfun <- function(factorscore){
    #pe mean
    factorscore <- factorscore[!is.na(factorscore)]
    factorscore <- factorscore[factorscore>0 & factorscore<1000]
    outlier_u <- mean(factorscore)+3*sd(factorscore)
    outlier_l <- mean(factorscore)-3*sd(factorscore)
    factorscore[factorscore > outlier_u] <- outlier_u
    factorscore[factorscore < outlier_l] <- outlier_l
    return(mean(factorscore))
  }

  pbmeanfun <- function(factorscore){
    factorscore <- factorscore[!is.na(factorscore)]
    factorscore <- factorscore[factorscore>0 & factorscore<100]
    outlier_u <- mean(factorscore)+3*sd(factorscore)
    outlier_l <- mean(factorscore)-3*sd(factorscore)
    factorscore[factorscore > outlier_u] <- outlier_u
    factorscore[factorscore < outlier_l] <- outlier_l
    return(mean(factorscore))
  }

  #summarize
  indexvalue <- alldata %>% dplyr::group_by(date,indexID) %>% dplyr::summarise(PE_median=median(pettm,na.rm = TRUE),
                                                                                PE_mean=pemeanfun(pettm),
                                                                                PB_median=median(pbmrq,na.rm = TRUE),
                                                                                PB_mean=pbmeanfun(pbmrq))
  indexvalue <- tidyr::gather(indexvalue,key = "key", value = "value",-date,-indexID)
  indexvalue <- tidyr::separate(indexvalue,key,c('valtype','caltype'))
  indexvalue <- dplyr::left_join(indexvalue,indexDate[,c('indexID','indexName')],by='indexID')
  indexvalue <- indexvalue[,c("indexID","indexName","date","value","valtype","caltype")]
  indexvalue <- transform(indexvalue,date=rdate2int(date))
  dbDisconnect(con)
  return(indexvalue)
}

QT_IndexValuation_subfunF <- function(indexDate){
  require(WindR)
  w.start(showmenu = FALSE)
  indexValue <- data.frame()
  for(i in 1:nrow(indexDate)){
    indexValue_<-w.wsd(indexDate$indexID[i],"pe_ttm,pb_lf",indexDate$begT[i],indexDate$endT[i])[[2]]
    indexValue_ <- transform(indexValue_,indexID=indexDate$indexID[i],indexName=indexDate$indexName[i])
    indexValue <- rbind(indexValue,indexValue_)
  }

  indexValue <- indexValue[!is.nan(indexValue$PE_TTM),]
  indexpe <- reshape2::dcast(indexValue,DATETIME~indexID,value.var = 'PE_TTM',fill = NA)
  indexpb <- reshape2::dcast(indexValue,DATETIME~indexID,value.var = 'PB_LF',fill = NA)
  indexpe <- zoo::na.locf(indexpe)
  indexpb <- zoo::na.locf(indexpb)
  indexpe <- reshape2::melt(indexpe,id='DATETIME',variable.name = "indexID",value.name = "PE_TTM",na.rm = TRUE)
  indexpb <- reshape2::melt(indexpb,id='DATETIME',variable.name = "indexID",value.name = "PB_LF",na.rm = TRUE)
  indexValue <- left_join(indexpe,indexpb)
  indexValue <- reshape2::melt(indexValue,id=c('DATETIME','indexID'),variable.name = "valtype",value.name = "value")
  indexValue <- transform(indexValue,date=rdate2int(as.Date(DATETIME)),
                          valtype=ifelse(valtype=='PE_TTM','PE','PB'),
                          caltype='median')
  indexValue <- left_join(indexValue,indexDate[,c('indexID','indexName')])
  indexValue <- indexValue[,c("indexID","indexName","date","value","valtype","caltype")]
  return(indexValue)
}


#' lcdb.update.QT_IndexValuation
#'
#' @rdname index_valuation
#' @author Andrew Dow
#' @examples
#' lcdb.update.QT_IndexValuation()
#' @export
lcdb.update.QT_IndexValuation<- function(begT,endT=Sys.Date()-1){
  con <- db.local('main')
  if(missing(begT)){
    begT <- dbGetQuery(con,"select max(date) 'date' from QT_IndexValuation")
    begT <- trday.nearby(intdate2r(begT$date),by=1)
  }

  if(begT<=endT){
    qr <- paste("delete from QT_IndexValuation where date>=",rdate2int(begT)," and date<=",rdate2int(endT))
    dbSendQuery(con,qr)

    indexDate <- dbGetQuery(con,"select distinct indexID,indexName from QT_IndexValuation")
    indexDate <- transform(indexDate,begT=begT,endT=endT)
    indexDateA <- indexDate[!(stringr::str_sub(indexDate$indexID,-3,-1) %in% c('.GI','.HI')),]
    indexDateF <- indexDate[stringr::str_sub(indexDate$indexID,-3,-1) %in% c('.GI','.HI'),]
    re <- QT_IndexValuation_subfun(indexDateA)
    if(begT!=Sys.Date() || endT!=Sys.Date()){
      reF <- QT_IndexValuation_subfunF(indexDateF)
      re <- rbind(re,reF)
    }

    dbWriteTable(con,'QT_IndexValuation',re,overwrite=FALSE,append=TRUE,row.names=FALSE)
  }
  dbDisconnect(con)
  return('Done!')
}


#' @rdname index_valuation
#' @export
#' @examples
#' re <- getIndexValuation()
#' #get newest valuation
#' re <- getIndexValuation(begT = Sys.Date(),endT = Sys.Date())
getIndexValuation <- function(valtype=c('PE','PB'),caltype='median',
                  begT=as.Date('2001-01-04'),endT=Sys.Date()-1){

  if(begT==Sys.Date() && endT==Sys.Date()){
    lcdb.update.QT_IndexValuation(begT,endT)
  }

  con <- db.local('main')
  qr <- paste("select * from QT_IndexValuation where date<=",rdate2int(endT),
              " and valtype in",brkQT(valtype)," and caltype in",brkQT(caltype),sep="")
  re <- dbGetQuery(con,qr)
  re <- re %>% mutate(date=intdate2r(date),value=round(value,digits = 2)) %>% tidyr::unite(type,valtype,caltype)
  Nindex <- unique(re[,c('indexID','indexName')])
  re <- reshape2::dcast(re,date+indexID+indexName~type,value.var = 'value')
  re <- dplyr::arrange(re,indexID,date)

  result <- data.frame()
  for(i in 1:nrow(Nindex)){
    Data <- re %>% dplyr::filter(indexID==Nindex$indexID[i]) %>% dplyr::select(-indexID,-indexName)

    for(j in 2:ncol(Data)){
      Datats <- xts::xts(Data[,j],order.by = Data[,1])
      Datats <- TTR::runPercentRank(Datats, n = 250, cumulative = TRUE, exact.multiplier = 0.5)
      Datats <-  data.frame(date=zoo::index(Datats),indexID=Nindex$indexID[i],
                            indexName=Nindex$indexName[i],
                            value=round(zoo::coredata(Datats),4),stringsAsFactors = FALSE)
      Datats <- Datats[251:nrow(Datats),]
      colnames(Datats) <- c('date','indexID','indexName',paste('per',colnames(Data)[j],sep = ''))
      if(j==2){
        result_ <- Datats
      }else{
        result_ <- dplyr::left_join(result_,Datats,by=c('date','indexID','indexName'))
      }
    }
    result <- rbind(result,result_)
  }

  result <- result %>% dplyr::filter(date>=begT,date<=endT) %>%
    left_join(re,by=c('date','indexID','indexName')) %>%
    dplyr::arrange(date,indexID)

  if(begT==Sys.Date() && endT==Sys.Date()){
    qr <- paste("delete from QT_IndexValuation where date=",rdate2int(Sys.Date()))
    dbSendQuery(con,qr)
  }
  dbDisconnect(con)
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
  qr <- "select 'EI'+s.SecuCode 'stockID',c.MS 'industryName'
  from LC_CorrIndexIndustry l,SecuMain s,CT_SystemConst c
  where l.IndustryStandard=24 and s.SecuMarket=83 and s.SecuCode like '80%'
  and l.IndexCode=s.InnerCode and l.IndustryCode=c.DM
  and c.LB=1804 and c.IVALUE=1"
  indexName <- queryAndClose.odbc(db.jy(),qr,stringsAsFactors=FALSE)

  indexQT <- getIndexQuote(indexName$stockID,begT,endT,variables='close',datasrc="jy")
  indexQT <- indexQT %>% arrange(stockID,date) %>% group_by(stockID) %>%
    mutate(MA1=TTR::SMA(close,8),MA2=TTR::SMA(close,13),
           MA3=TTR::SMA(close,21),MA4=TTR::SMA(close,34),
           MA5=TTR::SMA(close,55),MA6=TTR::SMA(close,89),
           MA7=TTR::SMA(close,144),MA8=TTR::SMA(close,233)) %>% ungroup()
  indexQT <- reshape2::melt(indexQT,id=c("stockID","date","close"),
                               variable.name = "MAtype", na.rm = TRUE, value.name = "MAclose")
  indexQT <- indexQT %>% mutate(upMA=ifelse(close>MAclose,1,0)) %>%
    group_by(stockID,date) %>% summarise(n=n(),score=sum(upMA)) %>% ungroup() %>%
    filter(n==max(n)) %>% mutate(stockID=as.character(stockID)) %>%
    left_join(indexName,by='stockID') %>%
    select(date,stockID,industryName,score) %>%
    arrange(date,desc(score))

  return(indexQT)
}


#' index.rotation
#'
#' @examples
#' indexID <-  CT_industryList(33,1)
#' indexID <-  sectorID2indexID(indexID$IndustryID)
#' re <- index.rotation(indexID)
#' @export
index.rotation <- function(indexID=c('EI000016','EI000905','EI000852'),begT=as.Date('2005-01-04'),
                           endT=Sys.Date(),MApara=20,fee=0.001,sell_count=1){

  rawdata <- getIndexQuote(indexID,begT = begT,endT = endT,variables = c('pre_close','open','close','pct_chg'),datasrc = 'jy')

  # fill zz500 rawdata
  if(begT<as.Date("2007-01-15") && 'EI000905' %in% indexID){
    require(WindR)
    w.start(showmenu = FALSE)
    rawdata_ <- w.wsd("000905.SH","pre_close,open,close,pct_chg",begT,as.Date("2007-01-15"))[[2]]
    rawdata_[is.na(rawdata_)] <- NA
    rawdata_ <- rawdata_ %>% mutate(stockID='EI000905',PCT_CHG=PCT_CHG/100) %>% select(stockID,everything())
    colnames(rawdata_) <- colnames(rawdata)
    rawdata <- rawdata[!(rawdata$date<=as.Date("2007-01-15") & rawdata$stockID=='EI000905'),]
    rawdata <- rbind(rawdata,rawdata_)
    rawdata <- dplyr::arrange(rawdata,stockID,date)
  }

  #get MA data
  indexClose <- reshape2::dcast(rawdata,date~stockID,value.var = 'close')
  quoteMA <- apply(indexClose[,-1], 2, TTR::SMA, n=MApara)
  quoteMA <- cbind(data.frame(date=as.Date(indexClose$date)),quoteMA)
  quoteMA <- quoteMA[MApara:nrow(quoteMA),]
  quoteMA <- reshape2::melt(quoteMA,id.vars='date',variable.name='stockID',value.name='MA')
  quote <- dplyr::left_join(quoteMA,rawdata[,c("date","stockID","close")],by=c("date","stockID"))

  #reshape index rtn
  indexrtn <- rawdata %>% dplyr::group_by(stockID) %>%
    dplyr::mutate(next_open=lead(open),pct_chg_buy=close/open-1-fee,
                  pct_chg_sell=next_open/pre_close-1-fee) %>%
    dplyr::ungroup() %>% dplyr::select(-next_open,-open,-pre_close,-close)


  quote <- quote %>% mutate(tag=ifelse(close>MA,1,0),trade='')
  for(j in indexID){
    quote_ <- quote[quote$stockID==j,]
    pos <- 0
    ncount <- 0
    for(i in 2:nrow(quote_)){
      #buy
      if(pos==0 && quote_[i-1,'tag']==1){
        quote_[i,'trade'] <- 'buy'
        pos <- 1
        if(quote_[i,'tag']==0){
          ncount <- 1
        }
        next
      }

      #hold
      if(pos==1 && quote_[i,'tag']==1){
        quote_[i,'trade'] <- 'hold'
        next
      }

      #sell
      if(pos==1 && quote_[i,'tag']==0){
        if(quote_[i-1,'tag']!=0){
          ncount <- 0
        }
        ncount <- ncount+1
        if(ncount==sell_count){
          quote_[i,'trade'] <- 'sell'
          pos <- 0
          ncount <- 0
        }else{
          quote_[i,'trade'] <- 'hold'
        }
      }# sell end
    } # inner for loop end
    quote <- quote[quote$stockID!=j,]
    quote <- rbind(quote,quote_)
  }

  #reduct fee from index return
  indexrtn_adj <- dplyr::left_join(quote,indexrtn,by=c('date','stockID'))
  indexrtn_adj <- transform(indexrtn_adj,pct_chg=ifelse(trade=='buy',pct_chg_buy,ifelse(trade=='sell',pct_chg_sell,pct_chg)))
  indexrtn_adj <- reshape2::dcast(indexrtn_adj,date~stockID,value.var = 'pct_chg')
  indexrtn_adj <- transform(indexrtn_adj,cash=0)

  #get daily wgt
  wgt <- quote %>% mutate(trade=ifelse(trade %in% c('buy','sell','hold'),1,0)) %>% select(date,stockID,trade)
  wgt_index <- wgt %>% arrange(date,stockID) %>% group_by(date) %>%
    mutate(trade=trade/sum(trade)) %>% ungroup()
  wgt_index[is.nan(wgt_index$trade),'trade'] <- 0
  wgt_cash <- wgt_index %>% group_by(date) %>% summarise(trade=1-sum(trade)) %>% ungroup() %>%
    mutate(stockID='cash') %>% select(date,stockID,trade)
  wgt <- rbind(as.data.frame(wgt_index),as.data.frame(wgt_cash))
  wgt <- reshape2::dcast(wgt,date~stockID,value.var = 'trade')
  wgt <- wgt[,colnames(indexrtn_adj)]

  wgt <- transform(wgt,tag=0)
  for(i in 1:nrow(wgt)){
    if(i==1){
      wgt$tag[i] <- 1
    }else{
      if(!all(wgt[i,2:(ncol(wgt)-1)]==wgt[i-1,2:(ncol(wgt)-1)])){
        wgt$tag[i] <- 1
      }
    }
  }
  wgt <- wgt[wgt$tag==1,]
  wgt$tag <- NULL

  wgt <- xts::xts(wgt[,-1],wgt[,1])
  indexrtn_adj <- xts::xts(indexrtn_adj[,-1],order.by = indexrtn_adj[,1])
  #get return
  re <- Return.backtesting(indexrtn_adj,weights = wgt)
  return(re)
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

    sf.info <- dbReadTable(db.local('main'), "SF_Info")

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
      con <- db.local('main')
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


#' read from or write to clipboard
#'
#' @description \code{read.clipboard} read data from clipboard
#' @description \code{write.clipboard} write data to clipboard
#' @name read_write_clipboard
#' @rdname read_write_clipboard
#'
#' @export
#' @examples
#' re <- read.clipboard()
#' write.clipboard(re)
read.clipboard <- function(file = "clipboard", sep = "\t",header = TRUE,...) {
  read.table(file = file, sep = sep, header=header,...)
}

#' @rdname read_write_clipboard
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
  con <- db.local('main')
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


#' lcdb.build.Bond_ConBDExchangeQuote
#'
#' @name cbondfuns
#' @export
#' @examples
#' lcdb.build.Bond_ConBDExchangeQuote()
#' lcdb.update.Bond_ConBDExchangeQuote()
lcdb.build.Bond_ConBDExchangeQuote <- function(){
  qr <- "select convert(varchar,cb.TradingDay,112) 'date',
  case c.SecuMarket
  when 83 then c.SecuCode+'.SH'
  when 90 then c.SecuCode+'.SZ'
  END 'bondID',c.SecuAbbr 'bondName',
  cb.BondNature,cb.Maturity,cb.YrMat,cb.ClosePrice,cb.ChangePCT,cb.TurnoverRate,cb.TurnoverValue
  ,cb.NewConvetPrice,cb.StockPrice,cb.ConvertPremiumRate
  from Bond_ConBDExchangeQuote cb
  INNER JOIN Bond_Code c on cb.InnerCode=c.InnerCode and c.BondNature in(10,29)
  where cb.TradingDay>='2005-01-01' and cb.YrMat is not NULL
  and cb.BondNature in(1,4)
  order by cb.TradingDay,c.SecuCode"
  cvbond <- queryAndClose.odbc(db.jy(),qr,stringsAsFactors =FALSE)

  require(WindR)
  w.start(showmenu = FALSE)

  #fix bugs to do
  cvbugs <- cvbond %>% filter(is.na(ConvertPremiumRate))
  if(nrow(cvbugs)>0){
    cvbugsbond <- cvbugs %>% group_by(bondID) %>%
      summarise(begT=intdate2r(min(date)),endT=intdate2r(max(date)))
    ConvetPrice <- data.frame(stringsAsFactors = FALSE)
    for(i in 1:nrow(cvbugsbond)){
      ConvetPrice_ <- w.wsd(cvbugsbond$bondID[i],'clause_conversion2_swapshareprice',cvbugsbond$begT[i],cvbugsbond$endT[i])[[2]]
      ConvetPrice_ <- ConvetPrice_ %>% mutate(bondID=as.character(cvbugsbond$bondID[i]),DATETIME=rdate2int(DATETIME)) %>%
        rename(date=DATETIME,NewConvetPrice=CLAUSE_CONVERSION2_SWAPSHAREPRICE) %>%
        select(date,bondID,NewConvetPrice)
      ConvetPrice <- rbind(ConvetPrice,ConvetPrice_)
    }
    cvbugs <- cvbugs %>% select(-NewConvetPrice) %>%
      left_join(ConvetPrice,by=c('date','bondID')) %>%
      mutate(ConvertPremiumRate=(ClosePrice/(StockPrice/NewConvetPrice*100)-1)*100)
    cvbugs <- cvbugs[,colnames(cvbond)]
    cvbond <- cvbond %>% filter(!is.na(ConvertPremiumRate)) %>%
      bind_rows(cvbugs) %>% arrange(date,bondID)

  }

  #add data
  cvstat <- cvbond %>% group_by(bondID) %>% summarise(begT=min(date),endT=max(date)) %>%
    ungroup() %>% mutate(begT=intdate2r(begT),endT=intdate2r(endT))
  strbvalue <- data.frame()

  for(i in 1:nrow(cvstat)){
    data <- w.wsd(cvstat$bondID[i],"strbvalue",cvstat$begT[i],cvstat$endT[i])[[2]]
    data <- rename(data,date=DATETIME,strbvalue=STRBVALUE)
    data <- cbind(data.frame(bondID=cvstat$bondID[i],row.names = NULL,stringsAsFactors = FALSE),data)
    strbvalue <- rbind(strbvalue,data)
  }
  strbvalue <- transform(strbvalue,date=rdate2int(date))
  cvbond <- left_join(cvbond,strbvalue,by=c('date','bondID'))
  cvbond <- transform(cvbond,strbPremiumRate=(ClosePrice/strbvalue-1)*100)
  con <- db.local('main')
  if(dbExistsTable(con,"Bond_ConBDExchangeQuote")){
    dbRemoveTable(con,"Bond_ConBDExchangeQuote")
  }
  dbWriteTable(con,'Bond_ConBDExchangeQuote',cvbond)
  dbDisconnect(con)
}


#' lcdb.update.Bond_ConBDExchangeQuote
#'
#' @rdname cbondfuns
#' @export
lcdb.update.Bond_ConBDExchangeQuote <- function(){
  con <- db.local('main')
  begT <- dbGetQuery(con,"select max(date) from Bond_ConBDExchangeQuote")[[1]]
  begT <- intdate2r(begT)
  endT <- trday.nearest(Sys.Date()-1)
  if(endT>begT){
    qr <- paste("select convert(varchar,cb.TradingDay,112) 'date',
                case c.SecuMarket
                when 83 then c.SecuCode+'.SH'
                when 90 then c.SecuCode+'.SZ'
                END 'bondID',c.SecuAbbr 'bondName',
                cb.BondNature,cb.Maturity,cb.YrMat,cb.ClosePrice,cb.ChangePCT,cb.TurnoverRate,cb.TurnoverValue
                ,cb.NewConvetPrice,cb.StockPrice,cb.ConvertPremiumRate
                from Bond_ConBDExchangeQuote cb
                INNER JOIN Bond_Code c on cb.InnerCode=c.InnerCode
                and c.BondNature in(10,29)
                where cb.TradingDay>",QT(begT)," and cb.TradingDay<=",QT(endT)," and cb.YrMat is not NULL
                order by cb.TradingDay,c.SecuCode")
    cvbond <- queryAndClose.odbc(db.jy(),qr)

    cvstat <- cvbond %>% filter(BondNature %in% c(1,4)) %>% mutate(date=intdate2r(date)) %>% select(date,bondID)
    dates <- unique(cvstat$date)
    strbvalue <- data.frame()
    require(WindR)
    w.start(showmenu = FALSE)
    for(i in 1:length(dates)){
      cvstat_ <- cvstat %>% filter(date==dates[i])
      data <- w.wss(cvstat_$bondID,'strbvalue',tradeDate=dates[i])[[2]]
      data <- rename(data,bondID=CODE,strbvalue=STRBVALUE)
      data <- cbind(data.frame(date=dates[i],row.names = NULL),data)
      strbvalue <- rbind(strbvalue,data)
    }
    strbvalue <- transform(strbvalue,date=rdate2int(date))
    cvbond <- left_join(cvbond,strbvalue,by=c('date','bondID'))
    cvbond <- transform(cvbond,strbPremiumRate=(ClosePrice/strbvalue-1)*100)
    dbWriteTable(con,'Bond_ConBDExchangeQuote',cvbond,overwrite=FALSE,append=TRUE)
    dbDisconnect(con)
  }
}

















