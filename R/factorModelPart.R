# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ======================
# ===================== series of lcdb functions  ===========================
# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ======================





#' combine funcs table.IC and table.Ngroup.spread
#'
#'
#' @author Andrew Dow
#' @param TSFR is a TSFR object
#' @param N seprate the stocks into N group
#' @param fee trading cost
#' @return a factor's IC summary and longshort portfolio's return summary.
#' @examples
#' modelPar <- modelPar.default()
#' TSFR <- Model.TSFR(modelPar)
#' table.factor.summary(TSFR)
#' @export
table.factor.summary <- function(TSFR,N=10,fee=0.001){
  seri <- seri.IC(TSFR)
  seri <- as.vector(seri)
  IC.mean <- mean(seri,na.rm=TRUE)
  IC.std <- sd(seri,na.rm=TRUE)
  IC.IR <- IC.mean/IC.std
  IC.hit <- hitRatio(seri)

  ICsum <- c(IC.mean, IC.std, IC.IR, IC.hit)
  ICsum <- matrix(ICsum,length(ICsum),1)
  rownames(ICsum) <- c("IC.mean","IC.std","IC.IR","IC.hitRatio")

  rtnseri <- seri.Ngroup.rtn(TSFR,N=N)
  turnoverseri <- seri.Ngroup.turnover(TSFR,N=N)
  spreadseri <- rtnseri[,1]-rtnseri[,ncol(rtnseri)]
  rtnsummary <- rtn.summary(spreadseri)
  turnover.annu <- Turnover.annualized(turnoverseri)
  turnover.annu <- sum(turnover.annu[,c(1,ncol(turnover.annu))])/2
  rtn.feefree <- rtnsummary[1,]-turnover.annu*fee*2*2   # two side trade and two groups
  rtnsum <- rbind(rtnsummary,turnover.annu,rtn.feefree)
  rownames(rtnsum)[c(nrow(rtnsum)-1,nrow(rtnsum))] <- c("Annualized Turnover","Annualized Return(fee cut)")

  re <- rbind(ICsum,rtnsum)
  colnames(re) <- 'factorSummary'
  re <- round(re,digits = 3)
  return(re)
}

#' lcdb.add.QT_IndexQuote
#'
#' @export
#' @examples
#' lcdb.add.QT_IndexQuote("EI000852")
lcdb.add.QT_IndexQuote <- function(indexID){
  con <- db.local('main')
  date <- dbGetQuery(con,"select min(TradingDay) 'begT',max(TradingDay) 'endT' from QT_IndexQuote")
  begT <- intdate2r(date$begT)
  endT <- intdate2r(date$endT)
  qr <- paste("SELECT q.InnerCode,convert(VARCHAR,TradingDay,112) 'TradingDay'
              ,PrevClosePrice,OpenPrice,HighPrice,LowPrice,ClosePrice
              ,TurnoverVolume,TurnoverValue,TurnoverDeals,ChangePCT
              ,NegotiableMV,q.XGRQ 'UpdateTime',ChangePCT/100 'DailyReturn',
              'EI'+s.SecuCode 'ID'
              FROM QT_IndexQuote q,SecuMain s
              WHERE q.InnerCode=s.InnerCode and s.SecuCode in",brkQT(substr(indexID,3,8)),
              " and TradingDay>=",QT(begT)," and TradingDay<=",QT(endT))
  re <- queryAndClose.odbc(db.jy(),qr,stringsAsFactors=FALSE)
  dbGetQuery(con,paste("delete from QT_IndexQuote where ID in",brkQT(indexID),sep=''))
  dbWriteTable(con,"QT_IndexQuote",re,overwrite=FALSE,append=TRUE,row.names=FALSE)
  dbDisconnect(con)
}


#' add weight to port
#'
#'
#' @author Andrew Dow
#' @param port is a  object.
#' @param wgtType a character string, giving the weighting type of portfolio,which could be "fs"(floatingshares),"fssqrt"(sqrt of floatingshares).
#' @param wgtmax weight upbar.
#' @param ... for Category Weighted method
#' @return return a Port object which are of dataframe class containing at least 3 cols("date","stockID","wgt").
#' @seealso \code{\link[RFactorModel]{addwgt2port}}
#' @examples
#' port <- portdemo[,c('date','stockID')]
#' port <- addwgt2port_amtao(port)
#' port <- addwgt2port_amtao(port,wgtmax=0.1)
#' @export
addwgt2port_amtao <- function(port,wgtType=c('fs','fssqrt','ffsMV'),wgtmax=NULL,totwgt=1,...){
  wgtType <- match.arg(wgtType)
  if(wgtType %in% c('fs','fssqrt')){
    port <- TS.getTech(port,variables="free_float_shares")
    if (wgtType=="fs") {
      port <- port %>% dplyr::group_by(date) %>% dplyr::mutate(wgt=free_float_shares/sum(free_float_shares,na.rm=TRUE)*totwgt)
    } else {
      port <- port %>% dplyr::group_by(date) %>% dplyr::mutate(wgt=sqrt(free_float_shares)/sum(sqrt(free_float_shares),na.rm=TRUE)*totwgt)
    }
    port$free_float_shares <- NULL
  }else{
    port <- gf.free_float_sharesMV(port)
    port <- port %>% dplyr::group_by(date) %>% dplyr::mutate(wgt=factorscore/sum(factorscore,na.rm=TRUE)*totwgt)
    port <- transform(port,factorscore=NULL,wgt=ifelse(is.na(wgt),0,wgt))
  }


  if(!is.null(wgtmax)){
    subfun <- function(wgt){
      if(length(wgt)*wgtmax<=totwgt){
        wgt_ <- rep(wgtmax,length(wgt))
      }else{
        df <- data.frame(wgt=wgt,rank=seq(1,length(wgt)))
        df <- arrange(df,plyr::desc(wgt))
        j <- 1
        while(max(df$wgt)>wgtmax){
          df$wgt[j] <- wgtmax
          df$wgt[(j+1):nrow(df)] <- df$wgt[(j+1):nrow(df)]/sum(df$wgt[(j+1):nrow(df)])*(totwgt-j*wgtmax)
          j <- j+1
        }
        df <- plyr::arrange(df,rank)
        wgt_ <- df$wgt
      }
      return(wgt_)
    }
    port <- port %>% dplyr::group_by(date) %>% dplyr::mutate(newwgt=subfun(wgt))
    port$wgt <- port$newwgt
    port$newwgt <- NULL
  }
  port <- as.data.frame(port)
  return(port)
}




# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ======================
# ===================== series of remove functions  ===========================
# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ======================



#' remove negative event stock from TS
#'
#' @author Andrew Dow
#' @param TS is a \bold{TS} object.
#' @param type is negative events' type.
#' @return return a \bold{TS} object.
#' @examples
#' RebDates <- getRebDates(as.Date('2013-03-17'),as.Date('2016-04-17'),'month')
#' TS <- getTS(RebDates,'EI000985')
#' TSnew <- rmNegativeEvents(TS)
#' @export
rmNegativeEvents <- function(TS,type=c('AnalystDown','PPUnFrozen','ShareholderReduction')){
  if(missing(type)) type <- 'AnalystDown'

  if('AnalystDown' %in% type){
    # analyst draw down company's profit forcast
    TS <- rmNegativeEvents.AnalystDown(TS)
  }

  if('PPUnFrozen' %in% type){
    #private placement offering unfrozen
    TS <- rmNegativeEvents.PPUnFrozen(TS)
  }

  if('ShareholderReduction' %in% type){


  }


  return(TS)
}


#' remove price limits
#'
#' @author Andrew Dow
#' @examples
#' RebDates <- getRebDates(as.Date('2013-03-17'),as.Date('2016-04-17'),'month')
#' TS <- getTS(RebDates,'EI000985')
#' TSnew <- rmPriceLimit(TS,dateType='today',priceType='downLimit')
#' @export
rmPriceLimit <- function(TS,dateType=c('nextday','today'),priceType=c('upLimit','downLimit')){
  dateType <- match.arg(dateType)
  priceType <- match.arg(priceType)
  if(dateType=='nextday'){
    TStmp <- data.frame(date=trday.nearby(TS$date,by=1), stockID=TS$stockID)
    TStmp$date <- rdate2int(TStmp$date)
  }else if(dateType=='today'){
    TStmp <- TS
    TStmp$date <- rdate2int(TStmp$date)
  }
  con <- db.local('qt')
  qr <- paste("SELECT u.TradingDay 'date',u.ID 'stockID',u.DailyReturn
          FROM QT_DailyQuote u
          where u.TradingDay in",paste("(",paste(unique(TStmp$date),collapse = ","),")"))
  re <- dbGetQuery(con,qr)
  dbDisconnect(con)
  suppressWarnings(TStmp <- dplyr::left_join(TStmp,re,by=c('date','stockID')))
  TStmp <- na.omit(TStmp)
  if(priceType=='upLimit'){
    re <- TS[TStmp$DailyReturn<0.099, ]
  }else if(priceType=='downLimit'){
    re <- TS[TStmp$DailyReturn>(-0.099), ]
  }

  return(re)

}


#' lcdb.update.QT_IndexQuoteamtao
#'
#' @export
lcdb.update.QT_IndexQuoteamtao <- function(begT,endT,IndexID,datasrc='quant'){
  con <- db.local('main')
  if(missing(begT)){
    if(missing(IndexID)){
      begT <- dbGetQuery(con,"select max(TradingDay) from QT_IndexQuote")[[1]]
    } else {
      begT <- dbGetQuery(con,"select min(TradingDay) from QT_IndexQuote")[[1]]
    }
  }

  if(missing(endT)){
    if(missing(IndexID)){
      endT <- rdate2int(Sys.Date())
    } else {
      endT <- dbGetQuery(con,"select max(TradingDay) from QT_IndexQuote")[[1]]
    }
  }

  begT_filt <- paste("TradingDay >=",begT)
  endT_filt <- paste("TradingDay < ",endT)

  if(missing(IndexID)){
    pool_filt <- "1>0"
  } else{
    pool_filt <- paste("ID in",brkQT(IndexID))
  }

  if(datasrc=='quant'){
    tb.from <- queryAndClose.odbc(db.quant(),query=paste("select * from QT_IndexQuote where ",begT_filt,"and",endT_filt,"and",pool_filt))
  }else if(datasrc=='jy'){
    begT_filt_ <- paste("TradingDay >=",QT(intdate2r(begT)))
    endT_filt_ <- paste("TradingDay < ",QT(intdate2r(endT)))
    IndexID_ <- stringr::str_replace(IndexID,'EI','')
    pool_filt_ <- paste("SecuCode in",brkQT(IndexID_))
    qr <- paste("SELECT q.InnerCode,
                year(TradingDay)*10000+month(TradingDay)*100+day(TradingDay) 'TradingDay',
                PrevClosePrice,OpenPrice,HighPrice,LowPrice,ClosePrice,TurnoverVolume,
                TurnoverValue,TurnoverDeals,ChangePCT,NegotiableMV,
                (case when PrevClosePrice is not null and PrevClosePrice <> 0 then ClosePrice/PrevClosePrice-1 else null end) 'DailyReturn',
                'EI'+s.SecuCode 'ID'
                FROM QT_IndexQuote q,SecuMain s
                where q.InnerCode=s.InnerCode and ",begT_filt_,"and",endT_filt_,"and",pool_filt_)
    tb.from <- queryAndClose.odbc(db.jy(),query=qr)
  }

  if(NROW(tb.from)==0){
    return()
  }
  dbGetQuery(con,paste("delete from QT_IndexQuote where",begT_filt,"and",endT_filt,"and",pool_filt))
  dbWriteTable(con,"QT_IndexQuote",tb.from,overwrite=FALSE,append=TRUE,row.names=FALSE)
  dbDisconnect(con)
}


# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ======================
# ===================== series of gf functions  ===========================
# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ======================

#' get factor
#'
#' @param TS is \bold{TS} object.
#' @name get_factor
#' @return a TSF
#' @examples
#' RebDates <- getRebDates(as.Date('2011-03-17'),as.Date('2012-04-17'),'month')
#' TS <- getTS(RebDates,'EI000300')
#' TSF <- gf.smartQ(TS)
#' TSF <- gf.dividendyield(TS)
#' TSF <- gf.doublePrice(TS)
NULL




#' @rdname get_factor
#' @export
gf.smartQ <- function(TS,nwin=20,cycle='cy_1m()',bar=0.2) {
  #load data
  newTS <- rm_suspend(TS)
  tmp <- newTS %>% group_by(stockID) %>%
    summarise(begT=min(date),endT=max(date))
  tmp <- transform(tmp,stockID=as.character(stockID)
                   ,begT=trday.nearby(begT,-nwin))

  pb <- txtProgressBar(style = 3)
  TSF <- data.frame()
  variables <- c("open","close","vol")
  for(i in 1:nrow(tmp)){
    #cat(i," : ",tmp$stockID[i],'\n')
    stocks <- stockID2stockID(tmp$stockID[i],'local','ts')
    qtdata <- getQuote_ts(stocks,tmp$begT[i],tmp$endT[i],variables,Cycle = cycle)
    qtdata <- dplyr::filter(qtdata,vol>0)
    qtdata$smart <- abs(qtdata$close/qtdata$open-1)/(qtdata$vol/100000000)

    tmp.TS <- subset(newTS,stockID==tmp$stockID[i])
    for(j in 1:nrow(tmp.TS)){
      tmp.begT <- trday.nearby(tmp.TS$date[j],-nwin)
      tmp.qtdata <- dplyr::filter(qtdata,date<=tmp.TS$date[j],date>tmp.begT)
      tmp.qtdata <- dplyr::arrange(tmp.qtdata,dplyr::desc(smart))
      tmp.qtdata$cumvol <- cumsum(tmp.qtdata$vol)/sum(tmp.qtdata$vol)
      tmp.qtdatasmart <- dplyr::filter(tmp.qtdata,cumvol<=bar)
      tmp.Q <- ((tmp.qtdatasmart$close %*% tmp.qtdatasmart$vol)/sum(tmp.qtdatasmart$vol))/((tmp.qtdata$close %*% tmp.qtdata$vol)/sum(tmp.qtdata$vol))
      tmp.TSF <- data.frame(tmp.TS[j,],Q=tmp.Q)
      TSF <- rbind(TSF,tmp.TSF)
    }

    setTxtProgressBar(pb, i/nrow(tmp))
  }
  close(pb)

  TSF <- dplyr::left_join(TS,TSF,by=c('date','stockID'))
  return(TSF)

}




#' @rdname get_factor
#' @export
gf.doublePrice <- function(TS,PBFun=c('gf.PB_mrq','gf.PB_lyr'),ROEFun=c('gf.ROE_ttm','gf.ROE','gf.F_ROE')){
  PBFun <- match.arg(PBFun)
  ROEFun <- match.arg(ROEFun)

  PB <- getTSF(TS,factorFun = PBFun)
  ROE <- getTSF(TS,factorFun = ROEFun)
  PB <- PB %>% dplyr::rename(pb=factorscore) %>% dplyr::select(date,stockID,pb)
  ROE <- ROE %>% dplyr::rename(roe=factorscore) %>% dplyr::select(date,stockID,roe)
  if(ROEFun!='gf.F_ROE'){
    ROE <- transform(ROE,roe=roe/100)
  }
  TSF <- dplyr::left_join(PB,ROE,by=c('date','stockID'))
  TSF <- dplyr::filter(TSF,!is.na(pb),roe>0)
  TSF$factorscore <- log(TSF$pb*2,base=(1+TSF$roe))
  TSF <- TSF[,c("date","stockID","factorscore")]
  TSF <- dplyr::left_join(TS,TSF,by=c('date','stockID'))
  return(TSF)
}



#' @rdname get_factor
#' @export
gf.dividendyield <- function(TS,datasrc=c('ts','jy','wind')){
  datasrc <- match.arg(datasrc)
  if(datasrc=='jy'){
    tmp <- brkQT(substr(unique(TS$stockID),3,8))
    qr <- paste("SELECT convert(varchar,TradingDay,112) 'date',
              'EQ'+s.SecuCode 'stockID',isnull(DividendRatio,0) 'factorscore'
              FROM LC_DIndicesForValuation d,SecuMain s
              where d.InnerCode=s.InnerCode and s.SecuCode in",tmp,
                " and d.TradingDay>=",QT(min(TS$date))," and d.TradingDay<=",QT(max(TS$date)),
                " ORDER by d.TradingDay")
    re <- queryAndClose.odbc(db.jy(),qr,stringsAsFactors=FALSE)
    re <- transform(re,date=intdate2r(date))
    TSF <- dplyr::left_join(TS,re,by=c('date','stockID'))
  }else if(datasrc=='wind'){
    require(WindR)
    w.start(showmenu = FALSE)
    newTS <- transform(TS,stockID=stockID2stockID(stockID,'local','wind'))
    dates <- unique(TS$date)
    re <- data.frame()
    for(i in dates){
      i <- as.Date(i,origin='1970-01-01')
      tmp <- w.wss(newTS[newTS$date==i,'stockID'],'dividendyield2',tradeDate=i)[[2]]
      colnames(tmp) <- c('stockID','factorscore')
      tmp$date <- i
      re <- rbind(re,tmp[,c('date','stockID','factorscore')])
    }
    re <- transform(re,stockID=stockID2stockID(stockID,'wind','local'),
                    factorscore=factorscore/100)
    TSF <- dplyr::left_join(TS,re,by=c('date','stockID'))
  }else if(datasrc=='ts'){
    dates <- unique(TS$date)
    TSF <- data.frame()
    for(i in 1:length(dates)){
      stocks <- c(TS[TS$date==dates[i],'stockID'])
      funchar <- paste("'factorscore',StockDividendYieldRatio(",rdate2ts(dates[i]),")",sep = '')
      re <- ts.wss(stocks,funchar)
      TSF <- rbind(TSF,data.frame(date=dates[i],re))
    }
    TSF <- dplyr::left_join(TS,TSF,by=c('date','stockID'))
  }

  return(TSF)
}



#' @rdname get_factor
#' @export
gf.BP_mrq <- function(TS) {
  TSF <- gf.PB_mrq(TS,fillna = FALSE)
  TSF <- transform(TSF,factorscore=1/factorscore)
  return(TSF)
}


#' @rdname get_factor
#' @export
gf.EP_ttm <- function(TS) {
  TSF <- gf.PE_ttm(TS,fillna = FALSE)
  TSF <- transform(TSF,factorscore=1/factorscore)
  return(TSF)
}


#' @rdname get_factor
#' @export
gf.CFP_ttm <- function(TS) {
  TSF <- gf.PCF_ttm(TS,fillna = FALSE)
  TSF <- transform(TSF,factorscore=1/factorscore)
  return(TSF)
}

#' @rdname get_factor
#' @export
gf.ROA_ttm <- function(TS){
  funchar <-  '"factorscore",Last12MData(Rdate,9900105)'
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  return(re)
}

#' @rdname get_factor
#' @export
gf.momentum <- function(TS,Nlong=250,Nshort=20){
  funchar <- paste("StockZf2(",Nlong,")",sep="")
  TSF <- getTech_ts(TS,funchar,varname="factorscore")
  if(!is.null(Nshort)){
    funchar <- paste("StockZf2(",Nshort,")",sep="")
    tmp <- getTech_ts(TS,funchar,varname="short")
    TSF <- dplyr::left_join(TSF,tmp,by = c("date", "stockID"))
    TSF <- transform(TSF,factorscore=factorscore-short,
                     short=NULL)
  }
  return(TSF)
}

#' @rdname get_factor
#' @export
gf.lev_dtoa <- function(TS){
  funchar='"factorscore",reportofall(9900203,RDate)'
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  return(re[,c("date","stockID","rptDate","factorscore")])
}

#' @rdname get_factor
#' @export
gf.lev_ldtoop <- function(TS){
  funchar='"factorscore",reportofall(9900205,RDate)'
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  return(re[,c("date","stockID","rptDate","factorscore")])
}

#' @rdname get_factor
#' @export
gf.nl_size <- function(TS){
  TSF <- getTSF(TS,factorFun = 'gf.ln_mkt_cap',factorDir = -1,
                factorRefine = refinePar_default("scale"))
  TSF <- TSF[!is.na(TSF$factorscore),]
  TSF <- dplyr::rename(TSF,ln_mkt_cap=factorscore)
  TSF <- transform(TSF,factorscore=ln_mkt_cap^3)
  TSF <- factor_orthogon_single(TSF,y='factorscore',x='ln_mkt_cap',sectorAttr = NULL)
  TSF <- dplyr::left_join(TS,TSF[,c('date','stockID','factorscore')],by=c('date','stockID'))
  return(TSF)
}

#' @rdname get_factor
#' @export
gf.inner_growth <- function(TS){
  funchar <- '"ROE",Last12MData(Rdate,9900100),
  "div",reportofall(9900500,RDate)'
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  re <- transform(re,factorscore=ROE*(1-div/100))
  re <- re[,c('date','stockID','factorscore')]
  return(re)
}

#' @rdname get_factor
#' @export
gf.G_NP_longterm <- function(TS,N=12,freq="q",funchar='"np",Last12MData(RDate,46078)/100000000',stat="slope/mean",rm_N = 6){
  TSnew <- getrptDate_newest(TS)
  rptTS <- unique(TSnew[,c("rptDate","stockID")])
  FinStat <- rptTS.getFinStat_ts(rptTS,N=N,freq=freq,funchar = funchar,varname = 'factorscore',stat=stat,rm_N=rm_N)
  TSF <- dplyr::left_join(TSnew,FinStat,by=c('rptDate','stockID'))
  TSF$rptDate <- NULL
  return(TSF)
}

#' @rdname get_factor
#' @export
gf.G_OR_longterm <- function(TS,N=12,freq="q",funchar='"or",Last12MData(RDate,46002)/100000000',stat="slope/mean",rm_N = 6){
  TSnew <- getrptDate_newest(TS)
  rptTS <- unique(TSnew[,c("rptDate","stockID")])
  FinStat <- rptTS.getFinStat_ts(rptTS,N=N,freq=freq,funchar = funchar,varname = 'factorscore',stat=stat,rm_N=rm_N)
  TSF <- dplyr::left_join(TSnew,FinStat,by=c('rptDate','stockID'))
  TSF$rptDate <- NULL
  return(TSF)
}





#' group factor
#'
#' @param TS is \bold{TS} object.
#' @name group_factor
#' @return a TSF
#' @examples
#'
#'
NULL

group_factor_subfun <- function(TS,factorType,refinePar,FactorLists,wgt){
  if(missing(FactorLists)){
    group_factor <- CT_GroupFactorLists()
    group_factor <- group_factor[group_factor$factorType==factorType,c("factorName","factorFun","factorPar","factorDir","wgt")]
    local_factor <- CT_FactorLists()
    local_factor <- local_factor[,c("factorID","factorName","factorPar")]
    local_factor <- rename(local_factor,factorPar2=factorPar)
    group_factor <- dplyr::left_join(group_factor,local_factor,by="factorName")
    for(i in 1:nrow(group_factor)){
      if(!is.na(group_factor$factorID[i]) && group_factor$factorPar[i]==group_factor$factorPar2[i]){
        FactorLists_ <- buildFactorLists_lcfs(group_factor$factorID[i],factorRefine = refinePar)
      }else{
        if(group_factor$factorPar[i]==""){
          FactorLists_ <- buildFactorLists(
            buildFactorList(factorFun=group_factor$factorFun[i],
                            factorPar=list(),
                            factorDir=group_factor$factorDir[i],
                            factorName = group_factor$factorName[i],
                            factorRefine=refinePar))
        }else{
          FactorLists_ <- buildFactorLists(
            buildFactorList(factorFun=group_factor$factorFun[i],
                            factorPar=group_factor$factorPar[i],
                            factorDir=group_factor$factorDir[i],
                            factorName = group_factor$factorName[i],
                            factorRefine=refinePar))
        }
      }
      if(i==1){
        FactorLists <- FactorLists_
      }else{
        FactorLists <- c(FactorLists,FactorLists_)
      }
    }
    wgt <- group_factor$wgt
  }else{
    if(missing(wgt)){
      wgt <- rep(1/length(FactorLists),length(FactorLists))
    }

  }

  TSF <- getMultiFactor(TS,FactorLists,wgt)
  return(TSF[,c("date","stockID","factorscore")])
}


#' @rdname group_factor
#' @export
gf.SIZE <- function(TS,refinePar=refinePar_default(type = 'scale',sectorAttr = defaultSectorAttr()),FactorLists,wgt){
  re <- group_factor_subfun(TS,factorType="SIZE",refinePar,FactorLists,wgt)
}


#' @rdname group_factor
#' @export
gf.GROWTH <- function(TS,refinePar=refinePar_default(type = 'scale',sectorAttr = defaultSectorAttr()),FactorLists,wgt){
  re <- group_factor_subfun(TS,"GROWTH",refinePar,FactorLists,wgt)
}



#' @rdname group_factor
#' @export
gf.TRADING <- function(TS,refinePar=refinePar_default(type = 'scale',sectorAttr = defaultSectorAttr()),FactorLists,wgt){
  re <- group_factor_subfun(TS,"TRADING",refinePar,FactorLists,wgt)
}

#' @rdname group_factor
#' @export
gf.EARNINGYIELD <- function(TS,refinePar=refinePar_default(type = 'scale',sectorAttr = defaultSectorAttr()),FactorLists,wgt){
  re <- group_factor_subfun(TS,"EARNINGYIELD",refinePar,FactorLists,wgt)
}

#' @rdname group_factor
#' @export
gf.VALUE <- function(TS,refinePar=refinePar_default(type = 'scale',sectorAttr = defaultSectorAttr()),FactorLists,wgt){
  re <- group_factor_subfun(TS,"VALUE",refinePar,FactorLists,wgt)
}


#' @rdname group_factor
#' @export
gf.OTHER <- function(TS,refinePar=refinePar_default(type = 'scale',sectorAttr = defaultSectorAttr()),FactorLists,wgt){
  re <- group_factor_subfun(TS,"OTHER",refinePar,FactorLists,wgt)
}

#' refinePar_zz
#'
#' @export
#' @examples
#' refinePar <- refinePar_zz()
refinePar_zz <- function(type=c("zz","none"),
         sectorAttr=defaultSectorAttr(),
         log=FALSE,
         regLists=list(fl_cap(log=TRUE))){
  type <- match.arg(type)
  if(type=="none"){
    re <- list(outlier=list(method = "none",
                            par=NULL,
                            sectorAttr= NULL),
               std=list(method = "none",
                        log=log,
                        sectorAttr=NULL,
                        regLists=NULL),
               na=list(method = "none",
                       sectorAttr=NULL)
    )
  }else if(type=="zz"){
    re <- list(outlier=list(method = "percentage",
                            par=5,
                            sectorAttr= NULL),
               std=list(method = "scale",
                        log=FALSE,
                        sectorAttr=NULL,
                        regLists=NULL),
               na=list(method = "mean",
                       sectorAttr=sectorAttr)
    )
  }
  return(re)
}



gf.liquidityold <- function(TS,nwin=22){
  check.TS(TS)
  begT <- trday.nearby(min(TS$date),-nwin)
  endT <- max(TS$date)

  conn <- db.local(dbname = 'qt')
  qr <- paste("select t.TradingDay ,t.ID 'stockID',t.TurnoverVolume/10000 'TurnoverVolume',t.NonRestrictedShares
              from QT_DailyQuote t where ID in",brkQT(unique(TS$stockID)),
              " and t.TradingDay>=",rdate2int(begT),
              " and t.TradingDay<",rdate2int(endT))
  rawdata <- RSQLite::dbGetQuery(conn,qr)
  RSQLite::dbDisconnect(conn)
  rawdata <- dplyr::filter(rawdata,TurnoverVolume>=0,NonRestrictedShares>0)
  rawdata <- transform(rawdata,TradingDay=intdate2r(TradingDay),
                       TurnoverRate=TurnoverVolume/NonRestrictedShares)
  rawdata <- rawdata[,c("TradingDay","stockID","TurnoverRate")]
  re <- expandTS2TSF(TS,nwin,rawdata)

  tmp.TSF <- re %>% dplyr::group_by(date, stockID) %>%
    dplyr::summarise(factorscore=sum(TurnoverRate,na.rm = T)) %>% dplyr::ungroup()
  tmp.TSF <- dplyr::filter(tmp.TSF,factorscore>0)
  tmp.TSF <- transform(tmp.TSF,factorscore=log(factorscore))

  TSF <- dplyr::left_join(TS,tmp.TSF,by=c('date','stockID'))
  return(TSF)
}


#' @rdname get_factor
#' @export
gf.ca_ratio <- function(TS,fintype=c('PE','PB','PCF'),N=5,inflaAdj=FALSE){
  fintype <- match.arg(fintype)

  TS_ <- getrptDate_newest(TS,freq = 'y',mult="last")
  TS_ <- TS_[!is.na(TS_$rptDate),]
  rptTS <- unique(TS_[, c("rptDate","stockID")])

  #get market cap
  size <- gf_cap(TS)
  size <- dplyr::rename(size,mktcap=factorscore)

  #get f-score
  if(fintype=='PE'){
    funchar <-  '"factorscore",ReportOfAll(46078,Rdate)/100000000'
  }else if(fintype=='PB'){
    funchar <-  '"factorscore",ReportOfAll(44140,Rdate)/100000000'
  }else if(fintype=='PCF'){
    funchar <-  '"factorscore",ReportOfAll(48061,Rdate)/100000000'
  }

  FinSeri <- rptTS.getFinSeri_ts(rptTS = rptTS,N = N-1,freq = 'y',funchar = funchar)

  if(inflaAdj){
    #get raw cpi data
    require(WindR)
    w.start(showmenu = FALSE)
    cpidb <- data.frame(rptDate=sort(unique(FinSeri$lag_rptDate)))
    cpidb_ <- w.edb('M0010990',min(cpidb$rptDate),max(cpidb$rptDate),'Fill=Previous')[['Data']]
    cpidb_ <- dplyr::rename(cpidb_,rptDate=DATETIME,inflation=CLOSE)
    cpidb <- dplyr::left_join(cpidb,cpidb_,by='rptDate')
    cpidb$inflation <- zoo::na.locf(cpidb$inflation) #fill na  with previous value
    cpidb <- transform(cpidb,inflation=100/inflation)

    rptDates <- sort(unique(FinSeri$rptDate))
    #get inflation adjusted ratio
    cpirate <- cbind(rptDate=rptDates,rptDate.offset(rptDates, by=-(N-1):0, freq = 'y'))
    cpirate <- reshape2::melt(cpirate,id.vars="rptDate",variable.name="lagN",value.name="lag_rptDate")
    cpirate <- dplyr::left_join(cpirate,cpidb,by = c("lag_rptDate" = "rptDate"))
    cpirate <- cpirate %>% dplyr::arrange(rptDate,lag_rptDate) %>%
      dplyr::group_by(rptDate) %>% dplyr::mutate(cuminf=cumprod(inflation)) %>% dplyr::ungroup()

    #inflation adjusted size
    cpi_for_size <- cpirate[cpirate$lagN=='y0',c('rptDate','cuminf')]
    TSF <- TS_ %>% dplyr::left_join(size,by=c("date","stockID")) %>%
      dplyr::left_join(cpi_for_size,by='rptDate') %>% dplyr::mutate(mktcap=mktcap*cuminf) %>%
      dplyr::select(-cuminf)

    #inflation adjusted f-score
    rptTS_stat <- FinSeri %>% dplyr::left_join(cpirate[,c("rptDate","lag_rptDate","cuminf")],by=c('rptDate','lag_rptDate')) %>%
      dplyr::mutate(factorscore=factorscore*cuminf) %>% dplyr::group_by(rptDate,stockID) %>%
      dplyr::summarise(factorscore=mean(factorscore,na.rm = TRUE))
  }else{
    rptTS_stat <- calcFinStat(FinSeri=FinSeri,stat = 'mean',fname = "factorscore")
    TSF <- dplyr::left_join(size, TS_, by=c("date","stockID"))
  }
  rptTS_stat <- rptTS_stat[rptTS_stat$factorscore!=0,]
  TSF <- dplyr::left_join(TSF, rptTS_stat, by=c("rptDate","stockID"))
  TSF <- transform(TSF,factorscore=mktcap/factorscore,rptDate=NULL,mktcap=NULL)
  TSF <- dplyr::left_join(TS,TSF,by=c("date","stockID"))
  return(TSF)
}

#' @rdname group_factor
#' @export
lcdb.build.CT_GroupFactorLists <- function(){
  gfconst <- rbind(tibble::tibble(factorType="SIZE",
                                  groupFun="gf.SIZE",
                                  groupDesc="",
                                  factorFun=c("gf.ln_mkt_cap","gf.free_float_sharesMV","gf.nl_size"),
                                  factorPar=c("","",""),
                                  factorDir=c(-1,-1,-1),
                                  factorName=c("ln_mkt_cap_","free_float_sharesMV_","nl_size_"),
                                  factorDesc=c("","",""),
                                  wgt=c(0.4,0.4,0.2)),
                   tibble::tibble(factorType="GROWTH",
                                  groupFun="gf.GROWTH",
                                  groupDesc="",
                                  factorFun=c("gf.NP_YOY","gf.G_OR_longterm","gf.F_NP_chg","gf.F_rank_chg"),
                                  factorPar=c("","",'span="w13",con_type="1,2"','lag=60,con_type="1,2"'),
                                  factorDir=c(1,1,1,1),
                                  factorName=c("NP_YOY","G_OR_longterm","F_NP_chg_w13","F_rank_chg_60"),
                                  factorDesc=c("","","",""),
                                  wgt=c(0.5,0.1,0.2,0.2)),
                   tibble::tibble(factorType="TRADING",
                                  groupFun="gf.TRADING",
                                  groupDesc="",
                                  factorFun=c("gf.liquidity","gf.pct_chg_per","gf.volatility","gf.beta","gf.F_target_rtn"),
                                  factorPar=c("","N=60","","",'con_type="1,2"'),
                                  factorDir=c(-1,-1,-1,-1,1),
                                  factorName=c("liquidity_","pct_chg_per_60_","volatility_","beta_","F_target_rtn"),
                                  factorDesc=c("","","","",""),
                                  wgt=c(0.2,0.4,0.1,0.1,0.2)),
                   tibble::tibble(factorType="EARNINGYIELD",
                                  groupFun="gf.EARNINGYIELD",
                                  groupDesc="",
                                  factorFun=c("gf.ROE_ttm","gf.pio_f_score"),
                                  factorPar=c("",""),
                                  factorDir=c(1,1),
                                  factorName=c("ROE_ttm","pio_f_score"),
                                  factorDesc=c("",""),
                                  wgt=c(0.2,0.8)),
                   tibble::tibble(factorType="VALUE",
                                  groupFun="gf.VALUE",
                                  groupDesc="",
                                  factorFun=c("gf.BP_mrq","gf.EP_ttm","gf.dividendyield"),
                                  factorPar=c("","",""),
                                  factorDir=c(1,1,1),
                                  factorName=c("BP_mrq","EP_ttm","dividendyield"),
                                  factorDesc=c("","",""),
                                  wgt=c(0.4,0.3,0.3)),
                   tibble::tibble(factorType="OTHER",
                                  groupFun="gf.OTHER",
                                  groupDesc="",
                                  factorFun=c("gf.momentum","gf.IVR","gf.disposition","gf.ILLIQ"),
                                  factorPar=c("","","",""),
                                  factorDir=c(1,-1,-1,1),
                                  factorName=c("momentum","IVR_","disposition_","ILLIQ"),
                                  factorDesc=c("","","",""),
                                  wgt=c(0.1,0.3,0.3,0.3)))
  gfconst <- as.data.frame(gfconst)
  con <- db.local('main')
  if(dbExistsTable(con,"CT_GroupFactorLists")){
    dbWriteTable(con,"CT_GroupFactorLists",gfconst,overwrite=TRUE,append=FALSE)
  }else{
    dbWriteTable(con,"CT_GroupFactorLists",gfconst)
  }
  dbDisconnect(con)
  return("Done!")
}



# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ======================
# ===================== other  ===========================
# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ======================



#' CT_GroupFactorLists
#'
#' @export
#' @examples
#' CT_GroupFactorLists()
CT_GroupFactorLists <- function(){
  con <- db.local('main')
  re <- dbReadTable(con,"CT_GroupFactorLists")
  dbDisconnect(con)
  return(re)
}

#' mTSF_refine
#'
#' @export
mTSF_refine <- function(mTSF,refinePar=refinePar_default(type ="scale")){
  factorNames <- guess_factorNames(mTSF,silence = TRUE)
  for(i in 1:length(factorNames)){
    TSF_ <- mTSF[,c('date','stockID',factorNames[i])]
    colnames(TSF_) <- c('date','stockID','factorscore')
    TSF_ <- factor_refine(TSF_,refinePar)
    if(i==1){
      mTSF_ <- TSF_
    }else{
      mTSF_ <- left_join(mTSF_,TSF_,by=c('date','stockID'))
    }
  }
  colnames(mTSF_) <- colnames(mTSF)
  return(mTSF_)
}

#' mTSF2groupf
#'
#' @export
mTSF2groupf <- function(mTSF,refinePar=refinePar_default(type ="scale"),keep_single_factors = FALSE){
  mTSF_ <- mTSF_refine(mTSF,refinePar)
  fgroupconst <- CT_GroupFactorLists()
  groupNames <- unique(fgroupconst$factorType)

  for(i in 1:length(groupNames)){
    fgroupconst_ <- fgroupconst[fgroupconst$factorType==groupNames[i],c('factorName','wgt')]
    gTSF_ <- MultiFactor2CombiFactor(mTSF_[,c('date','stockID',fgroupconst_$factorName)],wgts =fgroupconst_$wgt,keep_single_factors = FALSE)
    if(i==1){
      gTSF <- gTSF_
    }else{
      gTSF <- left_join(gTSF,gTSF_,by=c('date','stockID'))
    }
  }
  colnames(gTSF) <- c('date','stockID',groupNames)
  return(gTSF)
}



#' factorlists recommend
#'
#' @param indexID is index ID.
#' @export
#' @examples
#' ##################get the recommended factorLists of last 12 months##########
#' begT <- Sys.Date()-lubridate::years(1)
#' endT <- Sys.Date()-1
#' indexID <- 'EI000905'
#' FactorLists <- reg.factorlists_recommend(indexID,begT,endT)
#' ##################get the recommended factorLists of last 4 weeks##########
#' begT <- Sys.Date()-months(1)
#' endT <- Sys.Date()-1
#' indexID <- 'EI000985'
#' FactorLists <- reg.factorlists_recommend(indexID,begT,endT,rebFreq = "week")
reg.factorlists_recommend <- function(indexID,begT,endT,rebFreq = "month",rsqBar=1,forder){
  RebDates <- getRebDates(begT,endT,rebFreq)
  TS <- getTS(RebDates,indexID)
  FactorLists <- buildFactorLists(
    buildFactorList(factorFun="gf.SIZE"),
    buildFactorList(factorFun="gf.GROWTH"),
    buildFactorList(factorFun="gf.TRADING"),
    buildFactorList(factorFun="gf.EARNINGYIELD"),
    buildFactorList(factorFun="gf.VALUE"),
    buildFactorList(factorFun="gf.OTHER"))
  TSF <- getMultiFactor(TS,FactorLists)
  TSFR <- na.omit(getTSR(TSF))

  #factor select
  re <- reg.factor_select(TSFR,sectorAttr = NULL,forder)
  result <- re$result
  result <- result[c(TRUE,result$rsqPct[-1]>rsqBar),]
  TSFR <- TSFR[,c("date","date_end","stockID",result$fname,"periodrtn")]
  FactorLists <- FactorLists[sapply(FactorLists,function(x) x$factorName %in% result$fname)]
  re <- list(FactorLists=FactorLists,result=result,TSFR=TSFR)
  return(re)
}

#' MApl
#'
#' @export
#' @examples
#' TSF <- MApl(TS)
#' TSF <- MApl(TS,type='IsKtpl')
MApl <- function(TS,type=c('IsDtpl','IsKtpl'),MA=c(5,10,20,60,120,250)){
  type <- match.arg(type)
  qr <- paste("MA(Close(),",paste0(MA,sep =')'),sep = "")
  TS_ <- rm_suspend(TS,nearby = 0)
  TSF <- TS.getTech_ts(TS_, funchar = qr)
  TSF_ <- tidyr::gather(TSF,key='N',value='MA',-date,-stockID)
  TSF_ <- dplyr::mutate(TSF_,N=as.numeric(stringr::str_extract(N,"[0-9]+")))
  TSF_ <- TSF_ %>% arrange(date,stockID,N) %>% group_by(date,stockID) %>% mutate(MAlead=lead(MA))
  if(type=='IsDtpl'){
    TSF_ <- TSF_ %>% summarise(factorscore=sum(MA>MAlead,na.rm = TRUE)) %>%
      mutate(factorscore=ifelse(factorscore==length(MA)-1,1,0))
  }else{
    TSF_ <- TSF_ %>% summarise(factorscore=sum(MA<MAlead,na.rm = TRUE)) %>%
      mutate(factorscore=ifelse(factorscore==length(MA)-1,1,0))
  }
  TSF <- left_join(TS,TSF_,by=c('date','stockID'))
  TSF[is.na(TSF$factorscore),'factorscore'] <- 0
  return(TSF)
}


expandTS2TSF <- function(TS,nwin,rawdata){
  TS_ <- data.frame(date=unique(TS$date))
  TS_ <- transform(TS_,begT=trday.nearby(date,-nwin))
  TS_ <- TS_ %>% dplyr::rowwise() %>%
    dplyr::do(data.frame(date=.$date,TradingDay=trday.get(.$begT, .$date)))
  TS_ <- dplyr::full_join(TS_,TS,by='date')
  result <- dplyr::left_join(TS_,rawdata,by=c('stockID','TradingDay'))
  result <- na.omit(result)
  result <- dplyr::arrange(result,date,stockID,TradingDay)
  return(result)
}

#' TSFR.rptTSF_nextF
#'
#' @param funchar can be missing when ... arguments are passed.
#' @param ... argument for \code{\link[QDataGet]{getTSF}}.
#' @return a \bold{TSFR} object,\code{date_end} represents next report date,\code{factorscore} represents factorscore of next report date.
#' @examples
#' begT <- as.Date("2007-12-31")
#' endT <- as.Date("2017-12-31")
#' freq <- "y"
#' univ <- "EI000985"
#' funchar <- '"factorscore",reportofall(9900100,RDate)' #ROE
#' funchar <- '"factorscore",reportofall(9900604,RDate)' #growth of net profit
#' funchar <- '"factorscore",reportofall(9900501,RDate)' #divdendyield
#' refinePar <- refinePar_default(type="scale",sectorAttr=NULL)
#' refinePar <- refinePar_default(type="scale",sectorAttr=defaultSectorAttr())
#' TSFR <- TSFR.rptTSF_nextF(begT,endT,freq,univ,funchar,refinePar)
#' TSFR <- TSFR.rptTSF_nextF(begT,endT,freq,univ,refinePar=refinePar,factorFun='gf.PB_mrq', factorPar = list(fillna = TRUE))
#' @export
TSFR.rptTSF_nextF <- function(begT=as.Date("2007-12-31"),endT=as.Date("2016-12-31"),
                              freq='y',univ,funchar,refinePar=refinePar_default(),...){

  #get report TS
  rptDates <- rptDate.get(begT,endT,freq)
  rptTS <- getrptTS(univ=univ,rptDates=rptDates)
  rptTS <- rptDate.publ(rptTS)
  rptTS <- rptTS %>% dplyr::filter(!is.na(PublDate)) %>% dplyr::select(-PublDate)

  #get report TSF and refine
  if(missing(funchar)){
    TS <- rptTS %>% dplyr::rename(date=rptDate)
    rptTSF <- getTSF(TS,...)
    rptTSF <- rptTSF %>% dplyr::rename(rptDate=date)
  }else{
    rptTSF <- rptTS.getFin_ts(rptTS,funchar)
  }
  TSF <- rptTSF %>% dplyr::rename(date=rptDate)
  TSF <- factor_refine(TSF,refinePar)
  TSF <- TSF %>% dplyr::filter(!is.na(factorscore))

  #get next period factorscore and refine
  rptTS_ <- transform(rptTS,rptDate_end=rptDate.offset(rptDate,1,freq))
  rptTS2 <- rptTS_ %>% dplyr::select(rptDate_end,stockID) %>%
    dplyr::rename(rptDate=rptDate_end) %>% dplyr::setdiff(rptTS)
  if(nrow(rptTS2)>0){
    rptTS2 <- rptDate.publ(rptTS2)
    rptTS2 <- rptTS2 %>% dplyr::filter(!is.na(PublDate)) %>% dplyr::select(-PublDate)
    if(missing(funchar)){
      TS2 <- rptTS2 %>% dplyr::rename(date=rptDate)
      rptTSF2 <- getTSF(TS2,...)
      rptTSF2 <- rptTSF2 %>% dplyr::rename(rptDate=date)
    }else{
      rptTSF2 <- rptTS.getFin_ts(rptTS2,funchar)
    }
    rptTSF <- rbind(rptTSF,rptTSF2)
  }
  TSF2 <- rptTSF %>% dplyr::arrange(rptDate,stockID) %>% dplyr::rename(date=rptDate)
  TSF2 <- factor_refine(TSF2,refinePar)
  TSF2 <- TSF2 %>% dplyr::filter(!is.na(factorscore)) %>% dplyr::rename(date_end=date,periodrtn=factorscore)

  #get TSFR
  rptTS_ <- rptTS_ %>% dplyr::rename(date=rptDate,date_end=rptDate_end)
  TSFR <- TSF %>% dplyr::left_join(rptTS_,by=c('date','stockID')) %>%
    dplyr::left_join(TSF2,by=c('date_end','stockID'))
  return(TSFR)
}


