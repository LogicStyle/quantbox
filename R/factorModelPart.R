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
  con <- db.local()
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
addwgt2port_amtao <- function(port,wgtType=c('fs','fssqrt','ffsMV'),wgtmax=NULL,...){
  wgtType <- match.arg(wgtType)
  if(wgtType %in% c('fs','fssqrt')){
    port <- TS.getTech(port,variables="free_float_shares")
    if (wgtType=="fs") {
      port <- plyr::ddply(port,"date",transform,wgt=free_float_shares/sum(free_float_shares,na.rm=TRUE))
    } else {
      port <- plyr::ddply(port,"date",transform,wgt=sqrt(free_float_shares)/sum(sqrt(free_float_shares),na.rm=TRUE))
    }
    port$free_float_shares <- NULL
  }else{
    port <- gf.free_float_sharesMV(port)
    port <- plyr::ddply(port,"date",transform,wgt=factorscore/sum(factorscore,na.rm=TRUE))
    port <- transform(port,factorscore=NULL,wgt=ifelse(is.na(wgt),0,wgt))
  }


  if(!is.null(wgtmax)){
    subfun <- function(wgt){
      df <- data.frame(wgt=wgt,rank=seq(1,length(wgt)))
      df <- arrange(df,plyr::desc(wgt))
      j <- 1
      while(max(df$wgt)>wgtmax){
        df$wgt[j] <- wgtmax
        df$wgt[(j+1):nrow(df)] <- df$wgt[(j+1):nrow(df)]/sum(df$wgt[(j+1):nrow(df)])*(1-j*wgtmax)
        j <- j+1
      }
      df <- plyr::arrange(df,rank)
      return(df$wgt)
    }
    port <- plyr::ddply(port,'date',plyr::here(transform),newwgt=subfun(wgt))
    port$wgt <- port$newwgt
    port$newwgt <- NULL
  }
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
  con <- db.local()
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
gf.doublePrice <- function(TS,ROEType=c('ROE_ttm','F_ROE','ROE','ROE_Q')){
  ROEType <- match.arg(ROEType)

  TSF <- gf.PB_mrq(TS)
  if(ROEType=='ROE_ttm'){
    tmp <- gf.ROE_ttm(TS)
    tmp <- tmp[,c("date","stockID","factorscore")]
    tmp <- transform(tmp,factorscore=factorscore/100)
  }else if(ROEType=='F_ROE'){
    tmp <- gf.F_ROE(TS)
    tmp <- tmp[,c("date","stockID","factorscore")]

  }else if(ROEType=='ROE'){
    tmp <- gf.ROE(TS)
    tmp <- tmp[,c("date","stockID","factorscore")]
    tmp <- transform(tmp,factorscore=factorscore/100)
  }else{
    tmp <- gf.ROE_Q(TS)
    tmp <- tmp[,c("date","stockID","factorscore")]
    tmp <- transform(tmp,factorscore=factorscore/100)
  }

  TSF <- dplyr::left_join(TSF,tmp,by=c('date','stockID'))
  colnames(TSF) <- c("date","stockID","PB_mrq_","ROE")
  TSF <- na.omit(TSF)
  TSF <- dplyr::filter(TSF,PB_mrq_!=0)
  TSF <- dplyr::filter(TSF,ROE>0)

  TSF$factorscore <- log(TSF$PB_mrq_*2,base=(1+TSF$ROE))
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




#' @rdname get_factor
#' @author han.qian
#' @export
gf.pio_f_score <- function(TS){

  # TS manipulating
  check.TS(TS)
  TS_old <- TS
  TS_old$date <- trday.offset(TS$date, by = lubridate::years(-1))
  TS_rpt <- getrptDate_newest(TS)
  TS_old_rpt <- TS_rpt
  TS_old_rpt$rptDate <- rptDate.yoy(TS_old_rpt$rptDate)

  # get data part1
  multi_funchar <- '"OCF",reportofall(9900005,Rdate),
  "dOCF",reportofall(9900004,Rdate),
  "NetValue",reportofall(9900003,Rdate),
  "Debt",reportofall(9900024,Rdate),
  "Leverage",reportofall(9900203,Rdate),
  "CurrentRatio",reportofall(9900200,Rdate),
  "GrossMargin",reportofall(9900103,Rdate),
  "AssetTurnoverRate",reportofall(9900416,Rdate),
  "ROA",reportofall(9900100,Rdate)'

  dat <- rptTS.getFin_ts(TS_rpt, multi_funchar)
  dat_old <- rptTS.getFin_ts(TS_old_rpt, multi_funchar)

  # get data part2
  dat_extra <- gf.totalshares(TS)
  dat_extra_old <- gf.totalshares(TS_old)

  # data double checking
  if((nrow(dat) != nrow(TS)) | (nrow(dat_old) != nrow(TS)) | (nrow(dat_extra) != nrow(TS)) | (nrow(dat_extra_old) != nrow(TS))){
    stop("Data retrieving failed.")
  }

  # TSF
  TSF <- TS
  # PROFITABILITY
  ### ROA > 0
  TSF$score1 <- (dat$ROA > 0) + 0
  ### OCF > 0
  TSF$score2 <- (dat$OCF > 0) + 0
  ### dROA > 0
  TSF$score3 <- (dat$ROA > dat_old$ROA) + 0
  ### ACCRUALS [(OPERATING CASH FLOW/TOTAL ASSETS) > ROA]
  TSF$score4 <- ((dat$OCF/(dat$NetValue + dat$Debt)*100) > dat$ROA) + 0

  # LEVERAGE, LIQUIDITY AND SOURCE OF FUNDS
  ### dLEVERAGE(LONG-TERM) < 0
  TSF$score5 <- (dat$Leverage < dat_old$Leverage) + 0
  ### d(Current ratio) > 0
  TSF$score6 <- (dat$CurrentRatio > dat_old$CurrentRatio) + 0
  ### d(Number of shares) == 0
  TSF$score7 <- (dat_extra$factorscore == dat_extra_old$factorscore) + 0

  # OPERATING EFFICIENCY
  ### d(Gross Margin) > 0
  TSF$score8 <- (dat$GrossMargin > dat_old$GrossMargin) + 0
  ### d(Asset Turnover ratio) > 0
  TSF$score9 <- (dat$AssetTurnoverRate > dat_old$AssetTurnoverRate) + 0

  # output
  TSF$factorscore <- rowSums(TSF[,3:11], na.rm = TRUE)
  TSF <- TSF[,c("date","stockID","factorscore")]
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
gf.SIZE <- function(TS,refinePar=refinePar_default('scale'),FactorLists,wgt){
  re <- group_factor_subfun(TS,factorType="SIZE",refinePar,FactorLists,wgt)
}


#' @rdname group_factor
#' @export
gf.GROWTH <- function(TS,refinePar=refinePar_default('scale'),FactorLists,wgt){
  re <- group_factor_subfun(TS,"GROWTH",refinePar,FactorLists,wgt)
}



#' @rdname group_factor
#' @export
gf.TRADING <- function(TS,refinePar=refinePar_default('scale'),FactorLists,wgt){
  re <- group_factor_subfun(TS,"TRADING",refinePar,FactorLists,wgt)
}

#' @rdname group_factor
#' @export
gf.EARNINGYIELD <- function(TS,refinePar=refinePar_default('scale'),FactorLists,wgt){
  re <- group_factor_subfun(TS,"EARNINGYIELD",refinePar,FactorLists,wgt)
}

#' @rdname group_factor
#' @export
gf.VALUE <- function(TS,refinePar=refinePar_default('scale'),FactorLists,wgt){
  re <- group_factor_subfun(TS,"VALUE",refinePar,FactorLists,wgt)
}


#' @rdname group_factor
#' @export
gf.OTHER <- function(TS,refinePar=refinePar_default('scale'),FactorLists,wgt){
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

  conn <- db.local()
  qr <- paste("select t.TradingDay ,t.ID 'stockID',t.TurnoverVolume/10000 'TurnoverVolume',t.NonRestrictedShares
              from QT_DailyQuote2 t where ID in",brkQT(unique(TS$stockID)),
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








# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ======================
# ===================== other  ===========================
# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ======================



#' CT_GroupFactorLists
#'
#' @export
#' @examples
#' CT_GroupFactorLists()
CT_GroupFactorLists <- function(){
  con <- db.local()
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
MApl <- function(TS,type=c('IsDtpl','IsKtpl'),MA=c(10,20,120,250)){
  type <- match.arg(type)
  if(type=='IsDtpl'){
    qr <- paste("IsDtpl(",paste(MA,collapse = ","),")",sep="")
  }else{
    qr <- paste("IsKtpl(",paste(MA,collapse = ","),")",sep="")
  }
  TSF <- TS.getTech_ts(TS, funchar = qr, varname = "dtpl")
  return(TSF)
}

