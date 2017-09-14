# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ======================
# ===================== series of lcdb functions  ===========================
# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ======================

#' add a index to local database
#'
#'
#' @author Andrew Dow
#' @param indexID is index code,such as EI000300
#' @return nothing
#' @examples
#' add.index.lcdb(indexID="EI801003")
#' add.index.lcdb(indexID="EI000985")
#' @export
add.index.lcdb <- function(indexID){
  #check whether the index in local db
  if(indexID=='EI000985'){
    con <- db.local()
    qr <- paste("select * from SecuMain where ID="
                ,QT(indexID),sep="")
    re <- dbGetQuery(con,qr)
    dbDisconnect(con)
    if(nrow(re)>0) return("Already in local database!")

    #part 1 update local SecuMain
    con <- db.jy()
    qr <- paste("select ID,InnerCode,CompanyCode,SecuCode,SecuAbbr,SecuMarket,
                ListedSector,ListedState,JSID 'UpdateTime',SecuCode 'StockID_TS',
                SecuCategory,ListedDate,SecuCode 'StockID_wind'
                from SecuMain WHERE SecuCode=",
                QT(substr(indexID,3,8)),
                " and SecuCategory=4")
    indexInfo <- sqlQuery(con,qr)
    indexInfo <- transform(indexInfo,ID=indexID,
                           SecuCode='000985',
                           StockID_TS='SH000985',
                           StockID_wind='000985.SH')

    #part 2 update local LC_IndexComponent
    qr <- "SELECT 'EI'+s1.SecuCode 'IndexID','EQ'+s2.SecuCode 'SecuID',
    convert(varchar(8),l.InDate,112) 'InDate',convert(varchar(8),l.OutDate,112) 'OutDate',
    l.Flag,l.XGRQ 'UpdateTime',convert(varchar(8),s2.ListedDate,112) 'IPODate'
    FROM LC_IndexComponent l
    inner join SecuMain s1 on l.IndexInnerCode=s1.InnerCode and s1.SecuCode in('801003','000985')
    LEFT join SecuMain s2 on l.SecuInnerCode=s2.InnerCode
    order by s1.SecuCode,l.InDate"
    re <- sqlQuery(con,qr,stringsAsFactors=F)
    indexComp <- re[re$IndexID=='EI000985',c("IndexID","SecuID","InDate","OutDate","Flag","UpdateTime")]
    indexComp <- indexComp[substr(indexComp$SecuID,1,3) %in% c('EQ0','EQ3','EQ6'),]

    tmp <- re[re$IndexID=='EI801003' & re$InDate<20110802,]
    tmp <- tmp[substr(tmp$SecuID,1,3) %in% c('EQ0','EQ3','EQ6'),]
    tmp <- transform(tmp,InDate=intdate2r(InDate),
                           OutDate=intdate2r(OutDate),
                           IPODate=intdate2r(IPODate))
    tmp[(tmp$InDate-tmp$IPODate)<90,'InDate'] <- trday.offset(tmp[(tmp$InDate-tmp$IPODate)<90,'IPODate'],by = months(3))
    tmp <- tmp[tmp$InDate<as.Date('2011-08-02'),]
    tmp <- tmp[,c("IndexID","SecuID","InDate","OutDate","Flag","UpdateTime")]

    qr <- "select 'EQ'+s.SecuCode 'SecuID',st.SpecialTradeType,
    ct.MS,convert(varchar(8),st.SpecialTradeTime,112) 'SpecialTradeTime'
    from jydb.dbo.LC_SpecialTrade st,jydb.dbo.SecuMain s,jydb.dbo.CT_SystemConst ct
    where st.InnerCode=s.InnerCode and SecuCategory=1
    and st.SpecialTradeType=ct.DM and ct.LB=1185 and st.SpecialTradeType in(1,2,5,6)
    order by s.SecuCode"
    st <- sqlQuery(con,qr,stringsAsFactors=F)
    odbcCloseAll()
    st <- st[substr(st$SecuID,1,3) %in% c('EQ0','EQ3','EQ6'),]
    st <- st[st$SpecialTradeTime<20110802,]
    st$InDate <- ifelse(st$SpecialTradeType %in% c(2,6),st$SpecialTradeTime,NA)
    st$OutDate <- ifelse(st$SpecialTradeType %in% c(1,5),st$SpecialTradeTime,NA)
    st$InDate <- intdate2r(st$InDate)
    st$OutDate <- intdate2r(st$OutDate)
    st <- st[,c("SecuID","InDate","OutDate")]

    tmp <- rbind(tmp[,c("SecuID","InDate","OutDate")],st)
    tmp <- reshape2::melt(tmp,id=c('SecuID'))
    tmp <- tmp[!(is.na(tmp$value) & tmp$variable=='InDate'),]
    tmp <- unique(tmp)
    tmp[is.na(tmp$value),'value'] <- as.Date('2100-01-01')
    tmp <- plyr::arrange(tmp,SecuID,value)

    tmp$flag <- c(1)
    for(i in 2: nrow(tmp)){
      if(tmp$SecuID[i]==tmp$SecuID[i-1] && tmp$variable[i]==tmp$variable[i-1] && tmp$variable[i]=='InDate'){
        tmp$flag[i-1] <- 0
      }else if(tmp$SecuID[i]==tmp$SecuID[i-1] && tmp$variable[i]==tmp$variable[i-1] && tmp$variable[i]=='OutDate'){
        tmp$flag[i] <- 0
      }else{
        next
      }
    }
    tmp <- tmp[tmp$flag==1,c("SecuID","variable","value")]
    tmp <- plyr::arrange(tmp,SecuID,value)
    tmp1 <- tmp[tmp$variable=='InDate',]
    tmp2 <- tmp[tmp$variable=='OutDate',]
    tmp <- cbind(tmp1[,c("SecuID","value")],tmp2[,"value"])
    colnames(tmp) <- c("SecuID","InDate","OutDate")
    tmp[tmp$OutDate>as.Date('2011-08-02'),'OutDate'] <- as.Date('2011-08-02')
    tmp$IndexID <- 'EI000985'
    tmp$Flag <- 0
    tmp$UpdateTime <- Sys.time()
    tmp$InDate <- rdate2int(tmp$InDate)
    tmp$OutDate <- rdate2int(tmp$OutDate )
    tmp <- tmp[,c("IndexID","SecuID","InDate","OutDate","Flag","UpdateTime")]

    indexComp <- rbind(indexComp,tmp)
    con <- db.local()
    dbWriteTable(con,"SecuMain",indexInfo,overwrite=FALSE,append=TRUE,row.names=FALSE)
    dbWriteTable(con,"LC_IndexComponent",indexComp,overwrite=FALSE,append=TRUE,row.names=FALSE)
    dbDisconnect(con)
  }else{
    con <- db.local()
    qr <- paste("select * from SecuMain where ID="
                ,QT(indexID))
    re <- dbGetQuery(con,qr)
    dbDisconnect(con)
    if(nrow(re)>0) return("Already in local database!")

    #part 1 update local SecuMain
    con <- db.jy()
    qr <- paste("select ID,InnerCode,CompanyCode,SecuCode,SecuAbbr,SecuMarket,
                ListedSector,ListedState,JSID 'UpdateTime',SecuCode 'StockID_TS',
                SecuCategory,ListedDate,SecuCode 'StockID_wind'
                from SecuMain WHERE SecuCode=",
                QT(substr(indexID,3,8)),
                " and SecuCategory=4",sep='')
    indexInfo <- sqlQuery(con,qr)
    indexInfo <- transform(indexInfo,ID=indexID,
                     StockID_TS=ifelse(is.na(stockID2stockID(indexID,'local','ts')),substr(indexID,3,8),
                                       stockID2stockID(indexID,'local','ts')),
                     StockID_wind=ifelse(is.na(stockID2stockID(indexID,'local','wind')),substr(indexID,3,8),
                                         stockID2stockID(indexID,'local','wind')))

    #part 2 update local LC_IndexComponent
    qr <- paste("SELECT 'EI'+s1.SecuCode 'IndexID','EQ'+s2.SecuCode 'SecuID',
                convert(varchar(8),l.InDate,112) 'InDate',
                convert(varchar(8),l.OutDate,112) 'OutDate',l.Flag,l.XGRQ 'UpdateTime'
                FROM LC_IndexComponent l inner join SecuMain s1
                on l.IndexInnerCode=s1.InnerCode and s1.SecuCode=",
                QT(substr(indexID,3,8))," LEFT join SecuMain s2
                on l.SecuInnerCode=s2.InnerCode")
    indexComp <- sqlQuery(con,qr)
    indexComp <- indexComp[substr(indexComp$SecuID,1,3) %in% c('EQ0','EQ3','EQ6'),]
    odbcCloseAll()

    con <- db.local()
    dbWriteTable(con,"SecuMain",indexInfo,overwrite=FALSE,append=TRUE,row.names=FALSE)
    dbWriteTable(con,"LC_IndexComponent",indexComp,overwrite=FALSE,append=TRUE,row.names=FALSE)
    dbDisconnect(con)
  }

  return("Done!")
}





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



#' get purified factor's portfolio
#'
#' remove style and industry risky factor,get the pure alpha factor's return
#' @author Andrew Dow
#' @param TSFR is a TSFR objetc,factorscore should be standardized and remove NA values
#' @param riskfactorLists is a list of risky factor,such as market value,beta,etc.
#' @return a list of pure factor portfolio's return and weight.
#' @examples
#' begT <- as.Date('2012-01-31')
#' endT <- as.Date('2016-06-30')
#' RebDates <- getRebDates(begT,endT)
#' TS <- getTS(RebDates,'EI000905')
#' TSF <- getTSF(TS,factorFun = 'gf_lcfs',factorPar = list(factorID='F000017'),factorDir = -1,
#'               factorStd = 'norm')
#' TSFR <- getTSR(TSF)
#' riskfactorLists <- buildFactorLists(buildFactorList(factorFun = "gf.ln_mkt_cap",factorDir = -1,
#'                          factorStd = "norm"))
#' factorIDs <- c("F000006","F000015","F000016")
#' tmp <- buildFactorLists_lcfs(factorIDs,factorStd="norm")
#' riskfactorLists <- c(riskfactorLists,tmp)
#' purefp <- pure.factor.test(TSFR,riskfactorLists)
#' @export
pure.factor.test <- function(TSFR,riskfactorLists){
  TSFR <- TSFR[!is.na(TSFR$factorscore),]
  new.name <- colnames(TSFR)

  #get risky factor value
  TSFrisk <- getMultiFactor(TSFR[,c('date','stockID')],riskfactorLists)
  new.name <- c(new.name,colnames(TSFrisk)[-1:-2])

  #get sectorID
  TSS <- getSectorID(TSFR[,c('date','stockID')],sectorAttr = list(33,1))
  TSS[is.na(TSS$sector),'sector'] <- 'ES33510000'
  TSS <- reshape2::dcast(TSS,date+stockID~sector,length,fill=0,value.var = 'sector')
  new.name <- c(new.name,colnames(TSS)[-1:-2])

  #get sqrt floating market value
  TSFfv <- getTSF(TSFR[,c('date','stockID')],factorFun = 'gf_lcfs',factorPar = list(factorID='F000001'),
                  factorStd = 'none',factorNA = "median")
  TSFfv$factorscore <- sqrt(TSFfv$factorscore)
  colnames(TSFfv)[3] <- 'fv'
  new.name <- c(new.name,colnames(TSFfv)[-1:-2])

  TSFR <- merge.x(TSFR,TSFrisk,by = c('date','stockID'))
  TSFR <- merge.x(TSFR,TSS,by = c('date','stockID'))
  TSFR <- merge.x(TSFR,TSFfv,by = c('date','stockID'))
  TSFR <- TSFR[,c(new.name)]

  dates <- unique(TSFR$date)
  for(i in 1:(length(dates)-1)){
    tmp.tsfr <- TSFR[TSFR$date==dates[i],]
    tmp.x <- as.matrix(tmp.tsfr[,c(3,6:(ncol(tmp.tsfr)-1))])
    tmp.x <- subset(tmp.x,select = (colnames(tmp.x)[colSums(tmp.x)!=0]))
    tmp.r <- as.matrix(tmp.tsfr[,"periodrtn"])
    tmp.r[is.na(tmp.r)] <- mean(tmp.r,na.rm = T)
    tmp.w <- as.matrix(tmp.tsfr[,"fv"])
    tmp.w <- diag(c(tmp.w),length(tmp.w))
    tmp.f <- solve(crossprod(tmp.x,tmp.w) %*% tmp.x) %*% crossprod(tmp.x,tmp.w)
    if(i==1){
      portrtn <- data.frame(date=dates[i+1],purefactor= (tmp.f[1,] %*% tmp.r))
      portwgt <- data.frame(date=dates[i],stockID=tmp.tsfr$stockID,wgt=tmp.f[1,])
    }else{
      portrtn <- rbind(portrtn,data.frame(date=dates[i+1],purefactor= (tmp.f[1,] %*% tmp.r)))
      portwgt <- rbind(portwgt,data.frame(date=dates[i],stockID=tmp.tsfr$stockID,wgt=tmp.f[1,]))
    }
  }
  portrtn <- xts::xts(portrtn[,-1],order.by = portrtn[,1])
  colnames(portrtn) <- 'pureFactorPort'
  return(list(portRtn=portrtn,portWgt=portwgt))
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
    port$factorscore <- NULL
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



#' remove suspension stock from TS
#'
#' @author Andrew Dow
#' @param TS is a \bold{TS} object.
#' @param type is suspension type.
#' @return return a \bold{TS} object.
#' @examples
#' RebDates <- getRebDates(as.Date('2013-03-17'),as.Date('2016-04-17'),'month')
#' TS <- getTS(RebDates,'EI000985')
#' TSnew <- rmSuspend(TS,type='today')
#' @export
rmSuspend <- function(TS,type=c('nextday','today','both'),datasrc=defaultDataSRC()){
  type <- match.arg(type)

  if(datasrc=='local'){
    con <- db.local()
    if(type=='nextday'){
      re <- rmSuspend.nextday(TS)
    }else if(type=='today'){
      re <- rmSuspend.today(TS)
    }else{
      TS <- rmSuspend.today(TS)
      re <- rmSuspend.nextday(TS)
    }
    dbDisconnect(con)
  }else if(datasrc=='ts'){
    if(type=='nextday'){
      TS_next <- data.frame(date=trday.nearby(TS$date,by=1), stockID=TS$stockID)
      TS_next <- TS.getTech_ts(TS_next, funchar="istradeday4()",varname="trading")
      re <- TS[TS_next$trading == 1, ]
    }else if(type=='today'){
      TS_today <- TS.getTech_ts(TS, funchar="istradeday4()",varname="trading")
      re <- TS[TS_today$trading == 1, ]
    }else{
      TS_next <- data.frame(date=trday.nearby(TS$date,by=1), stockID=TS$stockID)
      TS_next <- TS.getTech_ts(TS_next, funchar="istradeday4()",varname="trading")
      TS <- TS[TS_next$trading == 1, ]
      TS_today <- TS.getTech_ts(TS, funchar="istradeday4()",varname="trading")
      re <- TS[TS_today$trading == 1, ]
    }
  }

  return(re)
}




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
#' TSF <- gf.dividend(TS)
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
gf.dividend <- function(TS,datasrc=c('jy','wind')){
  datasrc <- match.arg(datasrc)
  if(datasrc=='jy'){
    begT <- min(TS$date)
    endT <- max(TS$date)
    tmp <- paste("('",paste(substr(unique(TS$stockID),3,8),collapse = "','"),"')",sep="")
    qr <- paste("SELECT convert(varchar,TradingDay,112) 'date',
              'EQ'+s.SecuCode 'stockID',DividendRatio 'factorscore'
              FROM LC_DIndicesForValuation d,SecuMain s
              where d.InnerCode=s.InnerCode and s.SecuCode in",tmp,
                " and d.TradingDay>=",QT(begT)," and d.TradingDay<=",QT(endT),
                " ORDER by d.TradingDay")
    con <- db.jy()
    re <- sqlQuery(con,qr)
    odbcClose(con)
    re$date <- intdate2r(re$date)
    TSF <- merge.x(TS,re)
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
      tmp <- tmp[,c('date','stockID','factorscore')]
      re <- rbind(re,tmp)
    }
    re <- transform(re,stockID=stockID2stockID(stockID,'wind','local'))
    TSF <- dplyr::left_join(TS,re,by=c('date','stockID'))
  }

  return(TSF)
}



#' TS.getFinStat_ts
#'
#' get stats of financial indicators from tinysoft.
#' @param TS
#' @param funchar see \link[QDataGet]{TS.getFin_by_rptTS}
#' @param Nbin
#' @param growth whether get the growing rate of financial indicators,default value is \code{TRUE}.
#' @param stattype
#' @importFrom lubridate %m+%
#' @export
#' @examples
#' RebDates <- getRebDates(as.Date('2013-01-31'),as.Date('2017-05-31'),'month')
#' TS <- getTS(RebDates,'EI000905')
#' funchar <- 'LastQuarterData(RDate,46078,0)'
#' varname <- 'np'
#' TSF <- TS.getFinStat_ts(TS,funchar,varname,growth=TRUE)
#' funchar <- 'LastQuarterData(RDate,9900000,0)'
#' varname <- 'eps'
#' TSF <- TS.getFinStat_ts(TS,funchar,varname)
TS.getFinStat_ts <- function(TS,funchar,varname = funchar,Nbin=lubridate::years(-3),
                             growth=FALSE,stattype=c('mean','slope','sd','mean/sd','slope/sd')){
  stattype <- match.arg(stattype)

  getrptDate <- function(begT,endT,type=c('between','forward','backward')){
    type <- match.arg(type)

    tmp <- seq(begT,endT,by='day')
    if(type=='forward'){
      tmp <- lubridate::floor_date(tmp, "quarter")-lubridate::days(1)
    }else if(type=='backward'){
      tmp <- lubridate::ceiling_date(tmp, "quarter")-lubridate::days(1)
    }else{
      tmp <- c(lubridate::floor_date(tmp, "quarter")-lubridate::days(1),
               lubridate::ceiling_date(tmp, "quarter")-lubridate::days(1))

    }

    rptDate <- sort(unique(tmp))
    if(type=='between'){
      rptDate <- rptDate[rptDate>=begT]
      rptDate <- rptDate[rptDate<=endT]
    }
    return(rptDate)
  }


  #get report date
  begT <- trday.offset(min(TS$date),Nbin)
  if(growth){
    begT <- trday.offset(begT,lubridate::years(-1))
  }
  endT <- max(TS$date)
  rptDate <- getrptDate(begT,endT,type = 'forward')

  rptTS <- expand.grid(rptDate = rptDate, stockID = unique(TS$stockID),
                       KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)
  rptTS <- dplyr::arrange(rptTS,rptDate,stockID)

  #remove report dates before IPO
  tmp <- data.frame(date=max(TS$date),stockID = unique(TS$stockID),stringsAsFactors=FALSE)
  tmp <- TS.getTech_ts(tmp, funchar="Firstday()",varname='IPODay')
  tmp <- transform(tmp,date=NULL,IPODay=tsdate2r(IPODay))
  rptTS <- dplyr::left_join(rptTS,tmp,by='stockID')
  rptTS <- rptTS[rptTS$rptDate>rptTS$IPODay,c('rptDate','stockID')]

  funchar <- paste("'",varname,"',",funchar,sep = '')
  TSFdata <- rptTS.getFin_ts(rptTS,funchar)
  colnames(TSFdata) <- c("rptDate","stockID","factorscore")

  if(growth){
    TSFdata <- TSFdata %>% dplyr::group_by(stockID) %>%
      dplyr::mutate(growth =(factorscore - dplyr::lag(factorscore, 4))/abs(dplyr::lag(factorscore, 4)))
    TSFdata <- na.omit(TSFdata)
    TSFdata <- TSFdata[!is.infinite(TSFdata$growth),c("rptDate","stockID","growth")]
    colnames(TSFdata) <- c("rptDate","stockID","factorscore")
  }


  TSnew <- getrptDate_newest(TS)
  TSnew <- na.omit(TSnew)
  TSnew <- dplyr::rename(TSnew,rptDateEnd=rptDate)
  TSnew$rptDateBeg <- TSnew$rptDateEnd %m+% Nbin
  tmp <- dplyr::distinct(TSnew,rptDateBeg,rptDateEnd)
  tmp <- tmp %>% dplyr::rowwise() %>%
    dplyr::do(rptDateBeg=.$rptDateBeg,rptDateEnd=.$rptDateEnd,rptDate = getrptDate(.$rptDateBeg, .$rptDateEnd,type = 'between')) %>%
    dplyr::do(data.frame(rptDateBeg=.$rptDateBeg,rptDateEnd=.$rptDateEnd,rptDate = .$rptDate))
  TSnew <- dplyr::full_join(TSnew,tmp,by=c('rptDateBeg','rptDateEnd'))
  TSnew <- transform(TSnew,rptDateBeg=NULL,rptDateEnd=NULL)

  TSFdata <- dplyr::left_join(TSnew,TSFdata,by=c('stockID','rptDate'))
  TSFdata <- na.omit(TSFdata)

  TSFdata <- TSFdata %>% dplyr::group_by(date,stockID) %>% dplyr::mutate(id =row_number())
  N <- max(TSFdata$id)
  TSFdata <- TSFdata %>% dplyr::group_by(date,stockID) %>% dplyr::filter(max(id) > N/2)
  if(stattype=='mean'){
    TSF <- TSFdata %>%  dplyr::summarise(factorscore=mean(factorscore,na.rm = TRUE))
  }else if(stattype=='sd'){
    TSF <- TSFdata %>%  dplyr::summarise(factorscore=sd(factorscore,na.rm = TRUE))
  }else if(stattype=='mean/sd'){
    TSF <- TSFdata %>%  dplyr::summarise(factorscore=mean(factorscore,na.rm = TRUE)/sd(factorscore,na.rm = TRUE))
  }else if(stattype %in% c('slope','slope/sd')){
    tmp <- TSFdata %>% do(mod = lm(factorscore ~ id, data = .))
    tmp <- data.frame(tmp %>% broom::tidy(mod))
    TSF <- tmp[tmp$term=='id',c("date","stockID","estimate")]
    if(stattype=='slope'){
      colnames(TSF) <- c("date","stockID","factorscore")
    }else{
      tmp <- TSFdata %>%  dplyr::summarise(sd=sd(factorscore))
      TSF <- dplyr::left_join(TSF,tmp,by=c('date','stockID'))
      TSF$factorscore <- TSF$estimate/TSF$sd
      TSF <- transform(TSF,estimate=NULL,sd=NULL)
    }
  }

  TSF <- dplyr::left_join(TS,TSF,by=c('date','stockID'))
  colnames(TSF) <- c('date','stockID',varname)
  return(TSF)
}








