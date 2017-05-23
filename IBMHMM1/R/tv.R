tv <- function(input){
  library(plyr)
library(zoo)
library(markovchain)
library(HMM)
require(lubridate)
library(foreach)
library(dplyr)
library(lme4)
library(compiler)

library(doParallel)
library(rlist)

cl <- makeCluster(2)
registerDoParallel(cl)

last_day_for_data_processed ='2016-05-15'
catalog_month         = 'Jul 2016'
last_hist_date ="2010-08-31"
vr <- "v_17_4"
c1<- 0
c2 <- 1

#newly added paramerters for 
#mode of running test/train
#model pointers

mode<-input
update_model<-"TRUE"

model_saved_pointer<- "/home/data/" 
model_load_pointer <- "/home/hmm_model_Jul_v_17_4.RData"
##############

last_day_for_data_processed <- as.yearmon(last_day_for_data_processed )


#t_temp <- read.csv("/home/quanself/GRAPH/R_Result_Temp.csv",sep = "|",header = TRUE)
#t_purchase <- read.csv("/home/quanself/GRAPH/head_revenue_by_customer_in_date.csv",sep = ",",header = TRUE)
#t_temp <- read.csv("/root/workspace/SparkSklearn/src/R_Result_Temp_13_9.csv",sep = ",",header = TRUE)
#t_purchase <- read.csv("/root/workspace/SparkSklearn/src/head_revenue_by_customer_in_date_13_9.csv",sep = ",",header = TRUE)
t_temp <- read.csv("/home/IBMHMM1/data/R_Result_Temp_13_9.csv",sep = ",",header = TRUE)
t_purchase <- read.csv("/home/IBMHMM1/data/head_revenue_by_customer_in_date_13_9.csv",sep = ",",header = TRUE)

if(mode=="test"){
  load(model_load_pointer)
  
}

# t_temp <- read.csv("R_Result_Temp_purchased_since_6_01_2014.csv",sep = "|",header = TRUE)
# t_purchase <- read.csv("HeadRevenueByCustomerInDate_purchased_since_6_01_2014.csv",sep = "|",header = TRUE)
t_temp<-t_temp[,c(1,2,4)]
t_purchase$date<- as.character(t_purchase$date)
t_temp$date<- as.character(t_temp$date)

t_purchase<-t_purchase[as.Date(t_purchase$date,format="%d-%m-%Y")>as.Date(last_hist_date),]
t_temp<-t_temp[as.Date(t_temp$date,format="%d-%m-%Y")>as.Date(last_hist_date),]


#t_temp$date<- as.character(t_temp$date)
t_temp$promotioninhomedate<- as.character(t_temp$promotioninhomedate)
t_temp$customerid<- as.numeric(t_temp$customerid)

t_purchase$date<- as.character(t_purchase$date)

#filtering code for catlogmonth

d <- as.Date(zoo::as.yearmon(catalog_month))
month(d) <-month(d) 
m <- month(zoo::as.yearmon(catalog_month))
month_h <- as.character(month(c(m-1,m) , label = TRUE, abbr = TRUE))
if (m==1){
  month_h <- c("Dec","Jan")
}


# p_list <- t_purchase[(t_purchase$customerid>2400000 & t_purchase$customerid<=2410000),]
# t_list <-t_temp[(t_temp$customerid>2400000 & t_temp$customerid<=2410000),]
dl <- NULL
purchase<- NULL
temp<- NULL
realm <- NULL
resm <- NULL
framem <- NULL
glmer_lmer_data <- NULL
markov_model_list <- list()
hmm_model_list <- list()
deltat=1/12
tcheck<-data.frame('stage'="loop_start" ,'time'=Sys.time() )
for (cc in  c1:c2){
  print(paste("Starting loop ",cc,'time'=Sys.time()))
  #p_list <- t_purchase[(t_purchase$customerid>20000*cc & t_purchase$customerid<=(cc+1)*20000),]
  #t_list <-t_temp[(t_temp$customerid>20000*cc & t_temp$customerid<=(cc+1)*20000),]
  p_list <- t_purchase[(t_purchase$customerid>10000*cc ),]
  t_list <-t_temp[(t_temp$customerid>10000*cc ),]
  
  
  if(nrow(p_list)==0|nrow(t_list)==0){
    next
  }
  #   p_list <- t_purchase[t_purchase$customerid==9 ,]
  #   t_list <-t_temp[t_temp$customerid==9 ,]
  p_list$date<- as.Date(p_list$date)
  t_list$date<- as.Date(t_list$date)
  p <- split( p_list , f= p_list$customerid)
  #t <- split( t_list , f= t_list$customerid)
  t<- foreach(purchase=p) %dopar% t_list[t_list$customerid==purchase$customerid[1],]
  ml<- list()
  hl<- list()
  if (mode=="test") {
    for (cusd in p){
      cm<-list.clean(lapply(markov_model_list_stored,getElement,paste0("customerid",cusd$customerid[1])), fun = is.null, recursive = FALSE)
      ch<-list.clean(lapply(hmm_model_list_stored,getElement,paste0("customerid",cusd$customerid[1])), fun = is.null, recursive = FALSE)
      
      if(length(cm)==0){
        cm=NA
      } 
      if (length(ch)==0){
        ch=NA
      }
      ml<- c(ml,cm)
      hl<- c(hl,ch)
    } 
  } else if(mode=="train"){
    ml<-seq(1,(length(p)))
    hl<-seq(1,(length(p)))
    
  }
  
  dl <- foreach ( purchase=p , temp=t,ms=ml,hs=hl) %dopar% {
    real <- NULL
    res <- NULL
    frame <- NULL
    
    
    #     purchase=t_purchase[t_purchase$customerid==9,]
    #     temp=t_temp[t_temp$customerid==9,]
    print(purchase$customerid)
    temp1   <- merge(temp, purchase, by = c('customerid', 'date'), all = TRUE)
    purchase2 <- purchase
    purchase$date <- strptime(purchase$date, "%Y-%m-%d")
    purchase$date <- format(purchase$date, "%Y-%m")
    purchase <- dplyr::arrange(purchase, date)
    
    purchase <- dplyr::summarise(dplyr::group_by(purchase, date), headernetrevenue = sum(headernetrevenue))
    
    if (dim(purchase)[1]>1 & sum(zoo::as.yearmon(purchase$date)<=last_day_for_data_processed )>1){
      purchase.ts <- zoo::zoo(purchase$headernetrevenue, order.by=purchase$date)
      purchase.ts <- aggregate(purchase.ts, zoo::as.yearmon, identity) # convert to yearmon
      
      #creating a time series from start of the purchase date to next month in catalog month
      
      g <- zoo::zoo(ts(0, start(purchase.ts),
                       zoo::as.yearmon(d),
                       deltat=deltat)) # grid
      g <- aggregate(g, zoo::as.yearmon, identity) # convert to yearmon
      
      purchase <- merge(purchase.ts, g)
      purchase[is.na(purchase)] <- 0
      purchase[which(purchase$purchase.ts>0), 'purchase.ts'] <- 1
      purchase<- purchase$purchase.ts + purchase$g
      purchase.frame <- data.frame(time(purchase), as.numeric(purchase))
      
      purchase2$date <- strptime(purchase2$date, "%Y-%m-%d")
      purchase2$time.purchase. <- format(purchase2$date, "%Y-%m")
      purchase.frame$time.purchase. <- format(purchase.frame$time.purchase., "%Y-%m")
      
      purchase2$date<-as.Date(purchase2$date)
      purchase.frame <- merge(purchase.frame, purchase2, by = "time.purchase.", all = TRUE)
      
      
      
      temp1 <- temp1[!duplicated(temp1), 1:3]
      colnames(temp1) <- c('customerid', "date", "promotioninhomedate")
      purchase.frame <- merge(purchase.frame, temp1, by = "date", all = TRUE)
      
      purchase.frame$date <- strptime(purchase.frame$date, "%Y-%m-%d")
      purchase.frame$promotioninhomedate <- strptime(purchase.frame$promotioninhomedate, "%Y-%m-%d")
      purchase.frame$eff.promo <- 0
      purchase.frame[which(difftime(purchase.frame$date, purchase.frame$promotioninhomedate,
                                    units = 'days') < 25), "eff.promo"]<- 1
      purchase.frame <- purchase.frame[order(purchase.frame$time.purchase.), c("time.purchase.", "as.numeric.purchase.", "eff.promo")]
      
      
      real.data <- purchase.frame[which(purchase.frame$time.purchase.> last_day_for_data_processed ),]
      
      real.data <- real.data[complete.cases(real.data),]
      real.data <- dplyr::summarise(dplyr::group_by(real.data, time.purchase.),
                                    as.numeric.purchase. = sum(as.numeric.purchase.),
                                    eff.promo = sum(eff.promo))
      real.data$as.numeric.purchase.<- as.numeric(real.data$as.numeric.purchase.>=1)
      real.data$eff.promo <- as.numeric(real.data$eff.promo>=1)
      
      real.data  <- data.frame(real.data)
      purchase.frame <- purchase.frame[which(purchase.frame$time.purchase.<= last_day_for_data_processed ),]
      purchase.frame <- dplyr::summarise(dplyr::group_by(purchase.frame, time.purchase.),
                                         as.numeric.purchase. = sum(as.numeric.purchase.),
                                         eff.promo = sum(eff.promo))
      purchase.frame$as.numeric.purchase.<- as.numeric(purchase.frame$as.numeric.purchase.>=1)
      purchase.frame$eff.promo <- as.numeric(purchase.frame$eff.promo>=1)
      purchase.frame  <- data.frame(purchase.frame)
      purchase.frame$as.numeric.purchase. <- factor(purchase.frame$as.numeric.purchase., levels= c(0,1), labels = c('no_buy', 'buy'))
      purchase.frame$eff.promo <- factor(purchase.frame$eff.promo, levels= c(0,1), labels = c('no_promo', 'promo'))
      purchase.frame <- purchase.frame[complete.cases(purchase.frame),]
      #frame <- rbind(frame, cbind(purchase.frame, purchase2$customerid[1]))
      frame <- cbind(purchase.frame, customerid=purchase2$customerid[1])
      #
      # real <- rbind(real, cbind(real.data, purchase2$customerid[1]))
      real <-cbind(real.data, customerid=purchase2$customerid[1])
      
      if(mode=="train"){
        markov.model <- markovchain::markovchainFit(purchase.frame$as.numeric.purchase.)
        markov.model$customerid<- purchase2$customerid[1]
      } else if (mode=="test"){
        e<-parent.frame()
        if(is.na(ms)){
          return(list(NULL ,frame,real,NULL,NULL))
        }
        markov.model <-ms
      }
      
      
      
      dm <- dim(purchase.frame)[1]
      markov <- data.frame('customerid' = purchase2$customerid[1]   ,
                           'markovchain' = markovchain::predict(object = markov.model$estimate, newdata = as.character(purchase.frame$as.numeric.purchase.[c(dm,dm-1)]),  n.ahead = 2),
                           "month" = month_h,
                           'realpurch' = real.data$as.numeric.purchase.,
                           'realpromo' = real.data$eff.promo,
                           "hmm" = NA)
      
      emissionmatrix <- table( purchase.frame$as.numeric.purchase., purchase.frame$eff.promo)
      emissionmatrix <- emissionmatrix / sum(emissionmatrix)
      if (dim(emissionmatrix)[1]==2 & dim(emissionmatrix)[2]==2 & length(purchase.frame$eff.promo)>1){
        
        
        transitionmatrix <- markov.model$estimate
        
        if(mode=="train"){
          hmmmodel <- HMM::initHMM(States = levels(purchase.frame$as.numeric.purchase.),
                                   Symbols=levels(purchase.frame$eff.promo),
                                   transProbs=transitionmatrix, emissionProbs=emissionmatrix)
        } else if (mode=="test"){
          if(is.na(hs)){
            return(list(NULL ,frame,real,NULL,NULL))
          }
          hmmmodel<-hs
        }
        
        t <- exp(HMM::forward(hmmmodel, as.character(purchase.frame$eff.promo)))
        markov$hmm <- t[2, 1:2]
        hmmmodel$customerid <- purchase2$customerid[1]
        
      }   else {
        hmmmodel$customerid <- purchase2$customerid[1]
        hmmmodel <- NULL
      }
      
      
      
      
      #res <- rbind(res, markov)
      res <-markov
      ctag<-paste0("customerid",purchase2$customerid[1])
      markov_model <- list()
      hmm_model <-list()
      markov_model[[ctag]] <- markov.model
      hmm_model[[ctag]] <- hmmmodel
      return(list(res ,frame,real,markov_model,hmm_model))
      
    }
    
  }
  
  c<- lapply(dl,"[[",1)
  # c<- list.clean(c, fun = is.null , recursive = FALSE)
  res_s<- do.call("rbind",c)
  resm<- rbind(resm,res_s)
  
  c<- lapply(dl,"[[",2)
  frame_s<- do.call("rbind",c)
  framem <- rbind(framem,frame_s)
  
  c<- lapply(dl,"[[",3)
  real_s<- do.call("rbind",c)
  realm <- rbind(realm,real_s)
  tcheck <- rbind(tcheck,data.frame('stage'="loop_end" ,'time'=Sys.time()))
  
  c<- lapply(dl,"[[",4)
  c<- list.clean(c, fun = is.null, recursive = FALSE)
  markov_model_list <- c(markov_model_list,c)
  
  c<- lapply(dl,"[[",5)
  c<- list.clean(c, fun = is.null, recursive = FALSE)
  hmm_model_list <- c(hmm_model_list,c)
  
  
}

tcheck
stopCluster(cl)


resm$hmm_Prediction <- 0
resm[which(resm$hmm>0.20), "hmm_Prediction"] <- 1
#quanself modfy
#resm[which(resm$hmm>0.3), "hmm_Prediction"] <- 1

cm <- as.character(as.yearmon(catalog_month))
m <- unlist(strsplit(cm," "))
res_m <- resm[resm$month==m[1],]
fname_res <- paste0("res_hmm_",m[1],"_",c2,"_",vr,".csv")
write.csv(res_m,fname_res )

t <- as.matrix(table(factor(res_m$realpurch,levels=c(0,1)), factor(res_m$hmm_Prediction,levels=c(0,1))))

cm <-data.frame(Predicted_no_buy=c(t[1,1],t[2,1]),Predicted_buy=c(t[1,2],t[2,2]))
cm
rownames(cm) <- c("Actual_no_buy","Actual_buy")
hmm_accuracy <- (cm[1,1] + cm[2,2]) / sum(cm)
hmm_accuracy

#saving the models in file 
model_file_name<- paste0("hmm_model_",m[1],"_",vr,".RData")
markov_model_list_stored <- markov_model_list
hmm_model_list_stored <- hmm_model_list

if(mode=="train" & update_model=="TRUE"){
  print(model_file_name)
  save(markov_model_list_stored,hmm_model_list_stored,file=model_file_name)
}

Model_performance_metrics <- list(mode=mode,update_model=update_model,time_stamp=Sys.time(),Saved_model_pointer=model_file_name,accuracy=hmm_accuracy,confusion_matrix=cm) 
#perf_jason<-toJSON(Model_performance_metrics, pretty = TRUE, auto_unbox = TRUE)  

#model performance
#perf_jason
return(res_m)
}
