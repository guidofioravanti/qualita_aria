rm(list=objects())
library("dplyr")
library("lubridate")
library("readr")
library("xts")
library("lattice")
library("stringr")
library("gridExtra")
library("ggplot2")
source("funzioniAria.R")
options(warn=2,stringsAsFactors=FALSE,error=recover)
Sys.setenv(TZ="UTC") #linux..su windows??boh

#per scrittura file
stringa<-ottieniPrefisso()
stringa[1]->inquinante
stringa[2]->prefisso

#ottieni parametri: soglia annuale e probabilità
ottieniParametri(inquinante)->listaParametri
SOGLIA.ANNUALE<-listaParametri[["sogliaAnnuale"]]

#PROBS PER CALCOLO PERCENTILI
PROBABILITA<-listaParametri[["probs"]]

#validity_fk: flag per qualità del dato
read_delim("data_records_pm25_2013_2014_Basilicata_Sicilia.csv",delim=";",col_names = TRUE)->dati.tmp

#variabili che ci interessano
dati.tmp %>% 
  select(observation_fk,data_record_start_time,data_record_end_time,data_record_value,validity_fk)->dati
rm(dati.tmp)

#semplifichiamo nomi variabili
names(dati)->nomi.old
names(dati)<-c("idstaz","yymmddhh","yymmddhh.fine","valore","flag")

#flag validità trovati: -99 -1 
#In generale flag minore di 0 corrispondono a -9999, ma non sempre
#flag dati validi 1 2 3

#ricodifichiamo dati non validi come NA
dati[dati$flag<0,]$valore<-NA


dati$idstaz<-as.character(dati$idstaz)

#codici stazioni
unique(dati$idstaz)->codiciStazioni


lapply(codiciStazioni,FUN=function(codiceStaz){
  
  print(codiceStaz)
  #if(codiceStaz!="1214") return()
  dati %>% 
    filter(idstaz==codiceStaz) %>% 
    mutate(yymmddhh=as.POSIXct(yymmddhh,tz="UTC"),
           yymmddhh.fine=as.POSIXct(yymmddhh.fine,tz="UTC"),
           gapTime=yymmddhh.fine-yymmddhh) ->subDati
  
    if(all(is.na(subDati[,c("valore")]))) return(list(NULL,NULL))

    #quanti e quali anni nella serie?
    as.character(unique(year(subDati$yymmddhh)))->anni

    lapply(anni,FUN=function(yy){
      
      subDati[year(subDati$yymmddhh)==yy,]->tmp
      attributes(tmp$gapTime)$units->periodicita #hours,days etc etc
      
      stopifnot(nchar(periodicita)!=0)
      
      #le differenze temporali tra un timestamp e l'altro sono sempre uguali?
      #Mi aspetto che durante l'anno le osservazioni avvengano sempre con lo stesso passo temporale
      #Se non è così passa all'anno successivo (in attesa di decidere che fare)
      unique(tmp$gapTime)->timeStep

      if(length(timeStep)==2){
        sink(paste0(prefisso,".stazioni.timestampErrato.csv"),append=TRUE)
          cat(paste0(codiceStaz,";",timeStep[1],";",timeStep[2],"\n"))
        sink()
        return() #altre soluzioni?  
      }else if(length(timeStep)>2){
        browser() #timestamp disperati!
      }#fine su verifica timestamp errati

      xts(tmp[,c("valore")],order.by=tmp$yymmddhh)->datiAnno  
      rm(tmp)

      #dati già aggregati a livello giornaliero, restituiamoli così come sono 
      if(periodicita=="days"){
        names(datiAnno)<-"aggregato.giornaliero"
        xtsAttributes(datiAnno)<-list("tipo"="giornaliero")
        
        #dato giornaliero: se sono qui il passo temporale è unico. Assicuriamoci però che per il giornaliero
        #l'ora sia la mezzanotte.Se non è così annotiamo la stazione in un file che comunque elaboriamo
        if(!all(hour(index(datiAnno))==0)){
          sink("giornalieri_con_orario_non_00.txt",append=TRUE)
            cat(paste0(codiceStaz,"\n"))
          sink()
        }#fine if
        
        #!importante!!!!
        class(datiAnno)<-c("datiGiornalieri",class(datiAnno))
        
        return(datiAnno) #<--- return!! esco da lapply  
        
      }#fine if caso dati giornalieri  

      mediaGiornaliera(datiAnno)->medie
      #associamo attributo sul passo temporale. Ci serve per definire la soglia (75%) sul numero dati orari oltre la quale
      #una media giornaliera è valida
      ifelse(timeStep==1,"orario","biorario")->myguess
      xtsAttributes(medie)<-list("tipo"=myguess)
      
      #Attenzione: il dato è veramente un dato orario? Potrebbe essere un dato giornaliero
      #spalmato su tutte le 24 ore. Di sicuro, un dato giornaliero ripoertato come orario avrà tutte le 
      #24 ore presenti e quindi nel momento in cui definiamo le soglie per valutare la bontà del dato non ci sarà
      #nessun problema. Che succede però se un dato giornaliero, rappresentato come orario, non compare in tutte le 24 ore?
      
      #Implementiamo un controllo per capire se una serie oraria è in realtà giornaliera
      as.vector(datiAnno[.indexhour(datiAnno)==0])->valoriAnno00
      as.vector(datiAnno[.indexhour(datiAnno)==23])->valoriAnno23
      as.vector(medie[,1])->valoriMedie

      ##########################################################################################################  
      #La verifica su un giornaliero travestito da orario la faccio solo se valgono le condizioni che seguono
      length(valoriMedie[!is.na(valoriMedie)])->lenValidi
      
      if(lenValidi>=300 && length(valoriMedie[!is.na(valoriMedie)])==length(valoriAnno00[!is.na(valoriAnno00)]) &&
         length(valoriMedie[!is.na(valoriMedie)])==length(valoriAnno23[!is.na(valoriAnno23)])){

              confronto00<-all(valoriMedie[!is.na(valoriMedie)]==valoriAnno00[!is.na(valoriAnno00)])
              confronto23<-all(valoriMedie[!is.na(valoriMedie)]==valoriAnno23[!is.na(valoriAnno23)])
              
              #le serie delle medie coincidono con tutti i valori della serie alle 00
              #e alle 23 due possibilità: o si tratta di un caso rarissimo 
              #di coincidenza o la serie è in realta
              #una serie giornaliera
              if(confronto00 && confronto23){
                #aggiorno medie e l'attributo
                medie[,1]->medie
                xtsAttributes(medie)<-list("tipo"="giornaliero")
                sink(paste0(prefisso,"serieGiornaliere_formatoOrarie.txt"),append=TRUE)
                  cat(paste0(yy,";",codiceStaz,"\n"))
                sink()
              }
              
      }#fine if per verificare serie giornaliere finte orarie
      ##########################################################################################################  
      
      #calcoliamo e salviamo alcune info richieste in più per il pm25
      #Calcoliamo media annuale direttamente da orari e verifichiamo ilsuperamento e salviamo info
      #Serve per icontrollli al gruppo aria
      if(xtsAttributes(medie)!="giornaliero"){
        mean(datiAnno,na.rm=TRUE)->mediaAnnuale
        nrow(datiAnno[!is.na(datiAnno)])->numeroDatiValidi
        arrotonda(mediaAnnuale)->mediaArrotondata
        ifelse(mediaArrotondata > SOGLIA.ANNUALE,mediaArrotondata, NA)->mediaAnnualeArr

        sink(paste0(prefisso,".medieAnnualeDaOrarie.csv"),append=TRUE)
          cat(paste0(yy,";",codiceStaz,";",mediaAnnuale,";",numeroDatiValidi,";",mediaAnnualeArr,"\n"))
        sink()
      }#fine ifsu xtsAttributes 
      
      #!importante!!!!
      class(medie)<-c("datiGiornalieri",class(medie))
      
      medie
      
    })->listaAggregati #fine lapply su anno
    
    #listaAggregati contiene valori aggregati annuali (per ogni anno disponibile)
    #per una determinata serie
    names(listaAggregati)<-anni

    Filter(Negate(is.null),listaAggregati)->listaAggregati
    if(!length(listaAggregati)) return(list(NULL,NULL)) #passa a stazione successiva, per questa non ho dati
    
    ##### scrittura file con tutti i dati aggregati a livello giornaliero
    do.call("rbind",listaAggregati)->tabellaAggregati
    scriviTabella(tabellaAggregati,idstaz=codiceStaz,prefisso)
    #####

    lapply(listaAggregati,mediaAnnuale)->listaMediaAnnuale
    lapply(listaMediaAnnuale,superamentoThresholdAnnuale,sogliaInquinante=SOGLIA.ANNUALE)->listaSuperamentiMediaAnnuale
    lapply(listaAggregati,calcolaPercentili,tipo=0)->listaPercentili0
    lapply(listaAggregati,calcolaPercentili,tipo=8)->listaPercentili8

    #uniamo il tutto
    lapply(anni,FUN=function(yy){

      do.call("c",
              list(
              as.integer(yy),  
              listaMediaAnnuale[[yy]],
              listaSuperamentiMediaAnnuale[[yy]],              
              listaPercentili0[[yy]],              
              listaPercentili8[[yy]]))
      
    })->listaUnione #fine lapply su anni
    
    #ora posso cancellare subDati
    rm(subDati)
    
    as.data.frame(do.call("rbind",listaUnione))->dfRisultati
    dfRisultati$observation_fk<-codiceStaz
    
    dfRisultati
    
})->listaFinale

Filter(Negate(is.null),listaFinale)->listaFinale2

#scriviamo file dei risultati
as.data.frame(do.call("rbind",listaFinale2))->dfFinale

names(dfFinale)<-c("anno",
                   "media.annuale",
                   "numero.dati.per.media",
                   "media.annuale.oltre.soglia",
                   paste0("ptype0_",PROBABILITA),
                   "numero.dati.per.percentili.type0",                   
                   paste0("ptype8_",PROBABILITA),
                   "numero.dati.per.percentili.type8",
                   "observation_fk")

write_delim(dfFinale %>%select(anno,observation_fk,everything()),path=paste0(prefisso,".csv"),delim=";",col_names = TRUE)
