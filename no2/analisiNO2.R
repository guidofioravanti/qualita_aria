rm(list=objects())
library("dplyr")
library("lubridate")
library("readr")
library("xts")
library("lattice")
library("stringr")
source("funzioniAria.R")
options(warn=2,stringsAsFactors=FALSE,error=recover)
Sys.setenv(TZ="UTC") #linux..su windows??boh

#per scrittura file
#per scrittura file
stringa<-ottieniPrefisso()
stringa[1]->inquinante
stringa[2]->prefisso

#ottieni parametri: soglia annuale e probabilità
ottieniParametri(inquinante)->listaParametri

SOGLIA.ANNUALE<-listaParametri[["sogliaAnnuale"]]

#PROBS PER CALCOLO PERCENTILI
PROBABILITA<-listaParametri[["probs"]]

#soglia a livello giornaliero
SOGLIA.GIORNALIERA<-listaParametri[["sogliaGiornaliera"]]

#soglia sul numero di superamenti
SOGLIA.SUPERAMENTI<-listaParametri[["sogliaSuperamenti"]]

#validity_fk: flag per qualità del dato
read_delim("data_records_no2_2013_2014_Basilicata.csv",delim=";",col_names = TRUE)->dati.tmp

#variabili che ci interessano
dati.tmp %>% 
  select(observation_fk,data_record_start_time,data_record_end_time,data_record_value,validity_fk)->dati
rm(dati.tmp)

#semplifichiamo nomi variabili
names(dati)<-c("idstaz","yymmddhh","yymmddhh.fine","dato.orario","flag")

#ricodifichiamo dati non validi (flag <0) come NA
dati[dati$flag<0,]$dato.orario<-NA

dati$idstaz<-as.character(dati$idstaz)

#codici stazioni
unique(dati$idstaz)->codiciStazioni


lapply(codiciStazioni,FUN=function(codiceStaz){
  
  print(codiceStaz)
#  if(codiceStaz!="12648") return()
  dati %>% 
    filter(idstaz==codiceStaz) %>% 
    mutate(yymmddhh=as.POSIXct(yymmddhh,tz="UTC"),
           yymmddhh.fine=as.POSIXct(yymmddhh.fine,tz="UTC"),
           gapTime=yymmddhh.fine-yymmddhh) ->subDati

    if(all(is.na(subDati[,c("dato.orario")]))) return(list(NULL,NULL))

    #PS: a quanto pare gli observation_fk non corrispondono al codice della stazione bensi al codice
    #della serie in un determinato anno. Verifichiamo che anni abbia dimensione pari a 1
    as.character(unique(year(subDati$yymmddhh)))->anni
    stopifnot(length(anni)==1)
    
    attributes(subDati$gapTime)$units->periodicita #hours,days etc etc
    
    #controlli su periodicita  
    stopifnot(nchar(periodicita)!=0)
    stopifnot(periodicita=="hours") #so2: solo dati orari
  
    #il passo temporale è unico all'interno della serie?    
    unique(subDati$gapTime)->timeStep
      
    #nel caso di due timeStep distinti scrivi un file di output con il codice della stazione
    #e con i timeStep e poi passa alla stazione successiva
    if(length(timeStep)==2){
        sink(paste0(prefisso,".stazioni.timestampErrato.csv"),append=TRUE)
          cat(paste0(codiceStaz,";",timeStep[1],";",timeStep[2],"\n"))
        sink()
        return() #altre soluzioni?  
    }else if(length(timeStep)>2){
        message("Ho trovato più di due time Step distinti!")
        browser() # ho trovato più di due timeStep...fermati e guarda i dati
        stop()
    }#fine su verifica timestamp errati
      
    #Se arrivo qui: per tutta la serie il passo temporale è sempre lo stesso. 
    #Ultima verifica per NO2: timeStep==1? (non sono contempleati dati biorari, triorari e via dicendo)
    stopifnot(timeStep==1)
      
    xts(subDati[,c("dato.orario")],order.by=subDati$yymmddhh)->datiAnno
    xtsAttributes(datiAnno)<-list("tipo"="orario") #questa info per l'NO2 non mi serve a nulla

    #ora posso cancellare subDati
    rm(subDati)
    class(datiAnno)<-c("datiOrari",class(datiAnno))
    
    superamentoThreshold(datiAnno,sogliaInquinante=SOGLIA.GIORNALIERA)->listaSuperamenti
    numeroSuperamenti(listaSuperamenti,sogliaSuperamenti=SOGLIA.SUPERAMENTI)->listaNumeroSuperamenti
    mediaAnnuale(datiAnno)->listaMediaAnnuale
    superamentoThresholdAnnuale(listaMediaAnnuale,sogliaInquinante=SOGLIA.ANNUALE)->listaSuperamentiMediaAnnuale
    calcolaPercentili(datiAnno,tipo=0,air.prob=PROBABILITA)->listaPercentili0
    calcolaPercentili(datiAnno,tipo=8,air.prob=PROBABILITA)->listaPercentili8
    
    #uniamo il tutto
    as.data.frame(t(do.call("c",
              list(
              as.integer(anni),  
              listaNumeroSuperamenti,
              listaMediaAnnuale,
              listaSuperamentiMediaAnnuale,
              listaPercentili0,
              listaPercentili8))))->dfRisultati

    dfRisultati$observation_fk<-codiceStaz
    
    if(nrow(listaSuperamenti)){
      as.data.frame(listaSuperamenti)->listaSuperamenti
      listaSuperamenti$giorno<-rownames(listaSuperamenti)
      rownames(listaSuperamenti)<-NULL
      listaSuperamenti$observation_fk<-codiceStaz
      listaSuperamenti[,c("observation_fk","giorno","dato.orario")]->dfSuperamenti
    }else{
      dfSuperamenti<-NULL
    }

    list(dfRisultati,dfSuperamenti)
    
})->listaFinale


lapply(listaFinale,function(x)x[[1]])->listaRisultati
lapply(listaFinale,function(x)x[[2]])->listaSuperamenti

Filter(Negate(is.null),listaRisultati)->listaRisultati2
Filter(Negate(is.null),listaSuperamenti)->listaSuperamenti2

#se listaSuperamenti2 è vuoto non ho avuto alcun superamento. Questo dato deve
#coincidere con quanto in dfRisultati
if(length(listaSuperamenti2)){
  as.data.frame(do.call("rbind",listaSuperamenti2))->dfSuperamenti
  write_delim(dfSuperamenti,path=paste0(prefisso,".superamenti.csv"),delim=";",col_names = TRUE)
} 

#scriviamo file dei risultati
as.data.frame(do.call("rbind",listaRisultati2))->dfFinale

names(dfFinale)<-c("anno",
                   "numero.superamenti",
                   "numero.superamenti.oltre18",
                   "media.annuale",
                   "numero.dati.per.media",
                   "media.annuale.oltre.soglia",
                   paste0("ptype0_",PROBABILITA),
                   "numero.dati.per.percentili.type0",
                   paste0("ptype8_",PROBABILITA),
                   "numero.dati.per.percentili.type8",
                   "observation_fk")

write_delim(dfFinale %>%select(anno,observation_fk,everything()),path=paste0(prefisso,".csv"),delim=";",col_names = TRUE)
