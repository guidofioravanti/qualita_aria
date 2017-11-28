rm(list=objects())
library("memoise")
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

#SOGLIA.MENSILI.FISSA
#Se FALSE utilizza come soglia per considerare un mese valido 27 giorni (soglia fissa)
#Se TRUE utilizza come soglia il 90% dei dati mensili validi (quindi 28 giorni per gennaio, 25 per un febbraio in un anno non bisestile etc etc)
SOGLIA.MENSILI.FISSA<-FALSE

#importante definire quale sia il primo anno
annoI<-2013
annoF<-2014

#per scrittura file
stringa<-ottieniPrefisso()
stringa[1]->inquinante
stringa[2]->prefisso

#ottieni parametri: soglia annuale e probabilità
ottieniParametri(inquinante)->listaParametri

#soglia a livello giornaliero
SOGLIA.GIORNALIERA<-listaParametri[["sogliaGiornaliera"]]

#PROBS PER CALCOLO PERCENTILI
PROBABILITA<-listaParametri[["probs"]]

#SOGLIA.SUPERAMENTI
SOGLIA.SUPERAMENTI<-listaParametri[["sogliaSuperamenti"]]

#giorni in ciascun mese dell'anno
days_in_month(1:12)->g_in_mese


#memoise 
if(1==1){
  leggiDati<-function(){
    #validity_fk: flag per qualità del dato
    read_delim("Dati_O3_2013_2014_Sicilia_rielaborati_guido.csv",delim=";",col_names = TRUE,col_types = "c_ccn__i__")->dati.tmp
    names(dati.tmp)<-c("idstaz","yymmddhh","yymmddhh.fine","valore","flag")
    dati.tmp
  }#fine leggiDati
  
  memoise(leggiDati)->mleggiDati
}
####fine memoise

mleggiDati()->dati

#flag validità trovati: -99 -1 
#In generale flag minore di 0 corrispondono a -9999, ma non sempre
#flag dati validi 1 2 3

#ricodifichiamo dati non validi come NA
dati[dati$flag<0,]$valore<-NA

dati$idstaz<-as.character(dati$idstaz)

#codici stazioni
unique(dati$idstaz)->codiciStazioni

#Per l'ozono lavoriamo con tutti i dati, non facciamo subset per anno per via del calcolo
#degli ottetti
lapply(codiciStazioni,FUN=function(codiceStaz){
  
  print(codiceStaz)
  #if(codiceStaz!="1707602") return()
  dati %>% 
    filter(idstaz==codiceStaz) %>% 
    mutate(yymmddhh=as.POSIXct(yymmddhh,tz="UTC"),
           yymmddhh.fine=as.POSIXct(yymmddhh.fine,tz="UTC"),
           gapTime=yymmddhh.fine-yymmddhh) ->subDati

    if(all(is.na(subDati[,c("valore")]))) return(NULL)

    attributes(subDati$gapTime)$units->periodicita #hours,days etc etc
    stopifnot(periodicita=="hours")
    unique(subDati$gapTime)->timeStep
    
    #le differenze temporali tra un timestamp e l'altro sono sempre uguali 1?
    #Per l'ozono i dati devono essere solo orari
    if(length(timeStep)>1){
      sink(paste0(prefisso,".stazioni.timestampErrato.csv"),append=TRUE)
      cat(paste0(codiceStaz,";",timeStep[1],";",timeStep[2],"\n")) #altri gap temporali?
      sink()
      return()  
    }

    #oggetto xts su dati effettivi. NOTA: subDati potrebbe non essere una serie completa
    #rispetto al calendario necessario.
    xts(subDati[,c("valore")],order.by=subDati$yymmddhh)->datiAnno 
    rm(subDati)    

    #I dati dell'ozono potrebbero contenere porzioni di serie non relative agli anni in esame.
    #Se ad esempio i dati dell'ozono vengono utilizzati per calcolare gli ottetti, i dati del 2013
    #dovranno cominciare nel 2012 (31 dicembre). Per essere sicuri di non considerare dati/anni
    #non necessari: fissare all'inizio del file annoI e annoF, ovvero l'anno inizio e l'anno fine
    #dei periodi in esame. Se ad esempio i dati dell'ozono mi servono epr calcolare le statistiche
    #sul 2013 ma anche per calcolare gli ottetti NON devo annoI dovrà essere fissata == a 2013 anche
    #se parte dei dati si riferiscono al 2012.
    
    anni<-seq(annoI,annoF,by = 1)
    
    #AOT40
    lapply(anni,FUN=function(yy){
      
      datiAnno[year(index(datiAnno))==yy,]->subDati
      if(!nrow(subDati)) return(NULL)

      ############################ ci serve anche dopo
      month(index(subDati))->mesi
      ############################
      
      hour(index(subDati))->ore

      #seleziono tra le 8:00 e le 19:00 (ovvero fino alle 20)

      subDati[(mesi %in% c(5,6,7)) & (ore %in% seq(8,19,by=1) ),]->subDati820
      nrow(subDati820[!is.na(subDati820)])->NUMERO.VALIDI

      prop<-NA
      if(NUMERO.VALIDI){
         subDati820[subDati820[,c("valore")]>=80,]-80 ->datiAOT
        rm(subDati820)
  
        sum(datiAOT[,c("valore")])->somma.differenze
        prop<-1104/NUMERO.VALIDI
        ifelse(NUMERO.VALIDI>=994,somma.differenze,(somma.differenze*prop))->aot40
      }else{
        aot40<-NA
      }
      
      ifelse(arrotonda(aot40)>6000,aot40,NA)->oltv
      ifelse(arrotonda(aot40)>18000,aot40,NA)->vov
      
      #verifica 90% dei dati su stagione estiva e 75% su stagione invernale
      subDati[mesi %in% c(4,5,6,7,8,9)]->subDatiEstivi #183 giorni
      nrow(subDatiEstivi[!is.na(subDatiEstivi)])->ndatiEstivi 
      
      subDati[mesi %in% c(1,2,3,10,11,12)]->subDatiInvernali #182 giorni
      nrow(subDatiInvernali[!is.na(subDatiInvernali)])->ndatiInvernali 

      estiviCompleti<-183*24
      invernaliCompleti<-182*24
      
      flag.validita<- -1
      if((ndatiEstivi>= (estiviCompleti*0.9)) && (ndatiInvernali >= (invernaliCompleti*0.75)) ) flag.validita<-1
     
      mean(subDati,na.rm=TRUE)->media.annuale.o3
      
      names(subDati)<-"dato.orario"
      class(subDati)<-c("datiOrari",class(subDati))
      xtsAttributes(subDati)$tipo<-"orario"
      #soglia180
      superamentoThreshold(subDati,sogliaInquinante = 180)->risSuperamenti180
      unique(as.Date(index(risSuperamenti180)))->giorniSuperamenti180
      length(giorniSuperamenti180)->numeroGiorniSuperamenti180
      #soglia240      
      superamentoThreshold(subDati,sogliaInquinante = 240)->risSuperamenti240      
      unique(as.Date(index(risSuperamenti240)))->giorniSuperamenti240
      length(giorniSuperamenti240)->numeroGiorniSuperamenti240
 
      #qui nn arrivo mai se subDati ha 0 rows     
      calcolaPercentili(subDati,air.prob=PROBABILITA,tipo = 0)->perc0
      names(perc0)[length(PROBABILITA)+1]<-"ndati.percentili"
      names(perc0)<-paste0("tipo0.",names(perc0))
      calcolaPercentili(subDati,air.prob=PROBABILITA,tipo = 8)->perc8    
      names(perc8)[length(PROBABILITA)+1]<-"ndati.percentili"
      names(perc8)<-paste0("tipo8.",names(perc8))
      
      c("station_code"=codiceStaz,
        "anno"=yy,
        "numero.dati.aot40"=NUMERO.VALIDI,
        "aot40"=aot40,
        "prop"=prop,
        "oltv"=oltv,
        "vov"=vov,
        "flag.copertura.estate90%.inverno75%"=flag.validita,
        "media.annuale"=media.annuale.o3,
        "num.superamenti.180"=nrow(risSuperamenti180),
        "num.giorni.superamenti.180"=numeroGiorniSuperamenti180,
        "num.superamenti.240"=nrow(risSuperamenti240),
        "num.giorni.superamenti.240"=numeroGiorniSuperamenti240,
        perc0,
        perc8) 
      
    })->listaAOT40
    
    names(listaAOT40)<-anni
    Filter(Negate(is.null),listaAOT40)->listaAOT40
    if(!length(listaAOT40)) return(NULL)
    data.frame(do.call("rbind",listaAOT40))

})->listaFinale

names(listaFinale)<-codiciStazioni
Filter(Negate(is.null),listaFinale)->listaFinale2

if(length(listaFinale2)){
  #scriviamo file dei risultati
  as.data.frame(do.call("rbind",listaFinale2))->dfFinale
  dfFinale %>%
    write_delim(.,path=paste0(prefisso,".aot40.csv"),delim=";",col_names = TRUE)
}