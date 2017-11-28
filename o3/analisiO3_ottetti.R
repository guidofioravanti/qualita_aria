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

#Nel caso dell'ozono una serie per l'anno X deve iniziare il 31 dicembre dell'anno
#X-1 alle ore 17 e terminare l'anno X alle ora 16. Costruiamo un calendario
#che inizia il 31 dicembr eore 17 annoI e finisce 31 dicembre ore 16 dell'annoX
#i miei dati debbono partire dal 31 dicembre dell'anno precedente alle ore 17
as.POSIXct(paste0(annoI,"-01-01 00:00:00"))-hours(7)->giornoInizialeSupposto
as.POSIXct(paste0(annoF,"-12-31 16:00:00"))->giornoFinaleSupposto

#calendario è il calendario ipotetico di una serie completa. Lo utilizziamo per costruire
#gli ottetti
seq.POSIXt(from=giornoInizialeSupposto,to =giornoFinaleSupposto ,by="hour")->calendario
lenCal<-length(calendario)
stopifnot(lenCal!=0)   

#global.ottetto viene utilizzato per memorizzare gli ottetti di una serie e i relativi flag.
#Per evitare di ricrearlo a ogni ciclo lo definiamo come variabile globale
dfOttetti<-data.frame(media.ottetto=rep(NA_real_,lenCal),flag=rep(-1,lenCal),numero.dati=rep(0.0,lenCal))
xts(dfOttetti,order.by = calendario)->global.ottetto 
rm(dfOttetti)

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

#NOME DIRECTORY dove memorizzare gli ottetti
xotdir<-"ottetti"
if(!file.exists(xotdir)) dir.create(xotdir)


####### PULIZIA FILE per evitare append su file vecchi
list.files(pattern="^.+conteggio_dati_mensili.csv")->nome.file
if(length(nome.file)) file.remove(nome.file)

list.files(pattern="^.+massimi_giornalieri.csv")->nome.file
if(length(nome.file)) file.remove(nome.file)
#######


#######SCRITTURA INTESTAZIONI FILE SU CUI SI FA APPEND
#tabella con i conteggi dei dati mensili
sink(paste0(prefisso,"_conteggio_dati_mensili.csv"))
cat("station_code;data_record_start_time;ndati.giornalieri\n")
sink()

#file con i massimi giornalieri
sink(paste0(prefisso,".massimi_giornalieri.csv"))
cat("data;station_code;aggregato.giornaliero;ndati.per.massimo;flag.massimo;flag.serie.annuale.valida\n")
sink()
######################################################

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
  
  #memoise(leggiDati)->mleggiDati
}
####fine memoise

#variabili che ci interessano
# dati.tmp %>% 
#   select(station_code,data_record_start_time,data_record_end_time,data_record_value,validity_fk)->dati
# rm(dati.tmp)

#semplifichiamo nomi variabili
#names(dati)->nomi.old

leggiDati()->dati

#flag validità trovati: -99 -1 
#In generale flag minore di 0 corrispondono a -9999, ma non sempre
#flag dati validi 1 2 3

#ricodifichiamo dati non validi come NA
dati[dati$flag<0,]$valore<-NA

dati$idstaz<-as.character(dati$idstaz)

#codici stazioni
unique(dati$idstaz)->codiciStazioni
if(1==1){
#calcola ottetti e li scrive in directory ottetti.
#Se trova il file già esistente non fa nulla
sink(paste0(prefisso,".stazioni.timestampErrato.csv"),append=FALSE)

      parallel::mclapply(codiciStazioni,FUN=function(codiceStaz){
            
            print(codiceStaz)
          
            dati %>% filter(idstaz==codiceStaz) %>% 
              mutate(yymmddhh=as.POSIXct(yymmddhh,tz="UTC"),
                     yymmddhh.fine=as.POSIXct(yymmddhh.fine,tz="UTC"),
                     gapTime=yymmddhh.fine-yymmddhh) ->subDati
            
            if(all(is.na(subDati[,c("valore")]))) return()
            
            attributes(subDati$gapTime)$units->periodicita #hours,days etc etc
            stopifnot(periodicita=="hours")
            unique(subDati$gapTime)->timeStep
            
            #le differenze temporali tra un timestamp e l'altro sono sempre uguali 1?
            #Per l'ozono i dati devono essere solo orari
            if(length(timeStep)>1){
              cat(paste0(codiceStaz,";",timeStep[1],";",timeStep[2],"\n")) #altri gap temporali?
              return()  
            }
            
            #oggetto xts su dati effettivi. NOTA: subDati potrebbe non essere una serie completa
            #rispetto al calendario necessario.
            xts(subDati[,c("valore")],order.by=subDati$yymmddhh)->datiAnno 
            rm(subDati)    
            
            #esiste già il file con l'ottetto?? Se non esiste lo calcolo
            #Se già esiste lo leggo senza rifare i calcoli (oneroso)
            list.files(path=paste0(xotdir),pattern=paste0("^",codiceStaz,"\\.o3.+ottetti.csv$"),full.names=TRUE)->nome.file.ottetto
            stopifnot(length(nome.file.ottetto)<=1)
            if(!length(nome.file.ottetto)){
              
              global.ottetto->xottetto
              
              lapply(1:lenCal,FUN=function(dd){
                
                datiAnno[.index(datiAnno) %in% seq.POSIXt(calendario[dd],calendario[dd]+hours(7),by="hour"), ]->single.ottetto
                #single.ottetto senza NA: single.ottetto.validi
                single.ottetto[!is.na(single.ottetto[,c("valore")]),]->single.ottetto.validi
                nrow(single.ottetto.validi)->numValidi
                #se tra inizioOttetto e fine dell'ottetto ci sono dati validi allora:
                if(numValidi){
                  if(numValidi>=6) xottetto[dd,c("flag")]<<-1
                  xottetto[dd,c("media.ottetto")]<<-mean(single.ottetto.validi)
                  xottetto[dd,c("numero.dati")]<<-numValidi
                } 
                
              })#finelapply su calendario
              
              #no: ci servono dopo --> no! rm(datiAnno)
              #aggiorno la data: deve essere riferita all'ora finale dell'ottetto
              index(xottetto)<-index(xottetto)+hours(7)
              
              #SCRITTURA FILE OTTETTI#########################################
              as.data.frame(xottetto)->df.xottetto
              df.xottetto$station_code<-codiceStaz
              df.xottetto$data_record_end_time<-row.names(df.xottetto)
              df.xottetto %>%
                select(station_code,data_record_end_time,everything()) %>%
                write_delim(.,paste0(xotdir,"/",codiceStaz,".",prefisso,".ottetti.csv"),delim=";",col_names=TRUE)
              rm(df.xottetto)
              #FINE SCRITTURA FILE OTTETTI#####################################
              
            }#fine if su length nome.file.ottetto
          
      },mc.cores=2) #fine lapply su codiciStazioni per calcolo e scrittura ottetti  

sink()
#############################################################################
#Fine calcolo ottetti
#############################################################################
}          

#pdf(paste0(prefisso,".massimi.giornalieri.pdf"),onefile = TRUE)
######################################################################################
#Calcolo indicatori
######################################################################################
lapply(codiciStazioni,FUN=function(codiceStaz){
  
  print(codiceStaz)

    #esiste già il file con l'ottetto?? Se non esiste lo calcolo
    #Se già esiste lo leggo senza rifare i calcoli (oneroso)
    list.files(path=paste0(xotdir),pattern=paste0("^",codiceStaz,"\\.o3.+ottetti.csv$"),full.names=TRUE)->nome.file.ottetto
    stopifnot(length(nome.file.ottetto)<=1)
    if(!length(nome.file.ottetto)){

      print(sprintf("File ottetto stazione %s non trovato",codiceStaz))
      return()
          
    }else{          
        read.csv(nome.file.ottetto,check.names = FALSE,head=TRUE,sep=";")->temporaneo
        xts(temporaneo[,c("media.ottetto","flag","numero.dati")],order.by = as.POSIXct(temporaneo$data_record_end_time))->xottetto
        rm(temporaneo)
    }    
          
    #plot.zoo(xottetto)
    apply.daily(xottetto[,c("media.ottetto","flag")],FUN=massimoGiornaliero)->massimi.giornalieri
    index(massimi.giornalieri)<-as.Date(index(massimi.giornalieri))
    #aggregato.giornaliero sarebbe il massimo
    names(massimi.giornalieri)<-c("aggregato.giornaliero","ndati.per.massimo","flag.massimo")

    #qui comincio a lavorare annualmente
    #quanti e quali anni nella serie? Lo scopro qui, non all'inizio del programma.
    #Una serie "completa" per il 2013 inizia il 31 dicembre del 2012, ma l'anno di riferimento è il 2013.
    #Se utilizassi unique years... all'inizio del programma troverei in "anni" anche il 2012 (scorretto).
    as.character(unique(year(index(massimi.giornalieri))))->anni
    #print(plot(massimi.giornalieri,main=codiceStaz))
    lapply(anni,FUN=function(yy){

          massimi.giornalieri[year(index(massimi.giornalieri))==yy,]->massimi.yy
          if(!nrow(massimi.yy) || all(massimi.yy[,c("flag.massimo")]!=1)) return(NULL) 

          #aggrego a livello mensili i flag con valore 1 per vedere se la serie annuale ha senso in base ai criteri richiesti
          apply.monthly(massimi.yy[massimi.yy[,c("flag.massimo")]==1,c("flag.massimo")],sum)->conteggio.mensili
          names(conteggio.mensili)<-"ndati.giornalieri"
          
          ######################## scriviamo per debug il numero di dati
          # validi (con flag 1) in un file
          as.data.frame(conteggio.mensili)->df.mensili
          df.mensili$station_code<-codiceStaz
          df.mensili$data_record_start_time<-row.names(df.mensili)
          df.mensili %>% 
            select(station_code,data_record_start_time,ndati.giornalieri) %>%
            write_delim(.,path=paste0(prefisso,"_conteggio_dati_mensili.csv"),delim=";",col_names=FALSE,append=TRUE)
          ######################### fine scriviamo per debug il numero di dati

          #Verifichiamo che la serieannuale sia valida ovvero vi siano
          #nella stagione estiva (estesa, da aprile a sett) almeno 5 mesi validi
          #Un mese è valido se ha il 90% di dati validi.
          
          #Se SOGLIA.MENSILI.FISSA==TRUE: calcolo il 90% del numero di dati di un mese
          #Se SOGLIA.MENSILI.FISSA==FALSE (default): utilizza la soglia fissa di 27 giorni al mese

          if(SOGLIA.MENSILI.FISSA){
              #g_in_mese definito fuori dal ciclo 
              if(leap_year(as.integer(yy))) g_in_mese[2]<-29
              #90% dati validi
              arrotonda(g_in_mese*0.9)->g_in_mese
              #prendiamo da g_in_mese i mesi effettivi che compaiono in conteggio.mensili
              names(g_in_mese)<-str_pad(seq(1,12),pad="0",width=2,side="left")
              #questi sono i mesi effettivi su cui abbiamo i numeri di dati giornalieri cumulati
              g_in_mese[month(conteggio.mensili)]->soglie.ndati
          }else{
              rep(listaParametri[["soglia.numero.mensili"]],length(month(conteggio.mensili)))->soglie.ndati            
          }
          
          #mesi che soddisfano i vincoli del 90%
          (.indexmon(conteggio.mensili[conteggio.mensili[,"ndati.giornalieri"]>=soglie.ndati,])+1)->mesi.ok
          #estate.estesa: mesi da aprile a settembre 
          intersect(mesi.ok,seq(4,9))->mesi.ok2
          #mesi.ok2 sono i mesi in (estate estesa) che soddisfano il vincolo del 90%
          #se ho cinque mesi su sei validi la serie annuale è valida per gli indicatori che seguono
          ifelse(length(mesi.ok2)>=5,1,-1)->flag.serie.annuale
        
          ############################
          ######################## scrittura file con tutti i dati aggregati a livello giornaliero
          as.data.frame(massimi.yy)->dfMassimi
          dfMassimi$flag.serie.annuale.valida<-flag.serie.annuale
          dfMassimi$data_record_start_time<-row.names(dfMassimi)
          dfMassimi$station_code<-codiceStaz
          dfMassimi %>% 
            select(station_code,data_record_start_time,everything()) %>%
          write_delim(.,path=paste0(prefisso,".massimi_giornalieri.csv"),col_names = FALSE,delim=";",append=TRUE)
          rm(dfMassimi)
          ##### fine scrittura file con tutti i dati aggregati a livello giornaliero
          ############################
          
          #ora invalidiamo i massimi con flag -1
          massimi.yy[massimi.yy[,c("flag.massimo")]==-1,c("aggregato.giornaliero")]<-NA
          
          #restituiamo la serie dei massimi giornalieri con flag di validita sula serie
          xtsAttributes(massimi.yy)$tipo<-"giornaliero"
          xtsAttributes(massimi.yy)$flag.serie.annuale<-flag.serie.annuale
          class(massimi.yy)<-c("datiGiornalieri",class(massimi.yy))
          massimi.yy[,c("aggregato.giornaliero")]
          
    })->listaTemp  #fine lapply su anni/yy
  
    #listaAggregati contiene la serie dei  valori massimi giornalieri (per ogni anno disponibile)
    #per una determinata stazione
    names(listaTemp)<-anni
    stopifnot(length(listaTemp)<=length(anni))
    Filter(Negate(is.null),listaTemp)->listaAggregati
    rm(listaTemp)
    rm(massimi.giornalieri)
    
    if(!length(listaAggregati)) return(list(NULL)) #tanti null quanti gli output 

    #flag serie
    lapply(listaAggregati,FUN=function(x){xtsAttributes(x)$flag.serie.annuale})->listaFlag
    
    #obiettivo 120 mg
    lapply(listaAggregati,superamentoThreshold,sogliaInquinante=SOGLIA.GIORNALIERA)->listaSuperamenti
    lapply(listaSuperamenti,numeroSuperamenti,sogliaSuperamenti=SOGLIA.SUPERAMENTI)->listaNumeroSuperamenti
    
    #obiettivo protezione salute umana
    lapply(listaSuperamenti,numeroSuperamenti,sogliaSuperamenti=25)->listaNumeroSuperamentiSaluteUmana
    
    #obiettivo OMS
    lapply(listaAggregati,superamentoThreshold,sogliaInquinante=100)->listaSuperamentiOMS
    lapply(listaSuperamentiOMS,numeroSuperamenti,sogliaSuperamenti=0)->listaNumeroSuperamentiOMS
    
    #obiettivo SOMO CAFE/SOMO35/SOMO0
    lapply(listaAggregati,FUN=function(lll){
      nrow(lll[!is.na(lll[,c("aggregato.giornaliero")]),])->NUMERO.MASSIMI
      stopifnot(NUMERO.MASSIMI>0 && NUMERO.MASSIMI<=366) #abbiamo già filtrato gli elementi della lista vuoti, quindi se sono qui devo avere dei massimi giornalieri
      sum(lll[lll[,c("aggregato.giornaliero")]>=70,]-70)->somo.cafe
      somo35<-somo.cafe/NUMERO.MASSIMI
      somo0<-sum(lll[!is.na(lll[,c("aggregato.giornaliero")]),])/NUMERO.MASSIMI
      
      c("numero.massimi"=NUMERO.MASSIMI,"somo.cafe"=somo.cafe,"somo35"=somo35,"somo0"=somo0)
      
    })->listaSOMO
    

    #uniamo il tutto
    lapply(anni,FUN=function(yy){
      
      do.call("c",
              list(
                as.integer(yy),
                listaFlag[[yy]],
                listaNumeroSuperamenti[[yy]],
                listaNumeroSuperamentiSaluteUmana[[yy]],
                listaNumeroSuperamentiOMS[[yy]],
                listaSOMO[[yy]]))
      
    })->listaUnione #fine lapply su anni
    
    as.data.frame(do.call("rbind",listaUnione))->dfRisultati
    dfRisultati$station_code<-codiceStaz
    names(dfRisultati)[1:8]<-c("anno","flag.serie.annuale","conteggio.oltsa","conteggio.oltre.soglia.oltsa","conteggio.opsa",
                          "conteggio.oltre.soglia.opsa","conteggio.oms","conteggio.oltre.soglia.oms")
    
    
    elaboraListaSuperamenti<-function(lista,soglia){

      dfSuperamenti<-NULL            
      
      #ora uniamo listaSuperamenti: SALUTE UMANA e OMS
      Filter(nrow,lista)->listaSuperamenti
      if(length(listaSuperamenti)){
        as.data.frame(do.call("rbind",listaSuperamenti))->dfSuperamenti
        dfSuperamenti$data_record_start_time<-row.names(dfSuperamenti)
        rownames(dfSuperamenti)<-NULL
        dfSuperamenti$station_code<-codiceStaz
        dfSuperamenti$soglia<-soglia
        dfSuperamenti[,c("station_code","data_record_start_time","aggregato.giornaliero","soglia")]->dfSuperamenti
      }
      
      dfSuperamenti
      
    }#fine funzione
    
    elaboraListaSuperamenti(listaSuperamenti,soglia=120)->dfSuperamenti
    elaboraListaSuperamenti(listaSuperamenti,soglia=100)->dfSuperamentiOMS    


    list(dfSuperamenti,dfSuperamentiOMS)->listaFinaleSuperamenti
    Filter(Negate(is.null),listaFinaleSuperamenti)->listaFinaleSuperamenti
    
    if(length(listaFinaleSuperamenti)){
      data.frame(do.call("rbind",listaFinaleSuperamenti))->dfFinaleSuperamenti
    }else{
      dfFinaleSuperamenti<-NULL
    }
    
    #finalmente restituiamo
    list(dfRisultati,dfFinaleSuperamenti)    
    
    
})->listaFinale
#dev.off()
######################################################################################
#Calcolo indicatori
######################################################################################

######################################################################################
#Scrittura risultati
######################################################################################


lapply(listaFinale,function(x)x[[1]])->listaRisultati
lapply(listaFinale,function(x)x[[2]])->listaSuperamenti

Filter(Negate(is.null),listaRisultati)->listaRisultati2
Filter(Negate(is.null),listaSuperamenti)->listaSuperamenti2

#se listaSuperamenti2 è vuoto non ho avuto alcun superamento. Questo dato deve
#coincidere con quanto in dfRisultati
if(length(listaSuperamenti2)){
  as.data.frame(do.call("rbind",listaSuperamenti2))->dfSuperamenti
  write_delim(dfSuperamenti,path=paste0(prefisso,".massimi.giornalieri.superamenti.csv"),delim=";",col_names = TRUE,append=FALSE)
} 

#scriviamo file dei risultati
as.data.frame(do.call("rbind",listaRisultati2))->dfFinale


dfFinale %>%
  select(anno,station_code,everything()) %>%
  write_delim(.,path=paste0(prefisso,".indicatori.ottetti.csv"),delim=";",col_names = TRUE)

