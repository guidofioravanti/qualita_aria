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

#soglia sul numero di superamenti
SOGLIA.SUPERAMENTI<-listaParametri[["sogliaSuperamenti"]]

#soglia a livello giornaliero
SOGLIA.GIORNALIERA<-listaParametri[["sogliaGiornaliera"]]

#validity_fk: flag per qualità del dato
read_delim("data_records_pm10_2013_2014_Basilicata_Sicilia.csv",delim=";",col_names = TRUE)->dati.tmp

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
        
        #importante!!!
        class(datiAnno)<-c("datiGiornalieri",class(datiAnno))
        
        return(datiAnno) #<--- return!! esco da lapply  
        
      }#fine if caso dati giornalieri  

      mediaGiornaliera(datiAnno)->medie
      #associamo attributo sul passo temporale. Ci serve per definire la soglia (75%) sul numero dati orari oltre la quale
      #una media giornaliera è valida
      ifelse(timeStep==1,"orario","biorario")->myguess
      xtsAttributes(medie)<-list("tipo"=myguess)

      #importante!!!
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

    lapply(listaAggregati,superamentoThreshold,sogliaInquinante=SOGLIA.GIORNALIERA)->listaSuperamenti
    lapply(listaSuperamenti,numeroSuperamenti,sogliaSuperamenti=SOGLIA.SUPERAMENTI)->listaNumeroSuperamenti
    lapply(listaAggregati,mediaAnnuale)->listaMediaAnnuale
    lapply(listaMediaAnnuale,superamentoThresholdAnnuale,sogliaInquinante=SOGLIA.ANNUALE)->listaSuperamentiMediaAnnuale
    lapply(listaAggregati,calcolaPercentili,tipo=0)->listaPercentili0
    lapply(listaAggregati,calcolaPercentili,tipo=8)->listaPercentili8
    
    #uniamo il tutto
    lapply(anni,FUN=function(yy){

      do.call("c",
              list(
              as.integer(yy),  
              listaNumeroSuperamenti[[yy]],
              listaMediaAnnuale[[yy]],
              listaSuperamentiMediaAnnuale[[yy]],
              listaPercentili0[[yy]],
              listaPercentili8[[yy]]))
      
    })->listaUnione #fine lapply su anni
    
    #ora posso cancellare subDati
    rm(subDati)
    
    as.data.frame(do.call("rbind",listaUnione))->dfRisultati
    dfRisultati$observation_fk<-codiceStaz
    
    #ora uniamo listaSuperamenti solo se ci sono effettivamente dei dati (nrow >0)
    Filter(nrow,listaSuperamenti)->listaSuperamenti
    if(length(listaSuperamenti)){
      as.data.frame(do.call("rbind",listaSuperamenti))->dfSuperamenti
      dfSuperamenti$giorno<-as.Date(rownames(dfSuperamenti))
      rownames(dfSuperamenti)<-NULL
      dfSuperamenti$observation_fk<-codiceStaz
      dfSuperamenti[,c("observation_fk","giorno","aggregato.giornaliero")]->dfSuperamenti
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
                   "numero.superamenti.oltre35",
                   "media.annuale",
                   "numero.dati.per.media",
                   "media.annuale.oltre.soglia",
                   paste0("ptype0_",PROBABILITA),
                   "numero.dati.per.percentili.type0",                   
                   paste0("ptype8_",PROBABILITA),
                   "numero.dati.per.percentili.type8",
                   "observation_fk")

write_delim(dfFinale %>%select(anno,observation_fk,everything()),path=paste0(prefisso,".csv"),delim=";",col_names = TRUE)


# #grafici
# dfFinale %>%select(anno,observation_fk,everything())->zz
# 
# #boxplot media annuale
# zz %>% filter(!is.na(media.annuale)) %>% 
#   ggplot(.,aes(x=as.factor(anno),y=media.annuale))+
#   geom_boxplot(aes(fill=as.factor(anno) ))+
#   scale_fill_brewer()+
#   theme_bw()+
#   theme(legend.position="none")+
#   xlab("")+
#   ggtitle("Media annuale PM10")->graficoMedia
# 
# #sovrapponiamo punti oltre la soglia
#   graficoMedia+
#     geom_hline(aes(yintercept=40),colour="firebrick",alpha=0.5,lwd=2)+
#     annotate("text",y=39,x=1.5,label="Soglia annuale")->graficoMedia
#   
# # Superamenti  
#   zz %>% filter(!is.na(numero.superamenti)) %>% 
#     ggplot(.,aes(x=as.factor(anno),y=numero.superamenti))+
#     geom_boxplot(aes(fill=as.factor(anno) ))+
#     scale_fill_brewer(palette = "Blues")+
#     theme_bw()+
#     theme(legend.position="none")+
#     xlab("")+
#     ggtitle("Numero annuale superamenti PM10")->graficoNumeroSuperamenti
#   
#   #sovrapponiamo punti oltre la soglia
#   graficoNumeroSuperamenti+
#     geom_hline(aes(yintercept=35),colour="firebrick",alpha=0.5,lwd=2)+
#     annotate("text",y=33,x=1,label="35 superamenti")->graficoNumeroSuperamenti
# 
# pdf("pm10.pdf",paper="a4r")  
# print(graficoMedia)
# print(graficoNumeroSuperamenti)    
# dev.off()