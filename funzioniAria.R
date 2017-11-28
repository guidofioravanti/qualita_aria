###########################################################################################
#arrotondamento commerciale
###########################################################################################
arrotonda=function(x)
{
  
  x %% 1 -> resto
  
  ceiling(x[resto>=0.5])->x[resto>=0.5]
  floor(x[resto<0.5])->x[resto<0.5]
  
  x[is.nan(x)]<-NA
  
  x
  
}  

###########################################################################################
#calcola la media giornaliera di dati orari/biorari e restituisce il numero di dati non NA utilizzati
#per il calcolo
###########################################################################################
mediaGiornaliera=function(xx)
{
  #deve essere un oggetto xts (a una colonna!!)
  stopifnot(is.xts(xx) && names(xx)[1]=="valore")

  #calcola la media giornalieria
  apply.daily(xx,FUN=function(yy){
    
    #se non c'è neanche un'ora per quel determinato giorno, neanche avremo un ciclo in yy
    #se invece entriamo function significa che abbiamo almeno un dato..nn devo preoccuparmi di nrow(yy)
    
    #tutto vettore di NA?
    if(all(is.na(yy[,c(1)]))) return(c(NA,0))
    #restituisco media e numero di dati non NA utilizzati per il calcolo
    c(mean(yy[,c(1)],na.rm=TRUE),nrow(yy[!is.na(yy[,c(1)]),]))
    
  })->out
  
  names(out)<-c("aggregato.giornaliero","numero.dati")

  out 
  
} #mediaGiornaliera 


###########################################################################################
#calcola il massimo giornaliero sugli ottetti tenendo conto del falg di validita
###########################################################################################
massimoGiornaliero=function(xx)
{
  #deve essere un oggetto xts (a una colonna!!)
  stopifnot(is.xts(xx) && names(xx)[1]=="media.ottetto" && names(xx)[2]=="flag")
  
  #annulliamo i valori con flag -1 in modo di non avere problemi dopo con il massimo
  #nel caso di giorni con solo NA o dati con flag -1.Il max lancerebbe un warning
  #n ogni caso un dato con flag-1 è non valido
  xx[xx[,c("flag")]==-1,c("media.ottetto")]<-NA
  
  #tutto vettore di NA?
  if(all(is.na(xx[,"media.ottetto"]))) return(c(NA,0,-1))

  nrow(xx[xx[,"flag"]==1,])->numero.ottetti.validi
  ifelse(numero.ottetti.validi>=18,1,-1)->flag.massimo
  #restituisco massimo e numero di dati non NA utilizzati per il calcolo
  c(max(xx[xx[,"flag"]==1,"media.ottetto"],na.rm=TRUE),numero.ottetti.validi,flag.massimo)->out
    
  out 
  
} #massimoGiornaliero o3


###########################################################################################
#numerosuperamenti
###########################################################################################
numeroSuperamenti=function(xx,sogliaSuperamenti)
{
  if(is.null(xx)) return(0)
  
  nrow(xx)->nrighe
  
  #restituiamo il numero di righe (ovvero il numero di superamenti riscontrati)
  #e il numero di superamenti rispetto sogliaSuperamenti
  return(c("totale.superamenti"=nrighe,"totale.superamenti.oltre.soglia"=max(0,nrighe-sogliaSuperamenti)))
  
}#numeroSuperamenti  


###########################################################################################
#numerosuperamenti: restituisce il valore medio annuale se superiore a soglia altrimenti restituisce NULL
###########################################################################################
superamentoThresholdAnnuale=function(xx,sogliaInquinante)
{
  
  arrotonda(xx[c(1)])->media.arrotondata
  
  #devo restituire NA e non NULL, altrimenti quando farò l'unione di tutti i dati avrò
  #colonne differenti
  if(media.arrotondata <=sogliaInquinante || is.na(media.arrotondata)) return(NA)
  
  media.arrotondata
  
} #fine superamentoThreshold


######################################################
#Scrittura tabella aggregati giornalieri
######################################################
scriviTabella=function(xx,idstaz,...)
{
  
  stopifnot(is.xts(xx) && names(xx)[1]=="aggregato.giornaliero")
  
  xx$flag.valido<- -1
  
  if(xtsAttributes(xx)$tipo=="giornaliero"){
    xx[!is.na(xx[,c(1)]),c("flag.valido")]<-1  
    
    #numero dati qui è un'info che non abbiamo
    xx$numero.dati<-NA
    
  }else{
    ifelse(xtsAttributes(xx)$tipo=="orario",18,9)->numeroMinDati
    xx[!is.na(xx$aggregato.giornaliero) & xx$numero.dati>=numeroMinDati,c("flag.valido")]<-1   
  }  
  
  index(xx)->giornoInizio
  index(xx)+days(1)->giornoFine
  
  nrow(xx)->numeroRighe
  data.frame(observation_fk=rep(idstaz,numeroRighe),
             giornoI=as.Date(giornoInizio),
             giornoF=as.Date(giornoFine),
             aggregato=as.vector(xx[,c("aggregato.giornaliero")]),
             flag=as.vector(xx[,c("flag.valido")]),
             numero.dati=as.vector(xx[,c("numero.dati")]),
             dati.raw=rep(xtsAttributes(xx)$tipo,numeroRighe))->finale
  write_delim(finale,path=paste0(prefisso,".aggregatiGiornalieri.csv"),append=TRUE,col_names=FALSE,delim=";")
  
}#fine scriviTabella

######################################################
#Scrittura tabella degli ottetti aggregati. Specifico per O3
######################################################
scriviTabellaOttetti=function(xx=NULL,idstaz=NULL,scriviColNames=NULL,...)
{
  
  stopifnot(is.xts(xx) && is.logical(scriviColNames) && is.character(idstaz))
  as.data.frame(xx)->finale
  finale$observation_fk<-rep(idstaz,nrow(xx))

  row.names(finale)->finale$data
  finale %>%select(observation_fk,data,everything()) %>%
  write_delim(.,path=paste0(prefisso,".massimiGIornalieri.csv"),append=TRUE,col_names=scriviColNames,delim=";")
  
}#fine scriviTabella


######################################################
#Restituisce il prefisso utilizzato per il nome di ogni file di output
#La directorydi lavoro deve avere come prima parte il nome dell'inquinante
#ad esempio: pm10_etc_etc
######################################################
ottieniPrefisso=function(){
  
  as.Date(Sys.time())->ggmmyy
  inquinante<-tolower(str_split(basename(getwd()),"_")[[1]][1])
  
  c(inquinante,paste0(inquinante,".",ggmmyy))
}


######################################################
#Restituisce i parametri necessari per i cari indicatori
######################################################
ottieniParametri=function(inquinante){
  
  stopifnot(is.character(inquinante)) 
  
  if(inquinante=="pm10"){
    
    sogliaAnnuale<-40 #pm10
    #PROBS PER CALCOLO PERCENTILI
    c(0.25,0.5,0.75,0.904,0.98,0.992,0.999,1)->probs
    
    #soglia numero di superamenti
    sogliaSuperamenti<-35
    
    #soglia su valore giornaliero
    sogliaGiornaliera<-50
    
    listaParametri<-list(sogliaAnnuale,probs,sogliaSuperamenti,sogliaGiornaliera)
    names(listaParametri)<-c("sogliaAnnuale","probs","sogliaSuperamenti","sogliaGiornaliera")
    
  }else if(inquinante=="pm25"){
    
    sogliaAnnuale<-26 
    #PROBS PER CALCOLO PERCENTILI
    c(0.25,0.5,0.75,0.98,0.992,0.999,1)->probs
    
    listaParametri<-list(sogliaAnnuale,probs)
    names(listaParametri)<-c("sogliaAnnuale","probs")    
    
  }else if(inquinante=="no2"){
    
    sogliaAnnuale<-40
    #PROBS PER CALCOLO PERCENTILI
    c(0.25,0.5,0.75,0.98,0.998,0.999,1)->probs        
    
    #soglia su valore giornaliero
    sogliaGiornaliera<-200
    
    #soglia numero di superamenti
    sogliaSuperamenti<-18    
    
    listaParametri<-list(sogliaAnnuale,probs,sogliaGiornaliera,sogliaSuperamenti)
    names(listaParametri)<-c("sogliaAnnuale","probs","sogliaGiornaliera","sogliaSuperamenti")       
    
  }else if(inquinante=="o3"){  
    
    #soglia.numero.mensili numero di giorni validi in un mese estivo per considerare una serie
    #annuale (di massimi di ottetti) valida.
    #Se invece di utilizzare un valore fisso di 27 giorni si volesse calcolare il 90%
    #dei dati mensili, mettere a TRUE nel programma il parametro: SOGLIA.MENSILI.FISSA
    soglia.numero.mensili<-27
    
    #PROBS PER CALCOLO PERCENTILI
    c(0.25,0.5,0.75,0.98,0.999,1)->probs  
    
    #soglia su valore giornaliero
    sogliaGiornaliera<-120
    
    #soglia numero di superamenti
    sogliaSuperamenti<-0
    
    listaParametri<-list(soglia.numero.mensili,probs,sogliaGiornaliera,sogliaSuperamenti)
    names(listaParametri)<-c("soglia.numero.mensili","probs","sogliaGiornaliera","sogliaSuperamenti")       
    
        
  }else{
    
    stop("inquinante non riconosciuto!!")
    
  } #fine su inquinante 
  
  return(listaParametri)
  
}



###########################################################################################
#QUI INIZIANO LE FUNZIONI LA CUI IMPLEMENTAZIONE DIPENDE DAL PARAMETRO
###########################################################################################


###########################################################################################
#media Annuale, dipende se partiamo da dati orari o dati giornalieri
###########################################################################################
mediaAnnuale=function(xx){
  
  UseMethod("mediaAnnuale",xx)
  
}#fine mediaAnnuale


###########################################################################################
#calcola media annua (su dati non arrotondati) Questa per pm10 e pm25
###########################################################################################
mediaAnnuale.datiGiornalieri=function(xx){
  
  stopifnot(is.xts(xx) && !is.null(xtsAttributes(xx)$tipo) && names(xx)[1]=="aggregato.giornaliero")
  
  #In base all'attributo "tipo" calcolo la media annuale.
  #Se tipo == giornaliero, allora prendo i dati validi (non NA)
  #Se tipo == orario, allora devo prendere solo i dati validi con numero.dati >=18
  #Se tipo == biorario allora prendo solo i dati validi con numero.dati >=9
  
  if(xtsAttributes(xx)$tipo=="giornaliero"){
    xx[,c(1)]->subxx
  }else{
    ifelse(xtsAttributes(xx)$tipo=="orario",18,9)->numeroMinDati
    xx[xx$numero.dati>=numeroMinDati,c(1)]->subxx
  }

  #nessuna riga o tutte NA
  if(!nrow(subxx) || all(is.na(subxx))) return(c(NA,0))
  #da subxx che ho già filtrato in base alla soglia nel conteggio delle righe
  #devo togliere le righe NA
  c(mean(subxx,na.rm=TRUE),nrow(subxx[!is.na(subxx[,1]),]) )
    
} #fine media


###########################################################################################
#calcola media annua (su dati non arrotondati) Questa per no2
###########################################################################################
mediaAnnuale.datiOrari=function(xx){
  
  stopifnot(is.xts(xx) && names(xx)[1]=="dato.orario")
  
  #nessuna riga o tutte NA
  if(!nrow(xx) || all(is.na(xx[,c(1)]))) return(c(NA,0))
  
  #da xx devo togliere le righe NA da nrow
  c(mean(xx[,c(1)],na.rm=TRUE),nrow(xx[!is.na(xx[,1]),]) )
  
} #fine media





###########################################################################################
#superamento Threshold, dipende se partiamo da dati orari o dati giornalieri
###########################################################################################
superamentoThreshold=function(xx,...){
  
  UseMethod("superamentoThreshold",xx)
}

###########################################################################################
#superamento Threshold,  giornalieri
###########################################################################################
superamentoThreshold.datiGiornalieri=function(xx,sogliaInquinante)
{

  stopifnot(is.xts(xx) && !is.null(xtsAttributes(xx)$tipo) && names(xx)[1]=="aggregato.giornaliero" && is.numeric(sogliaInquinante))

  arrotonda(xx[,c(1)])->xx$rounded

  #In base all'attributo "tipo" calcolo la media annuale.
  #Se tipo == giornaliero, allora prendo i dati validi (non NA)
  #Se tipo == orario, allora devo prendere solo i dati validi con numero.dati >=18
  #Se tipo == biorario allora prendo solo i dati validi con numero.dati >=9
  
  if(xtsAttributes(xx)$tipo=="giornaliero"){

    xx[xx$rounded>sogliaInquinante,!(names(xx) %in% "rounded") ]->ris  
  }else{
    ifelse(xtsAttributes(xx)$tipo=="orario",18,9)->numeroMinDati
    xx[xx$rounded>sogliaInquinante & xx$numero.dati>=numeroMinDati,!(names(xx) %in% "rounded")]->ris  
  }  

  ris
  
} #fine superamentoThreshold


###########################################################################################
#superamento Threshold, orari
###########################################################################################
superamentoThreshold.datiOrari=function(xx,sogliaInquinante)
{
  
  stopifnot(is.xts(xx) && !is.null(xtsAttributes(xx)$tipo) && names(xx)[1]=="dato.orario" && is.numeric(sogliaInquinante))
  
  arrotonda(xx[,c(1)])->xx$rounded
  
  xx[xx$rounded>sogliaInquinante,!(names(xx) %in% "rounded") ]
  
} #fine superamentoThreshold

######################################################
#calcola Percentili
######################################################
calcolaPercentili=function(xx,...)
{
  UseMethod("calcolaPercentili",xx)
}

######################################################
#calcola Percentili: il paramatro tipo nn è l'attrbuto tipo restituito daxtsAttributes..
#è il tipo di percentile. il tipo 0 è basato sul calcolodei rangi con arrotondamento commerciale
######################################################
calcolaPercentili.datiGiornalieri=function(xx,air.prob=PROBABILITA,tipo=NULL)
{
  
  stopifnot(is.xts(xx) && !is.null(xtsAttributes(xx)$tipo) 
            && names(xx)[1]=="aggregato.giornaliero" && !is.null(tipo) && is.numeric(air.prob))

  #restituisci tanti NA quanti sono i percentili calcolati e numero dati
  
  #Nel caso di dati tutti NA non possiamo semplicemente restituire un vettore di NA tanti quanti sono i
  #quantili che vogliamo calcolare. IL problema sorgerebbe nel momento in cui, mettendo insieme i dati
  #di tutti gli anni e di tutte le stazioni, utilizziamo "rbind" su listaRisultati2 per crare dfRisultati.
  #Si genererebbe un errore: alcune righe avrebbero i quantili con i nomi degli stessi quantili
  #le righe invece NA non avrebbero i quantili con nomi..e questo non va bene a rbind.
  #Soluzione: calcolare i quantili air.prob con un valore NA ponendo na.rm=TRUE
  
  if(all(is.na(xx[,c(1)]))) return(c(quantile(NA,probs=air.prob,na.rm=TRUE),0))
  
  #In base all'attributo "tipo" filtro i dati per il calcolo dei percentili
  #infatti xx contiene tutte le medie giornaliere in un anno senza distinguere 
  #tra le valide (numero dati orari >= 75%) e le non valide
  
  #Se tipo == giornaliero, allora prendo i dati validi (non NA)
  #Se tipo == orario, allora devo prendere solo i dati validi con numero.dati >=18
  #Se tipo == biorario allora prendo solo i dati validi con numero.dati >=9
  
  
  if(xtsAttributes(xx)$tipo=="giornaliero"){
    xx[,c("aggregato.giornaliero")]->yy
  }else{
    ifelse(xtsAttributes(xx)$tipo=="orario",18,9)->numeroMinDati
    xx[xx$numero.dati>=numeroMinDati,c("aggregato.giornaliero")]->yy  
  }    

  if(!nrow(yy)) return(c(quantile(NA,probs=air.prob,na.rm=TRUE),0))
    
  as.vector(yy)->ris
  if(tipo==0){
    arrotonda(c(length(ris[!is.na(ris)])*air.prob))->ranghi
    ranghi[ranghi==0]<-1  #questo succede solo nei caso anomali con un dato ad esempio, in cui il primo dato ha rango 0!
    sort(ris,na.last=NA)[ranghi]->quantili
    names(quantili)<-paste0(paste(air.prob*100,"%",sep=""),"numero.dati.percentili")
  }else{
    quantile(ris,probs=air.prob,na.rm=TRUE,type = tipo)->quantili
  }  

  c(quantili,length(ris[!is.na(ris)]))
  
}#fine calcolaPercentili



######################################################
#calcola Percentili: dai orari
######################################################
calcolaPercentili.datiOrari=function(xx,air.prob,tipo=NULL)
{
  
  stopifnot(is.xts(xx) && names(xx)[1]=="dato.orario" && !is.null(tipo) && is.numeric(air.prob))
  
  #restituisci tanti NA quanti sono i percentili calcolati e numero dati
  
  #Nel caso di dati tutti NA non possiamo semplicemente restituire un vettore di NA tanti quanti sono i
  #quantili che vogliamo calcolare. IL problema sorgerebbe nel momento in cui, mettendo insieme i dati
  #di tutti gli anni e di tutte le stazioni, utilizziamo "rbind" su listaRisultati2 per crare dfRisultati.
  #Si genererebbe un errore: alcune righe avrebbero i quantili con i nomi degli stessi quantili
  #le righe invece NA non avrebbero i quantili con nomi..e questo non va bene a rbind.
  #Soluzione: calcolare i quantili air.prob con un valore NA ponendo na.rm=TRUE
  
  if(!nrow(xx[,c(1)]) || all(is.na(xx[,c(1)]))) return(c(quantile(NA,probs=air.prob,na.rm=TRUE),0))
  
  as.vector(xx[,c(1)])->ris
  if(tipo==0){
    arrotonda(c(length(ris[!is.na(ris)])*air.prob))->ranghi
    ranghi[ranghi==0]<-1  #questo succede solo nei caso anomali con un dato ad esempio, in cui il primo dato ha rango 0!
    sort(ris,na.last=NA)[ranghi]->quantili
    names(quantili)<-paste(air.prob*100,"%",sep="")
  }else{
    quantile(ris,probs=air.prob,na.rm=TRUE,type = tipo)->quantili
  }    
  
  c(quantili,length(ris[!is.na(ris)]))
  
}#fine calcolaPercentili





