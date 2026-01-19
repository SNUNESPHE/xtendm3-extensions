/**
*  @Name: EXT340MI.LstRfoTECCOM
*  @Description: Get item info, get route info, get availability of stock, sorts by fastest availability
*  @Authors: Kenylen Motean
*/

/**
* CHANGELOGS
* Version    Date    User        Description
* 1.0.0      150125  KMOTEAN     Initial Release
* 1.0.1      140126  KMOTEAN     Set default values for MITBAL fields / Adjusted function calculateNextDeliveryDate to take date according to timezone
*/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.LocalDate

public class LstRfoTECCOM extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private final MICallerAPI miCaller
  private int inCONO
  private String inDIVI
  private String inCUNO
  private String inPOPN
  private String inCEAN
  private String inCFI1
  private double inORQA
  private int inOALT=1
  private int inLNUM
  private String ortp610=""
  private String whlo610=""
  private String splm610=""
  private String padl610=""
  private String bcko610=""
  private String stat610=""
  private String achk610=""
  private List<String> listItemsCFI1=[]
  private String correctITNO=""
  private Boolean correctITNOFound=false
  private List<String> listItemsMMS025=[]
  private List<String> listCodeMarqueCugex=[]
  private String retrievedCorrectCFI1M3=""
  private Boolean codeMarqueM3Found=false
  private List<String> arrWHLO=[]
  private List<Map<String, String>> listWarehousePlaceOfLoadAndDesc= []
  private List<String> arrUniqueSDES=[]
  private List<Map<String, String>> listRoutes= []
  private List<Map<String, String>> listRoutesDetails= []
  private List<Map<String, String>> listRoutesAllDetails= []
  private List<Map<String, String>>  whloDetailsMMS059= []
  private boolean  outputPadlFlag= false
  private boolean  sortFlag= true
  private boolean  customerInfoRetrievedFlag= false
  private List<Map<String, String>> listFinalRoutes= []
  private double PBQA
  private String codeMarquePartenaire=""
  private String retievedTOMU=""
  private String sTIME
  private String sDATE
  private String timezoneDATE
  private String divisionName
  private List<Map<String, String>> listPrices= []
  private String retrievedCRAM
  private String retrievedFUDS=""
  private String retrievedITDS=""
  private String retrievedCFI1=""
  private String assortmentCheck=""
  private List<String> listAssortment=[]
  private List<String> listClient=[]
  private String currentDate

  
  public LstRfoTECCOM(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
  }
  
  public void main() {
    inCONO = mi.in.get("CONO") as Integer == null ? program.LDAZD.get("CONO") as Integer : mi.in.get("CONO") as Integer
    inDIVI = mi.in.get("DIVI") == null ? program.LDAZD.get("DIVI")  : mi.in.get("DIVI")
    inCUNO = mi.inData.get("CUNO") == null ? "" : mi.inData.get("CUNO").trim()
    inPOPN = mi.inData.get("POPN") == null ? "" : mi.inData.get("POPN").trim()
    inCEAN = mi.inData.get("CEAN") == null ? "" : mi.inData.get("CEAN").trim()
    inCFI1 = mi.inData.get("CFI1") == null ? "" : mi.inData.get("CFI1").trim()
    inORQA = mi.in.get("ORQA") == null ? 0 as double: mi.in.get("ORQA") as Double
    inLNUM = mi.in.get("LNUM") == null ? 0 as Integer : mi.in.get("LNUM") as Integer
    
    if(!validateCONO()){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.toString())
      mi.outData.put('RESP', "Error")
      mi.outData.put('CODE', "3200")
      mi.outData.put('TYPE', "2")
      mi.outData.put('TEXT', "Invalid CONO/DIVI")
      
      mi.write()
      return
    }

    

    if(!validateCUNO()){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.toString())
      mi.outData.put('RESP', "Error")
      mi.outData.put('CODE', "1006")
      mi.outData.put('TYPE', "0")
      mi.outData.put('TEXT', "Invalid customer no.: ${inCUNO}")
      mi.outData.put('CONM', divisionName)
      mi.write()
      return
    }
  

    if(stat610!="20"){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.toString())
      mi.outData.put('RESP', "Error")
      mi.outData.put('CODE', "1006")
      mi.outData.put('TYPE', "0")
      mi.outData.put('TEXT', "Invalid customer no.: ${inCUNO}")
      mi.outData.put('CONM', divisionName)
      mi.write()
      return
    }

    //error if ORQA=0
    if(inORQA==0){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.toString())
      mi.outData.put('RESP', "Error")
      mi.outData.put('CODE', "1003")
      mi.outData.put('TYPE', "2")
      mi.outData.put('TEXT', "Line item not accepted")
      mi.outData.put('LNTX', "Rejected")
      mi.outData.put('CONM', divisionName)
      mi.write()
      return
    }
    
    //error if ORQA has decimal values
    if(inORQA % 1 != 0){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.toString())
      mi.outData.put('RESP', "Error")
      mi.outData.put('CODE', "1008")
      mi.outData.put('TYPE', "0")
      mi.outData.put('TEXT', "No standard sales unit found for product: ${inPOPN}")
      mi.outData.put('LNTX', "Rejected")
      mi.outData.put('CONM', divisionName)
      mi.write()
      return
    }
    
    formatPOPN()
    
    searchITEM()

    //if error in retrieving ITNO
    if(!correctITNOFound){
      if(inCEAN!=""){
        mi.outData.put('CONO', inCONO.toString())
        mi.outData.put('DIVI', inDIVI)
        mi.outData.put('CUNO', inCUNO)
        mi.outData.put('LNUM', inLNUM.toString())
        mi.outData.put('POPN', inPOPN)
        mi.outData.put('CFI1', inCFI1)
        mi.outData.put('CEAN', inCEAN)
        mi.outData.put('ORQA', inORQA.toString())
        mi.outData.put('RESP', "Error")
        mi.outData.put('CODE', "1014")
        mi.outData.put('TYPE', "2")
        mi.outData.put('TEXT', "Impossible to convert EAN code ${inCEAN} into product no.")
        mi.outData.put('LNTX', "Rejected")
        mi.outData.put('CONM', divisionName)
        mi.write()
        return
      }
      else{
        mi.outData.put('CONO', inCONO.toString())
        mi.outData.put('DIVI', inDIVI)
        mi.outData.put('CUNO', inCUNO)
        mi.outData.put('LNUM', inLNUM.toString())
        mi.outData.put('POPN', inPOPN)
        mi.outData.put('CFI1', inCFI1)
        mi.outData.put('CEAN', inCEAN)
        mi.outData.put('ORQA', inORQA.toString())
        mi.outData.put('RESP', "Error")
        mi.outData.put('CODE', "1007")
        mi.outData.put('TYPE', "0")
        mi.outData.put('TEXT', "Material master does not contain product: ${inPOPN}")
        mi.outData.put('LNTX', "Rejected")
        mi.outData.put('CONM', divisionName)
        mi.write()
        return
      }
    }
    else{
      if(!validateITNO()){
        mi.outData.put('CONO', inCONO.toString())
        mi.outData.put('DIVI', inDIVI)
        mi.outData.put('CUNO', inCUNO)
        mi.outData.put('LNUM', inLNUM.toString())
        mi.outData.put('POPN', inPOPN)
        mi.outData.put('CFI1', inCFI1)
        mi.outData.put('CEAN', inCEAN)
        mi.outData.put('ORQA', inORQA.toString())
        mi.outData.put('RESP', "Error")
        mi.outData.put('CODE', "1007")
        mi.outData.put('TYPE', "0")
        mi.outData.put('TEXT', "Material master does not contain product: ${inPOPN}")
        mi.outData.put('LNTX', "Rejected")
        mi.outData.put('CONM', divisionName)
        mi.write()
        return
      }
    }

    retrieveITDSAndFUDS()//Gets ITDS/FUDS/CFI1/ACHK from MITMAS

    LocalDate date = LocalDate.now()
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd") 
    currentDate = date.format(formatter)

    //verify achk for client and item
    if(achk610=="0"){
    }
    else if(achk610=="1"){
      if(assortmentCheck=="0"){
      }
      else if(assortmentCheck=="1"){
        listClient.add(inCUNO)
        
        checkChaineCommercialeOIS039()
        retrieveAssortimentOIS071()

        if(listAssortment.size()>0){
          if(!checkAssortimentArticle()){
            mi.outData.put('CONO', inCONO.toString())
            mi.outData.put('DIVI', inDIVI)
            mi.outData.put('CUNO', inCUNO)
            mi.outData.put('LNUM', inLNUM.toString())
            mi.outData.put('POPN', inPOPN)
            mi.outData.put('CFI1', inCFI1)
            mi.outData.put('CEAN', inCEAN)
            mi.outData.put('ORQA', inORQA.toString())
            mi.outData.put('RESP', "Error")
            mi.outData.put('CODE', "3200")
            mi.outData.put('TYPE', "2")
            mi.outData.put('TEXT', "Infor Error - Erreur Controle assortiment")
            mi.outData.put('LNTX', "Rejected")
            mi.outData.put('CONM', divisionName)
            mi.write()
            return
          }
        }
        else{
          mi.outData.put('CONO', inCONO.toString())
          mi.outData.put('DIVI', inDIVI)
          mi.outData.put('CUNO', inCUNO)
          mi.outData.put('LNUM', inLNUM.toString())
          mi.outData.put('POPN', inPOPN)
          mi.outData.put('CFI1', inCFI1)
          mi.outData.put('CEAN', inCEAN)
          mi.outData.put('ORQA', inORQA.toString())
          mi.outData.put('RESP', "Error")
          mi.outData.put('CODE', "3200")
          mi.outData.put('TYPE', "2")
          mi.outData.put('TEXT', "Infor Error - Erreur Controle assortiment")
          mi.outData.put('LNTX', "Rejected")
          mi.outData.put('CONM', divisionName)
          mi.write()
          return
        }
      }
    }
    else if(achk610=="2"){
      if(assortmentCheck=="0"){
      }
      else if(assortmentCheck=="1"){
        listClient.add(inCUNO)

        retrieveAssortimentOIS071()

        if(listAssortment.size()>0){
          if(!checkAssortimentArticle()){
            mi.outData.put('CONO', inCONO.toString())
            mi.outData.put('DIVI', inDIVI)
            mi.outData.put('CUNO', inCUNO)
            mi.outData.put('LNUM', inLNUM.toString())
            mi.outData.put('POPN', inPOPN)
            mi.outData.put('CFI1', inCFI1)
            mi.outData.put('CEAN', inCEAN)
            mi.outData.put('ORQA', inORQA.toString())
            mi.outData.put('RESP', "Error")
            mi.outData.put('CODE', "3200")
            mi.outData.put('TYPE', "2")
            mi.outData.put('TEXT', "Infor Error - Erreur Controle assortiment")
            mi.outData.put('LNTX', "Rejected")
            mi.outData.put('CONM', divisionName)
            mi.write()
            return
          }
        }
        else{
          mi.outData.put('CONO', inCONO.toString())
          mi.outData.put('DIVI', inDIVI)
          mi.outData.put('CUNO', inCUNO)
          mi.outData.put('LNUM', inLNUM.toString())
          mi.outData.put('POPN', inPOPN)
          mi.outData.put('CFI1', inCFI1)
          mi.outData.put('CEAN', inCEAN)
          mi.outData.put('ORQA', inORQA.toString())
          mi.outData.put('RESP', "Error")
          mi.outData.put('CODE', "3200")
          mi.outData.put('TYPE', "2")
          mi.outData.put('TEXT', "Infor Error - Erreur Controle assortiment")
          mi.outData.put('LNTX', "Rejected")
          mi.outData.put('CONM', divisionName)
          mi.write()
          return
        }
      }
    }
    
    retrieveConsigne()//getConsigne
    
    if(retrievedCRAM=="" || retrievedCRAM==null){
      retrievedCRAM="0.0"
    }
    
    outputPadlFlag=true
    sortFlag=true

    if(splm610!=""){
      mms059ListApiCall(splm610,"5")
      arrWHLO=arrWHLO.unique()
    }
    else if(whlo610!=""){
      arrWHLO.add(whlo610)
    }
    else{
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.toString())
      mi.outData.put('RESP', "Error")
      mi.outData.put('CODE', "3200")
      mi.outData.put('TYPE', "2")
      mi.outData.put('TEXT', "Default warehouse for customer not defined")
      mi.outData.put('CONM', divisionName)
      mi.write()
      return
    }
    
    if(arrWHLO.isEmpty()){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.toString())
      mi.outData.put('RESP', "Error")
      mi.outData.put('CODE', "3200")
      mi.outData.put('TYPE', "2")
      mi.outData.put('TEXT', "Order type WAV for supply model not defined")
      mi.outData.put('CONM', divisionName)
      mi.write()
      return
    }

    searchAllSDES()//Retrieve all SDES and other info for available WHLOs
    
    searchAllValidRoutesForCustomerPO1PO2()//Retrieve all routes and other info for available WHLOs
    
    listRoutes=listRoutes.unique()//remove duplicate routes

    if(listRoutes.isEmpty()){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.toString())
      mi.outData.put('RESP', "Error")
      mi.outData.put('CODE', "3200")
      mi.outData.put('TYPE', "2")
      mi.outData.put('TEXT', "No routes found")
      mi.outData.put('CONM', divisionName)
      mi.write()
      return
    }
    
    retrieveRouteInfoFromDROUTE()//Retrieve routes info for available routes
    
    retrieveRouteInfoFromDROUDI()//Retrieve other routes info for available routes
    
    searchMITBALInfo()//Retrieve available stock in warehouses

    listRoutesAllDetails.each { record1 ->
      getTIME()//gets current time

      String nextDeliveryDateDate=calculateNextDeliveryDate(record1.DODW, record1.LILH,record1.LILM,record1.ARDY)
      record1.CODZ=nextDeliveryDateDate
      record1.COHZ=record1.ARHH.padLeft(2,'0')+record1.ARMM.padLeft(2,'0')
      record1.IDRO=record1.ROUT+"-"+record1.RODN
      record1.ORTP=ortp610
      record1.CFI1=inCFI1.toString()
      record1.ORQA=inORQA.toString()
      record1.OALT=inOALT.toString()
      
      if(outputPadlFlag){
        Map<String, String> matched = whloDetailsMMS059.find { detail -> 
          detail.WHLO == record1.WHLO 
        }
        
        // If a match is found, output PADL and BCKO
        if (matched) {
          record1.PADL=matched.PADL
          record1.BCKO=matched.BCKO
          record1.SPLA=matched.SPLA
        }
      }
      
      //Set AVTX with correct status text
      if(!correctITNOFound){
        record1.AVTX="Reference inconnue"
      }
      else if(Double.parseDouble(record1.AV01)==0.0){
        record1.AVTX="Indisponible"
      }
      else if(Double.parseDouble(record1.AV01) >= inORQA ){
        record1.AVTX="Disponible"
      }
      else if(Double.parseDouble(record1.AV01) < inORQA){
        record1.AVTX="Partiellement disponible"
      }

      //Set AVST with correct status
      if(!correctITNOFound){
        record1.AVST="I"
      }
      else if(Double.parseDouble(record1.AV01)==0.0){
        record1.AVST="I"
      }
      else if(Double.parseDouble(record1.AV01) >= inORQA ){
        record1.AVST="D"
      }
      else if(Double.parseDouble(record1.AV01) < inORQA){
        record1.AVST="P"
      }

      record1.SPLM=splm610
      

      retievedTOMU=retrieveTOMU(inCONO.toString(),record1.WHLO,record1.ITNO)//retrieve TOMU from MITBAL
      
      record1.TOMU=retievedTOMU

    }

    listRoutesAllDetails.each { record1 ->
      if(record1.OBV2=="P01" || record1.OBV2=="P02"){
        record1.PRIO="0"
      }
      else if (record1.OBV2=="Prio9"){
        record1.PRIO="2"
      }
      else{
        record1.PRIO="1"
      }
    }
    
    if (sortFlag) {
    
      // Sort by nearest date, time, and then SPLA, ensuring that OBV2="Prio9" records are last
      listRoutesAllDetails = listRoutesAllDetails.sort { a, b ->
        int aPriority = a.PRIO.toInteger()
        int bPriority = b.PRIO.toInteger()

        aPriority <=> bPriority ?: a.CODZ <=> b.CODZ ?: a.COHZ <=> b.COHZ ?: a.SPLA <=> b.SPLA
      }
  
      Map<String, Integer> whloCounters = [:]
  
      listRoutesAllDetails.eachWithIndex { item, index ->
        // If WHLO not in map, initialize counter
        if (!whloCounters.containsKey(item.WHLO)) {
            whloCounters[item.WHLO] = 1
        }

        item.IDEX = whloCounters[item.WHLO].toString() // set IDEX

        // Increment the counter for the current WHLO
        whloCounters[item.WHLO]++
      }
  
      Map<String, Integer> whloToIdwhMap = [:]
      int currentId = 1
  
      // Loop through the list and assign IDWH values based on WHLO
      listRoutesAllDetails.each { item ->
        if (!whloToIdwhMap.containsKey(item.WHLO)) {
            whloToIdwhMap[item.WHLO] = currentId++
        }

        item.IDWH = whloToIdwhMap[item.WHLO] // Assign the same IDWH to items with the same WHLO
      }
    }

    getCodeMarquePartenaire(retrievedCFI1)//Gets code marrque partenaire

    if(correctITNOFound){
    
      retrieveFastestRoute()//retrieves fastest route with avaiable quantity
      
      int count=0
      boolean allConqZero = listFinalRoutes.every { record -> record.CONQ == "0.0" }
      
      String totalNet=""
      double NEPR

      ext320MIGetLine(inCONO.toString(),inCUNO, correctITNO,PBQA.toString())//getPrices
      
      //added on 16/06/25 due to error
      if(listFinalRoutes.isEmpty()){
        mi.outData.put('CONO', inCONO.toString())
        mi.outData.put('DIVI', inDIVI)
        mi.outData.put('CUNO', inCUNO)
        mi.outData.put('LNUM', inLNUM.toString())
        mi.outData.put('POPN', inPOPN)
        mi.outData.put('CFI1', inCFI1)
        mi.outData.put('CEAN', inCEAN)
        mi.outData.put('ORQA', inORQA.toString())
        mi.outData.put('RESP', "Error")
        mi.outData.put('CODE', "3200")
        mi.outData.put('TYPE', "2")
        mi.outData.put('TEXT', "No routes found")
        mi.outData.put('CONM', divisionName)
        mi.write()
        return
      }

      //check if no warehouse has stock
      if(allConqZero){
        if(listFinalRoutes.isEmpty()){
          mi.outData.put('CONO', inCONO.toString())
          mi.outData.put('DIVI', inDIVI)
          mi.outData.put('CUNO', inCUNO)
          mi.outData.put('LNUM', inLNUM.toString())
          mi.outData.put('POPN', inPOPN)
          mi.outData.put('CFI1', inCFI1)
          mi.outData.put('CEAN', inCEAN)
          mi.outData.put('ORQA', inORQA.toString())
          mi.outData.put('RESP', "Error")
          mi.outData.put('CODE', "3200")
          mi.outData.put('TYPE', "2")
          mi.outData.put('TEXT', "No routes found")
          mi.outData.put('CONM', divisionName)
          mi.write()
          return
        }
        else{
          Map<String, String>  record1 = listFinalRoutes.first()
          mi.outData.put('CONO', inCONO.toString())
          mi.outData.put('DIVI', inDIVI)
          mi.outData.put('CONM', divisionName)
          mi.outData.put('LNUM', inLNUM.toString())
          mi.outData.put('CUNO',record1.CUNO)
          mi.outData.put('WHLO',record1.WHLO)
          mi.outData.put('ITNO',record1.ITNO)
          mi.outData.put('POPN',inPOPN)
          mi.outData.put('CEAN',inCEAN)
          mi.outData.put('AV01',record1.AV01)
          mi.outData.put('CODZ',record1.CODZ)
          mi.outData.put('COHZ',record1.COHZ)
          mi.outData.put('FUDS', retrievedFUDS)
          mi.outData.put('ITDS', retrievedITDS)
          mi.outData.put('CFI1', codeMarquePartenaire)
          mi.outData.put('ORQA', record1.ORQA)
          mi.outData.put('CONQ', record1.CONQ)
          mi.outData.put('PBQA',PBQA.toString())

          if(listPrices.size()>0){

            if(record1.CONQ.toString()!="" && (  listPrices.size()>0)){
              NEPR=listPrices[0].NETP.toString().toDouble()* record1.CONQ.toString().toDouble()
              totalNet = String.format("%.2f", NEPR)
            }

            mi.outData.put('SAPR',listPrices[0].SAPR.toString())
            mi.outData.put('SACD',listPrices[0].SACD.toString())
            mi.outData.put('LNAM',totalNet.toString())
            mi.outData.put('NEPR',listPrices[0].NETP.toString())
            mi.outData.put('CRAM',retrievedCRAM)
          }
          
          // set status
          if(record1.AVST=="I"){
            mi.outData.put('LNST', "I")
            mi.outData.put('LNTX', "Rejected")
          }
          else if(record1.AVST=="D"){
            mi.outData.put('LNST', "D")
          }
          else if(record1.AVST=="P"){
            mi.outData.put('LNST', "P")
          }
          
          // set status text
          if(PBQA == record1.ORQA.toFloat()  ){
            mi.outData.put('LNTX', "ConfirmedWithoutChanges")
          }
          else {
            mi.outData.put('LNTX', "ConfirmedWithChanges ")
          }     

          mi.write()
        }
        
      }
      else{
        listFinalRoutes.each { record1 ->
        
          //exclude warehouse with no stock
          if(record1.CONQ!="0.0"){
            count++
            mi.outData.put('CONO', inCONO.toString())
            mi.outData.put('DIVI', inDIVI)
            mi.outData.put('LNUM', inLNUM.toString())
            mi.outData.put('CONM', divisionName)
            mi.outData.put('CUNO',record1.CUNO)
            mi.outData.put('WHLO',record1.WHLO)
            mi.outData.put('ITNO',record1.ITNO)
            mi.outData.put('POPN',inPOPN)
            mi.outData.put('CEAN',inCEAN)
            mi.outData.put('AV01',record1.AV01)
            mi.outData.put('CODZ',record1.CODZ)
            mi.outData.put('COHZ',record1.COHZ)
            mi.outData.put('FUDS', retrievedFUDS)
            mi.outData.put('ITDS', retrievedITDS)
            mi.outData.put('CFI1', codeMarquePartenaire)
            mi.outData.put('ORQA', record1.ORQA)
            mi.outData.put('CONQ', record1.CONQ)
            mi.outData.put('PBQA',PBQA.toString())

            if(listPrices.size()>0){

              if(record1.CONQ.toString()!="" && (  listPrices.size()>0)){
                NEPR=listPrices[0].NETP.toString().toDouble()* record1.CONQ.toString().toDouble()
                totalNet = String.format("%.2f", NEPR)
              }
              mi.outData.put('SAPR',listPrices[0].SAPR.toString())
              mi.outData.put('SACD',listPrices[0].SACD.toString())
              mi.outData.put('LNAM',totalNet.toString())
              mi.outData.put('NEPR',listPrices[0].NETP.toString())
              mi.outData.put('CRAM',retrievedCRAM)
            }

            // set status
            if(record1.AVST=="I"){
              mi.outData.put('LNST', "I")
              mi.outData.put('LNTX', "Rejected")
            }
            else if(record1.AVST=="D"){
              mi.outData.put('LNST', "D")
            }
            else if(record1.AVST=="P"){
              mi.outData.put('LNST', "P")
            }
            
            // set status text
            if(PBQA == record1.ORQA.toFloat()  ){
              mi.outData.put('LNTX', "ConfirmedWithoutChanges")
            }
            else {
              mi.outData.put('LNTX', "ConfirmedWithChanges ")
            }            
            mi.write()
          }
        }

      } 
    }
    else{
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.toString())
      mi.outData.put('RESP', "Error")
      mi.outData.put('CODE', "1007")
      mi.outData.put('TYPE', "0")
      mi.outData.put('TEXT', "Material master does not contain product: ${inPOPN}")
      mi.outData.put('LNTX', "Rejected")
      mi.outData.put('CONM', divisionName)
      mi.write()
      return
    }
  }
  
  /**
   * @validateCONO - Validates CONO/DIVI
   * @params -
   * @returns - true/false
   */
  Boolean validateCONO() {
    if (!inCONO.toString().isBlank() && !inDIVI.isBlank()) {
      DBAction query = database.table("CMNDIV").index("00").selection("CCCONM").build()
      DBContainer container = query.getContainer()
      container.set("CCCONO", inCONO)
      container.set("CCDIVI", inDIVI)
      if(!query.read(container)){
        return false
      }
      else{
        divisionName=container.get("CCCONM").toString().trim()
      }
    }
    return true
  }
  
  /**
  * @formatPOPN - Santizes POPN
  * @params -
  * @returns 
  */ 
  void formatPOPN(){
    inPOPN = inPOPN.replaceAll("\\s", "").replaceAll("[-/._#]", "").toUpperCase()
  }
  
  
  /**
  * @searchITEM - Search Item By ITDS, then ALWT=4 AND THEN ALWT=2
  * @params -
  * @returns -
  */
  void searchITEM(){    
    if(!correctITNOFound){
      searchRefNormalise()//search by ALWT=4
    }
    
    if(!correctITNOFound){
      if(inCEAN==""){
        return
      }
    } 
    
    if(!correctITNOFound){
      searchRefComp()//search by ALWT=2
    }
  }
  
  
  /**
  * @searchItemByCFI1Partenaire - Search Item By CFI1 code marrque partenaire
  * @params -
  * @returns -
  */
  boolean searchItemByCFI1Partenaire(){
    if(inCFI1==""){
      return false
    }
    else{
      retrieveCodeMarqueM3(inCONO.toString(),inCFI1)
      
      if (listCodeMarqueCugex.isEmpty()) {
        codeMarqueM3Found=false
        return true
      }
      else{
        codeMarqueM3Found=true
        retrievedCorrectCFI1M3=listCodeMarqueCugex[0]
        return true
      }      
    }
  }
  
  
  /**
  * @searchRefNormalise - Search Item By RefNormalise/ALWT=4
  * @params -
  * @returns -
  */
  void searchRefNormalise(){
    listItemsMMS025=[]
    mms025ApiCall(inPOPN,"4")

    if (listItemsMMS025.isEmpty()) {
      correctITNOFound=false
    } 
    //IF ONLY ONE ITEM
    else if (listItemsMMS025.size() > 0 && listItemsMMS025.every { it == listItemsMMS025[0] }) {
      correctITNOFound=true
      correctITNO=listItemsMMS025[0]
    } 
    // IF DIFFERENT ITEMS
    else {

      if(!searchItemByCFI1Partenaire()){
        correctITNOFound=true
        correctITNO=listItemsMMS025[0]
        return
      }

      if(codeMarqueM3Found){
        //retrieveCFI1 for items
        ExpressionFactory expression = database.getExpressionFactory("MITMAS")
        expression = expression.in("MMITNO", listItemsMMS025 as String[])
    
        DBAction queryMITMAS = database.table("MITMAS").index("00").matching(expression).selection("MMCFI1").build()
        DBContainer containerMITMAS = queryMITMAS.getContainer()
        containerMITMAS.set("MMCONO", inCONO)
        
        queryMITMAS.readAll(containerMITMAS,1,1000, { DBContainer container ->
          if((container.get("MMCFI1").toString().trim()==retrievedCorrectCFI1M3) && (correctITNOFound==false)){
            correctITNO=container.get("MMITNO").toString().trim()
            correctITNOFound=true
          }
        })

        if(!correctITNOFound){
          correctITNO=listItemsMMS025[0]
          correctITNOFound=true
          return
        }
      }
      else{
        correctITNOFound=true
        correctITNO=listItemsMMS025[0]
      }
    }
  }
  

  /**
  * @mms025ApiCall - LstItems from MMS025 where ALWT=2
  * @params - popn,alwt
  * @returns -
  */
  void mms025ApiCall(String popn, String alwt) {
    Map<String, String> paramsMMS025 = ["POPN": "${popn}".toString(), "ALWT": "${alwt}".toString()]

    Closure<?> callbackMMS025 = { Map<String, String> responseMMS025 ->
        if (responseMMS025 != null) {
            if (responseMMS025.containsKey("error") && responseMMS025.error != null) {
              } else {
                  listItemsMMS025.add(responseMMS025.ITNO.toString().trim())
              }
        }
    }
    miCaller.setListMaxRecords(1000)
    miCaller.call("MMS025MI", "LstItem", paramsMMS025, callbackMMS025)
  }
  
  
  /**
  * @searchRefComp - Search Item By RefComp/ALWT=2
  * @params - popn,alwt
  * @returns -
  */
  void searchRefComp(){
    listItemsMMS025=[]
    mms025ApiCall(inCEAN,"2")

    if (listItemsMMS025.isEmpty()) {
      correctITNOFound=false
    } 
    //IF ONLY ONE ITEM
    else if (listItemsMMS025.size() > 0 && listItemsMMS025.every { it == listItemsMMS025[0] }) {
      correctITNOFound=true
      correctITNO=listItemsMMS025[0]
    } 
    // IF DIFFERENT ITEMS
    else {
      correctITNOFound=false
    }
  }
  
  
  
  /**
  * @retrieveCodeMarqueM3 - Search codeMarqueM3 by using codeMarquePartenaire
  * @params -
  * @returns -
  */
  void retrieveCodeMarqueM3(String CONO,String A230) {
    listCodeMarqueCugex=[]

    //retrieveCFI1 for items
    ExpressionFactory expression = database.getExpressionFactory("CUGEX1")
    expression = expression.eq("F1A230", A230)

    DBAction queryCUGEX1 = database.table("CUGEX1").index("00").matching(expression).selection("F1PK03").build()
    DBContainer containerCUGEX1 = queryCUGEX1.getContainer()
    containerCUGEX1.set("F1CONO", inCONO)
    containerCUGEX1.set("F1FILE", "CSYTAB")
    containerCUGEX1.set("F1PK01", "")
    containerCUGEX1.set("F1PK02", "CFI1")
      
    queryCUGEX1.readAll(containerCUGEX1,4,1, { DBContainer container ->
      listCodeMarqueCugex.add(container.get("F1PK03").toString().trim())
    })
  }
  
  /**
  * @getCodeMarquePartenaire - Search codeMarquePartenaire by using codeMarqueM3
  * @params - PK03
  * @returns -
  */
  void getCodeMarquePartenaire(String PK03) {
    Map<String, String> paramsCUSEXTMI = ["FILE": "CSYTAB".toString(), "PK02": "CFI1".toString(), "PK03": "${PK03}".toString()]
    Closure<?> callbackCUSEXTMI= { Map<String, String> responseCUSEXTMI ->

        if (responseCUSEXTMI != null) {
            if (responseCUSEXTMI.containsKey("error") && responseCUSEXTMI.error != null) {
              return
            } else {
              codeMarquePartenaire= responseCUSEXTMI.A230.toString().trim()                
            }
        }
    }
    miCaller.call("CUSEXTMI", "GetFieldValue", paramsCUSEXTMI, callbackCUSEXTMI)
  
    
  }
  


  /**
  * @validateCUNO - Validates CUNO
  * @params -
  * @returns - true/false
  */
  Boolean validateCUNO() {
    if (!inCUNO.isBlank()) {
      DBAction query = database.table("OCUSMA").index("00").selection("OKORTP","OKSPLM","OKWHLO","OKPADL","OKBCKO","OKSTAT","OKACHK").build()
      DBContainer container = query.getContainer()
      container.set("OKCONO", inCONO)
      container.set("OKCUNO", inCUNO)
      if (!query.read(container) ){
        return false
      }
      else{
        customerInfoRetrievedFlag=true
        ortp610 = container.get("OKORTP").toString().trim()
        splm610 =  container.get("OKSPLM").toString().trim()
        whlo610 =  container.get("OKWHLO").toString().trim()
        padl610 =  container.get("OKPADL").toString().trim()
        bcko610 =  container.get("OKBCKO").toString().trim()
        stat610 =  container.get("OKSTAT").toString().trim()
        achk610 =  container.get("OKACHK").toString().trim()
      }
    }
    return true
  }
  
  /**
  * @mms059ListApiCall - Retrieve available WHLOs From MMS059
  * @params -
  * @returns - 
  */
  void mms059ListApiCall(String SPLM,String priority){
    Map<String, String> paramsMMS059 = ["SPLM":"${SPLM}".toString()]
    Closure<?> callbackMMS059 = 
    {
      Map<String, String> responseMMS059 ->
      if(responseMMS059 != null){
        String obv1 = responseMMS059.OBV1?.trim() 
        String fwhl = responseMMS059.FWHL?.trim() 
        String prex = responseMMS059.PREX?.trim() 
        String padl = responseMMS059.PADL?.trim() 
        String bcko = responseMMS059.BCKO?.trim() 
        String spla = responseMMS059.SPLA?.trim() 
        if(priority=="5"){
          if (obv1 == "WAV" && prex=="5") {
            arrWHLO.add(fwhl.toString())
            whloDetailsMMS059.add([WHLO: fwhl.toString(),PADL: padl, BCKO: bcko,SPLA: spla])
          }
        }
      }
    }
    miCaller.setListMaxRecords(1000)
    miCaller.call("MMS059MI","List", paramsMMS059, callbackMMS059) 
  }
  
  
  
  /**
  * @searchAllSDES - Retrieve all SDES and other info for available WHLOs
  * @params -
  * @returns -
  */
  void searchAllSDES(){
    ExpressionFactory expression = database.getExpressionFactory("MITWHL")
    expression = expression.in("MWWHLO", arrWHLO as String[])

    DBAction queryMITWHL = database.table("MITWHL").index("00").matching(expression).selection("MWSDES","MWWHNM").build()
    DBContainer containerMITWHL = queryMITWHL.getContainer()
    containerMITWHL.set("MWCONO", inCONO)
    
    queryMITWHL.readAll(containerMITWHL,1,1000, { DBContainer container ->
      
      String dses=container.get("MWSDES").toString().trim()
      String whlo=container.get("MWWHLO").toString().trim()
      String whnm=container.get("MWWHNM").toString().trim()
      
      listWarehousePlaceOfLoadAndDesc.add([CONO: inCONO.toString(),SDES: dses, WHLO: whlo, WHNM: whnm, CUNO: inCUNO])
    })
  }
  
  /**
  * @searchAllValidRoutesForCustomerPO1PO2 - Retrieve all routes and other info for available WHLOs
  * @params -
  * @returns -
  */
  void searchAllValidRoutesForCustomerPO1PO2(){
    List<String> arrUniqueSDES = listWarehousePlaceOfLoadAndDesc.collect { it.SDES }
    List<String> arrMODL=["P01","P02"]

    ExpressionFactory expression = database.getExpressionFactory("DRODPR")
    expression = expression.in("DOEDES", arrUniqueSDES as String[] )
    .and(expression.eq("DOOBV1", inCUNO))

    expression=expression.and(expression.in("DOOBV2", arrMODL as String[] ))
    
    DBAction queryDRODPR = database.table("DRODPR").index("00").matching(expression).selection("DOROUT","DOEDES","DOOBV1").build()
    DBContainer containerDRODPR = queryDRODPR.getContainer()
    containerDRODPR.set("DOCONO", inCONO)
    
    queryDRODPR.readAll(containerDRODPR,1,1000, { DBContainer container ->

      if(container.get("DOPREX").toString().trim() =="5"){
        String rout=container.get("DOROUT").toString().trim()
        String edes=container.get("DOEDES").toString().trim()
        String obv2=container.get("DOOBV2").toString().trim()      
        listRoutes.add([ROUT: rout.toString(),EDES: edes.toString(),OBV2: obv2.toString()])
      }
      
    })

    List<String> missingSDES = []
    List<String> presentSDES = []

    arrMODL.each { modlValue ->
      // Filter listRoutes for routes where OBV2 matches modlValue
      listRoutes.each { route ->
          if (route.OBV2 == modlValue) {
              // Add EDES to presentSDES if OBV2 matches modlValue
              if (!presentSDES.contains(route.EDES)) {
                  presentSDES.add(route.EDES)
              }
          }
      }
    }

    listWarehousePlaceOfLoadAndDesc.each { warehouseEntry ->
      // Check if the SDES in the warehouse list is not in presentSDES
      if (!presentSDES.contains(warehouseEntry.SDES)) {
          // Add to missingSDES if SDES is not found in presentSDES
          missingSDES.add(warehouseEntry.SDES)
      }
    }

    if (!missingSDES.isEmpty()) {
      //find prio 9 for the routes
      ExpressionFactory expression2 = database.getExpressionFactory("DRODPR")
      expression2 = expression2.in("DOEDES", missingSDES as String[] )
      .and(expression2.eq("DOOBV1", "J+1"))

      DBAction queryDRODPR2 = database.table("DRODPR").index("00").matching(expression2).selection("DOROUT","DOEDES").build()
      DBContainer containerDRODPR2 = queryDRODPR2.getContainer()
      containerDRODPR2.set("DOCONO", inCONO)

      queryDRODPR2.readAll(containerDRODPR2,1,1000, { DBContainer container2 ->
        String rout=container2.get("DOROUT").toString().trim()
        String edes=container2.get("DOEDES").toString().trim()

        listRoutes.add([ROUT: rout.toString(),EDES: edes.toString(),OBV2: "Prio9"])
      })
    }

    List<Map<String, String>> listRoutes2=listRoutes
    List<Map<String, String>> listOriginalWarehousePlaceOfLoadAndDesc=listWarehousePlaceOfLoadAndDesc

    listRoutes2.each { item1 ->
      listWarehousePlaceOfLoadAndDesc.each { item2 ->
          if (item2.SDES == item1.EDES) {
              item1.CONO = item2.CONO
              item1.SDES = item2.SDES
              item1.WHLO = item2.WHLO
              item1.WHNM = item2.WHNM
              item1.CUNO = item2.CUNO
          }
      }
    }
    
    listWarehousePlaceOfLoadAndDesc=listRoutes2
    
  }


  
  
  /**
  * @retrieveRouteInfoFromDROUTE - Retrieve routes info for available routes
  * @params -
  * @returns -
  */
  void retrieveRouteInfoFromDROUTE(){
    List<String> arrUniqueROUT = listRoutes.collect { it.ROUT }
    
    ExpressionFactory expression = database.getExpressionFactory("DROUTE")
    expression = expression.in("DRROUT", arrUniqueROUT as String[] )
    
    DBAction queryDROUTE = database.table("DROUTE").index("00").matching(expression).selection("DRSDES","DRTX40","DRSDES").build()
    DBContainer containerDROUTE= queryDROUTE.getContainer()
    containerDROUTE.set("DRCONO", inCONO)
    
    queryDROUTE.readAll(containerDROUTE,1,1000, { DBContainer container ->
      String rout=container.get("DRROUT").toString().trim()
      String sdes=container.get("DRSDES").toString().trim()
      String tx40=container.get("DRTX40").toString().trim()
      
    // Find the matching item in listWarehousePlaceOfLoadAndDesc by SDES
      listWarehousePlaceOfLoadAndDesc.each { warehouse ->
          if (warehouse.ROUT == rout) {
              warehouse.SDES = sdes // Add ROUT to the matching entry
              warehouse.TX40 = tx40 
          }
      }

      listRoutesDetails.add([CONO: inCONO.toString(),ROUT: rout,SDES: sdes,TX40: tx40])
    })
  }
  
  /**
  * @getPriority - Returns obv2 with highest priority
  * @params -obv2
  * @returns - numeric priority as an integer
  */
  int getPriority(String obv2) {
    String digits = obv2.replaceAll("[^\\d]", "")
    return digits ? Integer.parseInt(digits) : Integer.MAX_VALUE
  }
  
  /**
  * @retrieveRouteInfoFromDROUDI - Retrieve other routes info for available routes
  * @params -
  * @returns -
  */
  void retrieveRouteInfoFromDROUDI(){
    List<String> arrUniqueROUT = listRoutesDetails.collect { it.ROUT }
    
    ExpressionFactory expression = database.getExpressionFactory("DROUDI")
    expression = expression.in("DSROUT", arrUniqueROUT as String[] ).and(expression.ne("DSMMDL", ""))
    
    DBAction queryDROUDI = database.table("DROUDI").index("00").matching(expression).selection("DSRODN","DSDDOW","DSARHH","DSARMM","DSLILH","DSLILM","DSMMDL","DSARDY").build()//
    DBContainer containerDROUDI= queryDROUDI.getContainer()
    containerDROUDI.set("DSCONO", inCONO)
    
    queryDROUDI.readAll(containerDROUDI,1,1000, { DBContainer container ->
      String rout=container.get("DSROUT").toString().trim()
      String rodn=container.get("DSRODN").toString().trim()
      String dodw=container.get("DSDDOW").toString().trim()
      String arhh=container.get("DSARHH").toString().trim()
      String armm=container.get("DSARMM").toString().trim()
      String lilh=container.get("DSLILH").toString().trim()
      String lilm=container.get("DSLILM").toString().trim()
      String mmdl=container.get("DSMMDL").toString().trim()
      String ardy=container.get("DSARDY").toString().trim()

      listRoutesAllDetails.add([CONO: inCONO.toString(),ROUT: rout,RODN: rodn,DODW: dodw,ARHH: arhh,ARMM: armm,LILH: lilh,LILM: lilm,MMDL:mmdl,ARDY:ardy])
    })
    
    
    //reomving similar routes keeping the one with highest priority OBV2
    List<Map<String, String>> filteredList = listWarehousePlaceOfLoadAndDesc
    .groupBy { it.get("ROUT") }
    .collect { route, entries ->
        entries.min { getPriority(it.get("OBV2")) }
    }
    
    listWarehousePlaceOfLoadAndDesc=filteredList
    

    
    listRoutesAllDetails.each { record1 ->
      listWarehousePlaceOfLoadAndDesc.each { record2 ->
        if (record1.ROUT == record2.ROUT) {
          record1.TX40 = record2.TX40
          record1.CUNO = record2.CUNO
          record1.WHNM = record2.WHNM
          record1.WHLO = record2.WHLO
          record1.SDES = record2.SDES
          record1.OBV2 = record2.OBV2
          record1.ITNO = correctITNO
        }
      }
    }
  }
  
  
  /**
  * @searchMITBALInfo - Retrieve available stock in warehouses
  * @params -
  * @returns -
  */
  void searchMITBALInfo(){
    ExpressionFactory expression = database.getExpressionFactory("MITBAL")
    expression = expression.in("MBWHLO", arrWHLO as String[]).and(expression.eq("MBITNO", correctITNO))

    DBAction queryMIBAL = database.table("MITBAL").index("00").matching(expression).selection("MBAVAL","MBALQT").build()
    DBContainer containerMITBAL = queryMIBAL.getContainer()
    containerMITBAL.set("MBCONO", inCONO)
    
    listRoutesAllDetails.each { record1 ->
      record1.AV01="0.0" 
    }

    queryMIBAL.readAll(containerMITBAL,1,1000, { DBContainer container ->

      String aval=container.get("MBAVAL").toString().trim()
      String alqt=container.get("MBALQT").toString().trim()
      
      Double avalNum = Double.parseDouble(aval)
      Double alqtNum = Double.parseDouble(alqt)

      Double AV01 = avalNum - alqtNum

      if(AV01<0){
        AV01=0
      }
      
      listRoutesAllDetails.each { record1 ->
          if(record1.WHLO==container.get("MBWHLO").toString().trim() && record1.ITNO==container.get("MBITNO").toString().trim() ){
            record1.AV01=AV01.toString()
          }
      }
    })
  }
  
  
  /**
  * @calculateNextDeliveryDate - Calculate next delivery date
  * @params -DODW,LILH,LILM,ARDY
  * @returns -
  */
  String calculateNextDeliveryDate(String days, String hour, String minute,String delay){
    if(days != "0000000")
    {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
      LocalDate baseDate = LocalDate.parse(timezoneDATE, formatter)
      int dayOfWeek = baseDate.getDayOfWeek().getValue()

      if(hour.length() == 1)
        hour = "0" + hour
      if(minute.length() == 1)
        minute = "0" + minute

      String releaseTime = hour + minute
      String formattedDays = days.substring(dayOfWeek-1, 7) + days.substring(0, dayOfWeek)
      char[] day = formattedDays.toCharArray()
      int count = 0
        for (char d : day) 
        {
          if(d == "1")
          {
            if (sTIME <= releaseTime)
            {
              LocalDate futureDate = baseDate.plusDays(count + delay.toInteger())
              String sDATE = futureDate.format(formatter)
              return sDATE
            }
            else if(count >  0)
            {
              LocalDate futureDate = baseDate.plusDays(count + delay.toInteger())
              String sDATE = futureDate.format(formatter)
              return sDATE
            }
          }
            count ++
        }
    }
    else
    {
      sDATE = "99999999"
    }

  }
  
  /**
  * @getTIME - Gets current time of system
  * @params -
  * @returns -
  */
  void getTIME() {
      Map<String, String> paramsDRS045 = ["TIZO": program.LDAZD.TIZO.toString()]
      Closure<?> callbackDRS045 = 
      {
        Map<String, String> responseDRS045 ->
        if(responseDRS045 != null){
          sTIME = responseDRS045.TIME.toString().substring(0,4)
          timezoneDATE = responseDRS045.DATE.toString().trim()
        }
      }
      miCaller.call("DRS045MI","GetTIZOData", paramsDRS045, callbackDRS045)
  }
  
  
   /**
  * @retrieveITDSAndFUDS - Gets ITDS/FUDS/CFI1 from MITMAS
  * @params -
  * @returns -
  */
  void retrieveITDSAndFUDS(){
    DBAction queryMITMAS = database.table("MITMAS").index("00").selection("MMFUDS","MMITDS","MMCFI1","MMACHK").build()
    DBContainer containerMITMAS = queryMITMAS.getContainer()
    containerMITMAS.set("MMCONO", inCONO)
    containerMITMAS.set("MMITNO", correctITNO)
    
    if(!queryMITMAS.read(containerMITMAS)){
    } 
    else {
      retrievedFUDS=containerMITMAS.get("MMFUDS").toString().trim()
      retrievedITDS=containerMITMAS.get("MMITDS").toString().trim()
      retrievedCFI1=containerMITMAS.get("MMCFI1").toString().trim()
      assortmentCheck=containerMITMAS.get("MMACHK").toString().trim()
    }
  }
  
  /**
  * @retrieveTOMU - Gets TOMU for current item/whlo from MITBAL
  * @params -
  * @returns -
  */
  String retrieveTOMU(String CONO,String WHLO,String ITNO){
    DBAction queryMITBAL = database.table("MITBAL").index("00").selection("MBTOMU").build()
    DBContainer containerMITBAL = queryMITBAL.getContainer()

    containerMITBAL.set("MBCONO", CONO as Integer)
    containerMITBAL.set("MBWHLO", WHLO)
    containerMITBAL.set("MBITNO", ITNO)
    
    if(!queryMITBAL.read(containerMITBAL)){
      return "1.0"
    } 
    else {
      if(containerMITBAL.get("MBTOMU").toString().trim()=="0.0"){
        return "1.0"
      }
      return containerMITBAL.get("MBTOMU").toString().trim()
    }
  }
  

  /**
  * @recalculateAV01 - sets AV01 to 0 for all records in listRoutesAllDetails
  * @params -
  * @returns -
  */
  void recalculateAV01() {
      def warehouseUsed = [:]

      listRoutesAllDetails.each { entry ->
          String warehouse = entry['WHLO']
          
          if (warehouseUsed.containsKey(warehouse)) {
              entry['AV01'] = '0.0'
          } else {
              warehouseUsed[warehouse] = true  // Mark warehouse as used
          }
      }
  }

  /**
  * @retrieveFastestRoute - retrieves fastest route with avaiable quantity
  * @params -
  * @returns -
  */
  void retrieveFastestRoute() {
    recalculateAV01() // resets all AV01 to 0
    boolean allAV01Zero = listRoutesAllDetails.every { it.AV01 == "0.0" || it.AV01 == "0"}
    if (allAV01Zero) {
        listFinalRoutes = listRoutesAllDetails

        listRoutesAllDetails.each { item ->
          item.CONQ = "0.0"
        }
        return
    }

    boolean allORQAZero = listRoutesAllDetails.every { it.ORQA == "0.0" || it.ORQA == "0"}
    if (allORQAZero) {
        listFinalRoutes = listRoutesAllDetails
        listRoutesAllDetails.each { item ->
          item.CONQ = "0.0"
        }
        return
    }

    double requiredQuantity = listRoutesAllDetails[0].ORQA as double  
    double remainingQuantity = requiredQuantity
    Map<String, Double> warehouseConsumed = [:]  

    double currentTOMU = 1.0
    double availableStock = 0.0

    listRoutesAllDetails.each { entry -> 
        String warehouse = entry['WHLO']
        
        availableStock = (
            entry['AV01'] != null &&
            entry['AV01'].toString().trim().isNumber()
                ? entry['AV01']
                : 0
        ).toString().toDouble()
        
        currentTOMU = (
            entry['TOMU'] != null &&
            entry['TOMU'].toString().trim().isNumber()
                ? entry['TOMU']
                : 0
        ).toString().toDouble()
        
        // Skip processing if no remaining quantity
        if (remainingQuantity <= 0) return

        // Calculate the maximum items to allocate and Allocate items only in multiples of TOMU
        double maxItemsToAllocate = ((availableStock / currentTOMU) as Integer) * currentTOMU

        // // Ensure that we don't allocate more than the remaining quantity
        double itemsToTake=0
        if(remainingQuantity>maxItemsToAllocate){
          itemsToTake = maxItemsToAllocate
        }
        else{
          itemsToTake = findMultiple(remainingQuantity,maxItemsToAllocate,currentTOMU)
        }

        // If we can't allocate any items due to TOMU and available stock, skip this warehouse
        if (itemsToTake <= 0) return
        
        // Allocate items
        remainingQuantity -= itemsToTake

        // Update warehouse consumption
        warehouseConsumed[warehouse] = (warehouseConsumed.get(warehouse, 0.0 as Double) + itemsToTake)

        // Store the route with allocated items
        listFinalRoutes << [
            WHLO: warehouse,
            ROUT: entry['ROUT'],
            RODN: entry['RODN'],
            AV01: entry['AV01'],
            CONQ: itemsToTake.toString(),
            CODZ: entry['CODZ'],
            COHZ: entry['COHZ'],
            DODW: entry['DODW'],
            ARHH: entry['ARHH'],
            ARMM: entry['ARMM'],
            LILH: entry['LILH'],
            LILM: entry['LILM'],
            MMDL: entry['MMDL'],
            TX40: entry['TX40'],
            CUNO: entry['CUNO'],
            WHNM: entry['WHNM'],
            SDES: entry['SDES'],
            ITNO: entry['ITNO'],
            IDRO: entry['IDRO'],
            ORTP: entry['ORTP'],
            CFI1: entry['CFI1'],
            AVTX: entry['AVTX'],
            AVST: entry['AVST'],
            SPLM: entry['SPLM'],
            IDEX: entry['IDEX'],
            FUDS: entry['FUDS'],
            ITDS: entry['ITDS'],
            ORQA: entry['ORQA'],
            OALT: entry['OALT'],
            TOMU: entry['TOMU']
        ]

        // Skip if warehouse stock is already consumed
        if (warehouseConsumed[warehouse] >= availableStock) {
            return 
        }
    }

    if (listFinalRoutes.isEmpty()) {
        listFinalRoutes = listRoutesAllDetails
        listRoutesAllDetails.each { item -> item.CONQ = "0.0" }
        listRoutesAllDetails.each { item -> item.AVST = "I" }

        return
    }

    PBQA = (listFinalRoutes.collect { it.CONQ.toDouble() }.sum() as Double).doubleValue()  
  
  }

  /**
  * @findMultiple - finds multiple of TOMU to allocate
  * @params -remainingQuantity,maxItemsToAllocate,TOMU
  * @returns -multiple
  */
  int findMultiple(double remainingQuantity,double maxItemsToAllocate, double currentTOMU) {

      double multiple = (((remainingQuantity + currentTOMU - 1) / currentTOMU) as Integer) * currentTOMU

      // Ensure the multiple is <= B and >= A
      if (multiple > maxItemsToAllocate) {
          return maxItemsToAllocate as int
      } else {
          return multiple as int
      }
  }

    /**
  * @ext320MIGetLine - Retrieves prix from EXT320MI-GetLine
  * @params - CONO,  CUNO,  ITNO
  * @returns -
  */
  void ext320MIGetLine(String CONO,String CUNO, String ITNO,String ORQA) {
    Map<String, String> paramsExt320MIGetLine = ["CONO": "${CONO}".toString(),"CUNO": "${CUNO}".toString(), "ITNO": "${ITNO}".toString(), "ORQA": "${ORQA}".toString(), "ORTP": "WAV"]
    Closure<?> callbackEXT320MIGetLine= { Map<String, String> responseEXT320MIGetLine ->

        if (responseEXT320MIGetLine != null) {
            if (responseEXT320MIGetLine.containsKey("error") && responseEXT320MIGetLine.error != null) {
              return
            } else {
                listPrices.add([
                  NETP:responseEXT320MIGetLine.NETP.toString().trim(),
                  SAPR:responseEXT320MIGetLine.SAPR.toString().trim(),
                  SACD:responseEXT320MIGetLine.SACD.toString().trim()
                ])
            }
        }
    }
    miCaller.call("OIS320MI", "GetPriceLine", paramsExt320MIGetLine, callbackEXT320MIGetLine)
  }
  
  /**
  * @retrieveConsigne - Retrieves CRAM 
  * @params -
  * @returns -
  */
  void retrieveConsigne(){
    DBAction query = database.table("OLICHR").index("00").selection("MICRAM").build()
    DBContainer container = query.getContainer()
    if(inCONO){
      container.set("MICONO", inCONO)
    }

    container.set("MICRID", "CONSIG")

    container.set("MIOBV1", correctITNO)
    
    //display all records
    String maxdate = "00000000"

    query.readAll(container,3,1000, { DBContainer container1 ->
      String date=container1.get("MIVFDT").toString().trim()
      if(date<= currentDate){
        if (date > maxdate) {
          maxdate = date  
          retrievedCRAM=container1.get("MICRAM").toString()
        } 
      }
      
    })
  }

  
  /**
  * @retrieveAssortimentOIS071 - Gets ssortiment from OIS071
  * @params -
  * @returns -
  */
  void retrieveAssortimentOIS071(){
    ExpressionFactory expression = database.getExpressionFactory("OASCUS")
    expression = expression.in("OCCUNO", listClient as String[])

    DBAction queryOASCUS = database.table("OASCUS").index("10").matching(expression).selection("OCTDAT").build()
    DBContainer containerOASCUS = queryOASCUS.getContainer()
    containerOASCUS.set("OCCONO", inCONO)

    queryOASCUS.readAll(containerOASCUS,1,1000, { DBContainer container ->
      String startDate=container.get("OCFDAT").toString().trim()
      String endDate=container.get("OCTDAT").toString().trim()

      if((currentDate >= startDate)  && (currentDate <= endDate)){
        listAssortment.add(container.get("OCASCD").toString().trim())

      }
      else{
      }
    })
  }


   /**
  * @checkChaineCommercialeOIS039 - Gets superior chain clients from OIS039
  * @params -
  * @returns -
  */
  void checkChaineCommercialeOIS039(){
    DBAction queryOCHCUS = database.table("OCHCUS").index("10").selection().build()
    DBContainer containerOCHCUS = queryOCHCUS.getContainer()
    containerOCHCUS.set("OSCONO", inCONO)
    containerOCHCUS.set("OSCUNO", inCUNO)
    
    queryOCHCUS.readAll(containerOCHCUS,2,1000, { DBContainer container ->
      String superiorChain=container.get("OSCHCT").toString().trim()
      listClient.add(superiorChain)
      
    })
  }


   /**
  * @checkAssortimentArticle - Checks assortiment article
  * @params -
  * @returns -true/false
  */
    Boolean checkAssortimentArticle(){
    ExpressionFactory expression = database.getExpressionFactory("OASITN")
    expression = expression.in("OIASCD", listAssortment as String[])

    DBAction queryOASITN = database.table("OASITN").index("20").matching(expression).selection("OIITNO","OITDAT").build()
    DBContainer containerOASITN = queryOASITN.getContainer()
    containerOASITN.set("OICONO", inCONO)
    containerOASITN.set("OIITNO", correctITNO)
    
    boolean assortimentArticleFound=false
    queryOASITN.readAll(containerOASITN,2,1000, { DBContainer container ->
      String startDate=container.get("OIFDAT").toString().trim()
      String endDate=container.get("OITDAT").toString().trim()

      if((currentDate >= startDate)  && (currentDate <= endDate)){
        assortimentArticleFound=true
      }
      
    })
    if(assortimentArticleFound){
      return true
    }
    return false
  }
  
    /**
   * @validateITNO - Validates ITNO
   * @params -
   * @returns - true/false
   */
  Boolean validateITNO() {
    if (!inCONO.toString().isBlank() && correctITNO!="") {
      DBAction query = database.table("MITMAS").index("00").selection("MMSTAT").build()
      DBContainer container = query.getContainer()
      container.set("MMCONO", inCONO)
      container.set("MMITNO", correctITNO)
      if(!query.read(container)){
        return false
      }
      else{
        int stat=container.get("MMSTAT").toString().trim() as Integer
        if(stat<20 || stat>50){
          return false
        }
      }
    }
    return true
  }
}