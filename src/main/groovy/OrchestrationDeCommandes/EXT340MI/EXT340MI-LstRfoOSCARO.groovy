/**
*  @Name: EXT340MI.LstRfoOSCARO
*  @Description: Get item info, get route info, get availability of stock, sorts by fastest availability
*  @Authors: Kenylen Motean
*/

/**
* CHANGELOGS
* Version    Date    User        Description
* 1.0.0      241125  KMOTEAN     Initial Release
* 1.0.1      101225  KMOTEAN     Default values added in case of missing lines in MITBAL
*/

import java.time.format.DateTimeFormatter
import java.time.LocalDate

public class LstRfoOSCARO extends ExtendM3Transaction {
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
  private String correctITNO=""
  private Boolean correctITNOFound=false
  private List<String> listItemsMMS025=[]
  private List<String> listCodeMarqueCugex=[]
  private String retrievedCorrectCFI1M3=""
  private Boolean codeMarqueM3Found=false
  private List<String> arrWHLO=[]
  private List<Map<String, String>> listWarehousePlaceOfLoadAndDesc= []
  private List<Map<String, String>> listRoutes= []
  private List<Map<String, String>> listRoutesDetails= []
  private List<Map<String, String>> listRoutesAllDetails= []
  private List<Map<String, String>>  whloDetailsMMS059= []
  private boolean  outputPadlFlag= false
  private boolean  sortFlag= true
  private List<Map<String, String>> listFinalRoutes= []
  private double PBQA
  private String codeMarquePartenaire=""
  private String retievedTOMU=""
  private String sTIME
  private String sDATE
  private String divisionName
  private String retrievedCRAM
  private String retrievedFUDS=""
  private String retrievedITDS=""
  private String retrievedCFI1=""
  private String assortmentCheck=""
  private List<String> listAssortment=[]
  private List<String> listClient=[]
  private String currentDate
  
  public LstRfoOSCARO(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller) {
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
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('LNST', "error")
      mi.outData.put('LNTX', "invalidConoDivi")
      mi.write()
      return
    }

    if(!validateCUNO()){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('LNST', "R")
      mi.outData.put('LNTX', "Client ${inCUNO} interdit, contacter ADV")
      mi.outData.put('CONM', divisionName)
      mi.write()
      return
    }
    
    if(stat610!="20"){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('LNST', "R")
      mi.outData.put('LNTX', "Client ${inCUNO} interdit, contacter ADV")
      mi.outData.put('CONM', divisionName)
      mi.write()
      return
    }

    //error if ORQA=0
    if(inORQA==0){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('LNST', "error")
      mi.outData.put('LNTX', "error quantity")
      mi.outData.put('CONM', divisionName)
      mi.write()
      return
    }
    
    //error if ORQA has decimal values
    if(inORQA % 1 != 0){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('LNST', "error")
      mi.outData.put('LNTX', "error quantity")
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
        mi.outData.put('LNUM', inLNUM.toString())
        mi.outData.put('CUNO', inCUNO)
        mi.outData.put('POPN', inPOPN)
        mi.outData.put('CFI1', inCFI1)
        mi.outData.put('CEAN', inCEAN)
        mi.outData.put('ORQA', inORQA.intValue().toString())
        mi.outData.put('LNST', "I")
        mi.outData.put('LNTX', "Reference inconnue")
        mi.outData.put('CONM', divisionName)
        mi.outData.put('TOMU', "0")
        mi.outData.put('PBQA', "0")
        mi.write()
        return
      }
      else{
        mi.outData.put('CONO', inCONO.toString())
        mi.outData.put('DIVI', inDIVI)
        mi.outData.put('LNUM', inLNUM.toString())
        mi.outData.put('CUNO', inCUNO)
        mi.outData.put('POPN', inPOPN)
        mi.outData.put('CFI1', inCFI1)
        mi.outData.put('CEAN', inCEAN)
        mi.outData.put('ORQA', inORQA.intValue().toString())
        mi.outData.put('LNST', "I")
        mi.outData.put('LNTX', "Reference inconnue")
        mi.outData.put('CONM', divisionName)
        mi.outData.put('TOMU', "0")
        mi.outData.put('PBQA', "0")
        mi.write()
        return
      }
    }
    else{
      if(!validateITNO()){
        mi.outData.put('CONO', inCONO.toString())
        mi.outData.put('DIVI', inDIVI)
        mi.outData.put('LNUM', inLNUM.toString())
        mi.outData.put('CUNO', inCUNO)
        mi.outData.put('POPN', inPOPN)
        mi.outData.put('CFI1', inCFI1)
        mi.outData.put('CEAN', inCEAN)
        mi.outData.put('ORQA', inORQA.intValue().toString())
        mi.outData.put('LNST', "I")
        mi.outData.put('LNTX', "Reference inconnue")
        mi.outData.put('CONM', divisionName)
        mi.outData.put('TOMU', "0")
        mi.outData.put('PBQA', "0")
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
            mi.outData.put('LNUM', inLNUM.toString())
            mi.outData.put('CUNO', inCUNO)
            mi.outData.put('POPN', inPOPN)
            mi.outData.put('CFI1', inCFI1)
            mi.outData.put('CEAN', inCEAN)
            mi.outData.put('ORQA', inORQA.intValue().toString())
            mi.outData.put('LNST', "I")
            mi.outData.put('LNTX', "Reference inconnue")
            mi.outData.put('CONM', divisionName)
            mi.outData.put('TOMU', "0")
            mi.outData.put('PBQA', "0")
            mi.write()
            return
          }
          
        }
        else{
          mi.outData.put('CONO', inCONO.toString())
          mi.outData.put('DIVI', inDIVI)
          mi.outData.put('LNUM', inLNUM.toString())
          mi.outData.put('CUNO', inCUNO)
          mi.outData.put('POPN', inPOPN)
          mi.outData.put('CFI1', inCFI1)
          mi.outData.put('CEAN', inCEAN)
          mi.outData.put('ORQA', inORQA.intValue().toString())
          mi.outData.put('LNST', "I")
          mi.outData.put('LNTX', "Reference inconnue")
          mi.outData.put('CONM', divisionName)
          mi.outData.put('TOMU', "0")
          mi.outData.put('PBQA', "0")
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
            mi.outData.put('LNUM', inLNUM.toString())
            mi.outData.put('CUNO', inCUNO)
            mi.outData.put('POPN', inPOPN)
            mi.outData.put('CFI1', inCFI1)
            mi.outData.put('CEAN', inCEAN)
            mi.outData.put('ORQA', inORQA.intValue().toString())
            mi.outData.put('LNST', "I")
            mi.outData.put('LNTX', "Reference inconnue")
            mi.outData.put('CONM', divisionName)
            mi.outData.put('TOMU', "0")
            mi.outData.put('PBQA', "0")
            mi.write()
            return
          }
        }
        else{
          mi.outData.put('CONO', inCONO.toString())
          mi.outData.put('DIVI', inDIVI)
          mi.outData.put('LNUM', inLNUM.toString())
          mi.outData.put('CUNO', inCUNO)
          mi.outData.put('POPN', inPOPN)
          mi.outData.put('CFI1', inCFI1)
          mi.outData.put('CEAN', inCEAN)
          mi.outData.put('ORQA', inORQA.intValue().toString())
          mi.outData.put('LNST', "I")
          mi.outData.put('LNTX', "Reference inconnue")
          mi.outData.put('CONM', divisionName)
          mi.outData.put('TOMU', "0")
          mi.outData.put('PBQA', "0")
          mi.write()
          return
        }
      }
    }

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
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('LNST', "R")
      mi.outData.put('LNTX', "Default warehouse for customer not defined")
      mi.outData.put('CONM', divisionName)
      mi.write()
      return
    }
    
    if(arrWHLO.isEmpty()){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('LNST', "R")
      mi.outData.put('LNTX', "Order type WAV for supply model not defined")
      mi.outData.put('CONM', divisionName)
      mi.write()
      return
    }

    arrWHLO.each { whlo ->
        listRoutesAllDetails << [
            "WHLO": whlo,       
            "ITNO": correctITNO    
        ]
    }
    
    searchMITBALInfo()

    getWarehousesNames()

    listRoutesAllDetails.each { record1 ->
      record1.ORQA=inORQA.toString()
      
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

      Map<String, String> warehouseMatch = listWarehousePlaceOfLoadAndDesc.find { w ->
          w.WHLO == record1.WHLO
      }

      if (warehouseMatch) {
          record1.WHNM = warehouseMatch.WHNM
      }

      record1.SPLM=splm610
      
      retievedTOMU=retrieveTOMU(inCONO.toString(),record1.WHLO,record1.ITNO)//retrieve TOMU from MITBAL
      record1.TOMU=retievedTOMU
    }

    
    if (sortFlag) {
      listRoutesAllDetails = listRoutesAllDetails.sort { it.SPLA.toInteger() }
    }

    getCodeMarquePartenaire(retrievedCFI1)//Gets code marrque partenaire
    
    if(correctITNOFound){
      retrieveFastestRoute()//retrieves fastest route with avaiable quantity
      
      int count=0
      boolean allConqZero = listFinalRoutes.every { record -> record.CONQ == "0.0" }
      
      //Set AVTX with correct status text
      String AVTX=""
      if(!correctITNOFound){
        AVTX="Reference inconnue"
      }
      else if((PBQA)==0.0){
        AVTX="Indisponible"
      }
      else if((PBQA) >= inORQA ){
        AVTX="Disponible"
      }
      else if((PBQA) < inORQA){
        AVTX="Partiellement disponible"
      }
      else{
        AVTX="Indisponible"
      }

      //Set AVST with correct status
      String AVST=""
      if(!correctITNOFound){
        AVST="I"
      }
      else if((PBQA)==0.0){
        AVST="I"
      }
      else if((PBQA) >= inORQA ){
        AVST="D"
      }
      else if((PBQA) < inORQA){
        AVST="P"
      }
      else{
        AVTX="Indisponible"
      }

      //check if no warehouse has stock
      if(allConqZero){
        if(listFinalRoutes.isEmpty()){
          mi.outData.put('CONO', inCONO.toString())
          mi.outData.put('DIVI', inDIVI)
          mi.outData.put('LNUM', inLNUM.toString())
          mi.outData.put('CUNO', inCUNO)
          mi.outData.put('POPN', inPOPN)
          mi.outData.put('CFI1', inCFI1)
          mi.outData.put('CEAN', inCEAN)
          mi.outData.put('ORQA', inORQA.intValue().toString())
          mi.outData.put('LNST', "I")
          mi.outData.put('LNTX', "No stock available")
          mi.outData.put('CONM', divisionName)
          mi.write()
          return
        }
        else if(AVTX=="Indisponible"){
          Map<String, String>  record1 = listFinalRoutes.first()
          mi.outData.put('CONO', inCONO.toString())
          mi.outData.put('DIVI', inDIVI)
          mi.outData.put('CONM', divisionName)
          mi.outData.put('LNUM', inLNUM.toString())
          mi.outData.put('CUNO',inCUNO)
          mi.outData.put('WHLO',"")
          mi.outData.put('WHNM',"")
          mi.outData.put('ITNO',correctITNO)
          mi.outData.put('POPN',inPOPN)
          mi.outData.put('CEAN',inCEAN)
          mi.outData.put('AV01',"0.0")
          mi.outData.put('CODZ',"")
          mi.outData.put('COHZ',"")
          mi.outData.put('LNTX', AVTX)
          mi.outData.put('LNST', AVST)
          mi.outData.put('FUDS', retrievedFUDS)
          mi.outData.put('ITDS', retrievedITDS)
          mi.outData.put('CFI1', codeMarquePartenaire)
          if(record1.ORQA==""){
            mi.outData.put('ORQA', "0")
          }
          else{
            mi.outData.put('ORQA', ((record1.ORQA as Double).intValue()).toString())
          }
          
          mi.outData.put('CONQ', "0.0")
          mi.outData.put('PBQA', ((PBQA).intValue()).toString())
          mi.outData.put('TOMU',record1.TOMU)        
          mi.write()
        }
        else{
          Map<String, String>  record1 = listFinalRoutes.first()
          mi.outData.put('CONO', inCONO.toString())
          mi.outData.put('DIVI', inDIVI)
          mi.outData.put('CONM', divisionName)
          mi.outData.put('LNUM', inLNUM.toString())
          mi.outData.put('CUNO',inCUNO)
          mi.outData.put('WHLO',record1.WHLO)
          mi.outData.put('WHNM',record1.WHNM)
          mi.outData.put('ITNO',record1.ITNO)
          mi.outData.put('POPN',inPOPN)
          mi.outData.put('CEAN',inCEAN)
          mi.outData.put('AV01',record1.AV01)
          mi.outData.put('LNTX', AVTX)
          mi.outData.put('LNST', AVST)
          mi.outData.put('FUDS', retrievedFUDS)
          mi.outData.put('ITDS', retrievedITDS)
          mi.outData.put('CFI1', codeMarquePartenaire)
          
          if(record1.ORQA==""){
            mi.outData.put('ORQA', "0")
          }
          else{
            mi.outData.put('ORQA', ((record1.ORQA as Double).intValue()).toString())
          }

          mi.outData.put('CONQ', record1.CONQ)
          mi.outData.put('PBQA', ((PBQA).intValue()).toString())
          mi.outData.put('TOMU',record1.TOMU)

          if(record1.WHLO==whlo610){
            mi.outData.put('DPAN',"false")
          }
          else{
            mi.outData.put('DPAN',"true")
          }

          mi.write()
        }
        
      }
      else if(AVTX=="Indisponible"){
        Map<String, String>  record1 = listFinalRoutes.first()
        mi.outData.put('CONO', inCONO.toString())
        mi.outData.put('DIVI', inDIVI)
        mi.outData.put('CONM', divisionName)
        mi.outData.put('LNUM', inLNUM.toString())
        mi.outData.put('CUNO',record1.CUNO)
        mi.outData.put('WHLO',"")
        mi.outData.put('WHNM',"")
        mi.outData.put('ITNO',record1.ITNO)
        mi.outData.put('POPN',inPOPN)
        mi.outData.put('CEAN',inCEAN)
        mi.outData.put('AV01',"0.0")
        mi.outData.put('LNTX', AVTX)
        mi.outData.put('LNST', AVST)
        mi.outData.put('FUDS', retrievedFUDS)
        mi.outData.put('ITDS', retrievedITDS)
        mi.outData.put('CFI1', codeMarquePartenaire)
        
        if(record1.ORQA==""){
          mi.outData.put('ORQA', "0")
        }
        else{
          mi.outData.put('ORQA', ((record1.ORQA as Double).intValue()).toString())
        }

        mi.outData.put('CONQ', "0.0")
        mi.outData.put('PBQA', ((PBQA).intValue()).toString())
        mi.outData.put('TOMU',record1.TOMU) 
      
        mi.write()
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
            mi.outData.put('CUNO',inCUNO)
            mi.outData.put('WHLO',record1.WHLO)
            mi.outData.put('WHNM',record1.WHNM)
            mi.outData.put('ITNO',correctITNO)
            mi.outData.put('POPN',inPOPN)
            mi.outData.put('CEAN',inCEAN)
            mi.outData.put('AV01',record1.AV01)
            mi.outData.put('LNTX', AVTX)
            mi.outData.put('LNST', AVST)
            mi.outData.put('FUDS', retrievedFUDS)
            mi.outData.put('ITDS', retrievedITDS)
            mi.outData.put('CFI1', codeMarquePartenaire)
            
            if(record1.ORQA==""){
              mi.outData.put('ORQA', "0")
            }
            else{
              mi.outData.put('ORQA', ((record1.ORQA as Double).intValue()).toString())
            }

            mi.outData.put('CONQ', record1.CONQ)
            mi.outData.put('PBQA', ((PBQA).intValue()).toString())
            mi.outData.put('TOMU',record1.TOMU)
            mi.write()
          }
        }

      } 
    }
    else{
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('LNTX', "Reference inconnue")
      mi.outData.put('LNST', "I")
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('AV01', "0.0")
      
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
  * @params - CONO,PK03
  * @returns -
  */
  void getCodeMarquePartenaire(String PK03) {
    Map<String, String> paramsCUSEXTMI = ["FILE": "CSYTAB".toString(), "PK02": "CFI1".toString(), "PK03": "${PK03}".toString()]
    Closure<?> callbackCUSEXTMI= { Map<String, String> responseCUSEXTMI ->

        if (responseCUSEXTMI != null) {
            if (responseCUSEXTMI.containsKey("error") && responseCUSEXTMI.error != null) {
              // mi.error("Aucune ligne dans la table de prix EXT345")
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
  * @searchMITBALInfo - Retrieve available stock in warehouses
  * @params -
  * @returns -
  */
  void searchMITBALInfo(){
    ExpressionFactory expression = database.getExpressionFactory("MITBAL")
    expression = expression.in("MBWHLO", arrWHLO as String[]).and(expression.eq("MBITNO", correctITNO))

    DBAction queryMIBAL = database.table("MITBAL").index("00").matching(expression).selection("MBAVAL","MBALQT","MBVTCS").build()
    DBContainer containerMITBAL = queryMIBAL.getContainer()
    containerMITBAL.set("MBCONO", inCONO)

    listRoutesAllDetails.each { it.AV01 = "0.0" }

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
      Map<String, Boolean> warehouseUsed = [:]

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
            TOMU: entry['TOMU'],
            SPLA: entry['SPLA']
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
  * @retrieveAssortimentOIS071 - Gets ACHK from OIS071
  * @params -
  * @returns -
  */
  void retrieveAssortimentOIS071(){
    ExpressionFactory expression = database.getExpressionFactory("OASCUS")
    expression = expression.in("OCCUNO", listClient as String[])

    DBAction queryOASCUS = database.table("OASCUS").index("10").matching(expression).selection("OCTDAT","OCFDAT","OCASCD").build()
    DBContainer containerOASCUS = queryOASCUS.getContainer()
    containerOASCUS.set("OCCONO", inCONO)

    queryOASCUS.readAll(containerOASCUS,1,1000, { DBContainer container ->
      String startDate=container.get("OCFDAT").toString().trim()
      String endDate=container.get("OCTDAT").toString().trim()

      if((currentDate >= startDate)  && (currentDate <= endDate)){
        listAssortment.add(container.get("OCASCD").toString().trim())
      }
    })
  }


   /**
  * @checkChaineCommercialeOIS039 - Gets List of superior clients from OIS039
  * @params -
  * @returns -
  */
  void checkChaineCommercialeOIS039(){
    DBAction queryOCHCUS = database.table("OCHCUS").index("10").selection("OSCHCT").build()
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

    DBAction queryOASITN = database.table("OASITN").index("20").matching(expression).selection("OIITNO","OITDAT","OIFDAT").build()
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


  /**
  * @getWarehousesNames - Retrieve all WHNM for available WHLOs
  * @params -
  * @returns -
  */
  void getWarehousesNames(){
    ExpressionFactory expression = database.getExpressionFactory("MITWHL")
    expression = expression.in("MWWHLO", arrWHLO as String[])

    DBAction queryMITWHL = database.table("MITWHL").index("00").matching(expression).selection("MWWHNM").build()
    DBContainer containerMITWHL = queryMITWHL.getContainer()
    containerMITWHL.set("MWCONO", inCONO)
    
    queryMITWHL.readAll(containerMITWHL,1,1000, { DBContainer container ->
      
      String whlo=container.get("MWWHLO").toString().trim()
      String whnm=container.get("MWWHNM").toString().trim()
      
      listWarehousePlaceOfLoadAndDesc.add([WHLO: whlo, WHNM: whnm])
    })
  }
}