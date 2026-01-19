/**
*  @Name: EXT340MI.LstOrderARIANE
*  @Description: Get item info, get route info, get availability of stock, sorts by fastest availability
*  @Authors: Kenylen Motean
*/

/**
* CHANGELOGS
* Version    Date    User        Description
* 1.0.0      241125  KMOTEAN     Initial Release
* 1.0.1      011225  KMOTEAN     Updated fields in selection
* 1.0.3      150126  KMOTEAN     Set MITBAL default values / Adjusted function calculateNextDeliveryDate to take date according to timezone
*/

import java.time.format.DateTimeFormatter
import java.time.LocalDate

public class LstOrderARIANE extends ExtendM3Transaction {
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
  private String inCO01
  private String ortp610=""
  private String whlo610=""
  private String splm610=""
  private String stat610=""
  private String achk610=""
  private String txap610=""
  private String vtcd610=""
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
  private double pbqa
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
  private String itemVTCD=""
  private List<String> listDiscounts=[]
  private boolean creditLimitExceeded=false
  private String inROID
  private String inWHLO
  private String inMMDL
  private String inROUT
  private String inRODN
  private String whnm610=""
  private String inTX40=""
  private String pyno610=""
  private String codeMarqueACR=""
  
  public LstOrderARIANE(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller) {
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
    inCO01 = mi.inData.get("CO01") == null ? "" : mi.inData.get("CO01").trim()
    inROID = mi.inData.get("ROID") == null ? "" : mi.inData.get("ROID").trim()
    inWHLO = mi.inData.get("WHLO") == null ? "" : mi.inData.get("WHLO").trim()

    String[] parts = inROID.split("-")
    if (parts != null && parts.length >= 3) {
        inMMDL = parts[0]
        inROUT = parts[1]
        inRODN = parts[2]
    } else {
        inMMDL = ""
        inROUT = ""
        inRODN = ""
    }

    if(!validateCONO()){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('CUPO', inLNUM.toString())
      mi.outData.put("CO01", inCO01)

      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('ITNO', inPOPN)
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('LNST', "error")
      mi.outData.put('LNTX', "Impossible")
      mi.outData.put('MDTA', "InvalidSupplier")
      mi.outData.put('TEXT', "rejected")
      mi.outData.put('ECHK', "header")
      mi.outData.put("ROID", inROID)
      mi.outData.put("ROUT", inROUT)
      mi.outData.put("RODN", inRODN)
      getRouteDescription(inROUT.trim())
      mi.outData.put("TX40", inTX40)
      mi.write()
      return
    }

    

    if(!validateCUNO()){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('CUPO', inLNUM.toString())
      mi.outData.put("CO01", inCO01)

      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('ITNO', inPOPN)
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('LNST', "error")
      mi.outData.put('LNTX', "invalidGarage")
      mi.outData.put('TEXT', "rejected")
      mi.outData.put('MDTA', "Code client non valide")
      mi.outData.put('CONM', divisionName)
      mi.outData.put('ECHK', "header")
      mi.outData.put("ROID", inROID)
      mi.outData.put("ROUT", inROUT)
      mi.outData.put("RODN", inRODN)
      getRouteDescription(inROUT.trim())
      mi.outData.put("TX40", inTX40)
      mi.write()
      return
    }

    if(pyno610==""){
      pyno610=inCUNO
    }

    if(stat610!="20"){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('CUPO', inLNUM.toString())
      mi.outData.put("CO01", inCO01)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('PYNO', pyno610)
      mi.outData.put('ITNO', inPOPN)
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('LNST', "error")
      mi.outData.put('LNTX', "invalidGarage")
      mi.outData.put('TEXT', "rejected")
      mi.outData.put('MDTA', "Code client non valide")
      mi.outData.put('CONM', divisionName)
      mi.outData.put('ECHK', "header")
      mi.outData.put("ROID", inROID)
      mi.outData.put("ROUT", inROUT)
      mi.outData.put("RODN", inRODN)
      getRouteDescription(inROUT.trim())
      mi.outData.put("TX40", inTX40)
      mi.outData.put('WHLO', inWHLO)
      getwarehouseName(inWHLO)
      mi.outData.put('WHNM', whnm610)
      mi.write()
      return
    }

    checkCreditLimit(pyno610)
    if(creditLimitExceeded){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('CUPO', inLNUM.toString())
      mi.outData.put("CO01", inCO01)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('PYNO', pyno610)
      mi.outData.put('ITNO', inPOPN)
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('LNST', "error")
      mi.outData.put('LNTX', "Impossible")
      mi.outData.put('TEXT', "rejected")
      mi.outData.put('MDTA', "Commande reçue payeur bloqué contacter le service comptabilité clients")
      mi.outData.put('CONM', divisionName)
      mi.outData.put('ECHK', "header")
      mi.outData.put("ROID", inROID)
      mi.outData.put("ROUT", inROUT)
      mi.outData.put("RODN", inRODN)
      getRouteDescription(inROUT.trim())
      mi.outData.put("TX40", inTX40)
      mi.outData.put('WHLO', inWHLO)
      getwarehouseName(inWHLO)
      mi.outData.put('WHNM', whnm610)
      mi.write()
      return
    }

    //error if ORQA=0
    if(inORQA==0){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('CUPO', inLNUM.toString())
      mi.outData.put("CO01", inCO01)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('PYNO', pyno610)
      mi.outData.put('ITNO', inPOPN)
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('LNST', "ack")
      mi.outData.put('LNTX', "complete")
      mi.outData.put('TEXT', "rejected")
      mi.outData.put('MDTA', "")
      mi.outData.put('CONM', divisionName)
      mi.outData.put('PBQA', "0")
      mi.outData.put('ECHK', "lines")
      mi.outData.put("ROID", inROID)
      mi.outData.put("ROUT", inROUT)
      mi.outData.put("RODN", inRODN)
      getRouteDescription(inROUT.trim())
      mi.outData.put("TX40", inTX40)
      mi.outData.put('WHLO', inWHLO)
      getwarehouseName(inWHLO)
      mi.outData.put('WHNM', whnm610)
      mi.write()
      return
    }
    
    //error if ORQA has decimal values
    if(inORQA % 1 != 0){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('CUPO', inLNUM.toString())
      mi.outData.put("CO01", inCO01)

      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('PYNO', pyno610)
      mi.outData.put('ITNO', inPOPN)
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('LNST', "ack")
      mi.outData.put('LNTX', "complete")
      mi.outData.put('TEXT', "rejected")
      mi.outData.put('MDTA', "")
      mi.outData.put('CONM', divisionName)
      mi.outData.put('PBQA', "0")
      mi.outData.put('ECHK', "lines")
      mi.outData.put("ROID", inROID)
      mi.outData.put("ROUT", inROUT)
      mi.outData.put("RODN", inRODN)
      getRouteDescription(inROUT.trim())
      mi.outData.put("TX40", inTX40)
      mi.outData.put('WHLO', inWHLO)
      getwarehouseName(inWHLO)
      mi.outData.put('WHNM', whnm610)
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
        mi.outData.put('CUPO', inLNUM.toString())
        mi.outData.put("CO01", inCO01)
        mi.outData.put('CUNO', inCUNO)
        mi.outData.put('PYNO', pyno610)
        mi.outData.put('ITNO', inPOPN)
        mi.outData.put('POPN', inPOPN)
        mi.outData.put('CFI1', inCFI1)
        mi.outData.put('CEAN', inCEAN)
        mi.outData.put('ORQA', inORQA.intValue().toString())
        mi.outData.put('LNST', "ack")
        mi.outData.put('LNTX', "complete")
        mi.outData.put('TEXT', "rejected")
        mi.outData.put('MDTA', "")
        mi.outData.put('CONM', divisionName)
        mi.outData.put('ECHK', "lines")
        mi.outData.put('PBQA', "0")
        mi.outData.put("ROID", inROID)
        mi.outData.put("ROUT", inROUT)
        mi.outData.put("RODN", inRODN)
        getRouteDescription(inROUT.trim())
        mi.outData.put("TX40", inTX40)
        mi.outData.put('WHLO', inWHLO)
        getwarehouseName(inWHLO)
        mi.outData.put('WHNM', whnm610)
        mi.write()
        return
      }
      else{
        mi.outData.put('CONO', inCONO.toString())
        mi.outData.put('DIVI', inDIVI)
        mi.outData.put('LNUM', inLNUM.toString())
        mi.outData.put('CUPO', inLNUM.toString())
        mi.outData.put("CO01", inCO01)
        mi.outData.put('CUNO', inCUNO)
        mi.outData.put('PYNO', pyno610)
        mi.outData.put('ITNO', inPOPN)
        mi.outData.put('POPN', inPOPN)
        mi.outData.put('CFI1', inCFI1)
        mi.outData.put('CEAN', inCEAN)
        mi.outData.put('ORQA', inORQA.intValue().toString())
        mi.outData.put('LNST', "ack")
        mi.outData.put('LNTX', "complete")
        mi.outData.put('TEXT', "rejected")
        mi.outData.put('MDTA', "")
        mi.outData.put('CONM', divisionName)
        mi.outData.put('ECHK', "lines")
        mi.outData.put('PBQA', "0")
        mi.outData.put("ROID", inROID)
        mi.outData.put("ROUT", inROUT)
        mi.outData.put("RODN", inRODN)
        getRouteDescription(inROUT.trim())
        mi.outData.put("TX40", inTX40)
        mi.outData.put('WHLO', inWHLO)
        getwarehouseName(inWHLO)
        mi.outData.put('WHNM', whnm610)
        mi.write()
        return
      }
    }

    retrieveITDSAndFUDS()//Gets ITDS/FUDS/CFI1/ACHK from MITMAS
    retrieveCodeACR(retrievedCFI1)

    LocalDate date = LocalDate.now()
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd") 
    currentDate = date.format(formatter)

    //verify achk for client and item
    if(achk610=="1"){
      if(assortmentCheck=="1"){
        listClient.add(inCUNO)
        
        checkChaineCommercialeOIS039()
        retrieveAssortimentOIS071()

        if(listAssortment.size()>0){
          if(!checkAssortimentArticle()){
            mi.outData.put('CONO', inCONO.toString())
            mi.outData.put('DIVI', inDIVI)
            mi.outData.put('LNUM', inLNUM.toString())
            mi.outData.put('CUPO', inLNUM.toString())
            mi.outData.put("CO01", inCO01)

            mi.outData.put('CUNO', inCUNO)
            mi.outData.put('PYNO', pyno610)
            mi.outData.put('ITNO', inPOPN)
            mi.outData.put('POPN', inPOPN)
            mi.outData.put('CFI1', inCFI1)
            mi.outData.put('CACR', codeMarqueACR)
            mi.outData.put('CEAN', inCEAN)
            mi.outData.put('ORQA', inORQA.intValue().toString())
            mi.outData.put('LNST', "ack")
            mi.outData.put('LNTX', "complete")
            mi.outData.put('TEXT', "rejected")
            mi.outData.put('MDTA', "")
            mi.outData.put('CONM', divisionName)
            mi.outData.put('PBQA', "0")
            mi.outData.put('ECHK', "lines")
            mi.outData.put("ROID", inROID)
            mi.outData.put("ROUT", inROUT)
            mi.outData.put("RODN", inRODN)
            getRouteDescription(inROUT.trim())
            mi.outData.put("TX40", inTX40)
            mi.outData.put('WHLO', inWHLO)
            getwarehouseName(inWHLO)
            mi.outData.put('WHNM', whnm610)
            mi.write()
            return
          }
          
        }
        else{
          mi.outData.put('CONO', inCONO.toString())
          mi.outData.put('DIVI', inDIVI)
          mi.outData.put('LNUM', inLNUM.toString())
          mi.outData.put('CUPO', inLNUM.toString())
          mi.outData.put("CO01", inCO01)

          mi.outData.put('CUNO', inCUNO)
          mi.outData.put('PYNO', pyno610)
          mi.outData.put('ITNO', inPOPN)
          mi.outData.put('POPN', inPOPN)
          mi.outData.put('CFI1', inCFI1)
          mi.outData.put('CACR', codeMarqueACR)
          mi.outData.put('CEAN', inCEAN)
          mi.outData.put('ORQA', inORQA.intValue().toString())
          mi.outData.put('LNST', "ack")
          mi.outData.put('LNTX', "complete")
          mi.outData.put('TEXT', "rejected")
          mi.outData.put('MDTA', "")
          mi.outData.put('CONM', divisionName)
          mi.outData.put('PBQA', "0")
          mi.outData.put('ECHK', "lines")
          mi.outData.put("ROID", inROID)
          mi.outData.put("ROUT", inROUT)
          mi.outData.put("RODN", inRODN)
          getRouteDescription(inROUT.trim())
          mi.outData.put("TX40", inTX40)
          mi.outData.put('WHLO', inWHLO)
          getwarehouseName(inWHLO)
          mi.outData.put('WHNM', whnm610)
          mi.write()
          return
        }
      }
    }
    else if(achk610=="2"){
      if(assortmentCheck=="1"){
        listClient.add(inCUNO)

        retrieveAssortimentOIS071()

        if(listAssortment.size()>0){
          if(!checkAssortimentArticle()){
            mi.outData.put('CONO', inCONO.toString())
            mi.outData.put('DIVI', inDIVI)
            mi.outData.put('LNUM', inLNUM.toString())
            mi.outData.put('CUPO', inLNUM.toString())
            mi.outData.put("CO01", inCO01)
            mi.outData.put('CUNO', inCUNO)
            mi.outData.put('PYNO', pyno610)
            mi.outData.put('ITNO', inPOPN)
            mi.outData.put('POPN', inPOPN)
            mi.outData.put('CFI1', inCFI1)
            mi.outData.put('CACR', codeMarqueACR)
            mi.outData.put('CEAN', inCEAN)
            mi.outData.put('ORQA', inORQA.intValue().toString())
            mi.outData.put('LNST', "ack")
            mi.outData.put('LNTX', "complete")
            mi.outData.put('TEXT', "rejected")
            mi.outData.put('MDTA', "")
            mi.outData.put('CONM', divisionName)
            mi.outData.put('PBQA', "0")
            mi.outData.put('ECHK', "lines")
            mi.outData.put("ROID", inROID)
            mi.outData.put("ROUT", inROUT)
            mi.outData.put("RODN", inRODN)
            getRouteDescription(inROUT.trim())
            mi.outData.put("TX40", inTX40)
            mi.outData.put('WHLO', inWHLO)
            getwarehouseName(inWHLO)
            mi.outData.put('WHNM', whnm610)
            mi.write()
            return
          }
        }
        else{
          mi.outData.put('CONO', inCONO.toString())
          mi.outData.put('DIVI', inDIVI)
          mi.outData.put('LNUM', inLNUM.toString())
          mi.outData.put('CUPO', inLNUM.toString())
          mi.outData.put("CO01", inCO01)

          mi.outData.put('CUNO', inCUNO)
          mi.outData.put('PYNO', pyno610)
          mi.outData.put('ITNO', inPOPN)
          mi.outData.put('POPN', inPOPN)
          mi.outData.put('CFI1', inCFI1)
          mi.outData.put('CACR', codeMarqueACR)
          mi.outData.put('CEAN', inCEAN)
          mi.outData.put('ORQA', inORQA.intValue().toString())
          mi.outData.put('LNST', "ack")
          mi.outData.put('LNTX', "complete")
          mi.outData.put('TEXT', "rejected")
          mi.outData.put('MDTA', "")
          mi.outData.put('CONM', divisionName)
          mi.outData.put('PBQA', "0")
          mi.outData.put('ECHK', "lines")
          mi.outData.put("ROID", inROID)
          mi.outData.put("ROUT", inROUT)
          mi.outData.put("RODN", inRODN)
          getRouteDescription(inROUT.trim())
          mi.outData.put("TX40", inTX40)
          mi.outData.put('WHLO', inWHLO)
          getwarehouseName(inWHLO)
          mi.outData.put('WHNM', whnm610)
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

    if(validateWarehouse(inWHLO) && inWHLO!=""){
      arrWHLO.add(inWHLO)
    }
    else if(splm610!=""){
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
      mi.outData.put('CUPO', inLNUM.toString())
      mi.outData.put("CO01", inCO01)

      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('PYNO', pyno610)
      mi.outData.put('ITNO', inPOPN)
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CACR', codeMarqueACR)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('LNST', "error")
      mi.outData.put('LNTX', "Impossible")
      mi.outData.put('TEXT', "rejected")
      mi.outData.put('MDTA', "Default warehouse for customer not defined")
      mi.outData.put('CONM', divisionName)
      mi.outData.put("ROID", inROID)
      mi.outData.put("ROUT", inROUT)
      mi.outData.put("RODN", inRODN)
      getRouteDescription(inROUT.trim())
      mi.outData.put("TX40", inTX40)
      mi.outData.put('WHLO', inWHLO)
      getwarehouseName(inWHLO)
      mi.outData.put('WHNM', whnm610)
      mi.write()
      return
    }

    if(arrWHLO.isEmpty()){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('CUPO', inLNUM.toString())
      mi.outData.put("CO01", inCO01)

      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('PYNO', pyno610)
      mi.outData.put('ITNO', inPOPN)
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CACR', codeMarqueACR)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('LNST', "error")
      mi.outData.put('LNTX', "Impossible")
      mi.outData.put('TEXT', "rejected")
      mi.outData.put('MDTA', "Order type WAV for supply model not defined")
      mi.outData.put('CONM', divisionName)
      mi.outData.put("ROID", inROID)
      mi.outData.put("ROUT", inROUT)
      mi.outData.put("RODN", inRODN)
      getRouteDescription(inROUT.trim())
      mi.outData.put("TX40", inTX40)
      mi.outData.put('WHLO', inWHLO)
      getwarehouseName(inWHLO)
      mi.outData.put('WHNM', whnm610)
      mi.write()
      return
    }

    searchAllSDES()//Retrieve all SDES and other info for available WHLOs
    
    searchAllValidRoutesForCustomerPO1PO2()//Retrieve all routes and other info for available WHLOs
    
    listRoutes=listRoutes.unique()//remove duplicate routes

    if(listRoutes.isEmpty()){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('CUPO', inLNUM.toString())
      mi.outData.put("CO01", inCO01)

      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('PYNO', pyno610)
      mi.outData.put('ITNO', inPOPN)
      mi.outData.put('POPN', inPOPN)
      mi.outData.put('CFI1', inCFI1)
      mi.outData.put('CACR', codeMarqueACR)
      mi.outData.put('CEAN', inCEAN)
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('LNST', "error")
      mi.outData.put('LNTX', "Impossible")
      mi.outData.put('TEXT', "rejected")
      mi.outData.put('MDTA', "Error Routes")
      mi.outData.put('CONM', divisionName)
      mi.outData.put("ROID", inROID)
      mi.outData.put("ROUT", inROUT)
      mi.outData.put("RODN", inRODN)
      getRouteDescription(inROUT.trim())
      mi.outData.put("TX40", inTX40)
      mi.outData.put('WHLO', inWHLO)
      getwarehouseName(inWHLO)
      mi.outData.put('WHNM', whnm610)
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
    

    listRoutesAllDetails.each { record ->
      record.isInputMatch = (record.WHLO == inWHLO && record.ROUT == inROUT )
    }

    // Filter based on whether any matches exist
    List<Map<String, String>> matchedRecords = listRoutesAllDetails.findAll { it.isInputMatch }

    if (matchedRecords) {
      listRoutesAllDetails = matchedRecords
    }
    
    
    if (sortFlag) {
      // Sort by nearest date, time, and then SPLA, ensuring that OBV2="Prio9" records are last
      listRoutesAllDetails = listRoutesAllDetails.sort { a, b ->
      	int matchCompare = (b.isInputMatch ? 1 : 0) <=> (a.isInputMatch ? 1 : 0) // true first
      	int aPriority = a.PRIO.toInteger()
      	int bPriority = b.PRIO.toInteger()
      
      	matchCompare ?: aPriority <=> bPriority ?: a.CODZ <=> b.CODZ ?: a.COHZ <=> b.COHZ ?: a.SPLA <=> b.SPLA
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
    
    if(correctITNOFound){
      retrieveFastestRoute()//retrieves fastest route with avaiable quantity
      
      int count=0
      boolean allConqZero = listFinalRoutes.every { record -> record.CONQ == "0.0" }
      
      //Set AVTX with correct status text
      String calculatedAVTX=""
      if(!correctITNOFound){
        calculatedAVTX="Reference inconnue"
      }
      else if((pbqa)==0.0){
        calculatedAVTX="Indisponible"
      }
      else if((pbqa) >= inORQA ){
        calculatedAVTX="Disponible"
      }
      else if((pbqa) < inORQA){
        calculatedAVTX="Partiellement disponible"
      }
      else{
        calculatedAVTX="Indisponible"
      }

      //Set AVST with correct status
      String calculatedAVST=""
      if(!correctITNOFound){
        calculatedAVST="I"
      }
      else if((pbqa)==0.0){
        calculatedAVST="I"
      }
      else if((pbqa) >= inORQA ){
        calculatedAVST="D"
      }
      else if((pbqa) < inORQA){
        calculatedAVST="P"
      }
      else{
        calculatedAVST="I"
      }

      //Set LNTX with correct status text
      String statutLigne=""
      if(!correctITNOFound){
        statutLigne="rejected"
      }
      else if((pbqa)==0.0){
        statutLigne="rejected"
      }
      else if((pbqa) >= inORQA ){
        statutLigne="complete"
      }
      else if((pbqa) < inORQA){
        statutLigne="partial"
      }
      else{
        statutLigne="rejected"
      }

      ext320MIGetLine(inCONO.toString(),inCUNO, correctITNO,pbqa.toString())//getPrices

      //check if no warehouse has stock
      if(allConqZero){
        if(listFinalRoutes.isEmpty()){
          mi.outData.put('CONO', inCONO.toString())
          mi.outData.put('DIVI', inDIVI)
          mi.outData.put('LNUM', inLNUM.toString())
          mi.outData.put('CUPO', inLNUM.toString())
          mi.outData.put("CO01", inCO01)

          mi.outData.put('CUNO', inCUNO)
          mi.outData.put('PYNO', pyno610)
          mi.outData.put('ITNO', inPOPN)
          mi.outData.put('POPN', inPOPN)
          mi.outData.put('CFI1', inCFI1)
          mi.outData.put('CACR', codeMarqueACR)
          mi.outData.put('CEAN', inCEAN)
          mi.outData.put('ORQA', inORQA.intValue().toString())
          mi.outData.put('LNST', "ack")
          mi.outData.put('LNTX', "complete")
          mi.outData.put('TEXT', "rejected")
          mi.outData.put('MDTA', "")
          mi.outData.put('CONM', divisionName)
          mi.outData.put("ROID", inROID)
          mi.outData.put("ROUT", inROUT)
          mi.outData.put("RODN", inRODN)
          getRouteDescription(inROUT.trim())
          mi.outData.put("TX40", inTX40)
          mi.outData.put('WHLO', inWHLO)
          getwarehouseName(inWHLO)
          mi.outData.put('WHNM', whnm610)
          mi.write()
          return
        }
        else if(calculatedAVTX=="Indisponible"){
          Map<String, String>  record1 = listFinalRoutes.first()
          mi.outData.put('CONO', inCONO.toString())
          mi.outData.put('DIVI', inDIVI)
          mi.outData.put('CONM', divisionName)
          mi.outData.put('LNUM', inLNUM.toString())
          mi.outData.put('CUPO', inLNUM.toString())
          mi.outData.put("CO01", inCO01)
          mi.outData.put('CUNO',record1.CUNO)
          mi.outData.put('PYNO', pyno610)
          mi.outData.put('ITNO',record1.ITNO)
          mi.outData.put('POPN',inPOPN)
          mi.outData.put('CACR', codeMarqueACR)
          mi.outData.put('CEAN',inCEAN)
          mi.outData.put('AV01',"0.0")
          mi.outData.put('CODZ',"")
          mi.outData.put('CODZ',record1.CODZ)
          mi.outData.put('COHZ',"")
          mi.outData.put('COHZ',record1.COHZ)
          mi.outData.put('MDTA', "")
          mi.outData.put('LNST', "ack")
          mi.outData.put('LNTX', "complete")
          mi.outData.put('TEXT', "rejected")
          mi.outData.put('FUDS', retrievedFUDS)
          mi.outData.put('ITDS', retrievedITDS)
          mi.outData.put('CFI1', inCFI1)
          if(record1.ORQA==""){
            mi.outData.put('ORQA', "0")
          }
          else{
            mi.outData.put('ORQA', ((record1.ORQA as Double).intValue()).toString())
          }
          mi.outData.put('CONQ', "0.0")
          mi.outData.put('PBQA', ((pbqa).intValue()).toString())
          mi.outData.put('TOMU',record1.TOMU)
          mi.outData.put('ROUT',"")
          mi.outData.put('RODN',"")
          mi.outData.put('TX40',"")
          mi.outData.put('TTVA',"0.0")
        
          if(listPrices.size()>0){
            mi.outData.put('SAPR',listPrices[0].SAPR.toString())
            mi.outData.put('SACD',listPrices[0].SACD.toString())
            mi.outData.put('LNAM',"0.0")
            mi.outData.put('NEPR',listPrices[0].NETP.toString())
            mi.outData.put('CRAM',"0.0")
            mi.outData.put('DIP1',"")
            mi.outData.put('DIP2',"")
            mi.outData.put('DIP3',"")
            mi.outData.put('DIP4',"")
            mi.outData.put('DIP5',"")
            mi.outData.put('DIP6',"")
            mi.outData.put('VTAM',"0.0")
            mi.outData.put('TOAM',"0.0")
          }   
          
          mi.outData.put("ROID", inROID)
          mi.outData.put("ROUT", inROUT)
          mi.outData.put("RODN", inRODN)
          getRouteDescription(inROUT.trim())
          mi.outData.put("TX40", inTX40)
          mi.outData.put('WHLO', inWHLO)
          getwarehouseName(inWHLO)
          mi.outData.put('WHNM', whnm610)
          mi.write()
        }
        else{
          Map<String, String>  record1 = listFinalRoutes.first()
          mi.outData.put('CONO', inCONO.toString())
          mi.outData.put('DIVI', inDIVI)
          mi.outData.put('CONM', divisionName)
          mi.outData.put('LNUM', inLNUM.toString())
          mi.outData.put('CUPO', inLNUM.toString())
          mi.outData.put("CO01", inCO01)
          mi.outData.put('CUNO', inCUNO)
          mi.outData.put('PYNO', pyno610)

          mi.outData.put('WHLO',record1.WHLO)
          mi.outData.put('WHNM',record1.WHNM)
          mi.outData.put('ITNO',record1.ITNO)
          mi.outData.put('POPN',inPOPN)
          mi.outData.put('CACR', codeMarqueACR)
          mi.outData.put('CEAN',inCEAN)
          mi.outData.put('AV01',record1.AV01)
          mi.outData.put('CODZ',record1.CODZ+" "+record1.COHZ)
          mi.outData.put('COHZ',record1.COHZ)
          mi.outData.put('MDTA', "")
          mi.outData.put('LNST', "ack")
          mi.outData.put('LNTX', "complete")
          mi.outData.put('TEXT', "rejected")
          mi.outData.put('FUDS', retrievedFUDS)
          mi.outData.put('ITDS', retrievedITDS)
          mi.outData.put('CFI1', inCFI1)
          
          if(record1.ORQA==""){
            mi.outData.put('ORQA', "0")
          }
          else{
            mi.outData.put('ORQA', ((record1.ORQA as Double).intValue()).toString())
          }

          mi.outData.put('CONQ', record1.CONQ)
          mi.outData.put('PBQA', ((pbqa).intValue()).toString())
          mi.outData.put('TOMU',record1.TOMU)
          mi.outData.put('ROUT',record1.ROUT)
          mi.outData.put('RODN',record1.RODN)
          mi.outData.put('TX40',record1.TX40)

          double tva=0

          if(txap610=="0" || txap610=="2"){
            record1.VATC="0"
            mi.outData.put('TTVA',"0.0")
          }
          else if(txap610=="1"){
            if(record1.MITBALVTCS!=null){
              if(record1.MITBALVTCS!="0"){
                String vatc=retrieveTVA(record1.MITBALVTCS)
                record1.VATC==vatc
                mi.outData.put('TTVA',vatc)
                tva=vatc as double
              }
              else if(vtcd610!=""){
                String vatc=retrieveTVA(vtcd610)

                record1.VATC==vatc
                mi.outData.put('TTVA',vatc)
                tva=vatc as double
              }
              else{
                record1.VATC="0"
                mi.outData.put('TTVA',"0")
              }
            }
            else{
              if(itemVTCD!="0"){
                String vatc=retrieveTVA(itemVTCD)
                record1.VATC==vatc
                mi.outData.put('TTVA',vatc)
                tva=vatc as double
              }
              else if(vtcd610!=""){
                String vatc=retrieveTVA(vtcd610)

                record1.VATC==vatc
                mi.outData.put('TTVA',vatc)
                tva=vatc as double
              }
              else{
                record1.VATC="0"
                mi.outData.put('TTVA',"0")
              }
            }
          }  

          if(listPrices.size()>0){
            mi.outData.put('SAPR',listPrices[0].SAPR.toString())
            mi.outData.put('SACD',listPrices[0].SACD.toString())
            mi.outData.put('LNAM', String.format("%.2f",     Double.parseDouble(listPrices[0].NETP.toString()) * Double.parseDouble(record1.CONQ)).trim()  )
            mi.outData.put('NEPR',listPrices[0].NETP.toString())
            mi.outData.put('NEPR',listPrices[0].NETP.toString())
            mi.outData.put('CRAM',retrievedCRAM)
            mi.outData.put('SACD',listPrices[0].SACD.toString())
            mi.outData.put('NEPR',listPrices[0].NETP.toString())
            mi.outData.put('DIP1',listPrices[0].DIP1.toString())
            mi.outData.put('DIP2',listPrices[0].DIP2.toString())
            mi.outData.put('DIP3',listPrices[0].DIP3.toString())
            mi.outData.put('DIP4',listPrices[0].DIP4.toString())
            mi.outData.put('DIP5',listPrices[0].DIP5.toString())
            mi.outData.put('DIP6',listPrices[0].DIP6.toString())

            if(listPrices[0].DIP1.toString()!=""){
              mi.outData.put('TX81',listPrices[0].TX81.toString())
            }
            if(listPrices[0].DIP2.toString()!=""){
              mi.outData.put('TX82',listPrices[0].TX82.toString())
            }
            if(listPrices[0].DIP3.toString()!=""){
              mi.outData.put('TX83',listPrices[0].TX83.toString())
            }
            if(listPrices[0].DIP4.toString()!=""){
              mi.outData.put('TX84',listPrices[0].TX84.toString())
            }
            if(listPrices[0].DIP5.toString()!=""){
              mi.outData.put('TX85',listPrices[0].TX85.toString())
            }
            if(listPrices[0].DIP6.toString()!=""){
              mi.outData.put('TX86',listPrices[0].TX86.toString())
            }
            
            double vtam=((listPrices[0].LNAM.toString().trim().toDouble()*tva)/100)
            mi.outData.put('VTAM',  String.format("%.2f",vtam ))
            mi.outData.put('TOAM',  String.format("%.2f",vtam+listPrices[0].LNAM.toString().trim().toDouble() ))
          }   

          mi.outData.put("ROID", inROID)
          mi.write()
        }
        
      }
      else if(calculatedAVTX=="Indisponible"){
        Map<String, String>  record1 = listFinalRoutes.first()
        mi.outData.put('CONO', inCONO.toString())
        mi.outData.put('DIVI', inDIVI)
        mi.outData.put('CONM', divisionName)
        mi.outData.put('LNUM', inLNUM.toString())
        mi.outData.put('CUPO', inLNUM.toString())
        mi.outData.put("CO01", inCO01)
        mi.outData.put('CUNO', inCUNO)
        mi.outData.put('PYNO', pyno610)
        mi.outData.put('ITNO',record1.ITNO)
        mi.outData.put('POPN',inPOPN)
        mi.outData.put('CACR', codeMarqueACR)
        mi.outData.put('CEAN',inCEAN)
        mi.outData.put('AV01',"0.0")
        mi.outData.put('CODZ',"")
        mi.outData.put('COHZ',"")
        mi.outData.put('MDTA', "")
        mi.outData.put('LNST', "ack")
        mi.outData.put('LNTX', "complete")
        mi.outData.put('TEXT', "rejected")
        mi.outData.put('FUDS', retrievedFUDS)
        mi.outData.put('ITDS', retrievedITDS)
        mi.outData.put('CFI1', inCFI1)
        
        if(record1.ORQA==""){
          mi.outData.put('ORQA', "0")
        }
        else{
          mi.outData.put('ORQA', ((record1.ORQA as Double).intValue()).toString())
        }

        mi.outData.put('CONQ', "0.0")
        mi.outData.put('PBQA', ((pbqa).intValue()).toString())
        mi.outData.put('TOMU',record1.TOMU)
        mi.outData.put('ROUT',"")
        mi.outData.put('RODN',"")
        mi.outData.put('TX40',"")
      
        mi.outData.put('TTVA',"0.0")
      
        if(listPrices.size()>0){
          mi.outData.put('SAPR',listPrices[0].SAPR.toString())
          mi.outData.put('SACD',listPrices[0].SACD.toString())
          mi.outData.put('LNAM',"0.0")
          mi.outData.put('NEPR',listPrices[0].NETP.toString())
          mi.outData.put('CRAM',"0.0")

          mi.outData.put('DIP1',listPrices[0].DIP1.toString())
          mi.outData.put('DIP2',listPrices[0].DIP2.toString())
          mi.outData.put('DIP3',listPrices[0].DIP3.toString())
          mi.outData.put('DIP4',listPrices[0].DIP4.toString())
          mi.outData.put('DIP5',listPrices[0].DIP5.toString())
          mi.outData.put('DIP6',listPrices[0].DIP6.toString())

          if(listPrices[0].DIP1.toString()!=""){
            mi.outData.put('TX81',listPrices[0].TX81.toString())
          }
          if(listPrices[0].DIP2.toString()!=""){
            mi.outData.put('TX82',listPrices[0].TX82.toString())
          }
          if(listPrices[0].DIP3.toString()!=""){
            mi.outData.put('TX83',listPrices[0].TX83.toString())
          }
          if(listPrices[0].DIP4.toString()!=""){
            mi.outData.put('TX84',listPrices[0].TX84.toString())
          }
          if(listPrices[0].DIP5.toString()!=""){
            mi.outData.put('TX85',listPrices[0].TX85.toString())
          }
          if(listPrices[0].DIP6.toString()!=""){
            mi.outData.put('TX86',listPrices[0].TX86.toString())
          }
        
          mi.outData.put('VTAM',"0.0")
          mi.outData.put('TOAM',"0.0")
        }   

        mi.outData.put("ROID", inROID)
        mi.outData.put("ROUT", inROUT)
        mi.outData.put("RODN", inRODN)
        getRouteDescription(inROUT.trim())
        mi.outData.put("TX40", inTX40)
        mi.outData.put('WHLO', inWHLO)
        getwarehouseName(inWHLO)
        mi.outData.put('WHNM', whnm610)
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
            mi.outData.put('CUPO', inLNUM.toString())
            mi.outData.put("CO01", inCO01)

            mi.outData.put('CONM', divisionName)
            mi.outData.put('CUNO', inCUNO)
            mi.outData.put('PYNO', pyno610)
            mi.outData.put('WHLO',record1.WHLO)
            mi.outData.put('WHNM',record1.WHNM)
            mi.outData.put('ITNO',record1.ITNO)
            mi.outData.put('POPN',inPOPN)
            mi.outData.put('CACR', codeMarqueACR)
            mi.outData.put('CEAN',inCEAN)
            mi.outData.put('AV01',record1.AV01)
            mi.outData.put('CODZ',record1.CODZ)
            mi.outData.put('COHZ',record1.COHZ)
            mi.outData.put('MDTA', "")
            mi.outData.put('LNST', "ack")
            mi.outData.put('LNTX', "complete")
            mi.outData.put('FUDS', retrievedFUDS)
            mi.outData.put('ITDS', retrievedITDS)
            mi.outData.put('CFI1', inCFI1)
            
            if(record1.ORQA==""){
              mi.outData.put('ORQA', "0")
            }
            else{
              mi.outData.put('ORQA', ((record1.ORQA as Double).intValue()).toString())
            }

            mi.outData.put('CONQ', record1.CONQ)

            if (record1.CONQ.toBigDecimal() ==0) {
                mi.outData.put('TEXT', "rejected")
            } 
            else if (record1.CONQ.toBigDecimal() >= record1.ORQA.toBigDecimal()) {
                mi.outData.put('TEXT', "complete")
            } 
            else{
              mi.outData.put('TEXT', "partial")
            }


            mi.outData.put('PBQA', ((pbqa).intValue()).toString())
            mi.outData.put('TOMU',record1.TOMU)
            mi.outData.put('ROUT',record1.ROUT)
            mi.outData.put('RODN',record1.RODN)
            mi.outData.put('TX40',record1.TX40)

            double tva=0


            if(txap610=="0" || txap610=="2"){
              record1.VATC="0"
              mi.outData.put('TTVA',"0.0")
            }
            else if(txap610=="1"){
              if(record1.MITBALVTCS!=null){
                if(record1.MITBALVTCS!="0"){
                  String vatc=retrieveTVA(record1.MITBALVTCS)
                  record1.VATC==vatc
                  mi.outData.put('TTVA',vatc)
                  tva=vatc as double
                }
                else if(vtcd610!=""){
                  String vatc=retrieveTVA(vtcd610)

                  record1.VATC==vatc
                  mi.outData.put('TTVA',vatc)
                  tva=vatc as double
                }
                else{
                  record1.VATC="0"
                  mi.outData.put('TTVA',"0")
                }
              }
              else{
                if(itemVTCD!="0"){
                  String vatc=retrieveTVA(itemVTCD)
                  record1.VATC==vatc
                  mi.outData.put('TTVA',vatc)
                  tva=vatc as double
                }
                else if(vtcd610!=""){
                  String vatc=retrieveTVA(vtcd610)

                  record1.VATC==vatc
                  mi.outData.put('TTVA',vatc)
                  tva=vatc as double
                }
                else{
                  record1.VATC="0"
                  mi.outData.put('TTVA',"0")
                }
              }
            }  

            if(listPrices.size()>0){
              mi.outData.put('SAPR',listPrices[0].SAPR.toString())
              mi.outData.put('SACD',listPrices[0].SACD.toString())

              mi.outData.put('LNAM', String.format("%.2f",     Double.parseDouble(listPrices[0].NETP.toString()) * Double.parseDouble(record1.CONQ)).trim()  )
              mi.outData.put('NEPR',listPrices[0].NETP.toString())
              mi.outData.put('CRAM',retrievedCRAM)
              mi.outData.put('CRAM',retrievedCRAM)
              mi.outData.put('DIP1',listPrices[0].DIP1.toString())
              mi.outData.put('DIP2',listPrices[0].DIP2.toString())
              mi.outData.put('DIP3',listPrices[0].DIP3.toString())
              mi.outData.put('DIP4',listPrices[0].DIP4.toString())
              mi.outData.put('DIP5',listPrices[0].DIP5.toString())
              mi.outData.put('DIP6',listPrices[0].DIP6.toString())

              if(listPrices[0].DIP1.toString()!=""){
                mi.outData.put('TX81',listPrices[0].TX81.toString())
              }
              if(listPrices[0].DIP2.toString()!=""){
                mi.outData.put('TX82',listPrices[0].TX82.toString())
              }
              if(listPrices[0].DIP3.toString()!=""){
                mi.outData.put('TX83',listPrices[0].TX83.toString())
              }
              if(listPrices[0].DIP4.toString()!=""){
                mi.outData.put('TX84',listPrices[0].TX84.toString())
              }
              if(listPrices[0].DIP5.toString()!=""){
                mi.outData.put('TX85',listPrices[0].TX85.toString())
              }
              if(listPrices[0].DIP6.toString()!=""){
                mi.outData.put('TX86',listPrices[0].TX86.toString())
              }
              
              double vtam=((listPrices[0].LNAM.toString().trim().toDouble()*tva)/100)
              mi.outData.put('VTAM',  String.format("%.2f",vtam ))
              mi.outData.put('TOAM',  String.format("%.2f",vtam+listPrices[0].LNAM.toString().trim().toDouble() ))
            } 

            mi.outData.put("ROID", inROID)
            mi.write()
          }
        }

      } 
    }
    else{
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('LNUM', inLNUM.toString())
      mi.outData.put('CUPO', inLNUM.toString())
      mi.outData.put("CO01", inCO01)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('PYNO', pyno610)
      mi.outData.put('MDTA', "")
      mi.outData.put('LNST', "ack")
      mi.outData.put('LNTX', "complete")
      mi.outData.put('TEXT', "rejected")
      mi.outData.put('ORQA', inORQA.intValue().toString())
      mi.outData.put('AV01', "0.0")
      mi.outData.put("ROID", inROID)
      mi.outData.put("ROUT", inROUT)
      mi.outData.put("RODN", inRODN)
      getRouteDescription(inROUT.trim())
      mi.outData.put("TX40", inTX40)
      mi.outData.put('WHLO', inWHLO)
      getwarehouseName(inWHLO)
      mi.outData.put('WHNM', whnm610)
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
  void retrieveCodeMarqueM3(String cono,String pk05) {
    listCodeMarqueCugex=[]

    //retrieveCFI1 for items
    ExpressionFactory expression = database.getExpressionFactory("CUGEX1")
    expression = expression.eq("F1PK05", pk05)

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
  * @validateCUNO - Validates CUNO
  * @params -
  * @returns - true/false
  */
  Boolean validateCUNO() {
    if (!inCUNO.isBlank()) {
      DBAction query = database.table("OCUSMA").index("00").selection("OKORTP","OKSPLM","OKWHLO","OKSTAT","OKACHK","OKTXAP","OKVTCD","OKPYNO").build()
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
        stat610 =  container.get("OKSTAT").toString().trim()
        achk610 =  container.get("OKACHK").toString().trim()
        txap610 =  container.get("OKTXAP").toString().trim()
        vtcd610 =  container.get("OKVTCD").toString().trim()
        pyno610 =  container.get("OKPYNO").toString().trim()
      }
    }
    return true
  }
  
  /**
  * @mms059ListApiCall - Retrieve available WHLOs From MMS059
  * @params -
  * @returns - 
  */
  void mms059ListApiCall(String splm,String priority){
    Map<String, String> paramsMMS059 = ["SPLM":"${splm}".toString()]
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
    
    // expression=expression.and(expression.in("DOOBV2", arrMODL as String[] ))
    
    
    DBAction queryDRODPR = database.table("DRODPR").index("00").matching(expression).selection("DOROUT","DOEDES","DOOBV1","DOOBV2","DOPREX").build()
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

    DBAction queryMIBAL = database.table("MITBAL").index("00").matching(expression).selection("MBAVAL","MBALQT","MBVTCS","MBSTAT").build()
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

      if (Integer.parseInt(container.get("MBSTAT").toString().trim()) < 20 ||
          Integer.parseInt(container.get("MBSTAT").toString().trim()) > 50) {
          AV01 = 0
      }
      
      listRoutesAllDetails.each { record1 ->
          if(record1.WHLO==container.get("MBWHLO").toString().trim() && record1.ITNO==container.get("MBITNO").toString().trim() ){
            record1.AV01=AV01.toString()
            record1.MITBALVTCS=container.get("MBVTCS").toString().trim()
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
    DBAction queryMITMAS = database.table("MITMAS").index("00").selection("MMFUDS","MMITDS","MMCFI1","MMACHK","MMVTCS").build()
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
      itemVTCD=containerMITMAS.get("MMVTCS").toString().trim()

    }
  }
  
  /**
  * @retrieveTOMU - Gets TOMU for current item/whlo from MITBAL
  * @params -
  * @returns -
  */
  String retrieveTOMU(String cono,String whlo,String itno){
    DBAction queryMITBAL = database.table("MITBAL").index("00").selection("MBTOMU").build()
    DBContainer containerMITBAL = queryMITBAL.getContainer()

    containerMITBAL.set("MBCONO", cono as Integer)
    containerMITBAL.set("MBWHLO", whlo)
    containerMITBAL.set("MBITNO", itno)
    
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
            MITBALVTCS: entry['MITBALVTCS']
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



    pbqa = requiredQuantity - remainingQuantity
    pbqa = (((pbqa + currentTOMU - 1) / currentTOMU) as Integer) * currentTOMU
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
  void ext320MIGetLine(String cono,String cuno, String itno,String orqa) {    
    Map<String, String> paramsExt320MIGetLine = ["CONO": "${cono}".toString(),"CUNO": "${cuno}".toString(), "ITNO": "${itno}".toString(), "ORTP": "WAV", "ORQA": "${orqa}".toString()]
    Closure<?> callbackEXT320MIGetLine= { Map<String, String> responseEXT320MIGetLine ->

        if (responseEXT320MIGetLine != null) {
            if (responseEXT320MIGetLine.containsKey("error") && responseEXT320MIGetLine.error != null) {
              return
            } else {
                listPrices.add([
                  NETP:responseEXT320MIGetLine.NETP.toString().trim(),
                  SAPR:responseEXT320MIGetLine.SAPR.toString().trim(),
                  SACD:responseEXT320MIGetLine.SACD.toString().trim(),
                  DIP1:responseEXT320MIGetLine.DIP1.toString().trim(),
                  DIP2:responseEXT320MIGetLine.DIP2.toString().trim(),
                  DIP3:responseEXT320MIGetLine.DIP3.toString().trim(),
                  DIP4:responseEXT320MIGetLine.DIP4.toString().trim(),
                  DIP5:responseEXT320MIGetLine.DIP5.toString().trim(),
                  DIP6:responseEXT320MIGetLine.DIP6.toString().trim(),
                  TX81:responseEXT320MIGetLine.TX81.toString().trim(),
                  TX82:responseEXT320MIGetLine.TX82.toString().trim(),
                  TX83:responseEXT320MIGetLine.TX83.toString().trim(),
                  TX84:responseEXT320MIGetLine.TX84.toString().trim(),
                  TX85:responseEXT320MIGetLine.TX85.toString().trim(),
                  TX86:responseEXT320MIGetLine.TX86.toString().trim(),
                  LNAM: (responseEXT320MIGetLine.LNAM.toString().trim() ?: "0")
                  
                ])
                if(responseEXT320MIGetLine.DIP1.toString().trim()!=""){
                  listDiscounts.add(responseEXT320MIGetLine.DIP1.toString().trim())
                }
                if(responseEXT320MIGetLine.DIP2.toString().trim()!=""){
                  listDiscounts.add(responseEXT320MIGetLine.DIP2.toString().trim())
                }
                if(responseEXT320MIGetLine.DIP3.toString().trim()!=""){
                  listDiscounts.add(responseEXT320MIGetLine.DIP3.toString().trim())
                }
                if(responseEXT320MIGetLine.DIP4.toString().trim()!=""){
                  listDiscounts.add(responseEXT320MIGetLine.DIP4.toString().trim())
                }
                if(responseEXT320MIGetLine.DIP5.toString().trim()!=""){
                  listDiscounts.add(responseEXT320MIGetLine.DIP5.toString().trim())
                }
                if(responseEXT320MIGetLine.DIP6.toString().trim()!=""){
                  listDiscounts.add(responseEXT320MIGetLine.DIP6.toString().trim())
                }
                
                
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
  * @retrieveTVA - Get vatCode from CVATPC
  * @params -
  * @returns -
  */
  String retrieveTVA(String vatCode){
    DBAction queryCVATPC = database.table("CVATPC").index("10").selection("CVVTP1","CVFRDT","CVDIVI").build()
    DBContainer containerCVATPC = queryCVATPC.getContainer()
    containerCVATPC.set("CVCONO", inCONO)
    containerCVATPC.set("CVVTCD", vatCode as Integer)

    String vatCodeDiviBlanche="0"
    String vatCodeDiviInput="0"
    queryCVATPC.readAll(containerCVATPC,2,1000, { DBContainer container ->
      String fromDate=container.get("CVFRDT").toString().trim()
      if((currentDate >= fromDate) ){
        if(container.get("CVDIVI").toString().trim()==inDIVI){
            vatCodeDiviInput=container.get("CVVTP1").toString().trim()
            return vatCodeDiviInput
        }
        else if(container.get("CVDIVI").toString().trim()==""){
            vatCodeDiviBlanche=container.get("CVVTP1").toString().trim()
        }
      }
      
      
    })
    return vatCodeDiviBlanche
  }


  /**
  * @checkCreditLimit - Check Credit Limit of Customer
  * @params - CUNO
  * @returns - 
  */
  void checkCreditLimit(String cuno) {
    Map<String, String> params = ["CUNO": cuno.toString()]
    Closure<?> callback= { Map<String, String> response ->

        if (response != null) {
            if (response.containsKey("error") && response.error != null) {
              creditLimitExceeded=false
            } else {
              if(response.MSG1.toString().trim()!=""){
                creditLimitExceeded=true
              }
            }
        }
  
    }
    miCaller.call("OIS100MI", "CheckCustomer", params, callback)
  }

  /**
  * @validateWarehouse - Check if warehouse is valid
  * @params - warehouse
  * @returns - boolean
  */ 
  boolean validateWarehouse(String warehouse){
    DBAction readQuery = database.table("MITWHL")
      .index("00")
      .selection("MWWHLO")
      .build()
    DBContainer readContainer = readQuery.getContainer()
    readContainer.set("MWCONO", inCONO)
    readContainer.set("MWWHLO", warehouse)
    if (readQuery.read(readContainer)) {
      return true
    } else {
      return false
    }
  }

    /**
  * @getwarehouseName - Gets warehouse name
  * @params - WHLO
  * @returns - 
  */
  void getwarehouseName(String whlo) {
    if (!whlo.isBlank()) {
      DBAction query = database.table("MITWHL").index("00").selection("MWWHNM").build()
      DBContainer container = query.getContainer()
      container.set("MWCONO", inCONO)
      container.set("MWWHLO", whlo)
      if (!query.read(container) ){
        whnm610=""
      }
      else{
        whnm610 = container.get("MWWHNM").toString().trim()
      }
    }
  }

    /**
  * @getRouteDescription - Gets warehouse name
  * @params - WHLO
  * @returns - 
  */
  void getRouteDescription(String route) {

    if (!route.isBlank()) {
      DBAction query = database.table("DROUTE").index("00").selection("DRTX40").build()
      DBContainer container = query.getContainer()
      container.set("DRCONO", inCONO)
      container.set("DRROUT", route)
      if (!query.read(container) ){
        inTX40=""
      }
      else{
        inTX40 = container.get("DRTX40").toString().trim()
      }
    }
  }

   /**
  * @retrieveCodeACR - Gets ITDS/FUDS/CFI1 from MITMAS
  * @params -
  * @returns -
  */
  void retrieveCodeACR(String keyValue){
    DBAction queryCSYTAB = database.table("CSYTAB").index("20").selection("CTTX15").build()
    DBContainer containerCSYTAB = queryCSYTAB.getContainer()
    containerCSYTAB.set("CTCONO", inCONO)
    containerCSYTAB.set("CTSTCO", "CFI1")
    containerCSYTAB.set("CTSTKY", keyValue)

    queryCSYTAB.readAll(containerCSYTAB,3,1, { DBContainer container ->
      codeMarqueACR=container.get("CTTX15").toString().trim()
    })
  }

}