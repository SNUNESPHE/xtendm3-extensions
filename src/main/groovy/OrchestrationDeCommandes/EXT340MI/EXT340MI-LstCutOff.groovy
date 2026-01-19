/**
*  @Name: EXT340MI.LstCutOff
*  @Description: Get item info, get route info, get availability of stock, Get salesorders info,
*  @Authors: Kenylen Motean
*/

/**
* CHANGELOGS
* Version    Date    User        Description
* 1.0.0      150125  KMOTEAN     Initial Release
* 1.0.1      140126  KMOTEAN     Adjusted function calculateNextDeliveryDate to take date according to timezone

*/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.LocalDate

public class LstCutOff extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private final MICallerAPI miCaller
  private int inCONO
  private String inDIVI
  private String inCUNO
  private int inOALT=0
  private String ortp610=""
  private String whlo610=""
  private String splm610=""
  private String padl610=""
  private String bcko610=""
  private String stat610=""
  private String achk610=""
  private List<String> arrWHLO=[]
  private List<Map<String, String>> listWarehousePlaceOfLoadAndDesc= []
  private List<String> arrUniqueSDES=[]
  private List<Map<String, String>> listRoutes= []
  private List<Map<String, String>> listRoutesDetails= []
  private List<Map<String, String>> listRoutesAllDetails= []
  private List<Map<String, String>>  whloDetailsMMS059= []
  private boolean  outputPadlFlag= false
  private boolean  sortFlag= true
  private String retievedTOMU=""
  private String sTIME
  private String sDATE
  private String timezoneDATE
  private String divisionName
  private List<Map<String, String>> listPrices= []
  private String currentDate
  private String retrievedA030
  private String retrievedA830
  private String retrievedA121
  private String retrievedN096

  
  public LstCutOff(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
  }
  
  public void main() {
    inCONO = mi.in.get("CONO") as Integer == null ? program.LDAZD.get("CONO") as Integer : mi.in.get("CONO") as Integer
    inDIVI = mi.in.get("DIVI") == null ? program.LDAZD.get("DIVI")  : mi.in.get("DIVI")
    inCUNO = mi.inData.get("CUNO") == null ? "" : mi.inData.get("CUNO").trim()
    
    if(!validateCONO()){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('LNST', "error")
      mi.outData.put('LNTX', "Impossible")
      mi.outData.put('TEXT', "invalidConoDivi")
      mi.write()
      return
    }
    

    if(!validateCUNO()){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('LNST', "error")
      mi.outData.put('LNTX', "invalidGarage")
      mi.write()
      return
    }
    
    if(stat610!="20"){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('LNST', "error")
      mi.outData.put('LNTX', "invalidGarage")
      mi.write()
      return
    }

    LocalDate date = LocalDate.now()
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd") 
    currentDate = date.format(formatter)

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
      mi.outData.put('LNST', "error")
      mi.outData.put('LNTX', "Impossible")
      mi.outData.put('TEXT', "Default warehouse for customer not defined")
      mi.write()
      return
    }
    
    if(arrWHLO.isEmpty()){
      mi.outData.put('CONO', inCONO.toString())
      mi.outData.put('DIVI', inDIVI)
      mi.outData.put('CUNO', inCUNO)
      mi.outData.put('LNST', "error")
      mi.outData.put('LNTX', "Impossible")
      mi.outData.put('TEXT', "Order type WAV for supply model not defined")
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
      mi.outData.put('LNST', "error")
      mi.outData.put('LNTX', "Impossible")
      mi.outData.put('TEXT', "No routes found")
      mi.write()
      return
    }
    
    retrieveRouteInfoFromDROUTE()//Retrieve routes info for available routes
    
    retrieveRouteInfoFromDROUDI()//Retrieve other routes info for available routes
    
    listRoutesAllDetails.each { record1 ->
      if(record1.OBV2=="P01" || record1.OBV2=="P02"){
        record1.PRIO="0"
      }
      else if (record1.OBV2=="Prio9"){
        record1.PRIO="1"
      }
      else{
        record1.PRIO="2"
      }
    }

    listRoutesAllDetails.each { record1 ->
      getTIME()//gets current time

      String nextDeliveryDateDate=calculateNextDeliveryDate(record1.DODW, record1.LILH,record1.LILM,record1.ARDY)
      record1.CODZ=nextDeliveryDateDate
      record1.COHZ=record1.ARHH.padLeft(2,'0')+record1.ARMM.padLeft(2,'0')
      record1.IDRO=record1.ROUT+"-"+record1.RODN
      record1.ORTP=ortp610
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
    
    listRoutesAllDetails = listRoutesAllDetails.sort { a, b ->
      a.IDWH <=> b.IDWH 
    }

    getCugexFields()

    getN096()

    String dlam=""
    if (retrievedA030.equals("00") || retrievedA030.equals("03") || retrievedA030.equals("04") || retrievedA030.equals("06")) {
      dlam = retrievedA121
    } else if (retrievedA030.equals("02") || retrievedA030.equals("05")) {
      dlam = retrievedN096
    } else if (retrievedA030.equals("01")) {
      dlam = Integer.parseInt(retrievedA121) == 0 ? retrievedN096 : retrievedA121
    } else {
      dlam="0"
    }

    if(dlam=="" || dlam==null){
      dlam="0"
    }

    if(inOALT==0){
      listRoutesAllDetails.each { record1 ->
        mi.outData.put('CONO', inCONO.toString())
        mi.outData.put('DIVI', inDIVI)
        mi.outData.put('LNST', "ack")
        mi.outData.put('LNTX', "complete")
        mi.outData.put('ROUT',record1.ROUT)
        mi.outData.put('RODN',record1.RODN)
        mi.outData.put('TX40',record1.TX40)
        mi.outData.put('CUNO',record1.CUNO)
        mi.outData.put('WHNM',record1.WHNM)
        mi.outData.put('WHLO',record1.WHLO)

        String rawDate = record1.CODZ
        String formattedDate =""
        try {
          DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
          DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("ddMMyyyy")

          LocalDate currentdate = LocalDate.parse(rawDate, inputFormatter)
          formattedDate = currentdate.format(outputFormatter)

        } catch (DateTimeParseException e) {
        }

        mi.outData.put('CODZ',formattedDate)
        mi.outData.put('COHZ',record1.COHZ)
        mi.outData.put('IDEX', record1.IDEX)
        mi.outData.put('IDWH', record1.IDWH.toString())
        mi.outData.put('IDEX', record1.IDEX)
        mi.outData.put('A030', retrievedA030)
        
        if(record1.MMDL=="CPT" || record1.MMDL=="ENL"){
          mi.outData.put('A830', "0")
          mi.outData.put('DLAM', "0")
          mi.outData.put('LNAM', "0")
        }
        else{
          mi.outData.put('A830', retrievedA830)
          mi.outData.put('DLAM', dlam)

          double lnam=readOOLINE(record1.WHLO, record1.ROUT, record1.RODN, retrievedA030)
          mi.outData.put('LNAM', lnam.toString())
        }
        

        if(record1.OBV2=="Prio9"){
          mi.outData.put('MODL', "J+1")
        }
        else{
          mi.outData.put('MODL', record1.OBV2)
        }
        
        mi.outData.put('CODD', record1.ARDY)
        mi.outData.put('COHH', record1.LILH.padLeft(2, '0')+" "+record1.LILM.padLeft(2, '0'))

        mi.write()
      }
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
    miCaller.setListMaxRecords(100)
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
    
    queryMITWHL.readAll(containerMITWHL,1,100, { DBContainer container ->
      
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
    
    DBAction queryDRODPR = database.table("DRODPR").index("00").matching(expression).selection("DOROUT","DOEDES","DOOBV1").build()
    DBContainer containerDRODPR = queryDRODPR.getContainer()
    containerDRODPR.set("DOCONO", inCONO)
    
    queryDRODPR.readAll(containerDRODPR,1,100, { DBContainer container ->
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

      queryDRODPR2.readAll(containerDRODPR2,1,100, { DBContainer container2 ->
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
    
    queryDROUTE.readAll(containerDROUTE,1,100, { DBContainer container ->
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
    
    queryDROUDI.readAll(containerDROUDI,1,40, { DBContainer container ->
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
        }
      }
    }
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
  * @findMultiple - finds multiple of TOMU to allocate
  * @params -remainingQuantity,maxItemsToAllocate,TOMU
  * @returns -multiple
  */
  int findMultiple(double remainingQuantity,double maxItemsToAllocate, double TOMU) {
      double multiple = (((remainingQuantity + TOMU - 1) / TOMU) as Integer) * TOMU

      // Ensure the multiple is <= B and >= A
      if (multiple > maxItemsToAllocate) {
          return maxItemsToAllocate as int
      } else {
          return multiple as int
      }
  }


      /**
  * @ois320MIGetLine - Retrieves prix from EXT320MI-GetLine
  * @params - CONO,  CUNO,  ITNO
  * @returns -
  */
  void ois320MIGetLine(String CONO,String CUNO, String ITNO) {
    Map<String, String> paramsOIS320MIGetLine = ["CONO": "${CONO}".toString(),"CUNO": "${CUNO}".toString(), "ITNO": "${ITNO}".toString(), "ORQA": "1", "ORTP": "WAV", "EOUT": "1"]
    Closure<?> callbackOIS320MIGetLine= { Map<String, String> responseOIS320MIGetLine ->

        if (responseOIS320MIGetLine != null) {
            if (responseOIS320MIGetLine.containsKey("error") && responseOIS320MIGetLine.error != null) {

              return
            } else {
                listPrices.add([
                  NETP:responseOIS320MIGetLine.NETP.toString().trim(),
                  SAPR:responseOIS320MIGetLine.SAPR.toString().trim(),
                  SACD:responseOIS320MIGetLine.SACD.toString().trim(),
                  DIP1:responseOIS320MIGetLine.DIP1.toString().trim(),
                  DIP2:responseOIS320MIGetLine.DIP2.toString().trim(),
                  DIP3:responseOIS320MIGetLine.DIP3.toString().trim(),
                  DIP4:responseOIS320MIGetLine.DIP4.toString().trim(),
                  DIP5:responseOIS320MIGetLine.DIP5.toString().trim(),
                  DIP6:responseOIS320MIGetLine.DIP6.toString().trim(),
                  TX81:responseOIS320MIGetLine.TX81.toString().trim(),
                  TX82:responseOIS320MIGetLine.TX82.toString().trim(),
                  TX83:responseOIS320MIGetLine.TX83.toString().trim(),
                  TX84:responseOIS320MIGetLine.TX84.toString().trim(),
                  TX85:responseOIS320MIGetLine.TX85.toString().trim(),
                  TX86:responseOIS320MIGetLine.TX86.toString().trim(),
                  SPUN:responseOIS320MIGetLine.SPUN.toString().trim()

                  
                ])
            }
        }
    }
    miCaller.call("OIS320MI", "GetPriceLine", paramsOIS320MIGetLine, callbackOIS320MIGetLine)
  }

      /**
  * @getCugexFields - Retrieves prix from EXT320MI-GetLine
  * @params - CONO,  CUNO,  ITNO
  * @returns -
  */
  void getCugexFields() {
    Map<String, String> paramsCUGEX =["FILE": "OCUSMA", "PK01": inCUNO]
    Closure<?> callbackCUGEX= { Map<String, String> responseCUGEX ->

        if (responseCUGEX != null) {
            if (responseCUGEX.containsKey("error") && responseCUGEX.error != null) {

              return
            } else {

              retrievedA030=responseCUGEX.A030.toString().trim()
              retrievedA830=responseCUGEX.A830.toString().trim()
              retrievedA121=responseCUGEX.A121.toString().trim()

              if(retrievedA830=="" || retrievedA830==null){
                retrievedA830="0"
              }
            }
        }
    }
    miCaller.call("CUSEXTMI", "GetFieldValue", paramsCUGEX, callbackCUGEX)
  }


  /**
    * @getN096 - Get CUGEX field with CUSEXTMI
    * @params - FILE : CMNDIV
    * @params - PK01 : Division
    * @returns - N096 : Shipping cost
    */
  String getN096() {
    Map<String, String> paramsCUGEX = ["FILE": "CMNDIV", "PK01": inDIVI]
    Closure<?> callbackCUGEX= { Map<String, String> responseCUGEX ->
      if (responseCUGEX != null) {
          if (responseCUGEX.containsKey("error") && responseCUGEX.error != null) {
            return
          } else {
            retrievedN096=responseCUGEX.N096.toString().trim()
          }
      }
    }
    miCaller.call("CUSEXTMI", "GetFieldValue", paramsCUGEX, callbackCUGEX)
  }


  /**
    * @readOOLINE - Read LNAM from OOLINE
    * @params - WHLO
    * @params - ROUT
    * @params - RODN
    * @params - A030
    * @returns - LNAM
    */
  double readOOLINE(String whlo, String rout, String rodn, String a030) {
    String currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))

    ExpressionFactory expression = database.getExpressionFactory("OOLINE")
    expression = expression.eq("OBCUNO", inCUNO)
    .and(expression.lt("OBORST", "90"))
    .and(expression.gt("OBORST", "32"))
    .and(expression.eq("OBCODZ", currentDate))
    .and(expression.like("OBITNO", "M%"))

    if (a030.trim().equals("02")) {
      expression=expression.and(expression.eq("OBWHLO", whlo.trim())).and(expression.eq("OBROUT", rout.trim())).and(expression.eq("OBRODN", rodn.trim()))
    }
    
    DBAction query = database.table("OOLINE").index("00").matching(expression).selection("OBLNA2", "OBROUT", "OBRODN", "OBWHLO").build()
    DBContainer container = query.getContainer()
    container.set("OBCONO", inCONO)
    double totalLNAM = 0

    query.readAll(container, 1, 250, { DBContainer data ->
        double lnamValue = Double.parseDouble(data.get("OBLNA2").toString())
        if(lnamValue>0){
          totalLNAM += lnamValue
        }
    })

    return totalLNAM.round(2)
    
    
  }

}