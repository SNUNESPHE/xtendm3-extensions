/**
*  @Name: EXT310MI.Update
*  @Description: Gets ItemNumber and runs MMS310MI-Update
*  @Authors: Kenylen Motean
*/

/**
* CHANGELOGS
* Version    Date    User        Description
* 1.0.0      190525  KMOTEAN     Initial Release
* 1.1.0      191225  KMOTEAN     Changed to check code Marque 3
*/

import java.time.LocalDateTime

public class Update extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private final MICallerAPI miCaller
  private int inCONO
  private String inWHLO
  private String inWHSL
  private String inPOPN
  private String inA130
  private int inSTQI
  private int inSTAG
  private String sCFI1 = ""
  
  private List<String> listItemsITDS=[]
  private String correctITNO=""
  private Boolean correctITNOFound=false
  private List<String> listItemsMMS025=[]
  private List<String> listCodeMarqueCugex=[]
  private String retrievedCorrectCFI1M3=""
  private Boolean codeMarqueM3Found=false
  private String errorMessage=""
  
  public Update(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
  }
  
  public void main() {
    inCONO = mi.in.get("CONO") as Integer == null ? program.LDAZD.get("CONO") as Integer : mi.in.get("CONO") as Integer
    inWHLO = mi.inData.get("WHLO") == null ? "" : mi.inData.get("WHLO").trim()
    inWHSL = mi.inData.get("WHSL") == null ? "" : mi.inData.get("WHSL").trim()
    inPOPN = mi.inData.get("POPN") == null ? "" : mi.inData.get("POPN").trim()
    inA130 = mi.inData.get("A130") == null ? "" : mi.inData.get("A130").trim()
    inSTQI = mi.in.get("STQI") == null ? 0 as Integer: mi.in.get("STQI") as Integer
    inSTAG = mi.in.get("STAG") == null ? 0 as Integer : mi.in.get("STAG") as Integer
    
    LocalDateTime  dateTime = LocalDateTime.now()
    
    if(!validateCONO() ){
      mi.outData.put("CONO", inCONO.toString())
      mi.outData.put("WHLO", inWHLO)
      mi.outData.put("WHSL", inWHSL)
      mi.outData.put("POPN", inPOPN)
      mi.outData.put("A130", inA130)
      mi.outData.put("STQI", inSTQI.toString())
      mi.outData.put("STAG", inSTAG.toString())
      mi.outData.put("ERRO", errorMessage)
      mi.outData.put("DATE", dateTime.toString())
      return
    }

    if(!validateWHLO()){
      mi.outData.put("CONO", inCONO.toString())
      mi.outData.put("WHLO", inWHLO)
      mi.outData.put("WHSL", inWHSL)
      mi.outData.put("POPN", inPOPN)
      mi.outData.put("A130", inA130)
      mi.outData.put("STQI", inSTQI.toString())
      mi.outData.put("STAG", inSTAG.toString())
      mi.outData.put("ERRO", errorMessage)
      mi.outData.put("DATE", dateTime.toString())
      return
    }

    if(!validateWHSL()){
      mi.outData.put("CONO", inCONO.toString())
      mi.outData.put("WHLO", inWHLO)
      mi.outData.put("WHSL", inWHSL)
      mi.outData.put("POPN", inPOPN)
      mi.outData.put("A130", inA130)
      mi.outData.put("STQI", inSTQI.toString())
      mi.outData.put("STAG", inSTAG.toString())
      mi.outData.put("ERRO", errorMessage)
      mi.outData.put("DATE", dateTime.toString())
      return
    }

    if(!validateSTQI()){
      mi.outData.put("CONO", inCONO.toString())
      mi.outData.put("WHLO", inWHLO)
      mi.outData.put("WHSL", inWHSL)
      mi.outData.put("POPN", inPOPN)
      mi.outData.put("A130", inA130)
      mi.outData.put("STQI", inSTQI.toString())
      mi.outData.put("STAG", inSTAG.toString())
      mi.outData.put("ERRO", errorMessage)
      mi.outData.put("DATE", dateTime.toString())
      return
    }
    
    formatPOPN()
    
    searchITEM()
    
    if(!correctITNOFound){
      mi.outData.put("CONO", inCONO.toString())
      mi.outData.put("WHLO", inWHLO)
      mi.outData.put("WHSL", inWHSL)
      mi.outData.put("POPN", inPOPN)
      mi.outData.put("A130", inA130)
      mi.outData.put("STQI", inSTQI.toString())
      mi.outData.put("STAG", inSTAG.toString())
      mi.outData.put("ERRO", "La référence est inconnue")
      mi.outData.put("DATE", dateTime.toString())
      return
    }
    else if(!validateITNO()){
      mi.outData.put("CONO", inCONO.toString())
      mi.outData.put("WHLO", inWHLO)
      mi.outData.put("WHSL", inWHSL)
      mi.outData.put("POPN", inPOPN)
      mi.outData.put("A130", inA130)
      mi.outData.put("STQI", inSTQI.toString())
      mi.outData.put("STAG", inSTAG.toString())
      mi.outData.put("ERRO", errorMessage + " - " + correctITNO)
      mi.outData.put("DATE", dateTime.toString())
      return
    }
    else{
      if(!mms310miUpdate(inCONO.toString(),inWHLO,correctITNO,inWHSL,inSTQI.toString(),inSTAG.toString())){
        mi.outData.put("CONO", inCONO.toString())
        mi.outData.put("WHLO", inWHLO)
        mi.outData.put("WHSL", inWHSL)
        mi.outData.put("POPN", inPOPN)
        mi.outData.put("A130", inA130)
        mi.outData.put("STQI", inSTQI.toString())
        mi.outData.put("STAG", inSTAG.toString())
        mi.outData.put("ERRO", errorMessage)
        mi.outData.put("DATE", dateTime.toString())
        return
      }
      else{
        mi.outData.put("CONO", inCONO.toString())
        mi.outData.put("WHLO", inWHLO)
        mi.outData.put("WHSL", inWHSL)
        mi.outData.put("POPN", inPOPN)
        mi.outData.put("A130", inA130)
        mi.outData.put("STQI", inSTQI.toString())
        mi.outData.put("STAG", inSTAG.toString())
        mi.outData.put("ERRO", "")
        mi.outData.put("DATE", dateTime.toString())
        return
      }
    }

  }
  
  /**
  * @formatPOPN - Sanitizes POPN
  * @params -
  * @returns 
  */ 
  void formatPOPN(){
    inPOPN = inPOPN.replaceAll("\\s", "").replaceAll("[-/._#]", "").toUpperCase()
  }
  
    /**
  * @searchITEM - Search Item By ITDS, then ALWT=4
  * @params -
  * @returns -
  */
  void searchITEM(){    
    if(!correctITNOFound){
      searchRefNormalise()//search by ALWT=4
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
      searchItemByCFI1Partenaire()
      if(listCodeMarqueCugex.size() > 0)
      {
        retrieveCFI1(listItemsMMS025[0])
        if(listCodeMarqueCugex[0].trim() != sCFI1.trim())
        {
          correctITNOFound=false
          return
        }
        else
        {
          correctITNOFound=true
          correctITNO=listItemsMMS025[0]
        }

      }
      else
      {
        correctITNOFound=true
        correctITNO=listItemsMMS025[0]
      }
    } 
    // IF DIFFERENT ITEMS
    else {
      if(!searchItemByCFI1Partenaire()){
        correctITNOFound=false
        return
      }
      
      if(codeMarqueM3Found){
        //retrieveCFI1 for items
        ExpressionFactory expression = database.getExpressionFactory("MITMAS")
        expression = expression.in("MMITNO", listItemsMMS025 as String[])
    
        DBAction queryMITMAS = database.table("MITMAS").index("00").matching(expression).selection("MMCFI1","MMITNO").build()
        DBContainer containerMITMAS = queryMITMAS.getContainer()
        containerMITMAS.set("MMCONO", inCONO)
        
        int maxRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords()
        listItemsITDS=[]
        queryMITMAS.readAll(containerMITMAS,1,maxRecords, { DBContainer container ->
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
  * @searchItemByCFI1Partenaire - Search Item By CFI1 code marrque partenaire
  * @params -
  * @returns -
  */
  boolean searchItemByCFI1Partenaire(){
    if(inA130==""){
      return false
    }
    else{
      retrieveCodeMarqueM3(inCONO.toString(),inA130)
      if (listCodeMarqueCugex.isEmpty()) {
        codeMarqueM3Found=false
        return true
      }
      else if(!checkCorrectITNO(listCodeMarqueCugex[0]))
      {
        codeMarqueM3Found = false
        return false
      }
      else{
        codeMarqueM3Found=true
        retrievedCorrectCFI1M3=listCodeMarqueCugex[0]
        return true
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
    miCaller.setListMaxRecords(10000)
    miCaller.call("MMS025MI", "LstItem", paramsMMS025, callbackMMS025)
  }
  
  /**
  * @retrieveCodeMarqueM3 - Search codeMarqueM3 by using codeMarquePartenaire
  * @params -
  * @returns -
  */
  void retrieveCodeMarqueM3(String CONO,String A130) {
    listCodeMarqueCugex=[]

    //retrieveCFI1 for items
    ExpressionFactory expression = database.getExpressionFactory("CUGEX1")
    expression = expression.eq("F1A130", A130)

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
  * @mms310miUpdate - MMS310MI-Update
  * @params -
  * @returns -
  */
  boolean mms310miUpdate(String cono,String whlo,String itno,String whsl,String stqi,String stag){
    Map<String, String> paramsMMS310 = [
      "CONO":"${cono}".toString(),
      "WHLO":"${whlo}".toString(),
      "ITNO":"${itno}".toString(),
      "WHSL":"${whsl}".toString(),
      "STQI":"${stqi}".toString(),
      "STAG":"${stag}".toString()]
    Closure<?> callbackMMS310 =
    {
      Map<String, String> responseMMS310 ->
      if(responseMMS310.errorMessage != null){
        mi.error(responseMMS310.errorMessage)
        errorMessage=responseMMS310.errorMessage
        return false
      }
      return true
    }
    miCaller.call("MMS310MI","Update", paramsMMS310, callbackMMS310)
  }
  
  /**
  * @retrieveCFI1 - Search CFI1 from MITMAS
  * @params -
  * @returns -
  */
  void retrieveCFI1(String sITNO) {

    DBAction queryMITMAS = database.table("MITMAS").index("00").selection("MMCFI1").build()
    DBContainer containerMITMAS = queryMITMAS.getContainer()
    containerMITMAS.set("MMCONO", inCONO)
    containerMITMAS.set("MMITNO", sITNO)
          
    if(queryMITMAS.read(containerMITMAS))
    {
      sCFI1 = containerMITMAS.get("MMCFI1") as String
    }
  }


  /**
  * @checkCorrectITNO - Check if the codeMarque belongs to one of the ITNO
  * @params -
  * @returns -
  */
  boolean checkCorrectITNO(String sCFI1) {
    
    boolean flag = false
    ExpressionFactory expression = database.getExpressionFactory("MITMAS")
    expression = expression.eq("MMCFI1", sCFI1).and(expression.eq("MMITDS", inPOPN))
    
    DBAction queryMITMAS = database.table("MITMAS").index("00").matching(expression).selection("MMITNO").build()
    DBContainer containerMITMAS = queryMITMAS.getContainer()
    containerMITMAS.set("MMCONO", inCONO)
          
    queryMITMAS.readAll(containerMITMAS,1,1000, { DBContainer container ->
      if(listItemsMMS025.contains(container.get("MMITNO").toString().trim()))
      {
        flag = true
      }
      
    })
    return flag
  }
  /**
   * @validateCONO - Validates CONO/DIVI
   * @params -
   * @returns - true/false
   */
  Boolean validateCONO() {
    if (!inCONO.toString().isBlank()) {
      DBAction query = database.table("CMNCMP").index("00").selection().build()
      DBContainer container = query.getContainer()
      container.set("JICONO", inCONO)
      if(!query.read(container)){
        errorMessage="CONO ${inCONO} inexistant!"
        return false
      }
    }
    return true
  }
  
  /**
   * @validateWHLO - Validates WHLO
   * @params -
   * @returns - true/false
   */
  Boolean validateWHLO() {
    if (!inWHLO.toString().isBlank()) {
      DBAction query = database.table("MITWHL").index("00").selection().build()
      DBContainer container = query.getContainer()
      container.set("MWCONO", inCONO)
      container.set("MWWHLO", inWHLO)
      if(!query.read(container)){
        errorMessage="WHLO ${inWHLO} inexistant!"
        return false
      }
    }
    return true
  }
  
  /**
   * @validateWHSL - Validates WHSL
   * @params -
   * @returns - true/false
   */
  Boolean validateWHSL() {
    if (!inWHLO.toString().isBlank()) {
      DBAction query = database.table("MITPCE").index("00").selection().build()
      DBContainer container = query.getContainer()
      container.set("MSCONO", inCONO)
      container.set("MSWHLO", inWHLO)
      container.set("MSWHSL", inWHSL)
      if(!query.read(container)){
        errorMessage="Emplacement ${inWHSL} inexistant pour le depot ${inWHLO}!"
        return false
      }
    }
    return true
  }
  
  /**
   * @validateSTQI - Validates STQI
   * @params -
   * @returns - true/false
   */
  Boolean validateSTQI() {
    if (!inSTQI.toString().isBlank()) {
      if(inSTQI<0){
         errorMessage="Quantité stock physique doit etre supérieur à zéro"
         return false
      }
    }
    return true
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
          errorMessage="La référence est non gérée par ACR"
          return false
        }
      }
    }
    return true
  }
}