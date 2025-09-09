/****************************************************************************************
 Extension Name: EXT015MI/UpdStockDate
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELO
 Date: 2025-01-24
 Description:
 * Add item into the table EXT015. 
    
 Revision History:
 Name                       Date             Version          Description of Changes
 Tovonirina ANDRIANARIELO   2025-01-27       1.0              Initial Release
 Tovonirina ANDRIANARIELO   2025-09-08       1.1              XtendM3 validation
******************************************************************************************/
import java.time.LocalDateTime
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit

public class UpdStockDate extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  // all the input variables
  private int inCONO
  private String inDIVI
  private String inFACI
  private String inWHLO
  private String inITNO
  private int inIDDT // first issue date
  private int inODDT // last issue date
  private Double inALQT // quantity in stock
  private int waitingPeriod = 12 // Minimum number of months without sales
  private String ext017File = "MITMAS"
  private int ext017CONT // counter of warehouse with item in stock
  private int ext017DAT1 // counter of warehouse with item in stock
  private int ext017DAT2 // counter of warehouse with item in stock
  private String ext018File = "MITFAC"
  private String ext019File = "MITBAL"
  private int ext019DAT1 //  first restocking date
  private int ext019DAT2 // date when stock became zero
  private int ext019IDDT // first issue date on EXT019
  private int ext019ODDT // last issue date on EXT019
  
  public UpdStockDate(MIAPI mi, ProgramAPI program, DatabaseAPI database, MICallerAPI miCaller) {
    this.mi = mi
    this.program = program
    this.database = database
    this.miCaller = miCaller
  }

  public void main() {
    // validate input variables
    if (!validateInputVariables()) {
      return
    }
    // get the current date
    LocalDate currentDate = LocalDate.now()
    int formattedCurrentDate = currentDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
    // get the dates on EXT019
    getEXT019Dates()
    // get the dates and counter on EXT017
    getEXT017Fields()
    // update the first stocking date if it match the conditions
    if(isFirstRestock()){
      updateEXT019DAT1()
      updateEXT018DAT1(inIDDT)
      if(
        ext017CONT == 0 && 
        ext017DAT2 != null && 
        isLargerThanWaitingPeriod(inIDDT, ext017DAT2)
      ){
        updateEXT017DAT1(inIDDT)
      }
      increEXT017Counter()
    }
    if(inALQT == 0){
    // update the date when stock became zero
      updateEXT019DAT2(formattedCurrentDate)
      decreEXT017Counter()
      // get the latest changes on EXT017
      getEXT017Fields()
      // test if the counter on EXT017 is zero
      if(ext017CONT == 0){
        updateEXT017DAT2(formattedCurrentDate)
      }
    }
  }
  
  /**
   * @description - update the first stocking date if it match the conditions
   * @params - 
   * @returns - void
   */
  void updateEXT019DAT1(){
    // update the first date in stock if it is the first restock after the waiting period
    Map<String, String> ext019Params = [
      'CONO': "${inCONO}".toString(), 
      'DIVI': "${inDIVI}".toString(),
      'FILE': "${ext019File}".toString(),
      'WHLO': "${inWHLO}".toString(), 
      'ITNO': "${inITNO}".toString(), 
      'DAT1': "${inIDDT}".toString(), 
      'IDDT':"${inIDDT}".toString(), 
      'ODDT':"${inODDT}".toString() 
    ]
    if(!miCaller.call('EXT019MI', 'UpdItem', ext019Params,{})){
      mi.error("Item/Warehouse not found for params:  ${ext019Params} on updateEXT019DAT1")
      return
    }
  }

  /**
   * @description - update the date when stock became zero(0)
   * @params - 
   * @returns - void
   */
  void updateEXT019DAT2(int date){
    // update the date when the stock of the article became zero
    //update on the level item/warehouse
    Map<String, String> ext019Params = [
      'CONO': "${inCONO}".toString(),
      'DIVI': "${inDIVI}".toString(),
      'FILE': "${ext019File}".toString(),
      'WHLO': "${inWHLO}".toString(), 
      'ITNO': "${inITNO}".toString(), 
      'DAT2': "${date}".toString(), 
      'IDDT':"${inIDDT}".toString(), 
      'ODDT':"${inODDT}".toString() 
    ]
    if(!miCaller.call('EXT019MI', 'UpdItem', ext019Params, {})){
      mi.error("Item/Warehouse not found for params:  ${ext019Params} on updateEXT019DAT2")
      return
    }
  }

   /**
   * @description - update the date when stock became zero(0)
   * @params - 
   * @returns - void
   */
  void decreEXT017Counter(){
    int decreCounter = ext017CONT > 0 ? ext017CONT - 1 : 0;
    //update on the level item/division
    Map<String, String> ext017Params = [
      'CONO': "${inCONO}".toString(), 
      'FILE': "${ext017File}".toString(),
      'ITNO': "${inITNO}".toString(), 
      'CONT':"${decreCounter}".toString(), 
    ]
    if(inDIVI != null){
      ext017Params.put('DIVI',"${inDIVI}".toString())
    }
    if(!miCaller.call('EXT017MI', 'UpdItem', ext017Params,{})){
      mi.error("Item/division not found for params:  ${ext017Params}")
      return
    }
  }

  /**
   * @description - update the date when stock became zero(0)
   * @params - 
   * @returns - void
   */
  void increEXT017Counter(){
    int increCounter = ext017CONT != null ? ext017CONT + 1 : 1;
    //update on the level item/division
    Map<String, String> ext017Params = [
      'CONO': "${inCONO}".toString(), 
      'DIVI': "${inDIVI}".toString(),
      'FILE': "${ext017File}".toString(),
      'ITNO': "${inITNO}".toString(), 
      'CONT':"${increCounter}".toString(), 
    ]
    if(inDIVI != null){
      ext017Params.put('DIVI',"${inDIVI}".toString())
    }
    if(!miCaller.call('EXT017MI', 'UpdItem', ext017Params, {})){
      mi.error("Item/division not found for params:  ${ext017Params}")
      return
    }
  }

  /**
   * @description - update the first stocking date
   * @params - 
   * @returns - void
   */
  void updateEXT017DAT1(int currentDate){
    //update on the level item/division
    Map<String, String> ext017Params = [
      'CONO': "${inCONO}".toString(), 
      'DIVI': "${inDIVI}".toString(),
      'FILE': "${ext017File}".toString(),
      'ITNO': "${inITNO}".toString(), 
      'DAT1':"${currentDate}".toString(), 
    ]
    if(inDIVI != null){
      ext017Params.put('DIVI',"${inDIVI}".toString())
    }
    if(!miCaller.call('EXT017MI', 'UpdItem', ext017Params, {})){
      mi.error("Item/division not found for params:  ${ext017Params}")
      return
    }
  }

  /**
   * @description - update the date when stock became zero(0)
   * @params - 
   * @returns - void
   */
  void updateEXT017DAT2(int currentDate){
    //update on the level item/division
    Map<String, String> ext017Params = [
      'CONO': "${inCONO}".toString(), 
      'DIVI': "${inDIVI}".toString(),
      'FILE': "${ext017File}".toString(),
      'ITNO': "${inITNO}".toString(), 
      'DAT2':"${currentDate}".toString(), 
    ]
    if(inDIVI != null){
      ext017Params.put('DIVI',"${inDIVI}".toString())
    }
    if(!miCaller.call('EXT017MI', 'UpdItem', ext017Params, {})){
      mi.error("Item/division not found for params:  ${ext017Params} on updateEXT017DAT2")
      return
    }
  }

  /**
   * @description - update the first stocking date on EXT018 if it match the conditions
   * @params - 
   * @returns - void
   */
  void updateEXT018DAT1(int currentDate){
    // update the first date in stock if it is the first restock after the waiting period
    Map<String, String> ext018Params = [
      'CONO': "${inCONO}".toString(),
      'DIVI': "${inDIVI}".toString(),
      'FILE': "${ext018File}".toString(),
      'FACI': "${inFACI}".toString(), 
      'ITNO': "${inITNO}".toString(), 
      'DAT1': "${currentDate}".toString()
    ]
    if(!miCaller.call('EXT018MI', 'UpdItem', ext018Params,{})){
      mi.error("Item/facility not found for params:  ${ext018Params}")
      return
    }
  }

  /**
   * @description - Check if it is the first restock after the stock became zero
   * @params - 
   * @returns -boolean
   */
  boolean isFirstRestock(){
    if(
      ext019DAT2 != null && inODDT != null && ext019IDDT != null && 
      ext019DAT2 == inODDT && // the last out transaction is when the stock became 0 
      ext019IDDT < ext019DAT2 && // the last in transaction was before the stock became 0 
      isLargerThanWaitingPeriod(inIDDT, ext019DAT2) &&
      inALQT > 0 
     ){
      return true
    }
    return false
  }

  /**
   * @description - get the date when the stock become null
   * @params - 
   * @returns - void
   */
  void getEXT019Dates(){
    // get the date when the stock become null
    Map<String, String> ext019Params = [
      'CONO': "${inCONO}".toString(),
      'DIVI': "${inDIVI}".toString(),
      'FILE': "${ext019File}".toString(),
      'WHLO': "${inWHLO}".toString(),
      'ITNO': "${inITNO}".toString()
    ]
    miCaller.call('EXT019MI', 'GetItem', ext019Params, { Map<String, String> response ->
      int exDAT1 = response.DAT1 as int
      int exDAT2 = response.DAT2 as int
      int exIDDT = response.IDDT as int
      int exODDT = response.ODDT as int
      if (response.DAT2 == null) {
        mi.error("Item/Warehouse not found for params:  ${ext019Params} on getEXT019Dates")
        return
      } 
      if(exDAT1 != 0){
        ext019DAT1 = exDAT1
      }
      if(exDAT2 != 0){
        ext019DAT2 = exDAT2
      }
      if(exIDDT != 0){
        ext019IDDT = exIDDT
      }
      if(exODDT != 0){
        ext019ODDT = exODDT
      }
    })
  }
  

   /**
   * @description - get the counter of warehouse with item in stock
   * @params - 
   * @returns - void
   */
  void getEXT017Fields(){
    // get the date when the stock become null
    Map<String, String> ext017Params = [
      'CONO': "${inCONO}".toString(), 
      'DIVI': "${inDIVI}".toString(),
      'FILE': "${ext017File}".toString(),
      'ITNO': "${inITNO}".toString()
    ]
    if(inDIVI != null){
      ext017Params.put('DIVI',"${inDIVI}".toString())
    }
    miCaller.call('EXT017MI', 'GetItem', ext017Params, { Map<String, String> response ->
      if (response.ITNO == null) {
        mi.error("Item/division not found for params:  ${ext017Params}")
        return
      } 
      ext017CONT = response.CONT as int
      if((response.DAT1 as int) != 0){
        ext017DAT1 = response.DAT1 as int
      }
      if((response.DAT2 as int) != 0){
        ext017DAT2 = response.DAT2 as int
      }
    })
  }

  /**
   * @description - check if the period without sales is superior to the waiting period
   * @params - date of the restock and tha date when the stock became null
   * @returns - true/false
   */
  boolean isLargerThanWaitingPeriod(int restockDate, int nullDate){
    // tranform this int into date and compare is the difference in month
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    LocalDate restockLocalDate = LocalDate.parse(restockDate.toString(), formatter)
    LocalDate nullLocalDate = LocalDate.parse(nullDate.toString(), formatter)
    long monthsBetween = ChronoUnit.MONTHS.between(nullLocalDate,restockLocalDate)
    return monthsBetween >= waitingPeriod
  }

  /**
   * @description - Validates input variables
   * @params -
   * @returns - true/false
   */
  boolean validateInputVariables() {
    // Handling Company
    if (!mi.in.get('CONO')) {
      inCONO = (Integer) program.getLDAZD().CONO
    } else {
      inCONO = mi.in.get('CONO') as int
    }

    // Handling Division
    if (mi.in.get('DIVI') != null) {
      inDIVI = mi.in.get('DIVI') as String
      if(!validateDIVI(inCONO, inDIVI)){
        return false
      }
    }else{
      mi.error("La division est obligatoire")
      return false
    }

    // Handling Warehouse
    if (!mi.in.get('WHLO')) {
      mi.error("Warehouse is required")
      return false
    } else {
      inWHLO = mi.in.get('WHLO') as String
      if(!validateWarehouse(inWHLO)){
        return false
      }
    }

    // Handling Item number
    if (!mi.in.get('ITNO')) {
      mi.error("Le numéro d'article est obligatoire")
      return false
    } else {
      inITNO = mi.in.get('ITNO') as String
      if(!validateItemNumber(inCONO, inITNO)){
        return false
      }
    }
    // Handling first issue date
    if (mi.in.get('IDDT') == null) {
      mi.error('IDDT is required')
      return false
    } else {
      inIDDT = mi.in.get('IDDT') as int
      if(!validateDateFormat(inIDDT)){
        return false
      }
    }
    
    // Handling first issue date
    if (mi.in.get('ODDT') == null) {
      mi.error('ODDT is required')
      return false
    } else {
      inODDT = mi.in.get('ODDT') as int
      if(!validateDateFormat(inODDT)){
        return false
      }
    }
    //handling the allocated quantity
    if (mi.in.get('ALQT') != null) {
      inALQT = mi.in.get('ALQT') as Double
    }
    return true
  }

   /**
   * @description - Validates item number
   * @params -
   * @returns - true/false
   */
  boolean validateItemNumber(int sCONO, String sITNO) {
    // check if the item number is valid
    DBAction readQuery = database.table("MITMAS")
      .index("00")
      .selection("MMITNO")
      .build()
    DBContainer readContainer = readQuery.getContainer()
    readContainer.set("MMCONO", sCONO)
    readContainer.set("MMITNO", sITNO)
    if (readQuery.read(readContainer)) {
      return true
    } else {
      mi.error("Le numéro d'article ${sITNO} n'existe pas sur la compagnie ${sCONO}")
      return false
    }
  }

  /**
    * @description - Check if warehouse is valid
    * @params - warehouse
    * @returns - boolean
    */ 
  public boolean validateWarehouse(String warehouse){
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
      mi.error("Warehouse do not exist")
      return false
    }
  }

  /**
    * @description - Check if date is valid
    * @params - date,format
    * @returns - boolean
    */ 
  public boolean validateDateFormat(int input){
    if(input == null || input == 0){
      return true
    }
    String date = input.toString()
    try {
      LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyyMMdd"))
      return true
    } catch (DateTimeParseException e) {
      return false
    }
  }

  /**
   * Validate the Division (DIVI) against the CMNDIV table.
   * @return true if valid, false otherwise
   */
  private boolean validateDIVI(int cono, String divi) {
    DBAction dbaCMNDIV = database.table("CMNDIV").index("00").build()
    DBContainer conCMNDIV = dbaCMNDIV.getContainer()
    conCMNDIV.set("CCCONO", cono)
    conCMNDIV.set("CCDIVI", divi)
    if (!dbaCMNDIV.read(conCMNDIV)){
      mi.error("Division: " + divi.toString() + " does not exist in Company " + cono)
      return false
    }
    return true
  }
}
