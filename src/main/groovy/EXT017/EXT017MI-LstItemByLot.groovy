/****************************************************************************************
 Extension Name: EXT017MI/LstItemByLot
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELo
 Date: 2025-04-15
 Description:
 * List items by lot from the table EXT017. 
    
 Revision History:
 Name                       Date             Version          Description of Changes
 Tovonirina ANDRIANARIELO   2025-01-22       1.0              Initial Release
 Tovonirina ANDRIANARIELO   2025-06-17       2.0              Retrieve rotation and couverture rates in this transaction
 Tovonirina ANDRIANARIELO   2025-07-02       2.1              Change logic to retrieve rotation and couverture rate
 Tovonirina ANDRIANARIELO   2025-07-15       2.2              Prioritize the 12 months with sold in the rotation rules
 Tovonirina ANDRIANARIELO   2025-07-17       2.3              new rules for the calculation of couverture rate
 Tovonirina ANDRIANARIELO   2025-07-08       2.4              XtendM3 validation
 Tovonirina ANDRIANARIELO   2025-11-12       3.0              Correction after validation submission
******************************************************************************************/
public class LstItemByLot extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  // all the input variables
  private int inCONO    // Company
  private String inDIVI // Division
  private String inFILE // File
  // limit the number of records to be read
  private int inNUMB // Number of records to be read
  private String inFROM // From ITNO
  private String inITTO   // To ITNO
  public LoggerAPI logger

  public LstItemByLot(MIAPI mi, ProgramAPI program, DatabaseAPI database,LoggerAPI logger) {
    this.mi = mi
    this.program = program
    this.database = database
    this.logger=logger
  }

  public void main() {
    // validate input variables
    if (!validateInputVariables()) {
      return
    }

    //limit the number of records to be read
    int maxRecords = inNUMB <= 0 || inNUMB >= 5000 ? 5000: inNUMB
    int nrOfKeys = 3
    ExpressionFactory expression = database.getExpressionFactory("EXT017")
    expression = expression.ge("EXITNO", inFROM).and(expression.le("EXITNO", inITTO)) 
    DBAction query = database.table("EXT017")
      .index("00")
      .matching(expression)
      .selection("EXDIVI", "EXITNO","EXDAT1","EXDAT2", "EXRGDT", "EXCONO", "EXCRAT", "EXRRAT")
      .build()

    DBContainer container = query.getContainer()
    container.set("EXCONO", inCONO)
    container.set("EXFILE", inFILE)
    container.set("EXDIVI", inDIVI)

    Closure < ? > listRecords = {
      DBContainer data ->
      mi.outData.put("CONO", data.("EXCONO").toString())
      mi.outData.put("DIVI", data.("EXDIVI").toString())
      mi.outData.put("ITNO", data.("EXITNO").toString())
      mi.outData.put("DAT1", data.("EXDAT1").toString())
      mi.outData.put("DAT2", data.("EXDAT2").toString())
      mi.outData.put("RGDT", data.("EXRGDT").toString())
      mi.outData.put("RRAT", data.("EXRRAT").toString())
      mi.outData.put("CRAT", data.("EXCRAT").toString())
      mi.write()
    }
    if (!query.readAll(container, nrOfKeys, maxRecords, listRecords)) {
      mi.error("Record(s) does not exist.")
      return
    }
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
    if (mi.in.get('DIVI')!= null) {
      String divi = mi.in.get('DIVI')
      inDIVI = !divi.isEmpty()?divi:null
    }else{
      mi.error('DIVI is mandatory')
      return false
    }

    // Handling FROM Item number
    if(mi.in.get('FROM') == null) {
      mi.error('FROM ITNO is mandatory')
      return false
    }else {
      inFROM = mi.in.get('FROM') as String
    }
    // Handling TO Item number
    if(mi.in.get('ITTO') == null) {
      mi.error('TO ITNO is mandatory')
      return false
    }else {
      inITTO = mi.in.get('ITTO') as String
    }
    // Handling Number of records to be read
    if(mi.in.get('NUMB') == null) {
      mi.error('Number of records to be read is mandatory')
      return false
    }else {
      inNUMB = mi.in.get('NUMB') as int
    }
    //handling FILE
    if(mi.in.get('FILE') != null) {
      inFILE = mi.in.get('FILE') as String
    }else {
      inFILE = "MITMAS"
    }
    return true
  }
}
