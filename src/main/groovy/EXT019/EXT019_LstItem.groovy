/****************************************************************************************
 Extension Name: EXT019MI/LstItem
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELo
 Date: 2025-01-22
 Description:
 * List items from the table EXT019. 
    
 Revision History:
 Name                       Date             Version          Description of Changes
 Tovonirina ANDRIANARIELO   2025-01-22       1.0              Initial Release
 Tovonirina ANDRIANARIELO   2025-09-08       1.1              XtendM3 validation
 Tovonirina ANDRIANARIELO   2025-09-22       1.2              XtendM3 validation, use indexes
******************************************************************************************/
public class LstItem extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  // all the input variables
  private int inCONO // Company
  private String inDIVI // Division
  private String inWHLO // Warehouse
  private String inITNO // Item number
  private String inFILE // File
  private int maxRecords

  public LstItem(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
    this.mi = mi
    this.program = program
    this.database = database
  }

  public void main() {
    String index = "00"
    int nrOfKeys = 2
    maxRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000 ? 10000: mi.getMaxRecords()
    // validate input variables
    if (!validateInputVariables()) {
      return
    }

    // Cache validation results to avoid repeated function calls
    boolean hasDIVI = validateField(inDIVI)
    boolean hasWHLO = validateField(inWHLO)
    boolean hasITNO = validateField(inITNO)

    // Determine index based on field combinations
    if (hasDIVI && !hasWHLO && hasITNO) {
      index = "10"
    } else if (hasWHLO) {
      index = "20"
    } else if (hasITNO) {
      index = "30"
    }
   
    DBAction query = database.table("EXT019")
      .index(index)
      .selection("EXCONO", "EXWHLO", "EXITNO", "EXFILE","EXDAT1","EXDAT2","EXIDDT", "EXODDT","EXRGDT","EXRGTM","EXCHID","EXCHNO","EXLMDT")
      .build()

    DBContainer container = query.getContainer()
    container.set("EXCONO", inCONO)
    container.set("EXFILE", inFILE)
     // Count the number of keys
    if (hasDIVI) {
      nrOfKeys++
      container.set("EXDIVI", inDIVI)
    }
    if (hasWHLO) {
      nrOfKeys++
      container.set("EXWHLO", inWHLO)
    }
    if (hasITNO) {
      nrOfKeys++
      container.set("EXITNO", inITNO)
    }

    if(!query.readAll(container, nrOfKeys, maxRecords,outData)){
      mi.error("No data found")
      return
    }
  }

  Closure<?> outData = { DBContainer dbContainer ->
    mi.outData.put("CONO", dbContainer.get("EXCONO") as String)
    mi.outData.put("DIVI", dbContainer.get("EXDIVI") as String)
    mi.outData.put("WHLO", dbContainer.get("EXWHLO") as String)
    mi.outData.put("ITNO", dbContainer.get("EXITNO") as String)
    mi.outData.put("FILE", dbContainer.get("EXFILE") as String)
    mi.outData.put("DAT1", dbContainer.get("EXDAT1") as String)
    mi.outData.put("DAT2", dbContainer.get("EXDAT2") as String)
    mi.outData.put("IDDT", dbContainer.get("EXIDDT") as String)
    mi.outData.put("ODDT", dbContainer.get("EXODDT") as String)
    mi.outData.put("RGDT", dbContainer.get("EXRGDT") as String)
    mi.outData.put("RGTM", dbContainer.get("EXRGTM") as String)
    mi.outData.put("CHID", dbContainer.get("EXCHID") as String)
    mi.outData.put("CHNO", dbContainer.get("EXCHNO") as String)
    mi.outData.put("LMDT", dbContainer.get("EXLMDT") as String)
    mi.write()
  }

  /**
   * @description - Validates a field
   * @params - fieldValue
   * @returns - true/false
   */
  boolean validateField(String fieldValue) {
    if (fieldValue == null || fieldValue.isEmpty()) {
      return false
    }
    return true
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
    if (mi.in.get('DIVI')) {
      inDIVI = mi.in.get('DIVI') as String
    }
    // Handling Warehouse
    if (mi.in.get('WHLO')) {
      inWHLO = mi.in.get('WHLO') as String
    }
    // Handling Item number
    if(mi.in.get('ITNO')) {
      inITNO = mi.in.get('ITNO') as String
    }
    //handling FILE
    if(mi.in.get('FILE')) {
      inFILE = mi.in.get('FILE') as String
    }else{
      inFILE = "MITBAL"
    }
    return true
  }
}
