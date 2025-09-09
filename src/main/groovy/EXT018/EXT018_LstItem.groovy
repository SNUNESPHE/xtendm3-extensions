/****************************************************************************************
 Extension Name: EXT018MI/LstItem
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELo
 Date: 2025-01-22
 Description:
 * List items from the table EXT018. 
    
 Revision History:
 Name                       Date             Version          Description of Changes
 Tovonirina ANDRIANARIELO   2025-01-22       1.0              Initial Release
 Tovonirina ANDRIANARIELO   2025-09-08       1.1              XtendM3 validation
******************************************************************************************/
public class LstItem extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  // all the input variables
  private int inCONO  // Company
  private String inDIVI // Division
  private String inFACI // Facility
  private String inITNO // Item number
  private String inFILE // File
  private int maxRecords
    
  public LstItem(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
    this.mi = mi
    this.program = program
    this.database = database
  }

  public void main() {
    maxRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000 ? 10000: mi.getMaxRecords()
    // validate input variables
    if (!validateInputVariables()) {
      return
    }
    int nrOfKeys = 1
    ExpressionFactory expression = database.getExpressionFactory("EXT018");
    expression = expression.eq("EXCONO", String.valueOf(inCONO))
    expression = expression.and(expression.eq("EXFILE", inFILE))
    if(inDIVI != null && !inDIVI.isEmpty()){
      expression = expression.and(expression.eq("EXDIVI", inDIVI))
    }
    if(inFACI != null && !inFACI.isEmpty()){
      expression = expression.and(expression.eq("EXFACI", inFACI))
    }
    if(inITNO != null && !inITNO.isEmpty()){
      expression = expression.and(expression.eq("EXITNO", inITNO))
    }
    DBAction query = database.table("EXT018")
      .index("00")
      .matching(expression)
      .selection("EXCONO", "EXDIVI", "EXFACI", "EXITNO", "EXFILE","EXDAT1","EXRGDT","EXRGTM","EXCHID","EXCHNO","EXLMDT")
      .build()

    DBContainer container = query.getContainer()
    container.set("EXCONO", inCONO)
    if(query.readAll(container, nrOfKeys, maxRecords,outData)){
      mi.error("No data found")
      return
    }
  }

  Closure<?> outData = { DBContainer dbContainer ->
    mi.outData.put("CONO", dbContainer.get("EXCONO") as String)
    mi.outData.put("DIVI", dbContainer.get("EXDIVI") as String)
    mi.outData.put("FACI", dbContainer.get("EXFACI") as String)
    mi.outData.put("ITNO", dbContainer.get("EXITNO") as String)
    mi.outData.put("FILE", dbContainer.get("EXFILE") as String)
    mi.outData.put("DAT1", dbContainer.get("EXDAT1") as String)
    mi.outData.put("RGDT", dbContainer.get("EXRGDT") as String)
    mi.outData.put("RGTM", dbContainer.get("EXRGTM") as String)
    mi.outData.put("CHID", dbContainer.get("EXCHID") as String)
    mi.outData.put("CHNO", dbContainer.get("EXCHNO") as String)
    mi.outData.put("LMDT", dbContainer.get("EXLMDT") as String)
    mi.write()
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
    // Handling Facility
    if (mi.in.get('FACI')) {
      inFACI = mi.in.get('FACI') as String
    }
    // Handling Item number
    if(mi.in.get('ITNO')) {
      inITNO = mi.in.get('ITNO') as String
    }
    //handling FILE
    if(mi.in.get('FILE')) {
      inFILE = mi.in.get('FILE') as String
    }else{
      inFILE = "MITFAC"
    }
    return true
  }
}
