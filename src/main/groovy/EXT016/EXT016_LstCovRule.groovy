/****************************************************************************************
 Extension Name: EXT016MI/LstCovRule
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELO
 Date: 2025-01-28
 Description:
 * Add item into the table EXT016. 
    
 Revision History:
 Name                       Date             Version          Description of Changes
 Tovonirina ANDRIANARIELO   2025-01-28       1.0              Initial Release
 Tovonirina ANDRIANARIELO   2025-09-08       1.1              XtendM3 validation
******************************************************************************************/
import java.time.LocalDateTime
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

public class LstCovRule extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  // all the input variables
  private int inCONO
  private String inDIVI
  private String inFACI
  private int inTRCH
  private int maxRecords
  
  public LstCovRule(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
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
    ExpressionFactory expression = database.getExpressionFactory("EXT016");
    expression = expression.eq("EXCONO", String.valueOf(inCONO))
    if(inDIVI != null && !inDIVI.isEmpty()){
      expression = expression.and(expression.eq("EXDIVI", inDIVI))
    }
    if(inFACI != null && !inFACI.isEmpty()){
      expression = expression.and(expression.eq("EXFACI", inFACI))
    }
    if(inTRCH != null && inTRCH > 0){
      expression = expression.and(expression.eq("EXTRCH", String.valueOf(inTRCH)))
    }
    // get the current date
    int nrOfKeys = 1
    DBAction query = database.table("EXT016")
      .index("00")
      .matching(expression)
      .selection("EXCONO", "EXDIVI", "EXFACI", "EXTRCH","EXRATE", "EXRGDT", "EXRGTM" ,"EXLMDT", "EXCHID", "EXCHNO")
      .build()
    DBContainer container = query.getContainer()
    // insert the inputs into the container
    container.setInt("EXCONO", inCONO)
    query.readAll(container, nrOfKeys, maxRecords, { DBContainer dbContainer ->
      mi.outData.put("CONO", dbContainer.get("EXCONO") as String)
      mi.outData.put("DIVI", dbContainer.get("EXDIVI") as String)
      mi.outData.put("FACI", dbContainer.get("EXFACI") as String)
      mi.outData.put("TRCH", dbContainer.get("EXTRCH") as String)
      mi.outData.put("RATE", dbContainer.get("EXRATE") as String)
      mi.outData.put("RGDT", dbContainer.get("EXRGDT") as String)
      mi.outData.put("RGTM", dbContainer.get("EXRGTM") as String)
      mi.outData.put("LMDT", dbContainer.get("EXLMDT") as String)
      mi.outData.put("CHID", dbContainer.get("EXCHID") as String)
      mi.outData.put("CHNO", dbContainer.get("EXCHNO") as String)
      mi.write()
    })
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
    // Handling Minimum month without sales
    if (mi.in.get('TRCH')) {
      inTRCH = mi.in.get('TRCH') as int
    }

    return true
  }
}
