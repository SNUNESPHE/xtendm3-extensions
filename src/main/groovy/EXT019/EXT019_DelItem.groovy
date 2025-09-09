/****************************************************************************************
 Extension Name: EXT019MI/DelItem
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELo
 Date: 2025-01-22
 Description:
 * Delete item from the table EXT019. 
    
 Revision History:
 Name                       Date             Version          Description of Changes
 Tovonirina ANDRIANARIELO   2025-01-22       1.0              Initial Release
 Tovonirina ANDRIANARIELO   2025-09-08       1.1              XtendM3 validation
******************************************************************************************/
public class DelItem extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  // all the input variables
  private int inCONO  // Company
  private String inDIVI //Division
  private String inWHLO // Warehouse
  private String inITNO // Item number
  private String inFILE // File
  private int maxRecords
  
  public DelItem(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
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
    ExpressionFactory expression = database.getExpressionFactory("EXT019");
    expression = expression.eq("EXCONO", String.valueOf(inCONO))
    expression = expression.and(expression.eq("EXFILE", inFILE))
    if(inDIVI != null && !inDIVI.isEmpty()){
      expression = expression.and(expression.eq("EXDIVI", inDIVI))
    }
    if(inWHLO != null && !inWHLO.isEmpty()){
      expression = expression.and(expression.eq("EXWHLO", inWHLO))
    }
    if(inITNO != null && !inITNO.isEmpty()){
      expression = expression.and(expression.eq("EXITNO", inITNO))
    }
    int nrOfKeys = 1
    DBAction query = database.table("EXT019")
      .index("00")
      .matching(expression)
      .build()

    DBContainer container = query.getContainer()
    container.set("EXCONO", inCONO)
    query.readAll(container, nrOfKeys, maxRecords, { DBContainer dbContainer -> 
      query.readLock(dbContainer,{ LockedResult lockedResult -> 
        lockedResult.delete()
      })
    })

  }

  /**
   * @description - Validates input variables
   * @params -
   * @returns - true/false
   */
  boolean validateInputVariables() {
    // Handling Company
    if(!mi.in.get('CONO')) {
      inCONO = (Integer) program.getLDAZD().CONO
    } else {
      inCONO = mi.in.get('CONO') as int
    }
    // Handling division
    if(mi.in.get('DIVI')) {
      inDIVI = mi.in.get('DIVI') as String
    }
    // Handling Warehouse
    if(mi.in.get('WHLO')) {
      inWHLO = mi.in.get('WHLO') as String
    }
    // Handling Item number
    if(mi.in.get('ITNO')) {
      inITNO = mi.in.get('ITNO') as String
    }
    //handling FILE
    if(mi.in.get('FILE')) {
      inFILE = mi.in.get('FILE') as String
    }
    return true
  }
}
