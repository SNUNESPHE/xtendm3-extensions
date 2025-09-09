/****************************************************************************************
 Extension Name: EXT015MI/DelRotRule
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELO
 Date: 2025-01-24
 Description:
 * Add item into the table EXT015. 
    
 Revision History:
 Name                       Date             Version          Description of Changes
 Tovonirina ANDRIANARIELO   2025-01-24       1.0              Initial Release
 Tovonirina ANDRIANARIELO   2025-09-08       1.1              Validation Xtend
******************************************************************************************/
import java.time.LocalDateTime
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

public class DelRotRule extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  // all the input variables
  private int inCONO
  private String inDIVI
  private String inFACI
  private int inMNTH
  private int maxRecords

  
  public DelRotRule(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
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
    // get the current date
    LocalDate currentDate = LocalDate.now()
    int formattedCurrentDate = currentDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
    int  nrOfKeys = 2    
    ExpressionFactory expression = database.getExpressionFactory("EXT015");
    expression = expression.eq("EXCONO", String.valueOf(inCONO))
    if(inFACI != null && !inFACI.isEmpty()){
      expression = expression.and(expression.eq("EXFACI", inFACI))
    }
    if(inMNTH != null && inMNTH >= 0){
      expression = expression.and(expression.eq("EXMNTH", String.valueOf(inMNTH)))
    }
    // create the query
    DBAction query = database.table("EXT015")
      .index("00")
      .matching(expression)
      .build()
    DBContainer container = query.getContainer()
    // insert the inputs into the container
    container.setInt("EXCONO", inCONO)
    container.setString("EXDIVI", inDIVI)
    query.readAll(container, nrOfKeys,maxRecords, { DBContainer dbContainer ->
      //delete the record
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
    if (mi.in.get('MNTH')) {
      inMNTH = mi.in.get('MNTH') as int
    }

    return true
  }
}
