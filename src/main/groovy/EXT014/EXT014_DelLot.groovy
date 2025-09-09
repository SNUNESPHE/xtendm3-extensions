/****************************************************************************************
 Extension Name: EXT014MI/DelLot
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELO
 Date: 2025-04-15
 Description:
 * Add item into the table EXT014. 
    
 Revision History:
 Name                       Date             Version          Description of Changes
 Tovonirina ANDRIANARIELO   2025-04-15       1.0              Initial Release
 Tovonirina ANDRIANARIELO   2025-09-04       1.1              xtend validation
******************************************************************************************/
import java.time.LocalDateTime
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

public class DelLot extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  // all the input variables
  private int inCONO
  private String inFROM
  private String inITTO
  private int inNUMB
  private String inLTDT
  private String inSUCC
  private int maxRecords
  
  public DelLot(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
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
    LocalDateTime  dateTime = LocalDateTime.now()
    int entryDate = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
    int entryTime = dateTime.format(DateTimeFormatter.ofPattern("HHmmss")).toInteger()
    int nrOfKeys = 1
    // check inputs and build the expression
    ExpressionFactory expression = database.getExpressionFactory("EXT014");
    expression = expression.eq("EXCONO", String.valueOf(inCONO))
    if(inFROM != null && !inFROM.isEmpty()){
      expression = expression.and(expression.eq("EXFROM", inFROM))
    }
    if(inITTO != null && !inITTO.isEmpty()){
      expression = expression.and(expression.eq("EXITTO", inITTO))
    }
    if(inNUMB != null && inNUMB > 0){
      expression = expression.and(expression.eq("EXNUMB", String.valueOf(inNUMB)))
    }
    if(inLTDT != null && !inLTDT.isEmpty()){
      expression = expression.and(expression.eq("EXLTDT", inLTDT))
    }
    if(inSUCC != null && !inSUCC.isEmpty()){
      expression = expression.and(expression.eq("EXSUCC", inSUCC))
    }
    DBAction query = database.table("EXT014")
      .index("00")
      .matching(expression)
      .build()
    DBContainer container = query.getContainer()
    container.setInt("EXCONO",inCONO)
    // insert the inputs into the container
    query.readAll(container,nrOfKeys, maxRecords, { DBContainer dbContainer ->
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
    if (mi.in.get('CONO') == null) {
      mi.error("Le champ CONO est obligatoire")
      return false
    } else {
      inCONO = mi.in.get('CONO') as int
    }
    // Handling FROM
    if (mi.in.get('FROM')) {
      inFROM = mi.in.get('FROM')
      if (!validateItemNumber(inCONO, inFROM)) {
        return false
      }
    }
    // Handling ITTO
    if (mi.in.get('ITTO')) {
      inITTO = mi.in.get('ITTO')
      if (!validateItemNumber(inCONO, inITTO)) {
        return false
      }
    }
    // Handling NUMB
    if (mi.in.get('NUMB')) {
      inNUMB = mi.in.get('NUMB') as int
    }
    // Handling LTDT
    if (mi.in.get('LTDT')) {
      inLTDT = mi.in.get('LTDT') as String
      try {
        LocalDate.parse(inLTDT, DateTimeFormatter.ofPattern("yyyyMMdd"))
      } catch (DateTimeParseException e) {
        mi.error("La date du lot n'est pas valide")
        return false
      }
    }
    // Handling SUCC
    if (mi.in.get('SUCC')) {
      inSUCC = mi.in.get('SUCC') as String
    }
    return true
  }

  /**
   * @description - Validates item number
   * @params - company, itemNumber
   * @returns - true/false
   */
  boolean validateItemNumber(int company, String itemNumber) {
    DBAction query = database.table("MITMAS")
      .index("00")
      .selection("MMITNO")
      .build()
    DBContainer container = query.getContainer()
    container.set("MMCONO", company)
    container.set("MMITNO", itemNumber)
    
    if (query.read(container)) {
      return true
    } else {
      mi.error("Le numéro d'article ${itemNumber} n'existe pas dans la table MITMAS")
      return false
    }
  }
}

