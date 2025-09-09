/****************************************************************************************
 Extension Name: EXT015MI/UpdRotRule
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELO
 Date: 2025-01-24
 Description:
 * Add item into the table EXT015. 
    
 Revision History:
 Name                       Date             Version          Description of Changes
 Tovonirina ANDRIANARIELO   2025-01-24       1.0              Initial Release
******************************************************************************************/
import java.time.LocalDateTime
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

public class UpdRotRule extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  // all the input variables
  private int inCONO
  private String inDIVI
  private String inFACI
  private int inMNTH
  private Double inRATE
  
  public UpdRotRule(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
    this.mi = mi
    this.program = program
    this.database = database
  }

  public void main() {
    // validate input variables
    if (!validateInputVariables()) {
      return
    }
    // get the current date
    LocalDate currentDate = LocalDate.now()
    int formattedCurrentDate = currentDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
    
    DBAction query = database.table("EXT015")
      .index("00")
      .build()
    DBContainer container = query.getContainer()
    // insert the inputs into the container
    container.setInt("EXCONO", inCONO)
    container.setString("EXDIVI", inDIVI)
    if(inFACI){
      container.setString("EXFACI", inFACI)
    }
    container.setInt("EXMNTH", inMNTH)
    query.readLock(container,{ LockedResult lockedResult ->
      lockedResult.set("EXRATE", inRATE)
      
      lockedResult.set("EXLMDT", formattedCurrentDate)
      lockedResult.set("EXCHID", program.getUser())
      lockedResult.set("EXCHNO", (int) lockedResult.get('EXCHNO') + 1)
      lockedResult.update()
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
    if (!mi.in.get('DIVI')) {
      inDIVI = (String) program.getLDAZD().DIVI
    } else {
      inDIVI = mi.in.get('DIVI') as String
    }

    // Handling Facility
    if (mi.in.get('FACI')) {
      inFACI = mi.in.get('FACI') as String
        if(!validateFacility(inFACI)) {
        return false
      }
    }
    // Handling Minimum month without sales
    if (mi.in.get('MNTH') == null) {
      mi.error('Le mois minimum est obligatoire')
      return false
    } else {
      inMNTH = mi.in.get('MNTH') as int
    }
    //handling Rate
    if(mi.in.get('RATE') == null) {
      inRATE = 0d
    } else {
      inRATE = (Double) mi.in.get('RATE')
      // Le taux de dépreciation doit être positif et ne doit pas dépassé 100
      if (inRATE < 0 || inRATE > 100) {
        mi.error('Le taux de dépreciation doit être positif et ne doit pas dépassé 100')
        return false
      } 
    }
    return true
  }
    /**
    * @description - Check if facility is valid
    * @params - facility
    * @returns - boolean
    */ 
  public boolean validateFacility(String facility){
    // check if the facility is valid
    DBAction readQuery = database.table("CFACIL")
      .index("00")
      .selection("CFCONO", "CFFACI")
      .build()
    DBContainer readContainer = readQuery.getContainer()
    readContainer.set("CFCONO", inCONO)
    readContainer.set("CFFACI", facility)
    if (readQuery.read(readContainer)) {
      return true
    } else {
      mi.error("La facilité ${facility} n'existe pas")
      return false
    }
  }
}