/****************************************************************************************
 Extension Name: EXT015MI/AddRotRule
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

public class AddRotRule extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  // all the input variables
  private int inCONO
  private String inDIVI
  private String inFACI
  private int inMNTH
  private Double inRATE
  
  public AddRotRule(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
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
    LocalDateTime  dateTime = LocalDateTime.now()
    int entryDate = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
    int entryTime = dateTime.format(DateTimeFormatter.ofPattern("HHmmss")).toInteger()
    
    DBAction insertQuery = database.table("EXT015")
      .index("00")
      .build()
    DBContainer insertContainer = insertQuery.getContainer()
    // insert the inputs into the container
    insertContainer.set("EXCONO", inCONO)
    insertContainer.set("EXDIVI", inDIVI)
    if(inFACI != null){
      insertContainer.set("EXFACI", inFACI)  
    }
    insertContainer.set("EXMNTH", inMNTH)
    insertContainer.set("EXRATE", inRATE)

    insertContainer.set("EXRGDT", entryDate)
    insertContainer.set("EXLMDT", entryDate)
    insertContainer.set("EXRGTM", entryTime)
    insertContainer.set("EXCHID", program.getUser())
    insertContainer.set("EXCHNO", 1)

    insertQuery.insert(insertContainer)
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

    if(!validateCompanyDivision(inCONO, inDIVI)) {
      return false
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
      mi.error('Le taux est obligatoire')
      return false
    }else {
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

  /**
    * @description - Check if company and division is valid
    * @params - Company , Division
    * @returns - boolean
    */ 
  public boolean validateCompanyDivision(int sCONO, String sDIVI) {
    DBAction readQuery = database.table("CMNDIV")
      .index("00")
      .build()
    DBContainer readContainer = readQuery.getContainer()
    readContainer.set("CCCONO", sCONO)
    readContainer.set("CCDIVI", sDIVI)
    if (readQuery.read(readContainer)) {
      return true
    } else {
      mi.error("La compagnie ${sCONO} et la division ${sDIVI} n'existent pas")
      return false
    }
  }
}
