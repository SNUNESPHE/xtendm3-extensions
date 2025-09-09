/****************************************************************************************
 Extension Name: EXT018MI/AddItem
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELo
 Date: 2025-01-22
 Description:
 * Add item into the table EXT018. 
    
 Revision History:
 Name                       Date             Version          Description of Changes
 Tovonirina ANDRIANARIELO   2025-01-22       1.0              Initial Release
 Tovonirina ANDRIANARIELO   2025-09-08       1.1              XtendM3 validation
******************************************************************************************/
import java.time.LocalDateTime
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

public class AddItem extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  // all the input variables
  private int inCONO
  private String inDIVI
  private String inFACI
  private String inITNO
  private String inFILE
  private int inDAT1
  
  public AddItem(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
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
    
    DBAction insertQuery = database.table("EXT018")
      .index("00")
      .build()
    DBContainer insertContainer = insertQuery.getContainer()
    // insert the inputs into the container
    insertContainer.set("EXCONO", inCONO)
    insertContainer.set("EXDIVI", inDIVI)
    insertContainer.set("EXFACI", inFACI)
    insertContainer.set("EXITNO", inITNO)
    insertContainer.set("EXFILE", inFILE)
    insertContainer.set("EXDAT1", inDAT1)

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
      mi.error("La division est obligatoire")
      return false
    } else {
      String divi = mi.in.get('DIVI')
      inDIVI = divi != null ? divi : ""
      if (!validateDIVI(inCONO, inDIVI)) {
        return false
      }
    }
    // Handling Facility
    if (!mi.in.get('FACI')) {
      mi.error("L'établissement est obligatoire")
      return false
    } else {
      inFACI = mi.in.get('FACI') as String
    }
    // Handling Item number
    if(!mi.in.get('ITNO')) {
      mi.error("Le numéro d'article est obligatoire")
      return false
    }else {
      inITNO = mi.in.get('ITNO')
      if(!validateItemNumber(inCONO, inITNO)) {
        return false
      }
    }
    //handling FILE
    if(!mi.in.get('FILE')) {
      mi.error("Le tableau est obligatoire")
      return false
    }else {
      inFILE = mi.in.get('FILE')
    }
    //handling DAT1 is  optional
    if(!mi.in.get('DAT1')) {
      inDAT1 = 0
    } else {
      inDAT1 = mi.in.get('DAT1')
      if(!validateDateFormat(inDAT1)) {
        mi.error("Le format de la date de premier réapprovisionnement doit être YYYYMMDD")
        return false
      }
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
