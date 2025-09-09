/****************************************************************************************
 Extension Name: EXT019MI/AddItem
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELo
 Date: 2025-01-22
 Description:
 * Add item into the table EXT019. 
    
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
  private String inWHLO
  private String inITNO
  private String inFILE
  private int inDAT1
  private int inDAT2
  private int inIDDT
  private int inODDT
  
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
    
    DBAction insertQuery = database.table("EXT019")
      .index("00")
      .build()
    DBContainer insertContainer = insertQuery.getContainer()
    // insert the inputs into the container
    insertContainer.set("EXCONO", inCONO)  
    insertContainer.set("EXDIVI", inDIVI)  
    insertContainer.set("EXFILE", inFILE)
    insertContainer.set("EXWHLO", inWHLO)
    insertContainer.set("EXITNO", inITNO)
    insertContainer.set("EXDAT1", inDAT1)
    insertContainer.set("EXDAT2", inDAT2)
    insertContainer.set("EXIDDT", inIDDT)
    insertContainer.set("EXODDT", inODDT)

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
    //handling Division
    if(mi.in.get('DIVI')) {
      String divi = mi.in.get('DIVI')
      inDIVI = divi.trim().isEmpty() ? null : divi
      if(!validateDIVI(inCONO, inDIVI)) {
        return false
      }
    }else {
      mi.error("La société est obligatoire")
      return false
    }
    // Handling Warehouse
    if (!mi.in.get('WHLO')) {
      mi.error('Warehouse is required')
      return false
    } else {
      inWHLO = mi.in.get('WHLO') as String
      if(!validateWarehouse(inWHLO)) {
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
    //handling DAT2 is  optional
    if(!mi.in.get('DAT2')) {
      inDAT2 = 0
    } else {
      inDAT2 = mi.in.get('DAT2')
      if(!validateDateFormat(inDAT2)) {
        mi.error("Le format de la date de stock zéro doit être YYYYMMDD")
        return false
      }
    }
    //handling IDDT is  optional
    if(!mi.in.get('IDDT')) {
      inIDDT = 0
    } else {
      inIDDT = mi.in.get('IDDT')
      if(!validateDateFormat(inIDDT)) {
        mi.error('Invalid date format for IDDT')
        return false
      }
    }
    //handling ODDT is  optional
    if(!mi.in.get('ODDT')) {
      inODDT = 0
    } else {
      inODDT = mi.in.get('ODDT')
      if(!validateDateFormat(inODDT)) {
        mi.error('Invalid date format for ODDT')
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
    * @description - Check if warehouse is valid
    * @params - warehouse
    * @returns - boolean
    */ 
  public boolean validateWarehouse(String warehouse){
    DBAction readQuery = database.table("MITWHL")
      .index("00")
      .selection("MWWHLO")
      .build()
    DBContainer readContainer = readQuery.getContainer()
    readContainer.set("MWCONO", inCONO)
    readContainer.set("MWWHLO", warehouse)
    if (readQuery.read(readContainer)) {
      return true
    } else {
      mi.error("Warehouse do not exist")
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
