/****************************************************************************************
 Extension Name: EXT019MI/UpdItem
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELo
 Date: 2025-01-22
 Description:
 * Update item from the table EXT019. 
    
 Revision History:
 Name                       Date             Version          Description of Changes
 Tovonirina ANDRIANARIELO   2025-01-22       1.0              Initial Release
 Tovonirina ANDRIANARIELO   2025-09-08       1.1              XtendM3 validation
******************************************************************************************/
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

public class UpdItem extends ExtendM3Transaction {
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
  
  public UpdItem(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
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
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern('yyyyMMdd')
    String formattedDate = currentDate.format(formatter)

    DBAction query = database.table("EXT019")
      .index("00")
      .build()
    DBContainer container = query.getContainer()    
    container.set("EXCONO", inCONO)
    container.set("EXDIVI", inDIVI)
    container.set("EXFILE", inFILE)
    container.set("EXWHLO", inWHLO)
    container.set("EXITNO", inITNO)
    query.readLock(container, { LockedResult lockedResult ->
      lockedResult.set("EXDAT1", inDAT1)
      lockedResult.set("EXDAT2", inDAT2)
      lockedResult.set("EXIDDT", inIDDT)
      lockedResult.set("EXODDT", inODDT)

      lockedResult.set("EXLMDT", Integer.parseInt(formattedDate))
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
      mi.error("La société est obligatoire")
      return false
    } else {
      inDIVI = mi.in.get('DIVI')
    }
    // Handling Warehouse
    if (!mi.in.get('WHLO')) {
      mi.error("L'entrepôt est obligatoire")
      return false
    } else {
      inWHLO = mi.in.get('WHLO')
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
    //handling DAT2 is  optional
    if(!mi.in.get('DAT2')) {
      inDAT2 = 0
    } else {
      inDAT2 = mi.in.get('DAT2')
      if(!validateDateFormat(inDAT2)) {
        mi.error("Le format de la date où le stock deviens zéro doit être YYYYMMDD")
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
}
