/****************************************************************************************
 Extension Name: EXT017MI/UpdLot
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELO
 Date: 2025-11-17
 Description:
 * Upd item into the table EXT014. 
    
 Revision History:
 Name                       Date             Version          Description of Changes
 Tovonirina ANDRIANARIELO   2025-11-17       1.0              Initial Release
******************************************************************************************/
import java.time.LocalDateTime
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

public class UpdLot extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  // all the input variables
  private int inCONO
  private String inDIVI
  private String inFROM
  private String inITTO
  private int inNUMB
  private String inLTDT
  private String inSUCC
  
  public UpdLot(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
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
    
    DBAction updateQuery = database.table("EXT014")
      .index("00")
      .build()
    DBContainer updateContainer = updateQuery.getContainer()
    // insert the inputs into the container
    updateContainer.set("EXCONO", inCONO)
    updateContainer.set("EXDIVI", inDIVI)
    updateContainer.set("EXFROM", inFROM)
    updateContainer.set("EXITTO", inITTO)
    updateContainer.set("EXNUMB", inNUMB)
    updateContainer.set("EXLTDT", Integer.parseInt(inLTDT))
    updateQuery.readLock(updateContainer, { LockedResult lockedResult ->
      lockedResult.set("EXSUCC", inSUCC)
      // audit fields 
      lockedResult.set("EXLMDT", entryDate)
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
    if (mi.in.get('DIVI') == null) {
      mi.error("La division est obligatoire")
      return false
    } else {
      inDIVI = mi.in.get('DIVI') as String
      // Validate Division
      if (!validateDIVI(inCONO, inDIVI)) {
        return false
      }
    }
    // Handling FROM
    if (mi.in.get('FROM') == null) {
      mi.error("Le numéro d'article de début est obligatoire")
      return false
    } else {
      inFROM = mi.in.get('FROM')
      if (!validateItemNumber(inCONO, inFROM)) {
        return false
      }
    }
    // Handling ITTO
    if (mi.in.get('ITTO') == null) {
      mi.error("Le numéro d'article de fin est obligatoire")
      return false
    } else {
      inITTO = mi.in.get('ITTO')
      if (!validateItemNumber(inCONO, inITTO)) {
        return false
      }
    }
    // Handling NUMB
    if (mi.in.get('NUMB') == null) {
      mi.error("Le nombre d'article est obligatoire")
      return false
    } else {
      inNUMB = mi.in.get('NUMB') as int
    }
    // Handling LTDT
    if (mi.in.get('LTDT') == null) {
      mi.error("La date du lot est obligatoire")
      return false
    } else {
      inLTDT = mi.in.get('LTDT') as String
      try {
        LocalDate.parse(inLTDT, DateTimeFormatter.ofPattern("yyyyMMdd"))
      } catch (DateTimeParseException e) {
        mi.error("La date du lot n'est pas valide")
        return false
      }
    }
    // Handling SUCC
    if (mi.in.get('SUCC') == null) {
     inSUCC = "PROCESSING"
    }else{
      inSUCC = mi.in.get('SUCC') as String
      inSUCC = inSUCC.toUpperCase()
      if(!validateSUCC(inSUCC)){
        return false
      }
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

  /**
   * @description - Validates the status of the lot
   * @params - status
   * @returns - true/false
   */
  boolean validateSUCC(String status) {
    List<String> validStatus = ["PROCESSING", "SUCCESS", "CANCELLED", "INPROGRESS"]
    if (validStatus.contains(status)) {
      return true
    } else {
      mi.error("Le statut ${status} n'est pas valide. Les valeurs valides sont PROCESSING, SUCCESS, INPROGRESS ou CANCELLED")
      return false
    }
  }

  /**
   * Validate the Division (DIVI) against the CMNDIV table.
   * @return true if valid, false otherwise
   */
  boolean validateDIVI(int cono, String divi) {
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

