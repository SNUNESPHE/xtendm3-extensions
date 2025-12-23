/************************************************************************************************************************************************
Extension Name: EXT806MI.UpdDetail
Type: ExtendM3Transaction
Script Author: NRAOEL
Date: 2024-10-22
Description:
* Add/Update amount in EXT810

Revision History:
Name          Date        Version   Description of Changes
NRAOEL        2025-11-28  1.0       Initial Release
NRAOEL        2025-12-16  1.1       Correction after validation submission
**************************************************************************************************************************************************/

import java.time.format.DateTimeFormatter
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeParseException

public class UpdDetail extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private int inCONO //Company
  private String inDIVI //Division
  private String inASGN
  private String inCUA1
  private String inCUA2
  private String inCUA3
  private String inCUA4
  private String inPYNO
  private String inCINO
  private int inACDT

  public UpdDetail(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
    this.mi = mi
    this.database = database
    this.program = program
  }

  public void main() {

    if (!mi.inData.get("CONO").isBlank()) {
      inCONO = mi.in.get("CONO") as Integer
    } else {
      inCONO = program.LDAZD.get("CONO") as Integer
    }
    inDIVI = mi.inData.get("DIVI").isBlank() ? program.LDAZD.get("DIVI") : mi.inData.get("DIVI").trim()
    inPYNO = mi.inData.get("PYNO").isBlank() ? "" : mi.inData.get("PYNO").trim()
    inCINO = mi.inData.get("CINO").isBlank() ? "" : mi.inData.get("CINO").trim()
    inASGN = mi.inData.get("ASGN").isBlank() ? "" : mi.inData.get("ASGN").trim()
    inCUA1 = mi.inData.get("CUA1").isBlank() ? "" : mi.inData.get("CUA1").trim()
    inCUA2 = mi.inData.get("CUA2").isBlank() ? "" : mi.inData.get("CUA2").trim()
    inCUA3 = mi.inData.get("CUA3").isBlank() ? "" : mi.inData.get("CUA3").trim()
    inCUA4 = mi.inData.get("CUA4").isBlank() ? "" : mi.inData.get("CUA4").trim()
    inACDT = mi.in.get("ACDT") as Integer == null ? 0 : mi.in.get("ACDT") as Integer

    // validate input variables
    if (!validateInputVariables()) {
      return
    }

    DBAction dbaEXT810 = database.table("EXT810").index("00").build()
    DBContainer conEXT810 = dbaEXT810.getContainer()
    conEXT810.set("EXCONO", inCONO)
    conEXT810.set("EXDIVI", inDIVI)
    conEXT810.set("EXPYNO", inPYNO)
    conEXT810.set("EXCINO", inCINO)

    if (dbaEXT810.read(conEXT810)) {
      update()
    } else {
      initiate()
    }

    mi.outData.put("RSLT", "OK")
  }

  /**
   * @initiate - initiate line to EXT810
   * @params - 
   * @returns - 
   */
  void initiate() {
    DBAction dbaEXT810 = database.table("EXT810").index("00").build()
    DBContainer conEXT810 = dbaEXT810.getContainer()

    LocalDateTime dateTime = LocalDateTime.now()
    int entryDate = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
    int entryTime = dateTime.format(DateTimeFormatter.ofPattern("HHmmss")).toInteger()

    conEXT810.set("EXCONO", inCONO)
    conEXT810.set("EXDIVI", inDIVI)
    conEXT810.set("EXASGN", inASGN)
    conEXT810.set("EXPYNO", inPYNO)
    conEXT810.set("EXCINO", inCINO)
    conEXT810.set("EXCUA1", inCUA1)
    conEXT810.set("EXCUA2", inCUA2)
    conEXT810.set("EXCUA3", inCUA3)
    conEXT810.set("EXCUA4", inCUA4)
    conEXT810.set("EXACDT", inACDT)
    conEXT810.set("EXRGDT", entryDate)
    conEXT810.set("EXRGTM", entryTime)
    conEXT810.set("EXLMDT", entryDate)
    conEXT810.set("EXCHNO", 1)
    conEXT810.set("EXCHID", program.getUser())
    dbaEXT810.insert(conEXT810)
  }

  /**
   * @update - update amount in EXT810  
   * @params - 
   * @returns - 
   */
  void update() {
    DBAction updEXT810 = database.table("EXT810").index("00").build()
    DBContainer conEXT810 = updEXT810.getContainer()

    conEXT810.set("EXCONO", inCONO)
    conEXT810.set("EXDIVI", inDIVI)
    conEXT810.set("EXPYNO", inPYNO)
    conEXT810.set("EXCINO", inCINO)

    updEXT810.readLock(conEXT810, {
      LockedResult lockedResult ->
      LocalDateTime dateTime = LocalDateTime.now()
      int today = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
      String chno = lockedResult.get("EXCHNO")
      int incrementedChno = Integer.parseInt(chno.trim()) + 1

      lockedResult.set("EXCUA1", inCUA1)
      lockedResult.set("EXCUA2", inCUA2)
      lockedResult.set("EXCUA3", inCUA3)
      lockedResult.set("EXCUA4", inCUA4)
      lockedResult.set("EXCHNO", incrementedChno)
      lockedResult.set("EXCHID", program.getUser())
      lockedResult.set("EXLMDT", today)

      lockedResult.update()
    })
  }

  boolean validateInputVariables() {

    if (!validateConoDivi(inCONO, inDIVI)) {
      return false
    }

    if (!validateAcdt(inACDT)) {
      return false
    }

    if (!validatePynoCino(inCONO, inPYNO, inCINO)) {
      return false
    }

    return true
  }

  /**
   * Validate the Company (CONO) and the Division (DIVI) against the CMNDIV table.
   * @return true if valid, false otherwise
   */
  boolean validateConoDivi(int cono, String divi) {
    DBAction dbaCMNDIV = database.table("CMNDIV").index("00").build()
    DBContainer conCMNDIV = dbaCMNDIV.getContainer()
    conCMNDIV.set("CCCONO", cono)
    conCMNDIV.set("CCDIVI", divi)
    if (!dbaCMNDIV.read(conCMNDIV)) {
      mi.error("Division: " + divi.toString() + " does not exist in Company " + cono)
      return false
    }
    return true
  }

  /**
   * Validate the accounting date.
   * @return true if valid, false otherwise
   */
  boolean validateAcdt(int acdt) {
    try {
      LocalDate.parse(acdt as String, DateTimeFormatter.ofPattern("yyyyMMdd"))
    } catch (DateTimeParseException e) {
      mi.error("The accounting date is not valid")
      return false
    }
  }

  /**
   * Validate the Payer (PYNO) against the OCUSMA table.
   * @return true if valid, false otherwise
   */
  boolean validatePynoCino(int cono, String pyno, String cino) {
    DBAction dbaFsledg = database.table("FSLEDG")
        .index("12")
        .build()

    DBContainer conFsledg = dbaFsledg.getContainer()
    conFsledg.set("ESCONO", cono)
    conFsledg.set("ESPYNO", pyno)
    conFsledg.set("ESCINO", cino)

    boolean found = false

    dbaFsledg.readAll(conFsledg, 3, 1, { DBContainer container ->
        found = true
    })
    if (!found) {
        mi.error("Invoice number: " + cino + " does not exist")
        return false
    }
    return true
}

}