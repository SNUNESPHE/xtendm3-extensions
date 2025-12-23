/************************************************************************************************************************************************
Extension Name: EXT806MI.UpdSynthese
Type: ExtendM3Transaction
Script Author: NRAOEL
Date: 2024-10-22
Description:
* Add/Update Lines in EXT809

Revision History:
Name          Date        Version   Description of Changes
NRAOEL        2025-11-28  1.0       Initial Release
NRAOEL        2025-12-16  1.1       Correction after validation submission
**************************************************************************************************************************************************/

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

public class UpdSynthese extends ExtendM3Transaction {
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
  private String inTOTL
  private String inTOT1
  private String inSTAT

  public UpdSynthese(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
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
    inSTAT = mi.inData.get("STAT").isBlank() ? "" : mi.inData.get("STAT").trim()
    inASGN = mi.inData.get("ASGN").isBlank() ? "" : mi.inData.get("ASGN").trim()
    inCUA1 = mi.inData.get("CUA1").isBlank() ? "" : mi.inData.get("CUA1").trim()
    inCUA2 = mi.inData.get("CUA2").isBlank() ? "" : mi.inData.get("CUA2").trim()
    inCUA3 = mi.inData.get("CUA3").isBlank() ? "" : mi.inData.get("CUA3").trim()
    inCUA4 = mi.inData.get("CUA4").isBlank() ? "" : mi.inData.get("CUA4").trim()
    inTOTL = mi.inData.get("TOTL").isBlank() ? "" : mi.inData.get("TOTL").trim()
    inTOT1 = mi.inData.get("TOT1").isBlank() ? "" : mi.inData.get("TOT1").trim()

    // validate input variables
    if (!validateInputVariables()) {
      return
    }

    DBAction dbaEXT809 = database.table("EXT809").index("00").build()
    DBContainer conEXT809 = dbaEXT809.getContainer()
    conEXT809.set("EXCONO", inCONO)
    conEXT809.set("EXDIVI", inDIVI)
    conEXT809.set("EXASGN", inASGN)
    conEXT809.set("EXSTAT", inSTAT)

    if (dbaEXT809.read(conEXT809)) {
      update()
    } else {
      initiate()
    }

    mi.outData.put("RSLT", "OK")
  }

  /**
   * @initiate - initiate line to EXT809
   * @params - 
   * @returns - 
   */
  void initiate() {
    DBAction dbaEXT809 = database.table("EXT809").index("00").build()
    DBContainer conEXT809 = dbaEXT809.getContainer()

    LocalDateTime dateTime = LocalDateTime.now()
    int entryDate = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
    int entryTime = dateTime.format(DateTimeFormatter.ofPattern("HHmmss")).toInteger()

    conEXT809.set("EXCONO", inCONO)
    conEXT809.set("EXDIVI", inDIVI)
    conEXT809.set("EXASGN", inASGN)
    conEXT809.set("EXSTAT", inSTAT)
    conEXT809.set("EXCUA1", inCUA1)
    conEXT809.set("EXCUA2", inCUA2)
    conEXT809.set("EXCUA3", inCUA3)
    conEXT809.set("EXCUA4", inCUA4)
    conEXT809.set("EXTOTL", inTOTL)
    conEXT809.set("EXTOT1", inTOT1)
    conEXT809.set("EXRGDT", entryDate)
    conEXT809.set("EXRGTM", entryTime)
    conEXT809.set("EXLMDT", entryDate)
    conEXT809.set("EXCHNO", 1)
    conEXT809.set("EXCHID", program.getUser())
    dbaEXT809.insert(conEXT809)
  }

  /**
   * @update - update amount in EXT809  
   * @params - 
   * @returns - 
   */
  void update() {
    DBAction updEXT809 = database.table("EXT809").index("00").build()
    DBContainer conEXT809 = updEXT809.getContainer()

    conEXT809.set("EXCONO", inCONO)
    conEXT809.set("EXDIVI", inDIVI)
    conEXT809.set("EXASGN", inASGN)
    conEXT809.set("EXSTAT", inSTAT)

    updEXT809.readLock(conEXT809, {
      LockedResult lockedResult ->
      LocalDateTime dateTime = LocalDateTime.now()
      int today = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
      String chno = lockedResult.get("EXCHNO")
      int incrementedChno = Integer.parseInt(chno.trim()) + 1

      lockedResult.set("EXCUA1", inCUA1)
      lockedResult.set("EXCUA2", inCUA2)
      lockedResult.set("EXCUA3", inCUA3)
      lockedResult.set("EXCUA4", inCUA4)
      lockedResult.set("EXTOTL", inTOTL)
      lockedResult.set("EXTOT1", inTOT1)
      lockedResult.set("EXCHNO", incrementedChno)
      lockedResult.set("EXCHID", program.getUser())
      lockedResult.set("EXLMDT", today)

      lockedResult.update()
    })
  }

  /**
   * @description - Validates input variables
   * @params -
   * @returns - true/false
   */
  boolean validateInputVariables() {

    if (!validateConoDivi(inCONO, inDIVI)) {
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
}