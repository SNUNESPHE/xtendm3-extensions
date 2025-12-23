/************************************************************************************************************************************************
Extension Name: EXT806MI.GetNextID
Type: ExtendM3Transaction
Script Author: NRAOEL
Date: 2024-10-22
Description:
* Get fileName

Revision History:
Name          Date        Version   Description of Changes
NRAOEL        2024-10-22  1.0       Initial Release
NRAOEL        2025-11-27  1.1       Correction after validation submission
**************************************************************************************************************************************************/

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

public class GetNextID extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private int inCONO //Company
  private String inDIVI //Division
  private String lastLetter
  private String ccd6
  private String newCCD6

  public GetNextID(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
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
    lastLetter = getLAST()

    if (lastLetter.trim().equals("")) {
      newCCD6 = getCCD6()
      if (!newCCD6.trim().equals("")) {
        int ccd6Int = Integer.parseInt(newCCD6)
        addLast(ccd6Int)
        lastLetter = "A"
      } else {
        mi.error("Cette société n'a pas de code cédant")
        return
      }
    } else {
      String newLast = increment(lastLetter)
      updLast(newLast)
    }
    ccd6 = getCCD6()

    String flid = ccd6 + "QS1" + LAST
    mi.outData.put("FLID", flid)
  }

  /**
   * @increment - Increment the last alphabet
   * @params - last : EXLAST field from EXT808
   * @returns - charNext : next char
   */

  String increment(String last) {
    char charLast = last.charAt(0)
    if (charLast == 'Z') {
      return "A"
    }

    int code = (int) charLast + 1
    char charNext = (char) code
    return String.valueOf(charNext)
  }

  /**
   * @addLast - add line to EXT808
   * @params - ccd6 : society code
   * @returns - last : EXLAST field from EXT808
   */
  String addLast(int ccd6) {
    DBAction dbaEXT808 = database.table("EXT808").index("00").build()
    DBContainer conEXT808 = dbaEXT808.getContainer()

    LocalDateTime dateTime = LocalDateTime.now()
    int entryDate = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
    int entryTime = dateTime.format(DateTimeFormatter.ofPattern("HHmmss")).toInteger()

    conEXT808.set("EXCONO", inCONO)
    conEXT808.set("EXDIVI", inDIVI)

    String result = "A"

    conEXT808.set("EXCONO", inCONO)
    conEXT808.set("EXDIVI", inDIVI)
    conEXT808.set("EXLAST", result)
    conEXT808.set("EXCCD6", ccd6.toString())
    conEXT808.set("EXRGDT", entryDate)
    conEXT808.set("EXRGTM", entryTime)
    conEXT808.set("EXLMDT", entryDate)
    conEXT808.set("EXCHNO", "1")
    conEXT808.set("EXCHID", program.getUser())
    dbaEXT808.insert(conEXT808)

    return result
  }

  /**
   * @updLast - update last alphabet in EXT808  
   * @params - last : EXLAST field from EXT808
   * @returns - the list of expected fields from OCUSMA
   */
  void updLast(String last) {

    DBAction updEXT808 = database.table("EXT808").index("00").build()
    DBContainer conEXT808 = updEXT808.getContainer()

    conEXT808.set("EXCONO", inCONO)
    conEXT808.set("EXDIVI", inDIVI)

    updEXT808.readLock(conEXT808, {
      LockedResult lockedResult ->
      
      LocalDateTime dateTime = LocalDateTime.now()
      int today = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()

      String chno = lockedResult.get("EXCHNO")
      int incrementedChno = Integer.parseInt(chno.trim()) + 1

      lockedResult.set("EXLAST", last)
      lockedResult.set("EXCHNO", Integer.toString(incrementedChno))
      lockedResult.set("EXCHID", program.getUser())
      conEXT808.set("EXLMDT", today)
      lockedResult.update()
    })
  }

  /**
   * @getLAST - get last from EXT808 
   * @params - 
   * @returns - last : EXLAST field from EXT808
   */
  String getLAST() {
    DBAction dbaEXT808 = database.table("EXT808").index("00").selection("EXLAST").build()
    DBContainer conEXT808 = dbaEXT808.getContainer()

    conEXT808.set("EXCONO", inCONO)
    conEXT808.set("EXDIVI", inDIVI)

    String result = ""
    dbaEXT808.readAll(conEXT808, 2, 1, {
      DBContainer container1 ->
      result = container1.get("EXLAST").toString().trim()
    })
    return result
  }

  /**
   * @getCCD6 - get CCD6 from CMNDIV
   * @params - 
   * @returns - ccd6
   */
  String getCCD6() {
    DBAction dbaEXT808 = database.table("CMNDIV").index("00").selection("CCCCD6").build()
    DBContainer conEXT808 = dbaEXT808.getContainer()

    conEXT808.set("CCCONO", inCONO)
    conEXT808.set("CCDIVI", inDIVI)

    String result = ""
    dbaEXT808.readAll(conEXT808, 2, 1, {
      DBContainer container1 ->
      result = container1.get("CCCCD6").toString().trim()
    })
    return result
  }
}