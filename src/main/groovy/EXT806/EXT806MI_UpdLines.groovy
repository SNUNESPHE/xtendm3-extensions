/************************************************************************************************************************************************
Extension Name: EXT806MI.UpdLines
Type: ExtendM3Transaction
Script Author: NRAOEL
Date: 2024-10-22
Description:
* Update fields in EXT806

Revision History:
Name          Date        Version   Description of Changes
NRAOEL        2024-10-22  1.0       Initial Release
NRAOEL        2025-11-27  1.1       Correction after validation submission
**************************************************************************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdLines extends ExtendM3Transaction {

  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private int inCONO //Company
  private String inDIVI //Division
  private String inTYLI // Line Type
  private int inCOMP // Counter
  private int inYEA4 //Year
  private int inJRNO //Journal number
  private int inJSNO //Journal sequence

  public UpdLines(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
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
    inTYLI = mi.inData.get("TYLI").isBlank() ? "" : mi.inData.get("TYLI").trim()
    inYEA4 = mi.in.get("YEA4") as Integer == null ? 0 : mi.in.get("YEA4") as Integer
    inJRNO = mi.in.get("JRNO") as Integer == null ? 0 : mi.in.get("JRNO") as Integer
    inJSNO = mi.in.get("JSNO") as Integer == null ? 0 : mi.in.get("JSNO") as Integer
    inCOMP = mi.in.get("COMP") as Integer == null ? 0 : mi.in.get("COMP") as Integer

    DBAction query = database.table("EXT806")
      .index("00")
      .build()
    DBContainer container = query.getContainer()
    container.set("EXCONO", inCONO)
    container.set("EXDIVI", inDIVI)
    container.set("EXYEA4", inYEA4)
    container.set("EXJRNO", inJRNO)
    container.set("EXJSNO", inJSNO)   
    container.set("EXTYLI", inTYLI)  

    query.readLock(container, { LockedResult lockedResult ->
    
    LocalDateTime dateTime = LocalDateTime.now()
    dateTime = java.time.LocalDateTime.now()
    int changedDate = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()

    String chno = lockedResult.get("EXCHNO")
    int incrementedChno = Integer.parseInt(chno.trim()) + 1

    lockedResult.set("EXSTAT", "90")
    lockedResult.set("EXCOMP", inCOMP)
    lockedResult.set("EXDATE", changedDate)
    lockedResult.set("EXCHNO", incrementedChno)
    lockedResult.set("EXLMDT", changedDate)
    lockedResult.set("EXCHID", program.getUser())
    lockedResult.update()

    mi.outData.put("RSLT", "OK")
    mi.write()
  })
  }

}