/************************************************************************************************************************************************
Extension Name: EXT806MI.AddCardV2
Type: ExtendM3Transaction
Script Author: NRAOEL
Date: 2024-10-22
Description:
* Add Line into the dynamic table EXT807 to trigger an event analytics

Revision History:
Name          Date        Version   Description of Changes
NRAOEL        2024-10-22  1.0       Initial Release
NRAOEL        2025-11-27  1.1       Correction after validation submission
**************************************************************************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddCardV2 extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  private int inCONO //Company
  private String inDIVI //Division
  private String inTYLI //Line type

  public AddCardV2(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
    this.mi = mi
    this.program = program
    this.database = database
  }

  public void main() {
    //inputs
    if (!mi.inData.get("CONO").isBlank()) {
      inCONO = mi.in.get("CONO") as Integer
    } else {
      inCONO = program.LDAZD.get("CONO") as Integer
    }
    inDIVI = mi.inData.get("DIVI").isBlank() ? program.LDAZD.get("DIVI") : mi.inData.get("DIVI").trim()
    inTYLI = mi.inData.get("TYLI").isBlank() ? "" : mi.inData.get("TYLI").trim()

    DBAction dbaEXT806 = database.table("EXT807").index("00").build()
    DBContainer conEXT806 = dbaEXT806.createContainer()

    LocalDateTime dateTime = LocalDateTime.now()
    int entryDate = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
    int entryTime = dateTime.format(DateTimeFormatter.ofPattern("HHmmss")).toInteger()
    int year = dateTime.format(DateTimeFormatter.ofPattern("yyyy")).toInteger()

    conEXT806.set("EXCONO", inCONO)
    conEXT806.set("EXDIVI", inDIVI)
    conEXT806.set("EXYEA4", year)
    conEXT806.set("EXJRNO", entryDate)
    conEXT806.set("EXJSNO", entryTime)
    conEXT806.set("EXTYLI", inTYLI)
    conEXT806.set("EXVTXT", "Extraction")
    conEXT806.set("EXRGDT", entryDate)
    conEXT806.set("EXRGTM", entryTime)
    conEXT806.set("EXLMDT", entryDate)
    conEXT806.set("EXCHID",  program.getUser())
    conEXT806.set("EXCHNO", 1)

    dbaEXT806.insert(conEXT806)
    mi.outData.put("RSLT", "OK")
    mi.write()
  }
}