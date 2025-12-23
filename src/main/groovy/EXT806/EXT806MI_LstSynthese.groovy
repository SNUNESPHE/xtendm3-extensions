/************************************************************************************************************************************************
Extension Name: EXT806MI.LstSynthese
Type: ExtendM3Transaction
Script Author: NRAOEL
Date: 2024-10-22
Description:
* List Lines from EXT809

Revision History:
Name          Date        Version   Description of Changes
NRAOEL        2025-11-28  1.0       Initial Release
NRAOEL        2025-12-16  1.1       Correction after validation submission
**************************************************************************************************************************************************/

public class LstSynthese extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database

  public int inCONO // Company
  public String inDIVI // Division
  public int maxRecords

  public LstSynthese(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
    this.mi = mi
    this.program = program
    this.database = database
  }

  public void main() {
    inCONO = !mi.inData.get("CONO").isBlank() ? mi.in.get("CONO") as Integer : program.LDAZD.get("CONO") as Integer
    inDIVI = mi.inData.get("DIVI").isBlank() ? program.LDAZD.get("DIVI") : mi.inData.get("DIVI").trim()
    maxRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() > 10000 ? 10000 : mi.getMaxRecords()
    
    DBAction dbaEXT809 = database.table("EXT809")
      .index("00")
      .selection("EXCUA1", "EXCUA2", "EXCUA3", "EXCUA4", "EXTOTL", "EXTOT1", "EXASGN", "EXSTAT")
      .build()

    DBContainer conEXT809 = dbaEXT809.getContainer()
    conEXT809.set("EXCONO", inCONO)
    conEXT809.set("EXDIVI", inDIVI)

    Closure<?> listRecords = { DBContainer data ->

      mi.outData.put("CONO", inCONO.toString())
      mi.outData.put("DIVI", inDIVI)
      mi.outData.put("ASGN", data.get("EXASGN").toString())
      mi.outData.put("STAT", data.get("EXSTAT").toString())
      mi.outData.put("CUA1", data.get("EXCUA1").toString())
      mi.outData.put("CUA2", data.get("EXCUA2").toString())
      mi.outData.put("CUA3", data.get("EXCUA3").toString())
      mi.outData.put("CUA4", data.get("EXCUA4").toString())
      mi.outData.put("TOT1", data.get("EXTOT1").toString())
      mi.outData.put("TOTL", data.get("EXTOTL").toString())

      mi.write()
    }

    if (!dbaEXT809.readAll(conEXT809, 2, maxRecords, listRecords)) {
      mi.error("Record(s) does not exist.")
      return
    }
  }
}
