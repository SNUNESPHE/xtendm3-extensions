/************************************************************************************************************************************************
Extension Name: EXT806MI.AddInfoCat
Type: ExtendM3Transaction
Script Author: NRAOEL
Date: 2024-10-22
Description:
* Add Line to the table FGLEDX

Revision History:
Name          Date        Version   Description of Changes
NRAOEL        2024-10-22  1.0       Initial Release
NRAOEL        2025-11-27  1.1       Correction after validation submission
**************************************************************************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddInfoCat extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  private int inCONO //Company
  private String inDIVI //Division
  private int inYEA4 //Year
  private int inJRNO //Journal Number
  private int inJSNO //Journal Sequence
  final int GEXN = 500 // GL Info Number
  final int GEXS = 1 // GL Info Sequence Number
  private String inGEXI // Additionnal Info

  public AddInfoCat(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
    this.mi = mi
    this.program = program
    this.database = database
  }

  public void main() {
    //inputs
    inCONO = mi.inData.get("CONO").isBlank() ? program.LDAZD.get("CONO") as Integer : mi.in.get("CONO") as Integer
    inDIVI = mi.inData.get("DIVI").isBlank() ? program.LDAZD.get("DIVI") : mi.inData.get("DIVI").trim()
    inJRNO = mi.in.get("JRNO") as Integer == null ? 0 : mi.in.get("JRNO") as Integer
    inJSNO = mi.in.get("JSNO") as Integer == null ? 0 : mi.in.get("JSNO") as Integer
    inYEA4 = mi.in.get("YEA4") as Integer == null ? 0 : mi.in.get("YEA4") as Integer
    inGEXI = mi.inData.get("GEXI").isBlank() ? "" : mi.inData.get("GEXI").trim()

    DBAction query = database.table("FGLEDG")
      .index("00")
      .build()
    DBContainer container = query.getContainer()
    container.set("EGCONO", inCONO)
    container.set("EGDIVI", inDIVI)
    container.set("EGYEA4", inYEA4)
    container.set("EGJSNO", inJSNO)
    container.set("EGJRNO", inJRNO)

    if (!query.read(container)) {
      mi.outData.put("RSLT", "KO")
      mi.write()
    } else {
      //add to table
      DBAction dba = database.table("FGLEDX").index("00").build()
      DBContainer container1 = dba.createContainer()
      LocalDateTime dateTime = LocalDateTime.now()
      int entryDate = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
      int entryTime = dateTime.format(DateTimeFormatter.ofPattern("HHmmss")).toInteger()

      if (inCONO && inDIVI && inJRNO && inJSNO && inYEA4 && inGEXI && GEXN && GEXS) {
        container1.set("EGCONO", inCONO)
        container1.set("EGDIVI", inDIVI)
        container1.set("EGJRNO", inJRNO)
        container1.set("EGJSNO", inJSNO)
        container1.set("EGYEA4", inYEA4)
        container1.set("EGGEXI", inGEXI)
        container1.set("EGGEXN", GEXN)
        container1.set("EGGEXS", GEXS)
        container1.set("EGTXID", 9999999999999)
        container1.set("EGRGDT", entryDate)
        container1.set("EGRGTM", entryTime)
        container1.set("EGLMDT", entryDate)
        container1.set("EGCHID", program.getUser())
        container1.set("EGCHNO", 1)

        dba.insert(container1)
        mi.outData.put("RSLT", "OK")
        mi.write()
      } else {
        mi.outData.put("RSLT", "NOK")
        mi.write()
      }
    }
  }
}
