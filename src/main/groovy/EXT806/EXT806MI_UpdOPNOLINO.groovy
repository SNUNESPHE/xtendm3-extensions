/************************************************************************************************************************************************
Extension Name: EXT806MI.UpdOPNOLINO
Type: ExtendM3Transaction
Script Author: NRAOEL
Date: 2024-10-22
Description:
* Update OPNO & LINO field in EXT806

Revision History:
Name          Date        Version   Description of Changes
NRAOEL        2024-10-22  1.0       Initial Release
NRAOEL        2025-11-27  1.1       Correction after validation submission
**************************************************************************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


public class UpdOPNOLINO extends ExtendM3Transaction {

  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private final MICallerAPI miCaller
  private int inCONO // Company
  private String inDIVI // Division
  private String inVONO // Voucher Number
  private String opno // Operation number
  private int maxRecords //10000

  public UpdOPNOLINO(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
  }

  public void main() {
    inCONO = program.LDAZD.CONO
    if (!mi.inData.get("CONO").isBlank()) {
      inCONO = mi.in.get("CONO")
    }
    inDIVI = mi.inData.get("DIVI").isBlank() ? program.LDAZD.get("DIVI") : mi.inData.get("DIVI").trim()
    inVONO = mi.inData.get("VONO").isBlank() ? "" : mi.inData.get("VONO").trim()
    
    maxRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000 ? 10000 : mi.getMaxRecords()

    opno = getOPNO()
    updateOpnoLino()

    mi.outData.put("RSLT", "OK")
    mi.write()
  }

  /**
   * @updateOpnoLino - update COMP field for 103 & 104
   * @params - 
   * @returns - 
   */
  void updateOpnoLino() {
    ExpressionFactory expression = database.getExpressionFactory("EXT806")
    expression = expression.eq("EXVONO", inVONO).and(expression.eq("EXOPNO", "0"))

    DBAction dbaEXT806 = database.table("EXT806")
      .index("00")
      .matching(expression)
      .build()
    DBContainer conEXT806 = dbaEXT806.getContainer()
    conEXT806.set("EXCONO", inCONO)
    conEXT806.set("EXDIVI", inDIVI)

    int lino = 1
    dbaEXT806.readAll(conEXT806, 2, maxRecords, {
      DBContainer container ->
      
      DBAction dbaEXT806lines = database.table("EXT806").index("00").build()
      DBContainer conEXT806lines = dbaEXT806lines.createContainer()

      conEXT806lines.set("EXCONO", inCONO)
      conEXT806lines.set("EXDIVI", inDIVI)
      conEXT806lines.set("EXJRNO", container.get("EXJRNO"))
      conEXT806lines.set("EXJSNO", container.get("EXJSNO"))
      conEXT806lines.set("EXYEA4", container.get("EXYEA4"))
      conEXT806lines.set("EXTYLI", container.get("EXTYLI"))
      
      dbaEXT806lines.readLock(conEXT806lines, {LockedResult lockedResult ->
      String yea4 = lockedResult.get("EXYEA4").toString().trim()
      String jrno = lockedResult.get("EXJRNO").toString().trim()
      String jsno = lockedResult.get("EXJSNO").toString().trim()
      
      LocalDateTime dateTime = LocalDateTime.now()
      dateTime = java.time.LocalDateTime.now()
      int changedDate = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()

      String chno = lockedResult.get("EXCHNO")
      int incrementedChno = Integer.parseInt(chno.trim()) + 1

      lockedResult.set("EXOPNO", Integer.parseInt(opno))
      lockedResult.set("EXLINO", lino)
      lino++
      lockedResult.set("EXCHNO", incrementedChno)
      lockedResult.set("EXLMDT", changedDate)
      lockedResult.set("EXCHID", program.getUser())
      lockedResult.update()

      // AddInfoCat API add info for extracted line in FGLEDX
      Map < String,
      String > params = ["CONO": inCONO.toString(), "DIVI": inDIVI, "YEA4": yea4, "JRNO": jrno, "JSNO": jsno, "GEXI": opno]
      String resultAddInfoCat = null
      Closure < String > callback = {
        Map < String,
        String > response ->
        if (response.RSLT != null) {
          resultAddInfoCat = response.RSLT
        }
      }
      miCaller.call("EXT806MI", "AddInfoCat", params, callback)
      })
    })
  }

  /**
   * @getOPNO - REG01 : Read the counter from CRS165
   * @params - 
   * @returns - counter's value
   */
  String getOPNO() {
    Map < String, String > params = ["NBTY": "ZA".toString(), "NBID": "0".toString()]
    String operationNumber = null
    Closure < String > callback = {
      Map < String,
      String > response ->
      if (response.NBNR != null) {
        operationNumber = response.NBNR
      }
    }
    miCaller.call("CRS165MI", "RtvNextNumber", params, callback)
    return operationNumber.trim()
  }
}