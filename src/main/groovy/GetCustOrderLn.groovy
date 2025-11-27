/****************************************************************************************
 Extension Name: EXT395MI.GetCustOrderLn
 Type: ExtendM3Transaction
 Script Author: YJANNIN
 Date: 2024-10-23
 Description:
 * Get customer order line from the EXT395 table
 * 5158 – Création des retours clients

 Revision History:
 Name                    Date             Version          Description of Changes
 YJANNIN                 2024-10-23       1.0              5158 Création des retours clients
 ARENARD                 2025-09-18       1.1              Standardization of the program header comment block
 ******************************************************************************************/

import java.time.LocalDateTime

public class GetCustOrderLn extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private int currentCompany
  private Integer inCONO
  private String inORNO
  private Integer inPONR

  public GetCustOrderLn(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    inCONO = mi.in.get("CONO")

    inORNO = mi.in.get("ORNO")

    inPONR = mi.in.get("PONR")

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction queryEXT395 = database.table("EXT395").index("00").selection("EXCUNO", "EXFACI", "EXDLDT", "EXDLIX", "EXITNO", "EXDLQA", "EXSAPR", "EXZCOS", "EXZRNC", "EXZTNA", "EXZREC", "EXZREA", "EXZTRC", "EXZTR1", "EXADR1", "EXADR2", "EXTOWN", "EXPONO", "EXCSCD", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
    DBContainer EXT395 = queryEXT395.getContainer()
    EXT395.set("EXCONO", currentCompany)
    EXT395.set("EXORNO", inORNO)
    EXT395.set("EXPONR", inPONR)
    if (queryEXT395.read(EXT395)) {
      String CUNO = EXT395.get("EXCUNO")
      String FACI = EXT395.get("EXFACI")
      String DLDT = EXT395.get("EXDLDT")
      String DLIX = EXT395.get("EXDLIX")
      String ITNO = EXT395.get("EXITNO")
      String DLQA = EXT395.get("EXDLQA")
      String SAPR = EXT395.get("EXSAPR")
      String ZCOS = EXT395.get("EXZCOS")
      String ZRNC = EXT395.get("EXZRNC")
      String ZTNA = EXT395.get("EXZTNA")
      String ZREC = EXT395.get("EXZREC")
      String ZREA = EXT395.get("EXZREA")
      String ZTRC = EXT395.get("EXZTRC")
      String ZTR1 = EXT395.get("EXZTR1")
      String ADR1 = EXT395.get("EXADR1")
      String ADR2 = EXT395.get("EXADR2")
      String TOWN = EXT395.get("EXTOWN")
      String PONO = EXT395.get("EXPONO")
      String CSCD = EXT395.get("EXCSCD")
      String entryDate = EXT395.get("EXRGDT")
      String entryTime = EXT395.get("EXRGTM")
      String changeDate = EXT395.get("EXLMDT")
      String changeNumber = EXT395.get("EXCHNO")
      String changedBy = EXT395.get("EXCHID")

      String CUNM = ""
      DBAction queryOCUSMA = database.table("OCUSMA").index("00").selection("OKCUNM").build()
      DBContainer OCUSMA = queryOCUSMA.getContainer()
      OCUSMA.set("OKCONO", currentCompany)
      OCUSMA.set("OKCUNO", CUNO)
      if (queryOCUSMA.read(OCUSMA)) {
        CUNM = OCUSMA.get("OKCUNM")
      }

      mi.outData.put("CUNO", CUNO)
      mi.outData.put("CUNM", CUNM)
      mi.outData.put("FACI", FACI)
      mi.outData.put("DLDT", DLDT)
      mi.outData.put("DLIX", DLIX)
      mi.outData.put("ITNO", ITNO)
      mi.outData.put("DLQA", DLQA)
      mi.outData.put("SAPR", SAPR)
      mi.outData.put("ZCOS", ZCOS)
      mi.outData.put("ZRNC", ZRNC)
      mi.outData.put("ZTNA", ZTNA)
      mi.outData.put("ZREC", ZREC)
      mi.outData.put("ZREA", ZREA)
      mi.outData.put("ZTRC", ZTRC)
      mi.outData.put("ZTR1", ZTR1)
      mi.outData.put("ADR1", ADR1)
      mi.outData.put("ADR2", ADR2)
      mi.outData.put("TOWN", TOWN)
      mi.outData.put("PONO", PONO)
      mi.outData.put("CSCD", CSCD)
      mi.outData.put("RGDT", entryDate)
      mi.outData.put("RGTM", entryTime)
      mi.outData.put("LMDT", changeDate)
      mi.outData.put("CHNO", changeNumber)
      mi.outData.put("CHID", changedBy)
      mi.write()
    } else {
      mi.error("Numéro de ligne " + inPONR + " n'existe pas")
      return
    }
  }
}
