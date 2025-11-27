/****************************************************************************************
 Extension Name: EXT395MI.DltCustOrderLn
 Type: ExtendM3Transaction
 Script Author: YJANNIN
 Date: 2024-10-23
 Description:
 * Delete customer order line from the EXT395 table
 * 5158 – Création des retours clients

 Revision History:
 Name                    Date             Version          Description of Changes
 YJANNIN                 2024-10-23       1.0              5158 Création des retours clients
 ARENARD                 2025-09-18       1.1              Standardization of the program header comment block / Extension has been fixed
 ******************************************************************************************/

public class DltCustOrderLn extends ExtendM3Transaction {
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

  public DltCustOrderLn(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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

    DBAction queryEXT395 = database.table("EXT395").index("00").build()
    DBContainer EXT395 = queryEXT395.getContainer()
    EXT395.set("EXCONO", currentCompany)
    EXT395.set("EXORNO", inORNO)
    EXT395.set("EXPONR", inPONR)
    if (!queryEXT395.readLock(EXT395, deleteEXT395)) {
      mi.error("Numéro de ligne " + inPONR + " n'existe pas")
      return
    }
  }

  // updateCallBackEXT391 :: Update EXT391
  Closure<?> deleteEXT395 = { LockedResult  lockedResult ->
    lockedResult.delete()
  }
}
