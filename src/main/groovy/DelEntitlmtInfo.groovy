/****************************************************************************************
 Extension Name: EXT090MI.DelEntitlmtInfo
 Type: ExtendM3Transaction
 Script Author: ARENARD
 Date: 2024-09-11
 Description:
 * Delete entitlement info
 * 5158 – Création des retours clients

 Revision History:
 Name                    Date             Version          Description of Changes
 ARENARD                 2024-09-11       1.0              5158 Création des retours clients
 ARENARD                 2025-09-18       1.1              Standardization of the program header comment block
 ******************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class DelEntitlmtInfo extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private int currentCompany = 0
  private String ENNO = ""

  public DelEntitlmtInfo(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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

    if(mi.in.get("ENNO") != null){
      ENNO = mi.in.get("ENNO")
      DBAction queryMITCEN = database.table("MITCEN").index("00").build()
      DBContainer MITCEN = queryMITCEN.getContainer()
      MITCEN.set("CTCONO", currentCompany)
      MITCEN.set("CTENNO", ENNO)
      if (!queryMITCEN.read(MITCEN)) {
        mi.error("Droit " + ENNO + " n'existe pas")
        return
      }
    } else {
      mi.error("Droit est obligatoire")
      return
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction queryEXT090 = database.table("EXT090").index("00").build()
    DBContainer EXT090 = queryEXT090.getContainer()
    EXT090.set("EXCONO", currentCompany)
    EXT090.set("EXENNO", ENNO)
    if (!queryEXT090.readLock(EXT090, deleteEXT090)) {
      mi.error("Droit " + ENNO + " n'existe pas")
      return
    }
  }

  // Delete EXT090
  Closure<?> deleteEXT090 = { LockedResult  lockedResult ->
    lockedResult.delete()
  }
}
