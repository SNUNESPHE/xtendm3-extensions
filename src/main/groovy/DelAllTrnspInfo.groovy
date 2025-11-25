/**
 * README
 * This extension is used by scheduler
 *
 * Name : EXT040MI.DelAllTrnspInfo
 * Description : Delete all transport info
 * Date         Changed By   Description
 * 20241105     ARENARD      5004 - Modèle étiquette transport - GEODIS
 * 20250825     ARENARD      Extension has been fixed
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class DelAllTrnspInfo extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private int currentCompany
  private Integer nbMaxRecord = 10000

  public DelAllTrnspInfo(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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

    // Delete file
    deleteEXT040()
  }
  // Delete EXT040
  public void deleteEXT040() {
    DBAction query = database.table("EXT040").index("00").build()
    DBContainer EXT040 = query.getContainer()
    EXT040.set("EXCONO", currentCompany)

    Closure<?> deleteWorkFile = { DBContainer readResult ->
      query.readLock(readResult, { LockedResult lockedResult ->
        lockedResult.delete()
      })
    }

    query.readAll(EXT040, 1, nbMaxRecord, deleteWorkFile)
  }
}
