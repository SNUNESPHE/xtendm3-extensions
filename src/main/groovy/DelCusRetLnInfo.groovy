/****************************************************************************************
 Extension Name: EXT391MI.DltCusRetLnInfo
 Type: ExtendM3Transaction
 Script Author: YJANNIN
 Date: 2024-09-09
 Description:
 * Delete customer return line info from the EXT391 table
 * 5158 – Création des retours clients

 Revision History:
 Name                    Date             Version          Description of Changes
 YJANNIN                 2024-09-09       1.0              5158 Création des retours clients
 ARENARD                 2025-09-18       1.1              Standardization of the program header comment block
 ******************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class DelCusRetLnInfo extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private int currentCompany
  private String WHLO
  private long REPN
  private Integer RELI


  public DelCusRetLnInfo(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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

    WHLO = mi.in.get("WHLO")
    if(mi.in.get("WHLO") != null){
      DBAction qMITWHL = database.table("MITWHL").index("00").selection("MWWHLO").build()
      DBContainer MITWHL = qMITWHL.getContainer()
      MITWHL.set("MWCONO", currentCompany)
      MITWHL.set("MWWHLO", WHLO)
      if (!qMITWHL.read(MITWHL)) {
        mi.error("Dépôt " + WHLO + " n'existe pas")
        return
      }
    } else {
      mi.error("Dépôt est obligatoire")
      return
    }

    REPN = mi.in.get("REPN") as long
    RELI = mi.in.get("RELI")
    if(mi.in.get("REPN") != null  && mi.in.get("RELI") != null){
      DBAction qOCLINE = database.table("OCLINE").index("00").selection("ODRELI").build()
      DBContainer OCLINE = qOCLINE.getContainer()
      OCLINE.set("ODCONO", currentCompany)
      OCLINE.set("ODWHLO", WHLO)
      OCLINE.set("ODREPN", REPN)
      OCLINE.set("ODRELI", RELI)
      if (!qOCLINE.read(OCLINE)) {
        mi.error("Ligne de réception " + RELI + " n'existe pas")
        return
      }
    } else {
      if(mi.in.get("REPN") == null){
        mi.error("Numéro de réception est obligatoire")
        return
      } else {
        mi.error("Ligne de réception est obligatoire")
        return
      }

    }

    DBAction qEXT391 = database.table("EXT391").index("00").selection("EXREPN").build()
    DBContainer EXT391 = qEXT391.getContainer()
    EXT391.set("EXCONO", currentCompany)
    EXT391.set("EXWHLO", WHLO)
    EXT391.set("EXREPN", REPN)
    EXT391.set("EXRELI", RELI)
    if (!qEXT391.readLock(EXT391, deleteEXT391)) {
      mi.error("Ligne de réception " + RELI + " n'existe pas")
      return
    }

  }

  // deleteEXT390 :: delete EXT390
  Closure<?> deleteEXT391 = { LockedResult  lockedResult ->
    lockedResult.delete()
  }
}
