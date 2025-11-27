/**
 * README
 * This extension is used by scheduler
 *
 * Name : EXT390MI.UpdHead
 * Description : Update customer return head from the OCHEAD table
 * Date         Changed By   Description
 * 20240909     YJANNIN       5158- Création des retours clients
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdHead extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private int currentCompany
  private String WHLO
  private long REPN
  private String CUNO
  private String RSCD
  private long ZRET
  private String ZNUM


  public UpdHead(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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

    CUNO = mi.in.get("CUNO")
    if(mi.in.get("CUNO") != null){
      DBAction qOCUSMA = database.table("OCUSMA").index("00").selection("OKCUNO").build()
      DBContainer OCUSMA = qOCUSMA.getContainer()
      OCUSMA.set("OKCONO", currentCompany)
      OCUSMA.set("OKCUNO", CUNO)
      if (!qOCUSMA.read(OCUSMA)) {
        mi.error("Code client " + CUNO + " n'existe pas")
        return
      }
    } else {
      mi.error("Code client est obligatoire")
      return
    }

    REPN = mi.in.get("REPN") as long
    if(mi.in.get("REPN") != null){
      DBAction qOCHEAD = database.table("OCHEAD").index("00").selection("OCREPN").build()
      DBContainer OCHEAD = qOCHEAD.getContainer()
      OCHEAD.set("OCCONO", currentCompany)
      OCHEAD.set("OCWHLO", WHLO)
      OCHEAD.set("OCREPN", REPN)
      OCHEAD.set("OCCUNO", CUNO)
      if (!qOCHEAD.read(OCHEAD)) {
        mi.error("Numéro de réception " + REPN + " n'existe pas")
        return
      }
    } else {
      mi.error("Numéro de réception est obligatoire")
      return
    }

    RSCD = ""
    if(mi.in.get("RSCD") != null){
      RSCD = mi.in.get("RSCD")
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction updOCHEAD = database.table("OCHEAD").index("00").selection("OCCHNO").build()
    DBContainer uOCHEAD = updOCHEAD.getContainer()
    uOCHEAD.set("OCCONO", currentCompany)
    uOCHEAD.set("OCWHLO", WHLO)
    uOCHEAD.set("OCREPN", REPN)
    uOCHEAD.set("OCCUNO", CUNO)
    if (!updOCHEAD.readLock(uOCHEAD, updateCallBackOCHEAD)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }

    mi.write()
  }


  // Update OCHEAD
  Closure<?> updateCallBackOCHEAD = { LockedResult lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("OCCHNO")
    lockedResult.set("OCRSCD", RSCD)
    lockedResult.setInt("OCLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("OCCHNO", changeNumber + 1)
    lockedResult.set("OCCHID", program.getUser())
    lockedResult.update()
  }
}
