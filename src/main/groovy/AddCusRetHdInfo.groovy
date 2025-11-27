/**
 * README
 * This extension is used by scheduler
 *
 * Name : EXT390MI.AddCusRetHdInfo
 * Description : Add customer return head info to the EXT390 table
 * Date         Changed By   Description
 * 20240909     YJANNIN       5158- Création des retours clients
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddCusRetHdInfo extends ExtendM3Transaction {
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
  private String ZPAN
  private long ZRET
  private String ZNUM


  public AddCusRetHdInfo(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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


    ZPAN = ""
    if(mi.in.get("ZPAN") != null){
      ZPAN = mi.in.get("ZPAN")
    }

    ZRET = 0
    if(mi.in.get("ZRET") != null){
      ZRET = mi.in.get("ZRET") as long
    }
    ZNUM = ""
    if(mi.in.get("ZPAN") != null){
      ZNUM = mi.in.get("ZNUM")
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction qEXT390 = database.table("EXT390").index("00").selection("EXREPN").build()
    DBContainer EXT390 = qEXT390.getContainer()
    EXT390.set("EXCONO", currentCompany)
    EXT390.set("EXWHLO", WHLO)
    EXT390.set("EXREPN", REPN)
    EXT390.set("EXCUNO", CUNO)
    if (!qEXT390.read(EXT390)) {
      EXT390.set("EXZPAN", ZPAN)
      EXT390.set("EXZRET", ZRET)
      EXT390.set("EXZNUM", ZNUM)
      EXT390.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT390.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT390.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT390.setInt("EXCHNO", 1)
      EXT390.set("EXCHID", program.getUser())
      qEXT390.insert(EXT390)
    } else {
      mi.error("Numéro de réception " + REPN + " existe déjà")
      return
    }

    mi.write()

  }
}
