/**
 * README
 * This extension is used by scheduler
 *
 * Name : EXT390MI.GetCusRetHdInfo
 * Description : Get customer return head info from the EXT390 table
 * Date         Changed By   Description
 * 20240909     YJANNIN       5158- Création des retours clients
 */

import java.time.LocalDateTime

public class GetCusRetHdInfo extends ExtendM3Transaction {
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


  public GetCusRetHdInfo(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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

    DBAction qEXT390 = database.table("EXT390").index("00").selection("EXZPAN", "EXZRET", "EXZNUM", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
    DBContainer EXT390 = qEXT390.getContainer()
    EXT390.set("EXCONO", currentCompany)
    EXT390.set("EXWHLO", WHLO)
    EXT390.set("EXREPN", REPN)
    EXT390.set("EXCUNO", CUNO)
    if (qEXT390.read(EXT390)) {
      String ZPAN = EXT390.get("EXZPAN")
      String ZRET = EXT390.get("EXZRET")
      String ZNUM = EXT390.get("EXZNUM")
      String entryDate = EXT390.get("EXRGDT")
      String entryTime = EXT390.get("EXRGTM")
      String changeDate = EXT390.get("EXLMDT")
      String changeNumber = EXT390.get("EXCHNO")
      String changedBy = EXT390.get("EXCHID")
      mi.outData.put("ZPAN", ZPAN)
      mi.outData.put("ZRET", ZRET)
      mi.outData.put("ZNUM", ZNUM)
      mi.outData.put("RGDT", entryDate)
      mi.outData.put("RGTM", entryTime)
      mi.outData.put("LMDT", changeDate)
      mi.outData.put("CHNO", changeNumber)
      mi.outData.put("CHID", changedBy)
      mi.write()
    } else {
      mi.error("Numéro de réception " + REPN + " n'existe pas")
      return
    }

  }
}
