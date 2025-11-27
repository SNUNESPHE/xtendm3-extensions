/**
 * README
 * This extension is used by scheduler
 *
 * Name : EXT390MI.LstCusRetHdInfo
 * Description :List customer return head info from the EXT390 table
 * Date         Changed By   Description
 * 20240909     YJANNIN       5158- Création des retours clients
 */

public class LstCusRetHdInfo extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer nbMaxRecord = 10000
  private int currentCompany
  private String inWHLO
  private long inREPN
  private String inCUNO


  public LstCusRetHdInfo(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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

    inWHLO = mi.in.get("WHLO")
    if(mi.in.get("WHLO") != null){
      DBAction qMITWHL = database.table("MITWHL").index("00").selection("MWWHLO").build()
      DBContainer MITWHL = qMITWHL.getContainer()
      MITWHL.set("MWCONO", currentCompany)
      MITWHL.set("MWWHLO", inWHLO)
      if (!qMITWHL.read(MITWHL)) {
        mi.error("Dépôt " + inWHLO + " n'existe pas")
        return
      }
    } else {
      mi.error("Dépôt est obligatoire")
      return
    }

    inCUNO = mi.in.get("CUNO")
    if(mi.in.get("CUNO") != null){
      DBAction qOCUSMA = database.table("OCUSMA").index("00").selection("OKCUNO").build()
      DBContainer OCUSMA = qOCUSMA.getContainer()
      OCUSMA.set("OKCONO", currentCompany)
      OCUSMA.set("OKCUNO", inCUNO)
      if (!qOCUSMA.read(OCUSMA)) {
        mi.error("Code client " + inCUNO + " n'existe pas")
        return
      }
    } else {
      mi.error("Code client est obligatoire")
      return
    }

    ExpressionFactory expression = database.getExpressionFactory("EXT390")
    expression = expression.eq("EXCUNO", inCUNO)

    if(mi.in.get("REPN") != null){
      inREPN = mi.in.get("REPN")
      expression = expression.and(expression.ge("EXREPN", inREPN as String))
      DBAction qEXT390 = database.table("EXT390").index("00").matching(expression).selection("EXCONO","EXWHLO","EXREPN","EXCUNO","EXZPAN", "EXZRET", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer EXT390 = qEXT390.getContainer()
      EXT390.set("EXCONO", currentCompany)
      EXT390.set("EXWHLO", inWHLO)
      if (!qEXT390.readAll(EXT390, 2, nbMaxRecord, outData)) {

      } else {
        mi.error("L'enregistrement n'existe pas")
        return
      }
    } else {
      DBAction qEXT390 = database.table("EXT390").index("00").matching(expression).selection("EXCONO","EXWHLO","EXREPN","EXCUNO","EXZPAN", "EXZRET", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer EXT390 = qEXT390.getContainer()
      EXT390.set("EXCONO", currentCompany)
      EXT390.set("EXWHLO", inWHLO)
      if (!qEXT390.readAll(EXT390, 2, nbMaxRecord, outData)) {

      } else {
        mi.error("L'enregistrement n'existe pas")
        return
      }
    }

  }

  Closure<?> outData = { DBContainer EXT390 ->
    String CONO = EXT390.get("EXCONO")
    String WHLO = EXT390.get("EXWHLO")
    String oREPN = EXT390.get("EXREPN")
    String CUNO = EXT390.get("EXCUNO")
    String ZPAN = EXT390.get("EXZPAN")
    String ZRET = EXT390.get("EXZRET")
    String ZNUM = EXT390.get("EXZNUM")
    String entryDate = EXT390.get("EXRGDT")
    String entryTime = EXT390.get("EXRGTM")
    String changeDate = EXT390.get("EXLMDT")
    String changeNumber = EXT390.get("EXCHNO")
    String changedBy = EXT390.get("EXCHID")
    mi.outData.put("CONO", CONO)
    mi.outData.put("WHLO", WHLO)
    mi.outData.put("REPN", oREPN)
    mi.outData.put("CUNO", CUNO)
    mi.outData.put("ZPAN", ZPAN)
    mi.outData.put("ZRET", ZRET)
    mi.outData.put("ZNUM", ZNUM)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.write()
  }
}
