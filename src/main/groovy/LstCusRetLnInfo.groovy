/****************************************************************************************
 Extension Name: EXT391MI.LstCusRetLnInfo
 Type: ExtendM3Transaction
 Script Author: YJANNIN
 Date: 2024-09-09
 Description:
 * List customer return line info from the EXT391 table
 * 5158 – Création des retours clients

 Revision History:
 Name                    Date             Version          Description of Changes
 YJANNIN                 2024-09-09       1.0              5158 Création des retours clients
 ARENARD                 2025-09-18       1.1              Standardization of the program header comment block / Extension has been fixed
 ******************************************************************************************/

public class LstCusRetLnInfo extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer nbMaxRecord = 10000
  private int currentCompany
  private String WHLO
  private long inREPN
  private Integer inRELI


  public LstCusRetLnInfo(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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

    if(mi.in.get("REPN") != null){
      inREPN = mi.in.get("REPN") as long
      ExpressionFactory expression = database.getExpressionFactory("EXT391")
      expression = expression.ge("EXREPN", inREPN as String)
      if(mi.in.get("RELI") != null){
        inRELI = mi.in.get("RELI")
        expression = expression.and(expression.ge("EXRELI", inRELI as String))
      }
      DBAction qEXT391 = database.table("EXT391").index("00").matching(expression).selection("EXREPN", "EXRELI", "EXZRSC", "EXZDC1", "EXZDC2", "EXZCOM", "EXZCO2", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer EXT391 = qEXT391.getContainer()
      EXT391.set("EXCONO", currentCompany)
      EXT391.set("EXWHLO", WHLO)
      if (!qEXT391.readAll(EXT391, 2, nbMaxRecord, outData)) {
        mi.error("L'enregistrement n'existe pas")
        return
      }
    } else {
      DBAction qEXT391 = database.table("EXT391").index("00").selection("EXREPN", "EXRELI", "EXZRSC", "EXZDC1", "EXZDC2", "EXZCOM", "EXZCO2", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer EXT391 = qEXT391.getContainer()
      EXT391.set("EXCONO", currentCompany)
      EXT391.set("EXWHLO", WHLO)
      if (!qEXT391.readAll(EXT391, 2, nbMaxRecord, outData)) {
        mi.error("L'enregistrement n'existe pas")
        return
      }
    }


  }

  Closure<?> outData = { DBContainer EXT391 ->
    String REPN = EXT391.get("EXREPN")
    String RELI = EXT391.get("EXRELI")
    String ZRSC = EXT391.get("EXZRSC")
    String ZDC1 = EXT391.get("EXZDC1")
    String ZDC2 = EXT391.get("EXZDC2")
    String ZCOM = EXT391.get("EXZCOM")
    String ZCO2 = EXT391.get("EXZCO2")
    String entryDate = EXT391.get("EXRGDT")
    String entryTime = EXT391.get("EXRGTM")
    String changeDate = EXT391.get("EXLMDT")
    String changeNumber = EXT391.get("EXCHNO")
    String changedBy = EXT391.get("EXCHID")
    mi.outData.put("REPN", REPN)
    mi.outData.put("RELI", RELI)
    mi.outData.put("ZRSC", ZRSC)
    mi.outData.put("ZDC1", ZDC1)
    mi.outData.put("ZDC2", ZDC2)
    mi.outData.put("ZCOM", ZCOM)
    mi.outData.put("ZCO2", ZCO2)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.write()
  }

}
