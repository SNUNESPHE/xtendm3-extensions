/****************************************************************************************
 Extension Name: EXT391MI.GetCusRetLnInfo
 Type: ExtendM3Transaction
 Script Author: YJANNIN
 Date: 2024-09-09
 Description:
 * Get customer return line info from the EXT391 table
 * 5158 – Création des retours clients

 Revision History:
 Name                    Date             Version          Description of Changes
 YJANNIN                 2024-09-09       1.0              5158 Création des retours clients
 ARENARD                 2025-09-18       1.1              Standardization of the program header comment block
 ******************************************************************************************/

public class GetCusRetLnInfo extends ExtendM3Transaction {
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


  public GetCusRetLnInfo(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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

    DBAction qEXT391 = database.table("EXT391").index("00").selection("EXZRSC", "EXZDC1", "EXZDC2", "EXZCOM", "EXZCO2", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
    DBContainer EXT391 = qEXT391.getContainer()
    EXT391.set("EXCONO", currentCompany)
    EXT391.set("EXWHLO", WHLO)
    EXT391.set("EXREPN", REPN)
    EXT391.set("EXRELI", RELI)
    if (qEXT391.read(EXT391)) {
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
    } else {
      mi.error("L'enregistrement n'existe pas")
      return
    }


  }
}
