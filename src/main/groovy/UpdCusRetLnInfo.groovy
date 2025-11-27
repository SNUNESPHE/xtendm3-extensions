/****************************************************************************************
 Extension Name: EXT391MI.UpdCusRetLnInfo
 Type: ExtendM3Transaction
 Script Author: YJANNIN
 Date: 2024-09-09
 Description:
 * Update customer return line info from the EXT391 table
 * 5158 – Création des retours clients

 Revision History:
 Name                    Date             Version          Description of Changes
 YJANNIN                 2024-09-09       1.0              5158 Création des retours clients
 ARENARD                 2025-07-30       1.1              Corrections
 ARENARD                 2025-09-18       1.2              Standardization of the program header comment block
 ******************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdCusRetLnInfo extends ExtendM3Transaction {
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
  private Integer ZRSC
  private Double ZDC1
  private Double ZDC2
  private String ZCOM
  private String ZCO2


  public UpdCusRetLnInfo(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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

    ZRSC = 0
    if(mi.in.get("ZRSC") != null){
      ZRSC = mi.in.get("ZRSC")
    }

    ZDC1 = 0
    if(mi.in.get("ZDC1") != null){
      ZDC1 = mi.in.get("ZDC1") as double
    }

    ZDC2 = 0
    if(mi.in.get("ZDC2") != null){
      ZDC2 = mi.in.get("ZDC2") as double
    }

    ZCOM = ""
    if(mi.in.get("ZCOM") != null){
      ZCOM = mi.in.get("ZCOM")
    }

    ZCO2 = ""
    if(mi.in.get("ZCO2") != null){
      ZCO2 = mi.in.get("ZCO2")
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction qEXT391 = database.table("EXT391").index("00").selection("EXRELI").build()
    DBContainer EXT391 = qEXT391.getContainer()
    EXT391.set("EXCONO", currentCompany)
    EXT391.set("EXWHLO", WHLO)
    EXT391.set("EXREPN", REPN)
    EXT391.set("EXRELI", RELI)
    if (!qEXT391.readLock(EXT391, updateCallBackEXT391)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }

    mi.write()

  }

  // updateCallBackEXT391 :: Update EXT391
  Closure<?> updateCallBackEXT391 = { LockedResult  lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("EXCHNO")
    if(mi.in.get("ZRSC") != null) {
      lockedResult.set("EXZRSC", ZRSC)
    }
    if(mi.in.get("ZDC1") != null){
      lockedResult.set("EXZDC1", ZDC1)
    }
    if(mi.in.get("ZDC2") != null){
      lockedResult.set("EXZDC2", ZDC2)
    }
    if(mi.in.get("ZCOM") != null){
      lockedResult.set("EXZCOM", ZCOM)
    }
    if(mi.in.get("ZCO2") != null){
      lockedResult.set("EXZCO2", ZCO2)
    }
    lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("EXCHNO", changeNumber + 1)
    lockedResult.set("EXCHID", program.getUser())
    lockedResult.update()
  }
}
