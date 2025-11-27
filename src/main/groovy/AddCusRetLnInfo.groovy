/****************************************************************************************
 Extension Name: EXT391MI.AddCusRetLnInfo
 Type: ExtendM3Transaction
 Script Author: YJANNIN
 Date: 2024-09-09
 Description:
 * Add customer return line info to the EXT391 table
 * 5158 – Création des retours clients

 Revision History:
 Name                    Date             Version          Description of Changes
 YJANNIN                 2024-09-09       1.0              5158 Création des retours clients
 ARENARD                 2025-09-18       1.1              Standardization of the program header comment block / Extension has been fixed
 ******************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddCusRetLnInfo extends ExtendM3Transaction {
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


  public AddCusRetLnInfo(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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
    if (!qEXT391.read(EXT391)) {
      EXT391.set("EXZRSC", ZRSC)
      EXT391.set("EXZDC1", ZDC1)
      EXT391.set("EXZDC2", ZDC2)
      EXT391.set("EXZCOM", ZCOM)
      EXT391.set("EXZCO2", ZCO2)
      EXT391.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT391.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT391.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT391.setInt("EXCHNO", 1)
      EXT391.set("EXCHID", program.getUser())
      qEXT391.insert(EXT391)
    } else {
      mi.error("Ligne de réception " + RELI + " existe déjà")
      return
    }
  }
}
