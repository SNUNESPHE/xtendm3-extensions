/****************************************************************************************
 Extension Name: EXT090MI.AddEntitlmtInfo
 Type: ExtendM3Transaction
 Script Author: ARENARD
 Date: 2024-09-11
 Description:
 * Add entitlement info
 * 5158 – Création des retours clients

 Revision History:
 Name                    Date             Version          Description of Changes
 ARENARD                 2024-09-11       1.0              5158 Création des retours clients
 ARENARD                 2025-09-18       1.1              Standardization of the program header comment block
 ******************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddEntitlmtInfo extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private int currentCompany = 0
  private String ENNO = ""
  private double inQTRC = 0
  private double inQTRA = 0
  private double inQREC = 0
  private double inQREA = 0
  private double inQTNC = 0
  private double inQTNA = 0


  public AddEntitlmtInfo(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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

    ENNO = mi.in.get("ENNO")
    if(mi.in.get("ENNO") != null){
      DBAction queryMITCEN = database.table("MITCEN").index("00").build()
      DBContainer MITCEN = queryMITCEN.getContainer()
      MITCEN.set("CTCONO", currentCompany)
      MITCEN.set("CTENNO", ENNO)
      if (!queryMITCEN.read(MITCEN)) {
        mi.error("Droit " + ENNO + " n'existe pas")
        return
      }
    } else {
      mi.error("Droit est obligatoire")
      return
    }

    if (mi.in.get("QTRC") != null) {
      if(!utility.call("NumberUtil","isValidNumber", mi.in.get("QTRC"),".")){
        mi.error("Format Quantité retournée (consigne) QTRC est invalide")
        return
      }
      inQTRC = mi.in.get("QTRC") as double
    }

    if (mi.in.get("QTRA") != null) {
      if(!utility.call("NumberUtil","isValidNumber", mi.in.get("QTRA"),".")){
        mi.error("Format Quantité retournée (Pièce neuve et garantie) QTRA est invalide")
        return
      }
      inQTRA = mi.in.get("QTRA") as double
    }

    if (mi.in.get("QREC") != null) {
      if(!utility.call("NumberUtil","isValidNumber", mi.in.get("QREC"),".")){
        mi.error("Format Quantité restante (consigne) QREC est invalide")
        return
      }
      inQREC = mi.in.get("QREC") as double
    }

    if (mi.in.get("QREA") != null) {
      if(!utility.call("NumberUtil","isValidNumber", mi.in.get("QREA"),".")){
        mi.error("Format Quantité restante (Pièce neuve et garantie) QREA est invalide")
        return
      }
      inQREA = mi.in.get("QREA") as double
    }

    if (mi.in.get("QTNC") != null) {
      if(!utility.call("NumberUtil","isValidNumber", mi.in.get("QTNC"),".")){
        mi.error("Format Quantité notifée (consigne) QTNC est invalide")
        return
      }
      inQTNC = mi.in.get("QTNC") as double
    }

    if (mi.in.get("QTNA") != null) {
      if(!utility.call("NumberUtil","isValidNumber", mi.in.get("QTNA"),".")){
        mi.error("Format Quantité notifée (Pièce neuve) QTNA est invalide")
        return
      }
      inQTNA = mi.in.get("QTNA") as double
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction queryEXT090 = database.table("EXT090").index("00").build()
    DBContainer EXT090 = queryEXT090.getContainer()
    EXT090.set("EXCONO", currentCompany)
    EXT090.set("EXENNO", ENNO)
    if (!queryEXT090.read(EXT090)) {
      EXT090.set("EXQTRC", inQTRC)
      EXT090.set("EXQTRA", inQTRA)
      EXT090.set("EXQREC", inQREC)
      EXT090.set("EXQREA", inQREA)
      EXT090.set("EXQTNC", inQTNC)
      EXT090.set("EXQTNA", inQTNA)
      EXT090.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT090.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT090.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT090.setInt("EXCHNO", 1)
      EXT090.set("EXCHID", program.getUser())
      queryEXT090.insert(EXT090)
    } else {
      mi.error("Droit " + ENNO + " existe déjà")
      return
    }
  }
}
