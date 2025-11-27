/****************************************************************************************
 Extension Name: EXT395MI.AddCustOrderLn
 Type: ExtendM3Transaction
 Script Author: ARENARD
 Date: 2024-10-07
 Description:
 * Add customer order line to the EXT395 table
 * 5158 – Création des retours clients

 Revision History:
 Name                    Date             Version          Description of Changes
 ARENARD                 2024-10-07       1.0              5158 Création des retours clients
 ARENARD                 2025-09-18       1.1              Standardization of the program header comment block / Extension has been fixed
 ARENARD                 2025-10-15       1.2              Expiration date added
 ******************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.LocalDate

public class AddCustOrderLn extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private int currentCompany
  private Integer inCONO
  private String inORNO
  private Integer inPONR
  private String inCUNO
  private String inFACI
  private Integer inDLDT
  private Long inDLIX
  private String inITNO
  private Double inDLQA
  private Double inSAPR
  private Double inZCOS
  private Double inZRNC
  private Double inZTNA
  private Double inZREC
  private Double inZREA
  private Double inZTRC
  private Double inZTR1
  private String inADR1
  private String inADR2
  private String inTOWN
  private String inPONO
  private String inCSCD

  public AddCustOrderLn(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  public void main() {
    // Check expiration date
    if (LocalDate.now().isAfter(LocalDate.of(2025, 12, 31))) {
      mi.error("Extension signature expired")
      logger.debug("Extension signature expired")
      return
    }

    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    // Compagnie
    inCONO = mi.in.get("CONO")

    // Numéro de commande
    if(mi.in.get("ORNO") != null && mi.in.get("ORNO") != ""){
      inORNO = mi.in.get("ORNO")
    } else {
      mi.error("Le N° de commande est obligatoire")
      return
    }

    // No ligne de commande
    if(mi.in.get("PONR") != null && mi.in.get("PONR") != ""){
      inPONR = mi.in.get("PONR")
    } else {
      mi.error("Le N° de ligne est obligatoire")
      return
    }
    // Client
    if(mi.in.get("CUNO") != null && mi.in.get("CUNO") != ""){
      inCUNO = mi.in.get("CUNO")
    } else {
      mi.error("Le code client est obligatoire")
      return
    }

    // Etablissement
    if(mi.in.get("FACI") != null && mi.in.get("FACI") != ""){
      inFACI = mi.in.get("FACI")
    } else {
      mi.error("L'établissement est obligatoire")
      return
    }

    // Date de livraison
    if(mi.in.get("DLDT") != null && mi.in.get("DLDT") != "") {
      if (!utility.call("DateUtil", "isDateValid", mi.in.get("DLDT"), "yyyyMMdd")) {
        mi.error("Date de livraison " + mi.in.get("DLDT") + " est invalide")
        return
      }
      inDLDT = mi.in.get("DLDT") as Integer
    } else {
      mi.error("La date de livraison est obligatoire")
      return
    }

    // Numéro de BL
    if(mi.in.get("DLIX") != null && mi.in.get("DLIX") != ""){
      inDLIX = mi.in.get("DLIX")
    } else {
      mi.error("L'index de livraison est obligatoire")
      return
    }

    // Code article
    if(mi.in.get("ITNO") != null && mi.in.get("ITNO") != ""){
      inITNO = mi.in.get("ITNO")
    } else {
      mi.error("Le code article est obligatoire")
      return
    }

    // Quantité livrée
    if(mi.in.get("DLQA") != null && mi.in.get("DLQA") != ""){
      inDLQA = mi.in.get("DLQA")
    } else {
      mi.error("La quantité livrée est obligatoire")
      return
    }

    // Prix de vente
    if(mi.in.get("SAPR") != null && mi.in.get("SAPR") != ""){
      inSAPR = mi.in.get("SAPR")
    } else {
      mi.error("Le prix de vente est obligatoire")
      return
    }

    // Montant consigne
    inZCOS = mi.in.get("ZCOS")

    // Quantité Notifiée (consigne)
    inZRNC = mi.in.get("ZRNC")

    // Quantité Notifiée (Pièce neuve et garantie)
    inZTNA = mi.in.get("ZTNA")

    // Quantité restante (consigne)
    inZREC = mi.in.get("ZREC")

    // Quantité restante (Pièce neuve et garantie)
    inZREA = mi.in.get("ZREA")

    // Quantité retournée (consigne)
    inZTRC = mi.in.get("ZTRC")

    // Quantité retournée (Pièce neuve et garantie)
    inZTR1 = mi.in.get("ZTR1")

    // Adress 1
    inADR1 = ""
    if(mi.in.get("ADR1") != null) {
      inADR1 = mi.in.get("ADR1")
    }

    // Adress 2
    inADR2 = ""
    if(mi.in.get("ADR2") != null) {
      inADR2 = mi.in.get("ADR2")
    }

    // Town
    inTOWN = ""
    if(mi.in.get("TOWN") != null) {
      inTOWN = mi.in.get("TOWN")
    }

    // Pono
    inPONO = ""
    if(mi.in.get("PONO") != null) {
      inPONO = mi.in.get("PONO")
    }

    // CSCD
    inCSCD = ""
    if(mi.in.get("CSCD") != null) {
      inCSCD = mi.in.get("CSCD")
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction queryEXT395 = database.table("EXT395").index("00").build()
    DBContainer EXT395 = queryEXT395.getContainer()
    EXT395.set("EXCONO", currentCompany)
    EXT395.set("EXORNO", inORNO)
    EXT395.set("EXPONR", inPONR)
    if (!queryEXT395.read(EXT395)) {
      EXT395.set("EXCUNO", inCUNO)
      EXT395.set("EXFACI", inFACI)
      EXT395.set("EXDLDT", inDLDT)
      EXT395.set("EXDLIX", inDLIX)
      EXT395.set("EXITNO", inITNO)
      EXT395.set("EXDLQA", inDLQA)
      EXT395.set("EXSAPR", inSAPR)
      EXT395.set("EXZCOS", inZCOS)
      EXT395.set("EXZRNC", inZRNC)
      EXT395.set("EXZTNA", inZTNA)
      EXT395.set("EXZREC", inZREC)
      EXT395.set("EXZREA", inZREA)
      EXT395.set("EXZTRC", inZTRC)
      EXT395.set("EXZTR1", inZTR1)
      EXT395.set("EXADR1", inADR1)
      EXT395.set("EXADR2", inADR2)
      EXT395.set("EXTOWN", inTOWN)
      EXT395.set("EXPONO", inPONO)
      EXT395.set("EXCSCD", inCSCD)
      EXT395.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT395.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT395.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT395.setInt("EXCHNO", 1)
      EXT395.set("EXCHID", program.getUser())
      queryEXT395.insert(EXT395)
    } else {
      mi.error("Numéro de ligne " + inPONR + " existe déjà")
      return
    }
  }
}
