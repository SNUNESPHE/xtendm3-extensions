/****************************************************************************************
 Extension Name: EXT395MI.UpdCustOrderLn
 Type: ExtendM3Transaction
 Script Author: YJANNIN
 Date: 2024-10-23
 Description:
 * Update customer order line from the EXT395 table
 * 5158 – Création des retours clients

 Revision History:
 Name                    Date             Version          Description of Changes
 YJANNIN                 2024-10-23       1.0              5158 Création des retours clients
 ARENARD                 2025-07-30       1.1              Corrections
 ARENARD                 2025-09-18       1.2              Standardization of the program header comment block / Extension has been fixed
 ARENARD                 2025-10-15       1.3              Expiration date added
 ******************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.LocalDate

public class UpdCustOrderLn extends ExtendM3Transaction {
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

  public UpdCustOrderLn(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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
    inORNO = mi.in.get("ORNO")

    // No ligne de commande
    inPONR = mi.in.get("PONR")

    // Client
    inCUNO = mi.in.get("CUNO")

    // Etablissement
    inFACI = mi.in.get("FACI")

    // Date de livraison
    if(mi.in.get("DLDT") != null && mi.in.get("DLDT") != "") {
      if (!utility.call("DateUtil", "isDateValid", mi.in.get("DLDT"), "yyyyMMdd")) {
        mi.error("Date de livraison " + mi.in.get("DLDT") + " est invalide")
        return
      }
      inDLDT = mi.in.get("DLDT") as Integer
    }

    // Numéro de BL
    inDLIX = mi.in.get("DLIX")

    // Code article
    inITNO = mi.in.get("ITNO")

    // Quantité livrée
    inDLQA = mi.in.get("DLQA")

    // Prix de vente
    inSAPR = mi.in.get("SAPR")

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
    if (!queryEXT395.readLock(EXT395, updateCallBackEXT395)) {
      mi.error("Numéro de ligne " + inPONR + " n'existe pas")
      return
    }
  }

  // updateCallBackEXT391 :: Update EXT391
  Closure<?> updateCallBackEXT395 = { LockedResult  lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("EXCHNO")
    if(mi.in.get("CUNO") != null) {
      lockedResult.set("EXCUNO", inCUNO)
    }
    if(mi.in.get("FACI") != null) {
      lockedResult.set("EXFACI", inFACI)
    }
    if (mi.in.get("DLDT") != null) {
      lockedResult.set("EXDLDT", inDLDT)
    }
    if (mi.in.get("DLIX") != null) {
      lockedResult.set("EXDLIX", inDLIX)
    }
    if (mi.in.get("ITNO") != null) {
      lockedResult.set("EXITNO", inITNO)
    }
    if (mi.in.get("DLQA") != null) {
      lockedResult.set("EXDLQA", inDLQA)
    }
    if (mi.in.get("SAPR") != null) {
      lockedResult.set("EXSAPR", inSAPR)
    }
    if (mi.in.get("ZCOS") != null) {
      lockedResult.set("EXZCOS", inZCOS)
    }
    if (mi.in.get("ZRNC") != null) {
      lockedResult.set("EXZRNC", inZRNC)
    }
    if (mi.in.get("ZTNA") != null) {
      lockedResult.set("EXZTNA", inZTNA)
    }
    if (mi.in.get("ZREC") != null) {
      lockedResult.set("EXZREC", inZREC)
    }
    if (mi.in.get("ZREA") != null) {
      lockedResult.set("EXZREA", inZREA)
    }
    if (mi.in.get("ZTRC") != null) {
      lockedResult.set("EXZTRC", inZTRC)
    }
    if (mi.in.get("ZTR1") != null) {
      lockedResult.set("EXZTR1", inZTR1)
    }
    if (mi.in.get("ADR1") != null) {
      lockedResult.set("EXADR1", inADR1)
    }
    if (mi.in.get("ADR2") != null) {
      lockedResult.set("EXADR2", inADR2)
    }
    if (mi.in.get("TOWN") != null) {
      lockedResult.set("EXTOWN", inTOWN)
    }
    if (mi.in.get("PONO") != null) {
      lockedResult.set("EXPONO", inPONO)
    }
    if (mi.in.get("CSCD") != null) {
      lockedResult.set("EXCSCD", inCSCD)
    }
    lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("EXCHNO", changeNumber + 1)
    lockedResult.set("EXCHID", program.getUser())
    lockedResult.update()
  }
}
