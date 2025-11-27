/****************************************************************************************************************************************************
 Extension Name: EXT392MI.GenCustRetByPay
 Type: ExtendM3Transaction
 Script Author: ARENARD
 Date: 20240909
 Description:   5158- Création des retours clients
 * Description of script functionality
 Generation of customer returns by payer

 Revision History:
 Name                    Date             Version          Description of Changes
 ARENARD                 2025-09-30       1.0              5158- Création des retours clients
 ARENARD                 2025-10-02       1.1              ReadAll limits have been significantly reduced to optimize performance
 ARENARD                 2025-10-15       1.2              logger.info removed
 ******************************************************************************************************************************************************/


import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

public class GenCustRetByPay extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer nbMaxRecord = 10000
  private int currentCompany
  private String inCUNO
  private LocalDate currentDate
  private boolean IN60
  private boolean foundPriority
  private double deliveredInvoicedQuantity
  private double returnedQuantity
  private double inREQ0
  private String wCUNO
  private String wRORN
  private Integer wPONR
  private Integer wPOSX
  private double wSAPR
  private double rSAPR
  private double rZCOS
  private Integer wDLDT
  private String wFACI
  private double wUCOS
  private boolean consignedItem
  private boolean consignmentReasonCode
  private boolean consignedReturn
  private double wREQ0
  private double cREQ0
  private String inITNO
  private double inAPPR
  private String wDIVI
  private String inWHLO
  private long wREPN
  private long receivingNumber
  private long receivingNumber2
  private long receivingNumber3
  private long receivingNumber4
  private long receivingNumber5
  private boolean researchEXT395
  private Integer lineCounter
  private String errorMsg
  private String inRESP
  private String inZPAN
  private boolean consignedPN
  private String inFACI
  private String inRSC1
  private long inZRET
  private String inZNUM
  private Integer wRORC
  private String currentDateMinusTwoYears
  private String currentDateAlpha
  private double inSAPR
  private Integer wRELI
  private String wENNO
  private Integer savedRORC
  private String inRSCD
  private double decote
  private boolean newStuff
  private String inORNO
  private Integer transactionDate
  private long daysDifference
  private String inZCOM
  private String inZCO2
  private double last12MonthsSales
  private double last12MonthsReturnsForNewParts
  private String abcClass
  private String abcClassRefused
  private double OIS399ConfiguredRate
  private double OIS399NumberOfLine
  private String inCFI1
  private boolean erreurSAPR
  private Integer inZIMP
  private Integer inDSP1
  private Integer inPONR

  public GenCustRetByPay(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  // Main
  public void main() {
    currentCompany = (Integer) program.getLDAZD().CONO
    inCUNO = mi.in.get("PYNO")
    if(mi.in.get("CUNO") != null){
      DBAction qOCUSMA = database.table("OCUSMA").index("00").selection("OKCUNO").build()
      DBContainer OCUSMA = qOCUSMA.getContainer()
      OCUSMA.set("OKCONO", currentCompany)
      OCUSMA.set("OKCUNO", inCUNO)
      if (!qOCUSMA.read(OCUSMA)) {
        mi.error("1002 Client n’existe pas ")
        return
      }
    } else {
      mi.error("1005 Code client est obligatoire")
      return
    }


    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    LocalDate dateMinusTwoYears = LocalDate.now().minusYears(2)
    currentDateMinusTwoYears = dateMinusTwoYears.format(formatter)

    LocalDate dateMinus366Days = LocalDate.now().minusDays(366)
    String currentDateMinus366 = dateMinus366Days.format(formatter)

    currentDate = LocalDate.now()
    currentDateAlpha = currentDate.format(formatter)

    last12MonthsSales = 0
    ExpressionFactory expressionOOHEAD = database.getExpressionFactory("OOHEAD")
    expressionOOHEAD = expressionOOHEAD.gt("OAORDT", currentDateMinus366)
    expressionOOHEAD = expressionOOHEAD.and(expressionOOHEAD.gt("OANTLA", "0"))
    expressionOOHEAD = expressionOOHEAD.and((expressionOOHEAD.ge("OAORST", "66")).or(expressionOOHEAD.eq("OAORST", "26")).or(expressionOOHEAD.eq("OAORST", "27")).or(expressionOOHEAD.eq("OAORST", "36")).or(expressionOOHEAD.eq("OAORST", "37")).or(expressionOOHEAD.eq("OAORST", "46")).or(expressionOOHEAD.eq("OAORST", "47")))
    expressionOOHEAD = expressionOOHEAD.and((expressionOOHEAD.ge("OAORSL", "66")).or(expressionOOHEAD.eq("OAORSL", "26")).or(expressionOOHEAD.eq("OAORSL", "27")).or(expressionOOHEAD.eq("OAORSL", "36")).or(expressionOOHEAD.eq("OAORSL", "37")).or(expressionOOHEAD.eq("OAORSL", "46")).or(expressionOOHEAD.eq("OAORSL", "47")))
    DBAction qOOHEAD = database.table("OOHEAD").index("10").matching(expressionOOHEAD).selection("OANTLA").build()
    DBContainer OOHEAD = qOOHEAD.getContainer()
    OOHEAD.set("OACONO", currentCompany)
    OOHEAD.set("OACUNO", inCUNO)
    if (!qOOHEAD.readAll(OOHEAD, 2, nbMaxRecord, outDataOOHEAD)) {

    }

    last12MonthsReturnsForNewParts = 0
    ExpressionFactory expressionOCLINE = database.getExpressionFactory("OCLINE")
    expressionOCLINE = expressionOCLINE.gt("ODEPDT", currentDateMinus366)
    DBAction qOCLINE = database.table("OCLINE").index("50").matching(expressionOCLINE).selection("ODRSCD", "ODSAPR", "ODREQ7").build()
    DBContainer OCLINE = qOCLINE.getContainer()
    OCLINE.set("ODCONO", currentCompany)
    OCLINE.set("ODCUNO", inCUNO)
    if (!qOCLINE.readAll(OCLINE, 2, nbMaxRecord, outDataOCLINE)) {

    }

    inWHLO = mi.in.get("WHLO")
    if(mi.in.get("WHLO") != null){
      DBAction qMITWHL = database.table("MITWHL").index("00").selection("MWFACI").build()
      DBContainer MITWHL = qMITWHL.getContainer()
      MITWHL.set("MWCONO", currentCompany)
      MITWHL.set("MWWHLO", inWHLO)
      if (!qMITWHL.read(MITWHL)) {
        mi.error("6002 Code dépôt n’existe pas ")
        return
      } else {
        inFACI = MITWHL.get("MWFACI")
      }
    } else {
      mi.error("6003 Code dépôt est obligatoire")
      return
    }

    OIS399ConfiguredRate = 0
    OIS399NumberOfLine = 0
    abcClassRefused = ""
    DBAction qCUGEX1OREPAR = database.table("CUGEX1").index("00").selection("F1N096", "F1N196", "F1A030").build()
    DBContainer CUGEX1OREPAR = qCUGEX1OREPAR.getContainer()
    CUGEX1OREPAR.set("F1CONO", currentCompany)
    CUGEX1OREPAR.set("F1FILE",  "OREPAR")
    CUGEX1OREPAR.set("F1PK01",  inWHLO)
    CUGEX1OREPAR.set("F1PK02",  "")
    CUGEX1OREPAR.set("F1PK03",  "")
    CUGEX1OREPAR.set("F1PK04",  "")
    CUGEX1OREPAR.set("F1PK05",  "")
    CUGEX1OREPAR.set("F1PK06",  "")
    CUGEX1OREPAR.set("F1PK07",  "")
    CUGEX1OREPAR.set("F1PK08",  "")
    if(qCUGEX1OREPAR.read(CUGEX1OREPAR)){
      OIS399NumberOfLine = CUGEX1OREPAR.get("F1N096")
      OIS399ConfiguredRate = CUGEX1OREPAR.get("F1N196")
      abcClassRefused = CUGEX1OREPAR.get("F1A030")
    } else {
      mi.error("6001 Retour sur le dépôt " + inWHLO + " n’est pas autorisé")
      return
    }

    logger.debug("last 12 Months Returns For New Parts = " + last12MonthsReturnsForNewParts)
    logger.debug("last 12 Months Sales = " + last12MonthsSales)
    logger.debug("OIS399 Configured Rate = " + OIS399ConfiguredRate)
    if(last12MonthsReturnsForNewParts != 0 && last12MonthsSales != 0){
      double cTaux = last12MonthsSales/last12MonthsReturnsForNewParts
      if(cTaux>OIS399ConfiguredRate){
        mi.error("1003 Taux de retour dépassé")
        return
      }
    }

    inRSC1 = mi.in.get("RSC1")
    if(mi.in.get("RSC1") != null){
      DBAction qCSYTAB = database.table("CSYTAB").index("00").selection("CTPARM").build()
      DBContainer CSYTAB = qCSYTAB.getContainer()
      CSYTAB.set("CTCONO", currentCompany)
      CSYTAB.set("CTDIVI", "")
      CSYTAB.set("CTSTCO", "RSCD")
      CSYTAB.set("CTSTKY", inRSC1)
      CSYTAB.set("CTLNCD", "")
      if (!qCSYTAB.read(CSYTAB)) {
        mi.error("2001 Motif de retour en-tête " + inRSC1 + " n’existe pas ")
        return
      }
    } else {
      mi.error("2003 Motif de retour est obligatoire")
      return
    }

    consignmentReasonCode = false
    newStuff = false
    consignedPN = false
    inRSCD = mi.in.get("RSCD")
    if(mi.in.get("RSCD") != null){
      DBAction qCSYTAB = database.table("CSYTAB").index("00").selection("CTPARM").build()
      DBContainer CSYTAB = qCSYTAB.getContainer()
      CSYTAB.set("CTCONO", currentCompany)
      CSYTAB.set("CTDIVI", "")
      CSYTAB.set("CTSTCO", "RSCD")
      CSYTAB.set("CTSTKY", inRSCD)
      CSYTAB.set("CTLNCD", "")
      if (!qCSYTAB.read(CSYTAB)) {
        mi.error("3001 Motif de retour ligne " + inRSCD + " n’existe pas ")
        return
      }
      DBAction qCUGEX1RSCD = database.table("CUGEX1").index("00").selection("F1A030").build()
      DBContainer CUGEX1RSCD = qCUGEX1RSCD.getContainer()
      CUGEX1RSCD.set("F1CONO", currentCompany)
      CUGEX1RSCD.set("F1FILE",  "CSYTAB")
      CUGEX1RSCD.set("F1PK01",  "")
      CUGEX1RSCD.set("F1PK02",  "RSCD")
      CUGEX1RSCD.set("F1PK03",  inRSCD)
      CUGEX1RSCD.set("F1PK04",  "")
      CUGEX1RSCD.set("F1PK05",  "")
      CUGEX1RSCD.set("F1PK06",  "")
      CUGEX1RSCD.set("F1PK07",  "")
      CUGEX1RSCD.set("F1PK08",  "")
      if(qCUGEX1RSCD.read(CUGEX1RSCD)){
        if(CUGEX1RSCD.get("F1A030") == "CN"){
          consignmentReasonCode = true
        }
        if(CUGEX1RSCD.get("F1A030") == "PN"){
          newStuff = true
        }
        if(CUGEX1RSCD.get("F1A030") == "PN" || CUGEX1RSCD.get("F1A030") == "GA"){
          consignedPN = true
        }
      }
    } else {
      mi.error("3002 Motif de retour est obligatoire")
      return
    }

    inCFI1 = ""
    if(mi.in.get("CFI1") != null){
      inCFI1 = mi.in.get("CFI1")
      DBAction qCSYTAB = database.table("CSYTAB").index("00").selection("CTPARM").build()
      DBContainer CSYTAB = qCSYTAB.getContainer()
      CSYTAB.set("CTCONO", currentCompany)
      CSYTAB.set("CTDIVI", "")
      CSYTAB.set("CTSTCO", "CFI1")
      CSYTAB.set("CTSTKY", inCFI1)
      CSYTAB.set("CTLNCD", "")
      if (!qCSYTAB.read(CSYTAB)) {
        mi.error("Marque n’existe pas ")
        return
      }
    }

    inITNO = mi.in.get("ITNO")
    if(mi.in.get("ITNO") != null){
      IN60 = false
      chkItno()
      if(IN60){
        mi.error(errorMsg)
        return
      }
      DBAction qMITMAS00 = database.table("MITMAS").index("00").selection("MMCFI1", "MMCRI1").build()
      DBContainer MITMAS00 = qMITMAS00.getContainer()
      MITMAS00.set("MMCONO", currentCompany)
      MITMAS00.set("MMITNO", inITNO)
      if (qMITMAS00.read(MITMAS00)) {
        String oCFI1 = MITMAS00.get("MMCFI1")
        String oCRI1 = MITMAS00.get("MMCRI1")
        if(inCFI1.trim() != oCFI1.trim() && inCFI1 != ""){
          mi.error("7001 Code article invalide")
          return
        }
        if(oCRI1.trim()!=""){
          consignedItem = true
        } else {
          consignedItem = false
        }

      } else {
        mi.error("7001 Code article " + inITNO + " est invalide")
        return
      }
    }


    logger.debug("Consigned item = " + consignedItem)
    logger.debug("Consigned PN = " + consignedPN)
    logger.debug("Consignement reason code = " + consignmentReasonCode)
    logger.debug("New Stuff = " + newStuff)
    if(consignmentReasonCode && !consignedItem){
      mi.error("9004 Motif de retour " + inRSCD + " non autorisé ")
      return
    }

    DBAction qMITBAL = database.table("MITBAL").index("00").selection("MBMABC").build()
    DBContainer MITBAL = qMITBAL.getContainer()
    MITBAL.set("MBCONO", currentCompany)
    MITBAL.set("MBWHLO", inWHLO)
    MITBAL.set("MBITNO", inITNO)
    if (qMITBAL.read(MITBAL)) {
      abcClass = MITBAL.get("MBMABC")
      if(abcClass.trim() == abcClassRefused.trim() && abcClassRefused.trim() != ""){
        mi.error("7002 Article à forte rotation interdit")
        return
      }
    }

    inAPPR = 0
    DBAction qMITFAC = database.table("MITFAC").index("00").selection("M9APPR").build()
    DBContainer MITFAC = qMITFAC.getContainer()
    MITFAC.set("M9CONO", currentCompany)
    MITFAC.set("M9FACI", inFACI)
    MITFAC.set("M9ITNO", inITNO)
    if (qMITFAC.read(MITFAC)) {
      inAPPR = MITFAC.get("M9APPR")
    }
    if(inAPPR == 0){
      mi.error("Prix moyen manquant")
      return
    }

    inREQ0 = 0
    if(mi.in.get("REQ0") != null){
      inREQ0 = mi.in.get("REQ0")
    }
    if(inREQ0<=0){
      mi.error("Quantité doit être supérieure à 0")
      return
    }

    inDSP1 = 0
    if(mi.in.get("DSP1") != null){
      inDSP1 = mi.in.get("DSP1")
      if(inDSP1 !=0 && inDSP1 != 1){
        mi.error("Validation avertissement incorrect")
        return
      }
    }

    inZIMP = 0
    if(mi.in.get("ZIMP") != null){
      inZIMP = mi.in.get("ZIMP")
      if(inZIMP !=0 && inZIMP != 1){
        mi.error("Impression AR incorrect")
        return
      }
    }

    inSAPR = 0
    if(mi.in.get("SAPR") != null) {
      inSAPR = mi.in.get("SAPR")
    }

    researchEXT395 = true
    inORNO = ""
    if(mi.in.get("ORNO") != null){
      inORNO = mi.in.get("ORNO")
      DBAction qOOHEAD0 = database.table("OOHEAD").index("00").selection("OACUNO").build()
      DBContainer OOHEAD0 = qOOHEAD0.getContainer()
      OOHEAD0.set("OACONO", currentCompany)
      OOHEAD0.set("OAORNO", inORNO)
      if (qOOHEAD0.read(OOHEAD0)) {
        logger.info("GenCustRetByPay => Commande " + inORNO + " existe")
        String oCUNO = OOHEAD0.get("OACUNO")
        if(inCUNO != oCUNO.trim()){
          logger.debug("GenCustRetByPay => 4001 Commande " + inORNO + " n’est pas reliée au client " + inCUNO)
          mi.error("4001 Commande " + inORNO + " n’est pas reliée au client " + inCUNO)
          return
        }

        if(inDSP1 == 0){
          erreurSAPR = false
          DBAction qOOLINE = database.table("OOLINE").index("80").selection("OBPONR", "OBPOSX", "OBNEPR").build()
          DBContainer OOLINE = qOOLINE.getContainer()
          OOLINE.set("OBCONO", currentCompany)
          OOLINE.set("OBITNO", inITNO)
          OOLINE.set("OBORNO", inORNO)
          if (!qOOLINE.readAll(OOLINE, 3, nbMaxRecord, outDataOOLINE)) {
            mi.error("L’article " + inITNO + " n’existe pas dans la commande")
            return
          }
          if(erreurSAPR){
            mi.error("Prix supérieur au prix de vente")
            return
          }
        }
      } else {
        logger.info("GenCustRetByPay => Commande " + inORNO + " n’existe pas")
        DBAction qEXT39500 = database.table("EXT395").index("00").selection("EXCUNO").build()
        DBContainer EXT39500 = qEXT39500.getContainer()
        EXT39500.set("EXCONO", currentCompany)
        EXT39500.set("EXORNO", inORNO)
        if (!qEXT39500.readAll(EXT39500, 2, 1, outDataEXT39500)) {
          mi.error("La commande " + inORNO + " n’existe pas")
          return
        }
        erreurSAPR = false
        DBAction qEXT39510 = database.table("EXT395").index("10").selection("EXITNO", "EXSAPR").build()
        DBContainer EXT39510 = qEXT39510.getContainer()
        EXT39510.set("EXCONO", currentCompany)
        EXT39510.set("EXORNO", inORNO)
        EXT39510.set("EXCUNO", inCUNO)
        if (!qEXT39510.readAll(EXT39510, 3, nbMaxRecord, outDataEXT39510)) {
          mi.error("La commande " + inORNO + " n’est pas reliée au client " + inCUNO)
          return
        }
        if(erreurSAPR){
          mi.error("Prix supérieur au prix de vente")
          return
        }
      }

    } else {
      if(inSAPR!=0){
        mi.error("Le prix ne peut pas être renseigné sans numéro de commande associé")
        return
      }
    }

    inPONR = 0
    if(mi.in.get("PONR") != null) {
      inPONR = mi.in.get("PONR")
    }

    if(inPONR!=0 && inORNO=="") {
      mi.error("Le numéro de commande est obligatoire si un numéro de ligne est saisie")
      return
    }

    inRESP = ""
    if(mi.in.get("RESP") != null) {
      inRESP = mi.in.get("RESP")
      DBAction qCMNUSR = database.table("CMNUSR").index("00").selection("JUUSID").build()
      DBContainer CMNUSR = qCMNUSR.getContainer()
      CMNUSR.set("JUCONO", 0)
      CMNUSR.set("JUDIVI", "")
      CMNUSR.set("JUUSID", inRESP)
      if (!qCMNUSR.read(CMNUSR)) {
        mi.error("Responsable n'existe pas")
        return
      }
    }

    inZCOM = ""
    if(mi.in.get("ZCOM") != null) {
      inZCOM = mi.in.get("ZCOM")
    }

    inZCO2 = ""
    if(mi.in.get("ZCO2") != null) {
      inZCO2 = mi.in.get("ZCO2")
    }

    inZPAN = ""
    if(mi.in.get("ZPAN") != null) {
      inZPAN = mi.in.get("ZPAN")
    }

    inZNUM = ""
    if(mi.in.get("ZNUM") != null) {
      inZNUM = mi.in.get("ZNUM")
    }

    inZRET = 0
    if(mi.in.get("ZRET") != null) {
      inZRET = mi.in.get("ZRET")
    }

    receivingNumber = 0
    receivingNumber2 = 0
    receivingNumber3 = 0
    receivingNumber4 = 0
    receivingNumber5 = 0
    IN60 = false

    wCUNO = mi.in.get("CUNO")
    logger.debug("OKCUNO = " + wCUNO)
    ExpressionFactory expressionEXT395 = database.getExpressionFactory("EXT395")
    expressionEXT395 = expressionEXT395.ge("EXDLDT", currentDateMinusTwoYears)
    expressionEXT395 = expressionEXT395.and(expressionEXT395.le("EXDLDT", currentDateAlpha))
    DBAction qEXT395 = database.table("EXT395").index("20").matching(expressionEXT395).selection("EXORNO", "EXPONR",  "EXZREC", "EXZREA", "EXSAPR", "EXDLQA", "EXZCOS", "EXDLDT", "EXFACI").build()
    DBContainer EXT395 = qEXT395.getContainer()
    EXT395.set("EXCONO", currentCompany)
    EXT395.set("EXCUNO", wCUNO)
    EXT395.set("EXITNO", inITNO)
    if (!qEXT395.readAll(EXT395, 3, 100, outDataEXT395)) {
      logger.debug("No record for  = " + currentCompany + " / " + wCUNO + " / " + inITNO)
    }

    if(IN60){
      mi.error(errorMsg)
      return
    }

    mi.outData.put("REPN", receivingNumber as String)
    mi.outData.put("REP2", receivingNumber2 as String)
    mi.outData.put("REP3", receivingNumber3 as String)
    mi.outData.put("REP4", receivingNumber4 as String)
    mi.outData.put("REP5", receivingNumber5 as String)
    mi.outData.put("ZRET", inZRET as String)
    mi.outData.put("REQ0", inREQ0 as String)
    mi.write()
  }

  Closure<?> outDataEXT395 = { DBContainer EXT395 ->
    logger.debug("inREQ0 = " + inREQ0)
    logger.debug("IN60 = " + IN60 )
    if(inREQ0==0||IN60){
      return
    }

    foundPriority = true
    wRORN = EXT395.get("EXORNO")
    wPONR = EXT395.get("EXPONR")
    wPOSX = 0
    rSAPR = EXT395.get("EXSAPR")
    rZCOS = EXT395.get("EXZCOS")
    wDLDT = EXT395.get("EXDLDT")
    wFACI = EXT395.get("EXFACI")
    DBAction qCFACIL = database.table("CFACIL").index("00").selection("CFDIVI").build()
    DBContainer CFACIL = qCFACIL.getContainer()
    CFACIL.set("CFCONO", currentCompany)
    CFACIL.set("CFFACI", wFACI)
    if (qCFACIL.read(CFACIL)) {
      wDIVI = CFACIL.get("CFDIVI")
    }
    wUCOS = inAPPR
    double oQREC = EXT395.get("EXZREC")
    double oQREA = EXT395.get("EXZREA")
    double wQREA
    logger.debug("Return EXT395")
    logger.debug("CUNO = " + wCUNO)
    logger.debug("ORNO = " + wRORN )
    logger.debug("Line = " + wPONR )
    logger.debug("Consigned item = " + consignedItem)
    logger.debug("Consigned PN = " + consignedPN)
    logger.debug("Consignement reason code = " + consignmentReasonCode)
    logger.debug("inREQ0 = " + inREQ0 )
    logger.debug("ZREC = " + oQREC )
    logger.debug("ZREA = " + oQREA )
    logger.debug("EXSAPR = " + rSAPR )
    logger.debug("EXZCOS = " + rZCOS )
    logger.debug("EXDLDT = " + wDLDT )
    logger.debug("EXFACI = " + wFACI )
    logger.debug("wDIVI = " + wDIVI )
    if(consignedItem){
      if(consignmentReasonCode){
        if (oQREC>=inREQ0){
          consignedReturn = false
          wREQ0 = inREQ0
          cREQ0 = cREQ0 + wREQ0
          inREQ0 = 0
          crtOCLINE()
          return
        } else {
          if(oQREC>0) {
            consignedReturn = false
            wREQ0 = oQREC
            cREQ0 = cREQ0 + wREQ0
            inREQ0 = inREQ0 - oQREC
            crtOCLINE()
            return
          }
        }
      }

      if(!consignmentReasonCode){
        if(oQREA>0){
          if(oQREC>=inREQ0) {
            consignedReturn = false
            wREQ0 = inREQ0
            inREQ0 = 0
            crtOCLINE()
            return
          } else {
            if(oQREC>0){
              consignedReturn = false
              wREQ0 = oQREC
              cREQ0 = cREQ0 + wREQ0
              crtOCLINE()
              inREQ0 = inREQ0 - oQREC
              wQREA = oQREA - oQREC
              if(wQREA>=inREQ0) {
                consignedReturn = true
                wREQ0 = inREQ0
                inREQ0 = 0
                crtOCLINE()
                return
              } else {
                consignedReturn = true
                wREQ0 = wQREA
                inREQ0 = inREQ0 - wREQ0
                cREQ0 = cREQ0 + wREQ0
                crtOCLINE()
                return
              }
            } else {
              if(oQREA>=inREQ0) {
                consignedReturn = true
                wREQ0 = inREQ0
                inREQ0 = 0
                crtOCLINE()
                return
              } else {
                consignedReturn = true
                wREQ0 = oQREA
                inREQ0 = inREQ0 - wREQ0
                cREQ0 = cREQ0 + wREQ0
                crtOCLINE()
                return
              }
            }
          }
        }
      }
    }

    if(!consignedItem){
      double oDLQT = EXT395.get("EXDLQA")
      deliveredInvoicedQuantity = oDLQT
      returnedQuantity = 0
      ExpressionFactory expressionOCLINE = database.getExpressionFactory("OCLINE")
      expressionOCLINE = expressionOCLINE.le("ODREST", "34")
      DBAction qOCLINE = database.table("OCLINE").index("60").matching(expressionOCLINE).selection("ODREQ0", "ODREQ5").build()
      DBContainer OCLINE = qOCLINE.getContainer()
      OCLINE.set("ODCONO", currentCompany)
      OCLINE.set("ODRORC", 0)
      OCLINE.set("ODRORN", wRORN)
      OCLINE.set("ODRORL", wPONR)
      OCLINE.set("ODRORX", wPOSX)
      if (qOCLINE.readAll(OCLINE, 5, nbMaxRecord, outDataOCLINE60)) {

      }
      if(deliveredInvoicedQuantity>returnedQuantity){
        double oDeliveredInvoicedQuantity = deliveredInvoicedQuantity - returnedQuantity
        if(oDeliveredInvoicedQuantity>inREQ0){
          consignedReturn = false
          wREQ0 = inREQ0
          inREQ0 = 0
          crtOCLINE()
          return
        } else {
          consignedReturn = false
          wREQ0 = oDeliveredInvoicedQuantity
          inREQ0 = inREQ0 - wREQ0
          cREQ0 = cREQ0 + wREQ0
          crtOCLINE()
          return
        }
      }

    }
  }
  // create OCLINE
  private void crtOCLINE(){
    mngOCHEAD()
    if(IN60){
      return
    }
    logger.debug("Create OCLINE")
    logger.info("GenCustRetByPay - crtOCLINE - researchEXT395 before =" + researchEXT395)
    if(researchEXT395) {
      logger.info("GenCustRetByPay - crtOCLINE - researchEXT395 after =" + researchEXT395)
      DBAction qOCHEAD = database.table("OCHEAD").index("00").selection("OCRORC").build()
      DBContainer OCHEAD = qOCHEAD.getContainer()
      OCHEAD.set("OCCONO", currentCompany)
      OCHEAD.set("OCWHLO", inWHLO)
      OCHEAD.set("OCREPN", wREPN)
      OCHEAD.set("OCCUNO", wCUNO)
      if (qOCHEAD.read(OCHEAD)) {
        savedRORC = OCHEAD.get("OCRORC")
      }
      wRORC = 0
      DBAction updOCHEAD = database.table("OCHEAD").index("00").build()
      DBContainer uOCHEAD = updOCHEAD.getContainer()
      uOCHEAD.set("OCCONO", currentCompany)
      uOCHEAD.set("OCWHLO", inWHLO)
      uOCHEAD.set("OCREPN", wREPN)
      uOCHEAD.set("OCCUNO", wCUNO)
      if (!updOCHEAD.readLock(uOCHEAD, updateCallBackOCHEAD00)) {

      }
    }

    calculSAPR()
    if(wSAPR!=0){
      wSAPR = round(wSAPR, 6)
      logger.info("executeOIS390MIAddLine 1 with inWHLO=" + inWHLO + " wREPN=" + wREPN + " wUCOS=" + wUCOS + " wREQ0=" + wREQ0 + " wPONR=" + wPONR + " wPOSX=" + wPOSX + " inITNO=" + inITNO + " inRSCD=" + inRSCD + " wSAPR=" + wSAPR)
      executeOIS390MIAddLine(inWHLO, wREPN as String, wUCOS as String, wREQ0 as String, wPONR as String, wPOSX as String, inITNO, inRSCD, wSAPR as String)
    } else {
      logger.info("executeOIS390MIAddLine 2 with inWHLO=" + inWHLO + " wREPN=" + wREPN + " wUCOS=" + wUCOS + " wREQ0=" + wREQ0 + " wPONR=" + wPONR + " wPOSX=" + wPOSX + " inITNO=" + inITNO + " inRSCD=" + inRSCD)
      executeOIS390MIAddLine(inWHLO, wREPN as String, wUCOS as String, wREQ0 as String, wPONR as String, wPOSX as String, inITNO, inRSCD, "")
    }

    if(IN60){
      return
    }
    updOCLINE()
    calcDecote()
    if(researchEXT395) {
      wRORC = savedRORC
      DBAction updOCHEAD = database.table("OCHEAD").index("00").build()
      DBContainer uOCHEAD = updOCHEAD.getContainer()
      uOCHEAD.set("OCCONO", currentCompany)
      uOCHEAD.set("OCWHLO", inWHLO)
      uOCHEAD.set("OCREPN", wREPN)
      uOCHEAD.set("OCCUNO", wCUNO)
      if (!updOCHEAD.readLock(uOCHEAD, updateCallBackOCHEAD00)) {

      }
    }
    updateHead()
  }
  // Manage OCHEAD
  private void mngOCHEAD(){
    rtvHead()
    if(wREPN==0){
      mngZRET()
      crtOCHEAD()
      if(IN60){
        return
      }
      crtEXT390()
    }
    if (receivingNumber == 0){
      receivingNumber = wREPN
    } else {
      if (receivingNumber2 == 0){
        receivingNumber2 = wREPN
      } else {
        if (receivingNumber3 == 0){
          receivingNumber3 = wREPN
        } else {
          if (receivingNumber4 == 0){
            receivingNumber4 = wREPN
          } else {
            if (receivingNumber5 == 0){
              receivingNumber5 = wREPN
            }
          }
        }
      }
    }
  }
  // Manage ZRET
  private void mngZRET(){
    if(inZRET==0){
      executeCRS165MIRtvNextNumber("04", "B")
    }
  }

  // Retrive Head
  private void rtvHead(){
    wREPN = 0
    ExpressionFactory expressionEXT390 = database.getExpressionFactory("EXT390")
    expressionEXT390 = expressionEXT390.eq("EXWHLO", inWHLO)
    expressionEXT390 = expressionEXT390.and(expressionEXT390.eq("EXCUNO", wCUNO))
    DBAction qEXT390 = database.table("EXT390").index("10").matching(expressionEXT390).selection("EXREPN", "EXZRET").build()
    DBContainer EXT390 = qEXT390.getContainer()
    EXT390.set("EXCONO", currentCompany)
    EXT390.set("EXZPAN", inZPAN)
    if (!qEXT390.readAll(EXT390, 2, 100, outDataEXT390)) {
    }
  }

  // Create OCHEAD
  private void crtOCHEAD(){
    String oRERE
    if(inRESP!=""){
      oRERE = inRESP
    } else {
      oRERE = program.getUser()
    }
    logger.debug("Create OCHEAD = ")
    logger.debug("executeOIS390MIAddHead with inWHLO=" + inWHLO + " wCUNO=" + wCUNO + " wRORN=" + wRORN + " inFACI=" + inFACI + " inRSC1=" + inRSC1 + " oRERE=" + oRERE)
    executeOIS390MIAddHead(inWHLO, wCUNO, wRORN, inFACI, inRSC1, oRERE)
  }
  // Update OCHEAD
  private void updateHead() {
    logger.debug("Update OCHEAD")
    if (OIS399NumberOfLine > 0) {
      lineCounter = 0
      DBAction qEXT39000 = database.table("EXT390").index("00").selection("EXZPAN").build()
      DBContainer EXT39000 = qEXT39000.getContainer()
      EXT39000.set("EXCONO", currentCompany)
      EXT39000.set("EXWHLO", inWHLO)
      EXT39000.set("EXREPN", wREPN)
      EXT39000.set("EXCUNO", wCUNO)
      if (qEXT39000.read(EXT39000)) {
        String oZPAN = EXT39000.get("EXZPAN")
        DBAction qEXT39010 = database.table("EXT390").index("10").selection("EXWHLO", "EXREPN", "EXCUNO").build()
        DBContainer EXT39010 = qEXT39010.getContainer()
        EXT39010.set("EXCONO", currentCompany)
        EXT39010.set("EXZPAN", oZPAN)
        if (qEXT39010.readAll(EXT39010, 2, 10, outDataEXT39010)) {

        }
        if(lineCounter>OIS399NumberOfLine){
          EXT39010.set("EXCONO", currentCompany)
          EXT39010.set("EXZPAN", oZPAN)
          if (qEXT39010.readAll(EXT39010, 2, 100, outDataUpdEXT39010)) {

          }
        }
      }
    }
  }
  Closure<?> outDataEXT390 = { DBContainer EXT390 ->
    if(inZRET==0){
      inZRET = EXT390.get("EXZRET")
    }
    if(wREPN == 0){
      long oREPN = EXT390.get("EXREPN")
      DBAction qOCHEAD = database.table("OCHEAD").index("00").selection("OCRORN").build()
      DBContainer OCHEAD = qOCHEAD.getContainer()
      OCHEAD.set("OCCONO", currentCompany)
      OCHEAD.set("OCWHLO", inWHLO)
      OCHEAD.set("OCREPN", oREPN)
      OCHEAD.set("OCCUNO", wCUNO)
      if (qOCHEAD.read(OCHEAD)) {
        String oRORN = OCHEAD.get("OCRORN")
        logger.debug("inRORN = " + wRORN)
        logger.debug("oRORN = " + oRORN)
        if(wRORN.trim() == oRORN.trim()){
          wREPN = oREPN
          return false
        }
      }
    }
  }
  // Create EXT390
  private void crtEXT390(){
    executeEXT390MIAddCusRetHdInfo(currentCompany as String, inWHLO, wREPN as String, wCUNO, inZPAN, inZRET as String, inZNUM)
  }
  Closure<?> outDataEXT39010 = { DBContainer EXT390 ->
    long oREPN = EXT390.get("EXREPN")
    DBAction qOCLINE = database.table("OCLINE").index("00").selection("ODRELI").build()
    DBContainer OCLINE = qOCLINE.getContainer()
    OCLINE.set("ODCONO", currentCompany)
    OCLINE.set("ODWHLO", inWHLO)
    OCLINE.set("ODREPN", oREPN)
    if (qOCLINE.readAll(OCLINE, 3, 20, outDataCountOCLINE)) {
    }
  }
  Closure<?> outDataCountOCLINE = { DBContainer OCLINE ->
    lineCounter = lineCounter + 1
  }
  Closure<?> outDataUpdEXT39010 = { DBContainer EXT390 ->
    String oWHLO = EXT390.get("EXWHLO")
    long oREPN = EXT390.get("EXREPN")
    String oCUNO = EXT390.get("EXCUNO")
    DBAction qOCHEAD = database.table("OCHEAD").index("00").selection("OCCHNO").build()
    DBContainer OCHEAD = qOCHEAD.getContainer()
    OCHEAD.set("OCCONO", currentCompany)
    OCHEAD.set("OCWHLO", oWHLO)
    OCHEAD.set("OCREPN", oREPN)
    OCHEAD.set("OCCUNO", oCUNO)
    if (!qOCHEAD.readLock(OCHEAD, updateCallBackOCHEAD)) {

    }
  }
  // Update OCHEAD
  Closure<?> updateCallBackOCHEAD = { LockedResult lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("OCCHNO")
    lockedResult.set("OCRSCD", "AVD")
    lockedResult.setInt("OCLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("OCCHNO", changeNumber + 1)
    lockedResult.set("OCCHID", program.getUser())
    lockedResult.update()
  }
  // Update OCHEAD
  Closure<?> updateCallBackOCHEAD00 = { LockedResult lockedResult ->
    logger.info("GenCustRetByPay - updateCallBackOCHEAD00 - wRORC =" + wRORC)

    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("OCCHNO")
    lockedResult.set("OCRORC", wRORC)
    lockedResult.setInt("OCLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("OCCHNO", changeNumber + 1)
    lockedResult.set("OCCHID", program.getUser())
    lockedResult.update()
  }
  Closure<?> outDataOCLINE60 = { DBContainer OCLINE ->
    double oREQ0 = OCLINE.get("ODREQ0")
    double oREQ5 = OCLINE.get("ODREQ5")
    returnedQuantity = returnedQuantity + (oREQ0 - oREQ5)
  }
  // Calcul SAPR
  private void calculSAPR(){
    wSAPR = 0
    if(inSAPR!=0){
      wSAPR = inSAPR
      return
    } else {
      if(researchEXT395){
        if(consignmentReasonCode){
          wSAPR = rZCOS
        } else {
          if(consignedReturn){
            wSAPR = rSAPR
          } else {
            if(consignedPN){
              wSAPR = rZCOS + rSAPR
            } else {
              wSAPR = rSAPR
            }
          }
        }
        return
      }
      if(consignedItem && consignmentReasonCode){
        DBAction qOOLICH = database.table("OOLICH").index("00").selection("O7CRAM").build()
        DBContainer OOLICH = qOOLICH.getContainer()
        OOLICH.set("O7CONO", currentCompany)
        OOLICH.set("O7ORNO", inORNO)
        OOLICH.set("O7PONR", wPONR)
        OOLICH.set("O7POSX", wPOSX)
        OOLICH.set("O7CRID", "CONSIG")
        if (qOOLICH.read(OOLICH)) {
          wSAPR = OOLICH.get("O7CRAM")
          return
        } else {
          return
        }
      }
      if(consignedItem && !consignmentReasonCode){
        if(consignedReturn){
          wSAPR = rSAPR
          return
        } else {
          DBAction qOOLICH = database.table("OOLICH").index("00").selection("O7CRAM").build()
          DBContainer OOLICH = qOOLICH.getContainer()
          OOLICH.set("O7CONO", currentCompany)
          OOLICH.set("O7ORNO", inORNO)
          OOLICH.set("O7PONR", wPONR)
          OOLICH.set("O7POSX", wPOSX)
          OOLICH.set("O7CRID", "CONSIG")
          if (qOOLICH.read(OOLICH)) {
            double oCRAM = OOLICH.get("O7CRAM")
            wSAPR = oCRAM + rSAPR
            return
          } else {
            wSAPR = rSAPR
            return
          }
        }

      }

    }
  }

  // Calcul Decote
  private void calcDecote(){
    logger.debug("Create EXT391 ")
    String oZRSC = ""
    decote = 0
    if(consignedReturn){
      oZRSC = "1"
    }
    if(newStuff) {
      if (researchEXT395) {
        transactionDate = wDLDT
      } else {
        transactionDate = 0
        ExpressionFactory expressionMITTRA = database.getExpressionFactory("MITTRA")
        expressionMITTRA = expressionMITTRA.lt("MTTRQT", "0")
        expressionMITTRA = expressionMITTRA.and(expressionMITTRA.lt("MTTRDT", currentDateAlpha))
        DBAction qMITTRA = database.table("MITTRA").index("30").matching(expressionMITTRA).selection("MTTRDT").build()
        DBContainer MITTRA = qMITTRA.getContainer()
        MITTRA.set("MTCONO", currentCompany)
        MITTRA.set("MTTTYP", 31)
        MITTRA.set("MTRIDN", wRORN)
        MITTRA.set("MTRIDL", wPONR)
        MITTRA.set("MTRIDX", wPOSX)
        if (!qMITTRA.readAll(MITTRA, 5, 10, outDataMITTRA)) {

        }
      }
      if(transactionDate>0){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd")

        LocalDate oTRDT = LocalDate.parse(transactionDate as String, formatter)
        logger.debug("TRDT = " + transactionDate)
        logger.debug("CUDATE = " + currentDate)
        daysDifference = ChronoUnit.DAYS.between(oTRDT, currentDate)
        logger.debug("Day difference = " + daysDifference)
        logger.debug("current Company = " + currentCompany)
        logger.debug("DIVI = " + wDIVI)
        DBAction qCUGEX1 = database.table("CUGEX1").index("00").selection("F1N096", "F1PK03", "F1PK04").build()
        DBContainer CUGEX1 = qCUGEX1.getContainer()
        CUGEX1.set("F1CONO", currentCompany)
        CUGEX1.set("F1FILE", "DECOTE")
        CUGEX1.set("F1PK01", wDIVI)
        CUGEX1.set("F1PK02", "1")
        if (!qCUGEX1.readAll(CUGEX1, 4, 10, outDataCUGEX1)){

        }

      }
    }
    executeEXT391MIAddCusRetLnInfo(currentCompany as String, inWHLO, wREPN as String, wRELI as String, oZRSC, decote as String, "", inZCOM, inZCO2)
  }
  // Update OCLINE
  private void updOCLINE(){
    logger.debug("Update OCLINE")
    logger.debug("ENNO = " + wENNO)
    if(wENNO!=null){
      DBAction updOCLINE = database.table("OCLINE").index("00").build()
      DBContainer uOCLINE = updOCLINE.getContainer()
      uOCLINE.set("ODCONO", currentCompany)
      uOCLINE.set("ODWHLO", inWHLO)
      uOCLINE.set("ODREPN", wREPN)
      uOCLINE.set("ODRELI", wRELI)
      if (!updOCLINE.readLock(uOCLINE, updatCallBackOCLINE)) {}
    }
  }
  // Round
  private double round(double number, int decimal){
    String roundedNumber = "0"
    if(decimal==0) roundedNumber = (double)Math.round(number)
    if(decimal==1) roundedNumber = (double)Math.round(number*10)/10
    if(decimal==2) roundedNumber = (double)Math.round(number*100)/100
    if(decimal==3) roundedNumber = (double)Math.round(number*1000)/1000
    if(decimal==4) roundedNumber = (double)Math.round(number*10000)/10000
    if(decimal==5) roundedNumber = (double)Math.round(number*100000)/100000
    if(decimal==6) roundedNumber = (double)Math.round(number*1000000)/1000000
    return roundedNumber as double;
  }
  Closure<?> outDataMITTRA = { DBContainer MITTRA ->
    Integer oTRDT = MITTRA.get("MTTRDT")
    if(oTRDT>transactionDate){
      transactionDate = oTRDT
    }
  }
  Closure<?> outDataCUGEX1 = { DBContainer CUGEX1 ->
    long oPK03 = CUGEX1.get("F1PK03") as long
    long oPK04 = CUGEX1.get("F1PK04") as long
    logger.debug("PK03 = " + oPK03)
    logger.debug("PK04 = " + oPK04)
    logger.debug("days Difference = " + daysDifference)
    if(oPK03 <= daysDifference && oPK04 >= daysDifference){
      decote = CUGEX1.get("F1N096")
      return false
    }
  }
  // Update OCLINE
  Closure<?> updatCallBackOCLINE = { LockedResult lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("ODCHNO")
    lockedResult.set("ODENNO", wENNO)
    lockedResult.setInt("ODLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("ODCHNO", changeNumber + 1)
    lockedResult.set("ODCHID", program.getUser())
    lockedResult.update()
  }
  Closure<?> outDataOOHEAD = { DBContainer OOHEAD ->
    double oNTLA = OOHEAD.get("OANTLA")
    last12MonthsSales = last12MonthsSales + oNTLA
  }

  Closure<?> outDataOCLINE = { DBContainer OCLINE ->
    double oSAPR = OCLINE.get("ODSAPR")
    double oREQ7 = OCLINE.get("ODREQ7")
    String oRSCD = OCLINE.get("ODRSCD")
    DBAction qCUGEX1RSCD = database.table("CUGEX1").index("00").selection("F1A030").build()
    DBContainer CUGEX1RSCD = qCUGEX1RSCD.getContainer()
    CUGEX1RSCD.set("F1CONO", currentCompany)
    CUGEX1RSCD.set("F1FILE",  "CSYTAB")
    CUGEX1RSCD.set("F1PK01",  "")
    CUGEX1RSCD.set("F1PK02",  "RSCD")
    CUGEX1RSCD.set("F1PK03",  oRSCD)
    CUGEX1RSCD.set("F1PK04",  "")
    CUGEX1RSCD.set("F1PK05",  "")
    CUGEX1RSCD.set("F1PK06",  "")
    CUGEX1RSCD.set("F1PK07",  "")
    CUGEX1RSCD.set("F1PK08",  "")
    if(qCUGEX1RSCD.read(CUGEX1RSCD)){
      logger.debug("SAPR = " + oSAPR)
      logger.debug("REQ7 = " + oREQ7)
      last12MonthsReturnsForNewParts = last12MonthsReturnsForNewParts + (oSAPR * oREQ7)
    }

  }
  // Check Item number
  private void chkItno(){
    DBAction qMITMAS00 = database.table("MITMAS").index("00").selection("MMCFI1").build()
    DBContainer MITMAS00 = qMITMAS00.getContainer()
    MITMAS00.set("MMCONO", currentCompany)
    MITMAS00.set("MMITNO", inITNO)
    if (qMITMAS00.read(MITMAS00)) {
      return
    }

    if(inCFI1==""){
      IN60 = true
      errorMsg = ("Marque est obligatoire")
      return
    }

    ExpressionFactory expressionMITMAS10 = database.getExpressionFactory("MITMAS")
    expressionMITMAS10 = expressionMITMAS10.eq("MMCFI1", inCFI1)
    DBAction qMITMAS10 = database.table("MITMAS").index("10").matching(expressionMITMAS10).selection("MMITNO").build()
    DBContainer MITMAS10 = qMITMAS10.getContainer()
    MITMAS10.set("MMCONO", currentCompany)
    MITMAS10.set("MMITDS", inITNO)
    if (qMITMAS10.read(MITMAS10)) {
      inITNO = MITMAS10.get("MMITNO")
      return

    }

    DBAction qMITPOP10 = database.table("MITPOP").index("10").selection("MPITNO").build()
    DBContainer MITPOP10 = qMITPOP10.getContainer()
    MITPOP10.set("MPCONO", currentCompany)
    MITPOP10.set("MPALWT", 3)
    MITPOP10.set("MPALWQ", "RAU")
    MITPOP10.set("MPPOPN", inITNO)
    if (!qMITPOP10.readAll(MITPOP10, 4, 1, outdateMITPOP10)) {
      MITPOP10.set("MPCONO", currentCompany)
      MITPOP10.set("MPALWT", 5)
      MITPOP10.set("MPALWQ", "")
      MITPOP10.set("MPPOPN", inITNO)
      if (!qMITPOP10.readAll(MITPOP10, 4, 1, outdateMITPOP10)) {
        return
      }
    }
  }
  Closure<?> outdateMITPOP10 = { DBContainer MITPOP10 ->
    inITNO = MITPOP10.get("MPITNO")
  }
  Closure<?> outDataOOLINE = { DBContainer OOLINE ->
    if(inSAPR!=0){
      Integer oPONR = OOLINE.get("OBPONR")
      Integer oPOSX = OOLINE.get("OBPOSX")
      double oNEPR = OOLINE.get("OBNEPR")
      double oCRAM = 0
      DBAction qOOLICH = database.table("OOLICH").index("00").selection("O7CRAM").build()
      DBContainer OOLICH = qOOLICH.getContainer()
      OOLICH.set("O7CONO", currentCompany)
      OOLICH.set("O7ORNO", inORNO)
      OOLICH.set("O7PONR", oPONR)
      OOLICH.set("O7POSX", oPOSX)
      OOLICH.set("O7CRID", "CONSIG")
      if (qOOLICH.read(OOLICH)) {
        oCRAM = OOLICH.get("O7CRAM")
      }
      logger.debug("OBNEPR = " + oNEPR)
      logger.debug("O7CRAM = " + oCRAM)
      if(inSAPR>(oNEPR+oCRAM)){
        erreurSAPR = true
      }

    }
  }
  Closure<?> outDataEXT39500 = { DBContainer EXT395 ->
    logger.info("GenCustRetByPay - outDataEXT39500 - EXT395 found")
    researchEXT395 = true
  }

  Closure<?> outDataEXT39510 = { DBContainer EXT395 ->
    if(inDSP1==0 && inSAPR!=0){
      String oITNO = EXT395.get("EXITNO")
      if(inITNO==oITNO){
        double oSAPR = EXT395.get("EXSAPR")
        if(inSAPR>(oSAPR)){
          erreurSAPR = true
        }
      }
    }
  }
  // Execute CRS165MI.RtvNextNumber
  private executeCRS165MIRtvNextNumber(String NBTY, String NBID){
    Map<String, String> parameters = ["NBTY": NBTY, "NBID": NBID]
    Closure<?> handler = { Map<String, String> response ->
      inZRET = response.NBNR.trim() as long

      if (response.error != null) {
        IN60 = true
        logger.debug("CRS165MI RtvNextNumber error")
        errorMsg = ("Failed CRS165MI.RtvNextNumber: "+ response.errorMessage)
        return
      }
    }
    miCaller.call("CRS165MI", "RtvNextNumber", parameters, handler)
  }
  // Add Customer Return Head Info
  private executeEXT390MIAddCusRetHdInfo(String CONO, String WHLO, String REPN, String CUNO, String ZPAN, String ZRET, String ZNUM){
    logger.debug("CONO = " + CONO + " WHLO = "+ WHLO+ " REPN = "+ REPN+ " CUNO = "+ CUNO + " ZPAN = "+ ZPAN + " ZRET = "+ ZRET+ " ZNUM = "+ ZNUM)
    Map<String, String>  parameters = ["CONO": CONO, "WHLO": WHLO, "REPN": REPN, "CUNO": CUNO, "ZPAN": ZPAN, "ZRET": ZRET, "ZNUM": ZNUM]
    Closure<?> handlerEXT390 = { Map<String, String> response ->
      if (response.error != null) {
        IN60 = true
        logger.debug("EXT390MI AddCusRetHdInfo error")
        errorMsg = ("Failed EXT390MI.AddCusRetHdInfo: "+ response.errorMessage)
        return
      }
    }
    miCaller.call("EXT390MI", "AddCusRetHdInfo", parameters, handlerEXT390)
  }
  // Add Customer Return Head Info
  private executeEXT391MIAddCusRetLnInfo(String CONO, String WHLO, String REPN, String RELI, String ZRSC, String ZDC1, String ZDC2, String ZCOM, String ZCO2){
    logger.debug("CONO = " + CONO + " WHLO = "+ WHLO+ " REPN = "+ REPN+ " RELI = "+ RELI+ " ZRSC = "+ ZRSC+ " ZDC1 = "+ ZDC1+ " ZDC2 = "+ ZDC2+ " ZCOM = "+ ZCOM+ " ZCO2 = "+ ZCO2)
    Map<String, String>  parameters = ["CONO": CONO, "WHLO": WHLO, "REPN": REPN, "RELI": RELI, "ZRSC": ZRSC, "ZDC1": ZDC1, "ZDC2": ZDC2, "ZCOM": ZCOM, "ZCO2": ZCO2]
    Closure<?> handlerEXT391 = { Map<String, String> response ->
      if (response.error != null) {
        IN60 = true
        logger.debug("EXT391MI AddCusRetLnInfo error")
        errorMsg = ("Failed EXT391MI.AddCusRetLnInfo: "+ response.errorMessage)
        return
      }
    }
    miCaller.call("EXT391MI", "AddCusRetLnInfo", parameters, handlerEXT391)
  }
  // Add Head OCHEAD
  private executeOIS390MIAddHead(String WHLO, String CUNO, String RORN, String FACI, String RSCD, String RERE){
    logger.debug("WHLO = " + WHLO + " CUNO = " + CUNO + " RORN = " + RORN + " FACI = " + FACI + " RSCD = " + RSCD + " RERE = " + RERE)
    Map<String, String>  parameters = ["WHLO": WHLO, "CUNO": CUNO, "RORN": RORN, "FACI": FACI, "RSCD": RSCD, "RERE": RERE]
    Closure<?> handlerOIS390 = { Map<String, String> response ->
      if (response.error != null) {
        IN60 = true
        logger.debug("OIS390MI AddHead error")
        errorMsg = ("Failed OIS390MI.AddHead: "+ response.errorMessage)
        return
      }
      if (response.REPN != null)
        wREPN = response.REPN as long
    }
    miCaller.call("OIS390MI", "AddHead", parameters, handlerOIS390)
  }
  // Add Line OCLINE
  private executeOIS390MIAddLine(String WHLO, String REPN, String UCOS, String REQ0, String RORL, String RORX, String ITNO, String RSCD, String SAPR){
    LocalDateTime timeOfCreation = LocalDateTime.now()
    String CUDATE = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as String
    logger.debug("WHLO = " + WHLO + " REPN = " + REPN + " UCOS = " + UCOS + " REQ0 = " + REQ0 + " RORL = " + RORL + " RORX = " + RORX + " ITNO = " + ITNO + " RSCD =" + RSCD + " SAPR =" + SAPR + " EPDT = " + CUDATE)
    Map<String, String>  parameters = ["WHLO": WHLO, "REPN": REPN, "UCOS": UCOS, "REQ0": REQ0, "RORL": RORL, "RORX": RORX, "ITNO": ITNO, "RSCD": RSCD, "SAPR": SAPR, "EPDT": CUDATE ]
    Closure<?> handlerOIS390 = { Map<String, String> response ->
      if (response.error != null) {
        IN60 = true
        logger.debug("OIS390MI AddLine error")
        errorMsg = ("Failed OIS390MI.AddLine: "+ response.errorMessage)
        return
      }
      if (response.RELI != null)
        logger.debug("OIS390MI RELI " + response.RELI)
      wRELI = response.RELI as Integer
    }
    miCaller.call("OIS390MI", "AddLine", parameters, handlerOIS390)
  }
}
