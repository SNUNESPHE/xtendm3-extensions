/**
 * README
 * This extension is used by script
 *
 * Name : EXT020MI.GetOrdShipChg
 * Description : Calculate shipping charge for a sales order.
 * Date         Changed By   Description
 * 20241007     ARENARD      5236 - Visualisation du frais de port en saisie de commande
 * 20250826     ARENARD      Extension has been fixed
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class GetOrdShipChg extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private String inORNO
  private String jobNumber
  private String customerNumber
  private String calculationMethod
  private String calculationMethodName
  private Double fixedChargeAmount = 0
  private Double freeAmount = 0
  private Double flatRateAmount = 0
  private String savedECAR
  private String forwardingAgent
  private String carrierIDChargeCalc
  private boolean departureIsExcluded
  private Double shippingChargeAmount = 0
  private Double totalWeight = 0
  private Double priceByWeight = 0
  private Double savedFRQT = 0
  private Double totalAmount = 0
  private Integer currentCompany
  private String savedROUT
  private Integer savedRODN = 0
  private Integer savedPLDT = 0
  private String ext020ROUT
  private Integer ext020RODN = 0
  private Integer ext020PLDT = 0
  private Integer ext020PONR = 0
  private Integer ext020POSX = 0
  private String savedADID
  private String savedFROP
  private Integer nbMaxRecord = 10000

  public GetOrdShipChg(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
  }

  public void main() {
    // Get job number
    LocalDateTime timeOfCreation = LocalDateTime.now()
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))

    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    // Check order number
    if(mi.in.get("ORNO") != null){
      DBAction queryOOHEAD = database.table("OOHEAD").index("00").selection("OACUNO").build()
      DBContainer OOHEAD = queryOOHEAD.getContainer()
      OOHEAD.set("OACONO", currentCompany)
      OOHEAD.set("OAORNO", mi.in.get("ORNO"))
      if(!queryOOHEAD.read(OOHEAD)){
        mi.error("La commande n'existe pas")
        return
      } else {
        customerNumber = OOHEAD.get("OACUNO")
      }
      inORNO = mi.in.get("ORNO")
    } else {
      mi.error("Le N° de commande est obligatoire")
      return
    }
    logger.debug("customerNumber = " + customerNumber)

    // Retrieve customer state
    savedECAR = ""
    DBAction queryOCUSMA = database.table("OCUSMA").index("00").selection("OKECAR").build()
    DBContainer OCUSMA = queryOCUSMA.getContainer()
    OCUSMA.set("OKCONO", currentCompany)
    OCUSMA.set("OKCUNO", customerNumber)
    if (queryOCUSMA.read(OCUSMA)) {
      if(OCUSMA.get("OKECAR") != "") {
        savedECAR = OCUSMA.get("OKECAR")
      }
    }
    logger.debug("---------- RETRIEVE STATE ----------------------------------------------------------------------------------------------------")
    logger.debug("savedECAR = " + savedECAR)

    // Retrieve calculation variables
    calculationMethod = ""
    fixedChargeAmount = 0
    freeAmount = 0
    flatRateAmount = 0
    DBAction queryCUGEX1 = database.table("CUGEX1").index("00").selection("F1A030","F1A121","F1A830","F1A930").build()
    DBContainer CUGEX1 = queryCUGEX1.getContainer()
    CUGEX1.set("F1CONO", currentCompany)
    CUGEX1.set("F1FILE",  "OCUSMA")
    CUGEX1.set("F1PK01",  customerNumber)
    if(queryCUGEX1.read(CUGEX1)){
      calculationMethod = CUGEX1.get("F1A030")
      if(CUGEX1.get("F1A121") != "")
        fixedChargeAmount = CUGEX1.get("F1A121") as Double
      if(CUGEX1.get("F1A830") != "")
        freeAmount = CUGEX1.get("F1A830") as Double
      if(CUGEX1.get("F1A930") != "")
        flatRateAmount = CUGEX1.get("F1A930") as Double
    }
    logger.debug("---------- RETRIEVE PARAMETERS ----------------------------------------------------------------------------------------------------")
    logger.debug("calculationMethod (A030) = " + calculationMethod)
    logger.debug("fixedChargeAmount (A121) = " + fixedChargeAmount)
    logger.debug("freeAmount (A830) = " + freeAmount)
    logger.debug("flatRateAmount (A930) = " + flatRateAmount)

    // Initialize calculationMethodName and call calculation method
    logger.debug("---------- CALCULATION METHOD ----------------------------------------------------------------------------------------------------")
    switch (calculationMethod.trim()) {
      case "00":
        calculationMethodName = "Franco total"
        method00()
        break
      case "01":
        calculationMethodName = "Groupé"
        method01()
        break
      case "02":
        calculationMethodName = "Tournée"
        method02()
        break
      case "03":
        calculationMethodName = "Forfait hebdo"
        method03()
        break
      case "04":
        calculationMethodName = "Forfait mensuel"
        method04()
        break
      case "05":
        calculationMethodName = "Frais réel"
        method05()
        break
      case "06":
        calculationMethodName = "Frais export (Manuel)"
        method06()
        break
      default:
        calculationMethodName = ""
    }

    // Delete records from the working file
    DBAction queryEXT020 = database.table("EXT020").index("00").build()
    DBContainer EXT020 = queryEXT020.getContainer()
    EXT020.set("EXBJNO", jobNumber)

    Closure<?> deleteWorkFile = { DBContainer readResult ->
      queryEXT020.readLock(readResult, { LockedResult lockedResult ->
        lockedResult.delete()
      })
    }

    queryEXT020.readAll(EXT020, 1, nbMaxRecord, deleteWorkFile)

    logger.debug("---------- OUTPUT PARAMETERS ----------------------------------------------------------------------------------------------------")
    logger.debug("calculationMethodName = " + calculationMethodName)
    logger.debug("freeAmount (A830) = " + freeAmount)
    logger.debug("shippingChargeAmount = " + shippingChargeAmount)

    mi.outData.put("ZMTH", calculationMethodName)
    mi.outData.put("ZMFC", freeAmount as String)
    mi.outData.put("ZMFR", String.format("%.2f", shippingChargeAmount))
    mi.write()
  }

  // Calculation method 00 - "Non Applicable"
  public void method00() {
    logger.debug("method00")

    // Montant frais
    shippingChargeAmount = 0

    // Montant franco
    freeAmount = 0
  }

  // Calculation method 01 - "Groupé"
  public void method01() {
    logger.debug("method01")

    totalAmount = 0
    // Calculate total net amount
    DBAction queryOOLINE = database.table("OOLINE").index("00").selection("OBLNAM").build()
    DBContainer OOLINE = queryOOLINE.getContainer()
    OOLINE.set("OBCONO", currentCompany)
    OOLINE.set("OBORNO", inORNO)
    queryOOLINE.readAll(OOLINE, 2, nbMaxRecord, outDataOOLINE)

    logger.debug("totalAmount = " + totalAmount)
    logger.debug("freeAmount (A830) = " + freeAmount)
    logger.debug("fixedChargeAmount (A121) = " + fixedChargeAmount)

    if (totalAmount >= freeAmount){
      shippingChargeAmount = 0
    } else{
      shippingChargeAmount = fixedChargeAmount
    }
  }

  // Calculation method 02 - "Tournée"
  public void method02() {
    logger.debug("method02")

    calculationBasedOnWeight()
  }

  // Calculation method 03 - "Forfait  hebdo"
  public void method03() {
    logger.debug("method03")

    // Montant frais
    shippingChargeAmount = flatRateAmount

    logger.debug("flatRateAmount (A930) = " + flatRateAmount)

    // Montant franco
    freeAmount = 0
  }

  // Calculation method 04 - "Forfait mensuel"
  public void method04() {
    logger.debug("method04")

    // Montant frais
    shippingChargeAmount = flatRateAmount

    logger.debug("flatRateAmount (A930) = " + flatRateAmount)

    // Montant franco
    freeAmount = 0
  }

  // Calculation method 05 - "Frais réel"
  public void method05() {
    logger.debug("method05")

    calculationBasedOnWeight()

    freeAmount = 0
  }

  // Calculation method 06 - "Frais export (Manuel)"
  public void method06() {
    logger.debug("method06")

    calculationBasedOnWeight()

    freeAmount = 0
  }

  // Retrieve OOLINE
  Closure<?> outDataOOLINE = { DBContainer OOLINE ->
    totalAmount += (double) OOLINE.get("OBLNAM")
  }

  // Retrieve OOLINE
  Closure<?> outDataOOLINE2 = { DBContainer OOLINE ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction queryEXT020 = database.table("EXT020").index("00").build()
    DBContainer EXT020 = queryEXT020.getContainer()
    EXT020.set("EXBJNO", jobNumber)
    EXT020.set("EXCONO", OOLINE.get("OBCONO"))
    EXT020.set("EXROUT", OOLINE.get("OBROUT"))
    EXT020.set("EXRODN", OOLINE.get("OBRODN"))
    EXT020.set("EXPLDT", OOLINE.get("OBPLDT"))
    EXT020.set("EXORNO", OOLINE.get("OBORNO"))
    EXT020.set("EXPONR", OOLINE.get("OBPONR"))
    EXT020.set("EXPOSX", OOLINE.get("OBPOSX"))
    if (!queryEXT020.read(EXT020)) {
      EXT020.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT020.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT020.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT020.setInt("EXCHNO", 1)
      EXT020.set("EXCHID", program.getUser())
      queryEXT020.insert(EXT020)
      logger.debug("insert EXT020")
    }
  }

  // Retrieve EXT020
  Closure<?> outDataEXT020 = { DBContainer EXT020 ->
    ext020ROUT = EXT020.get("EXROUT")
    ext020RODN = EXT020.get("EXRODN")
    ext020PLDT = EXT020.get("EXPLDT")
    ext020PONR = EXT020.get("EXPONR")
    ext020POSX = EXT020.get("EXPOSX")

    logger.debug("Found EXT020 - ext020ROUT = " + ext020ROUT)
    logger.debug("Found EXT020 - ext020RODN = " + ext020RODN)
    logger.debug("Found EXT020 - ext020PLDT = " + ext020PLDT)
    logger.debug("Found EXT020 - ext020PONR = " + ext020PONR)
    logger.debug("Found EXT020 - ext020POSX = " + ext020POSX)

    // First record
    if (savedROUT.trim() == "") {
      logger.debug("First record")
      savedROUT = EXT020.get("EXROUT")
      savedRODN = EXT020.get("EXRODN")
      savedPLDT = EXT020.get("EXPLDT")
      totalWeight = 0
      totalAmount = 0
      retrieveDepartureInformation()
    }

    // New combination ROUT, RODN, PLDT
    if (savedROUT.trim() != ext020ROUT.trim() ||
      savedRODN != ext020RODN ||
      savedPLDT != ext020PLDT) {
      calculationCombination()
    }

    logger.debug("Lecture OOLINE - ORNO = " + inORNO)
    logger.debug("Lecture OOLINE - PONR = " + ext020PONR)
    logger.debug("Lecture OOLINE - POSX = " + ext020POSX)
    // Cumulate the order's gross weight
    DBAction queryOOLINE = database.table("OOLINE").index("00").selection("OBITNO","OBORQT","OBADID","OBLNAM").build()
    DBContainer OOLINE = queryOOLINE.getContainer()
    OOLINE.set("OBCONO", currentCompany)
    OOLINE.set("OBORNO", inORNO)
    OOLINE.set("OBPONR", ext020PONR)
    OOLINE.set("OBPOSX", ext020POSX)
    if (queryOOLINE.read(OOLINE)) {
      // Retrieve item
      DBAction queryMITMAS = database.table("MITMAS").index("00").selection("MMGRWE").build()
      DBContainer MITMAS = queryMITMAS.getContainer()
      MITMAS.set("MMCONO", currentCompany)
      MITMAS.set("MMITNO", OOLINE.get("OBITNO"))
      if (queryMITMAS.read(MITMAS)) {
        logger.debug("Lecture OOLINE - ORQT = " + OOLINE.get("OBORQT"))
        logger.debug("Lecture OOLINE - GRWE = " + MITMAS.get("MMGRWE"))
        logger.debug("Lecture OOLINE - totalWeight avant ajout = " + totalWeight)

        totalWeight += (double) OOLINE.get("OBORQT") * (double) MITMAS.get("MMGRWE")
        totalAmount += (double) OOLINE.get("OBLNAM")

        logger.debug("Lecture OOLINE - totalWeight après ajout = " + totalWeight)
      }
      savedADID = OOLINE.get("OBADID")
    }
  }

  // Retrieve MPAGRL
  Closure<?> outDataMPAGRL = { DBContainer MPAGRL ->
    logger.debug("MPAGRL found - Retrieve price by weight --------------------------------------------------------------------------------")
    // Retrieve price by weight
    ExpressionFactory expression = database.getExpressionFactory("MPAGRE")
    expression = expression.eq("F1FROP", "12")
    DBAction queryMPAGRE = database.table("MPAGRE").index("00").matching(expression).selection("F1CONO","F1SUNO","F1AGNB","F1GRPI","F1OBV1","F1OBV2","F1OBV3","F1OBV4","F1FVDT","F1FDSE","F1FROP").build()
    DBContainer MPAGRE = queryMPAGRE.getContainer()
    MPAGRE.set("F1CONO", MPAGRL.get("AICONO"))
    MPAGRE.set("F1SUNO", MPAGRL.get("AISUNO"))
    MPAGRE.set("F1AGNB", MPAGRL.get("AIAGNB"))
    MPAGRE.set("F1GRPI", MPAGRL.get("AIGRPI"))
    MPAGRE.set("F1OBV1", MPAGRL.get("AIOBV1"))
    MPAGRE.set("F1OBV2", MPAGRL.get("AIOBV2"))
    MPAGRE.set("F1OBV3", MPAGRL.get("AIOBV3"))
    MPAGRE.set("F1OBV4", MPAGRL.get("AIOBV4"))
    MPAGRE.set("F1FVDT", MPAGRL.get("AIFVDT"))
    queryMPAGRE.readAll(MPAGRE, 9, nbMaxRecord, outDataMPAGRE)

    // Retrieve flat rate amount
    logger.debug("MPAGRL found - Retrieve flat rate amount --------------------------------------------------------------------------------")
    ExpressionFactory expression2 = database.getExpressionFactory("MPAGRE")
    expression2 = expression2.eq("F1FROP", "02")
    DBAction queryMPAGRE2 = database.table("MPAGRE").index("00").matching(expression2).selection("F1CONO","F1SUNO","F1AGNB","F1GRPI","F1OBV1","F1OBV2","F1OBV3","F1OBV4","F1FVDT","F1FDSE","F1FROP").build()
    DBContainer MPAGRE2 = queryMPAGRE2.getContainer()
    MPAGRE2.set("F1CONO", MPAGRL.get("AICONO"))
    MPAGRE2.set("F1SUNO", MPAGRL.get("AISUNO"))
    MPAGRE2.set("F1AGNB", MPAGRL.get("AIAGNB"))
    MPAGRE2.set("F1GRPI", MPAGRL.get("AIGRPI"))
    MPAGRE2.set("F1OBV1", MPAGRL.get("AIOBV1"))
    MPAGRE2.set("F1OBV2", MPAGRL.get("AIOBV2"))
    MPAGRE2.set("F1OBV3", MPAGRL.get("AIOBV3"))
    MPAGRE2.set("F1OBV4", MPAGRL.get("AIOBV4"))
    MPAGRE2.set("F1FVDT", MPAGRL.get("AIFVDT"))
    queryMPAGRE2.readAll(MPAGRE2, 9, nbMaxRecord, outDataMPAGRE)
  }
  // Retrieve MPAGRE
  Closure<?> outDataMPAGRE = { DBContainer MPAGRE ->
    logger.debug("MPAGRE found")
    savedFROP = MPAGRE.get("F1FROP")
    logger.debug("savedFROP = " + savedFROP)
    DBAction queryMPAGRF = database.table("MPAGRF").index("10").selection("F2FRQT","F2FRRA","F2OBV1","F2RBV1","F2RBV2").build()
    DBContainer MPAGRF = queryMPAGRF.getContainer()
    MPAGRF.set("F2CONO", MPAGRE.get("F1CONO"))
    MPAGRF.set("F2SUNO", MPAGRE.get("F1SUNO"))
    MPAGRF.set("F2AGNB", MPAGRE.get("F1AGNB"))
    MPAGRF.set("F2GRPI", MPAGRE.get("F1GRPI"))
    MPAGRF.set("F2OBV1", MPAGRE.get("F1OBV1"))
    MPAGRF.set("F2OBV2", MPAGRE.get("F1OBV2"))
    MPAGRF.set("F2OBV3", MPAGRE.get("F1OBV3"))
    MPAGRF.set("F2OBV4", MPAGRE.get("F1OBV4"))
    MPAGRF.set("F2FVDT", MPAGRE.get("F1FVDT"))
    MPAGRF.set("F2FDSE", MPAGRE.get("F1FDSE"))
    MPAGRF.set("F2RBV1", savedROUT)
    MPAGRF.set("F2RBV2", savedECAR)
    if (!queryMPAGRF.readAll(MPAGRF, 12, nbMaxRecord, outDataMPAGRF)) {
      MPAGRF.set("F2RBV1", savedROUT)
      MPAGRF.set("F2RBV2", "")
      if (!queryMPAGRF.readAll(MPAGRF, 12, nbMaxRecord, outDataMPAGRF)) {
        MPAGRF.set("F2RBV1", "")
        MPAGRF.set("F2RBV2", "")
        queryMPAGRF.readAll(MPAGRF, 12, nbMaxRecord, outDataMPAGRF)
      }
    }
  }

  // Retrieve MPAGRF
  Closure<?> outDataMPAGRF = { DBContainer MPAGRF ->
    logger.debug("totalWeight = " + totalWeight)
    logger.debug("MPAGRF found - OBV1 = " + MPAGRF.get("F2OBV1"))
    logger.debug("MPAGRF found - RBV1 = " + MPAGRF.get("F2RBV1"))
    logger.debug("MPAGRF found - RBV2 = " + MPAGRF.get("F2RBV2"))
    logger.debug("MPAGRF found - FRQT = " + MPAGRF.get("F2FRQT"))
    logger.debug("MPAGRF found - FRRA = " + MPAGRF.get("F2FRRA"))
    savedFRQT = MPAGRF.get("F2FRQT")
    switch (savedFROP) {
      case "12":
        if(savedFRQT <= totalWeight && priceByWeight == 0) {
          priceByWeight = MPAGRF.get("F2FRRA")
          logger.debug("priceByWeight found = " + priceByWeight)
        }
        break
      case "02":
        flatRateAmount = MPAGRF.get("F2FRRA")
        break
    }
    logger.debug("priceByWeight = " + priceByWeight)
    logger.debug("flatRateAmount (FRRA) = " + flatRateAmount)
  }

  // Calculate charge based on order weight
  public void calculationBasedOnWeight(){
    // Add records to EXT020
    DBAction queryOOLINE = database.table("OOLINE").index("00").selection("OBCONO","OBROUT","OBRODN","OBPLDT","OBORNO","OBPONR","OBPOSX").build()
    DBContainer OOLINE = queryOOLINE.getContainer()
    OOLINE.set("OBCONO", currentCompany)
    OOLINE.set("OBORNO", inORNO)
    queryOOLINE.readAll(OOLINE, 2, nbMaxRecord, outDataOOLINE2)

    // Read EXT020 and calculate charge amount per combination ROUT, RODN, PLDT
    shippingChargeAmount = 0
    savedROUT = ""
    ext020ROUT = ""
    DBAction queryEXT020 = database.table("EXT020").index("00").selection("EXROUT","EXRODN","EXPLDT","EXPONR","EXPOSX").build()
    DBContainer EXT020 = queryEXT020.getContainer()
    EXT020.set("EXBJNO", jobNumber)
    queryEXT020.readAll(EXT020, 1, nbMaxRecord, outDataEXT020)
    // Calculation for the last combination
    calculationCombination()
  }

  // Retrieve departure information
  public void retrieveDepartureInformation(){
    forwardingAgent = ""
    DBAction queryDROUDI = database.table("DROUDI").index("00").selection("DSFWNO").build()
    DBContainer DROUDI = queryDROUDI.getContainer()
    DROUDI.set("DSCONO", currentCompany)
    DROUDI.set("DSROUT", savedROUT)
    DROUDI.set("DSRODN", savedRODN)
    if (queryDROUDI.read(DROUDI)) {
      forwardingAgent = DROUDI.get("DSFWNO")
    }
    logger.debug("forwardingAgent = " + forwardingAgent)

    carrierIDChargeCalc = ""
    departureIsExcluded = false
    DBAction queryCUGEX1 = database.table("CUGEX1").index("00").selection("F1A030","F1CHB1").build()
    DBContainer CUGEX1 = queryCUGEX1.getContainer()
    CUGEX1.set("F1CONO", currentCompany)
    CUGEX1.set("F1FILE",  "DROUDI")
    CUGEX1.set("F1PK01",  savedROUT)
    CUGEX1.set("F1PK02",  String.format("%03d", savedRODN))
    if(queryCUGEX1.read(CUGEX1)){
      carrierIDChargeCalc = CUGEX1.get("F1A030")
      if(CUGEX1.get("F1CHB1") == 1){
        departureIsExcluded = true
      }
    }
    logger.debug("carrierIDChargeCalc = " + carrierIDChargeCalc)
  }

  // Retrieve price by weight
  public void retrievePriceByWeight(){
    priceByWeight = 0

    // Retrieve address state
    DBAction queryOOADRE = database.table("OOADRE").index("00").selection("ODECAR").build()
    DBContainer OOADRE = queryOOADRE.getContainer()
    OOADRE.set("ODCONO", currentCompany)
    OOADRE.set("ODORNO", inORNO)
    OOADRE.set("ODADRT", 1)
    OOADRE.set("ODADID", savedADID)
    if (queryOOADRE.read(OOADRE)) {
      if(OOADRE.get("ODECAR") != "") {
        savedECAR = OOADRE.get("ODECAR")
        logger.debug("OOADRE.ECAR = " + savedECAR)
      }
    } else {
      DBAction queryOCUSAD = database.table("OCUSAD").index("00").selection("OPECAR").build()
      DBContainer OCUSAD = queryOCUSAD.getContainer()
      OCUSAD.set("OPCONO", currentCompany)
      OCUSAD.set("OPCUNO",  customerNumber)
      OCUSAD.set("OPADRT",  1)
      OCUSAD.set("OPADID",  savedADID)
      if (queryOCUSAD.read(OCUSAD)) {
        if(OCUSAD.get("OPECAR") != "") {
          savedECAR = OCUSAD.get("OPECAR")
          logger.debug("OCUSAD.ECAR = " + savedECAR)
        }
      }
    }
    logger.debug("savedECAR = " + savedECAR)

    logger.debug("savedPLDT = " + savedPLDT)

    // Retrieve the price by weight
    ExpressionFactory expression = database.getExpressionFactory("MPAGRL")
    expression = expression.eq("AIOBV1", carrierIDChargeCalc)
    expression = expression.and(expression.le("AIFVDT", savedPLDT as String))
    expression = expression.and(expression.ge("AIUVDT", savedPLDT as String))
    DBAction queryMPAGRL = database.table("MPAGRL").index("00").matching(expression).selection("AICONO","AISUNO","AIAGNB","AIGRPI","AIOBV1","AIOBV2","AIOBV3","AIOBV4","AIFVDT").build()
    DBContainer MPAGRL = queryMPAGRL.getContainer()
    MPAGRL.set("AICONO", currentCompany)
    MPAGRL.set("AISUNO", forwardingAgent)
    queryMPAGRL.readAll(MPAGRL, 2, nbMaxRecord, outDataMPAGRL)
  }
  // Calculation per combination ROUT, RODN, PLDT
  public void calculationCombination(){
    logger.debug("New combination - Previous combination : " + savedROUT+"/"+savedRODN+"/"+savedPLDT)
    // Retrieve departure information for the previous combination ROUT, RODN
    retrieveDepartureInformation()
    if(departureIsExcluded){
      logger.debug("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! DEPARTURE : " +savedROUT+"/"+savedRODN+ " IS EXCLUDED, NO CHARGE CALCULATION !!!!!!!!!!!!!!!!!!!!!!!")
      return
    }
    // Retrieve the price by weight based on the totalWeight of the previous combination ROUT, RODN, PLDT
    retrievePriceByWeight()

    logger.debug("CALCULATION FOR COMBINATION : " + savedROUT+"/"+savedRODN+"/"+savedPLDT + " XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    logger.debug("totalWeight = " + totalWeight)
    logger.debug("priceByWeight = " + priceByWeight)

    // For method 2, if charge amount is greater or equal than free amount, charge is zero
    if (calculationMethod.trim() == "02"){
      logger.debug("Method 02 - totalAmount = " + totalAmount)
      logger.debug("Method 02 - freeAmount = " + freeAmount)
      if(totalAmount < freeAmount) {
        // Calculate the charge amount for previous combination ROUT, RODN, PLDT
        shippingChargeAmount += (totalWeight * priceByWeight)
        logger.debug("Add totalWeight x priceByWeight XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
        logger.debug("shippingChargeAmount = " + shippingChargeAmount)
        // Add flat rate amount (once per conbination ROUT, RODN, PLDT)
        shippingChargeAmount += flatRateAmount
        logger.debug("Add flat rate XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
        logger.debug("flatRateAmount (A930) = " + flatRateAmount)
      } else {
        logger.debug("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! METHOD 02 - totalAmount >= freeAmount = NO CHARGE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      }
    } else {
      // Calculate the charge amount for previous combination ROUT, RODN, PLDT
      shippingChargeAmount += (totalWeight * priceByWeight)
      logger.debug("Add totalWeight x priceByWeight XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
      logger.debug("shippingChargeAmount = " + shippingChargeAmount)
      // Add flat rate amount (once per conbination ROUT, RODN, PLDT)
      shippingChargeAmount += flatRateAmount
      logger.debug("Add flat rate XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
      logger.debug("flatRateAmount (A930) = " + flatRateAmount)
    }

    logger.debug("TOTAL AMOUNT XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    logger.debug("shippingChargeAmount = " + shippingChargeAmount)

    totalWeight = 0
    totalAmount = 0
    savedROUT = ext020ROUT
    savedRODN = ext020RODN
    savedPLDT = ext020PLDT
  }
}
