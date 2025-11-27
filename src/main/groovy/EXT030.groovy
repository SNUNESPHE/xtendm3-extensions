/***********************************************************************************************************************************************************************************************
 Extension Name: EXT030
 Type: ExtendM3Batch
 Script Author: YJANNIN
 Date: 20241010
 Description:   5209 - Franco de port
 * Description of script functionality
 Shipping charge handling

 Revision History:
 Name                    Date             Version          Description of Changes
 YJANNIN                 2024-10-10       1.0              Shipping charge handling
 ARENARD                 2025-08-26       1.1              Extension has been fixed
 ARENARD                 2025-09-29       1.2              Extension has been optimized : inCUNO become mandatory, CUNO filters added
 ARENARD                 2025-10-02       1.3              ReadAll limits have been significantly reduced to optimize performance
 ARENARD                 2025-10-17       1.4              A030 renamed / Optimisation : executeEXT030MIRtvSlsStatAmnt replaced by a single reading of EXT033, counter and maxLimit handling introduced
 ARENARD                 2025-11-24       1.5              retrieveIndexWeight has been modified (NTWM is no longer taken into account)
 *************************************************************************************************************************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import java.time.temporal.WeekFields

public class EXT030 extends ExtendM3Batch {
  private final LoggerAPI logger
  private final DatabaseAPI database
  private final ProgramAPI program
  private final BatchAPI batch
  private final MICallerAPI miCaller
  private final TextFilesAPI textFiles
  private final UtilityAPI utility

  private Integer currentCompany
  private String rawData
  private int rawDataLength
  private int beginIndex
  private int endIndex
  private boolean IN60
  private String jobNumber
  private Integer nbMaxRecord = 10000

  private String currentDivision
  private Integer currentDate
  private String odheadCUNO
  private String odheadDIVI
  private String odheadORNO
  private Long odheadDLIX
  private String odheadROUT
  private Integer odheadRODN
  private Integer odheadDLDT
  private String odheadWHLO
  private Double odheadNTAM
  private Integer droudiLILH
  private Integer droudiLILM
  private String ocusmaWHLO
  private Double sumGWTM
  private String drodprROUT
  private String calculationMethod
  private Double fixedChargeAmount
  private Double administrativeFeeAmount
  private String typeAdministrativeFeeAmount
  private Double freeAmount
  private Double flatRateAmount
  private String savedECAR
  private String forwardingAgent
  private String carrierIDChargeCalc
  private Double shippingChargeAmount
  private Double totalWeight
  private Double priceByWeight
  private Double savedFRQT
  private String savedROUT
  private Integer savedRODN
  private String savedFROP
  private Double beforeCutoffAmount
  private Double afterCutoffAmount
  private Integer ext030DLDT
  private String ext030CUNO
  private Long ext031DLIX
  private String ext030ROUT
  private Integer ext030LILH
  private Integer ext030LILM
  private Integer ext031DLDT
  private String ext031CUNO
  private String ext031WHLO
  private String mwwhnm
  private Integer ooheadRLHM
  private Double sumZFF1
  private Double sumZFF2
  private Double sumZFPR
  private Double sumZFPA
  private Double sumZFFO
  private boolean departureIsExcluded
  private Double totalAmount
  private Integer ext030RODN
  private Integer previousRGDT
  private String newORNO
  private Integer newPONR
  private Integer newPOSX
  private String listDLIX
  private String ext031ROUT
  private Integer ext031RODN
  private String text240
  private String MVXD
  private String modeLivraison
  private String orderType
  private String SKUFraisAdmin
  private String SKUFraisTransport
  private Integer dldtWeekNumber
  private boolean sameWeek
  private boolean flagBeforeCutoffAmount
  private boolean flagAfterCutoffAmount
  private Integer CUTO
  private Double sumNTA1
  private Double sumNTA2
  private boolean excludedOrderType
  private String odheadORTP
  private String ext030BJNO
  private boolean txtHead
  private String drtx40
  private String sunoName
  private Double txtTotalWeight
  private Integer counter
  private Integer maxLimit

  public EXT030(LoggerAPI logger, UtilityAPI utility,DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles) {
    this.logger = logger
    this.utility = utility
    this.database = database
    this.program = program
    this.batch = batch
    this.miCaller = miCaller
    this.textFiles = textFiles
  }

  public void main() {
    // Get job number
    LocalDateTime timeOfCreation = LocalDateTime.now()
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))

    logger.debug("Début" + program.getProgramName())
    logger.debug("referenceId = " + batch.getReferenceId().get())
    if(batch.getReferenceId().isPresent()){
      Optional<String> data = getJobData(batch.getReferenceId().get())
      logger.debug("data = " + data)
      performActualJob(data)
    } else {
      logger.debug("Job data for job ${batch.getJobId()} is missing")
    }
  }
  // Get job data
  private Optional<String> getJobData(String referenceId){
    DBAction query = database.table("EXTJOB").index("00").selection("EXDATA").build()
    DBContainer container = query.createContainer()
    container.set("EXRFID", referenceId)
    if (query.read(container)){
      logger.debug("EXDATA = " + container.getString("EXDATA"))
      return Optional.of(container.getString("EXDATA"))
    } else {
      logger.debug("EXTJOB not found")
    }
    return Optional.empty()
  }
  // Perform actual job
  private performActualJob(Optional<String> data){
    if(!data.isPresent()){
      logger.debug("Job reference Id ${batch.getReferenceId().get()} is passed but data was not found")
      return
    }
    rawData = data.get()
    logger.debug("Début performActualJob")
    String inDLDT = getFirstParameter()
    String inCUNO = getNextParameter()

    logger.debug("value inDLDT= {$inDLDT}")
    logger.debug("value inCUNO= {$inCUNO}")

    currentCompany = (Integer)program.getLDAZD().CONO
    currentDivision = program.getLDAZD().DIVI
    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer

    if(inDLDT == null || inDLDT.trim() == "") {
      inDLDT = currentDate
    }

    if(inDLDT == null || inDLDT.trim() == "") {
      logger.debug("Date de livraison non renseignée")
      return
    }
    if (!utility.call("DateUtil", "isDateValid", inDLDT, "yyyyMMdd")) {
      logger.debug("Date de livraison invalide")
      return
    }

    if(inCUNO == null || inCUNO.trim() == "") {
      logger.debug("Code client non renseigné")
      return
    }

    retrieveParameters()

    if(inCUNO != null && inCUNO.trim() != "") {
      DBAction queryOCUSMA = database.table("OCUSMA").index("00").build()
      DBContainer OCUSMA = queryOCUSMA.getContainer()
      OCUSMA.set("OKCONO", currentCompany)
      OCUSMA.set("OKCUNO", inCUNO)
      if (!queryOCUSMA.read(OCUSMA)) {
        logger.debug("Code client invalide")
        return
      }
    }

    logger.debug("LECTURE ODHEAD, SELECTION DES INDEX XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    ExpressionFactory expression = database.getExpressionFactory("ODHEAD")
    expression = expression.eq("UADLDT", inDLDT)
    expression = expression.and(expression.ge("UAORST", "60"))
    if(inCUNO != null && inCUNO.trim() != "") {
      expression = expression.and(expression.eq("UACUNO", inCUNO))
    }
    DBAction queryODHEAD = database.table("ODHEAD").index("40").matching(expression).selection("UADIVI","UACUNO","UAORNO","UADLIX","UAROUT","UARODN","UADLDT","UAWHLO","UANTAM","UAORTP").build()
    DBContainer ODHEAD = queryODHEAD.getContainer()
    ODHEAD.set("UACONO", currentCompany)
    ODHEAD.set("UADIVI", currentDivision)
    if(!queryODHEAD.readAll(ODHEAD, 2, 1000, outDataODHEAD)){
      logger.debug("Aucun index ne correspond à la sélection")
      return
    }

    logger.debug("LECTURE EXT030 PAR CLIENT XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    DBAction queryEXT0302 = database.table("EXT030").index("00").selection("EXDLDT","EXCUNO","EXROUT","EXLILH","EXLILM", "EXZGBL").build()
    DBContainer EXT0302 = queryEXT0302.getContainer()
    EXT0302.set("EXCONO", currentCompany)
    EXT0302.set("EXDIVI", currentDivision)
    EXT0302.set("EXDLDT", inDLDT as Integer)
    EXT0302.set("EXCUNO", inCUNO)
    queryEXT0302.readAll(EXT0302, 4, 1, outDataEXT0302)

    logger.debug("LECTURE EXT030 PAR CLIENT Pour creation commande XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    DBAction queryEXT0303 = database.table("EXT030").index("00").selection("EXDLDT","EXCUNO","EXZFF1","EXZFF2","EXZFFO","EXZFPR", "EXZFAD","EXNTA1","EXNTA2", "EXZGBL").build()
    DBContainer EXT0303 = queryEXT0303.getContainer()
    EXT0303.set("EXCONO", currentCompany)
    EXT0303.set("EXDIVI", currentDivision)
    EXT0303.set("EXDLDT", inDLDT as Integer)
    EXT0303.set("EXCUNO", inCUNO)
    queryEXT0303.readAll(EXT0303, 4, 1, outDataEXT0303)
  }
  // Retrieve ODHEAD
  Closure<?> outDataODHEAD = { DBContainer ODHEAD ->
    odheadCUNO = ODHEAD.get("UACUNO")
    odheadDIVI = ODHEAD.get("UADIVI")
    odheadORNO = ODHEAD.get("UAORNO")
    odheadDLIX = ODHEAD.get("UADLIX")
    odheadROUT = ODHEAD.get("UAROUT")
    odheadRODN = ODHEAD.get("UARODN")
    odheadDLDT = ODHEAD.get("UADLDT")
    odheadWHLO = ODHEAD.get("UAWHLO")
    odheadORTP = ODHEAD.get("UAORTP")
    odheadNTAM = 0

    DBAction queryEXT030 = database.table("EXT030").index("00").selection("EXBJNO").build()
    DBContainer EXT030 = queryEXT030.getContainer()
    EXT030.set("EXCONO", currentCompany)
    EXT030.set("EXDIVI", currentDivision)
    EXT030.set("EXDLDT", odheadDLDT)
    EXT030.set("EXCUNO", odheadCUNO)
    if(queryEXT030.read(EXT030)){
      ext030BJNO = EXT030.get("EXBJNO")
      if(ext030BJNO.trim() != jobNumber.trim()) {
        logger.debug("Date de livraison déjà traitée pour le client " + odheadCUNO)
        return
      }
    }

    if(excludedOrderType())
      return

    DBAction queryOSBSTD = database.table("OSBSTD").index("00").selection("UCSAAM").build()
    DBContainer OSBSTD = queryOSBSTD.getContainer()
    OSBSTD.set("UCCONO", currentCompany)
    OSBSTD.set("UCDIVI", odheadDIVI)
    OSBSTD.set("UCORNO", odheadORNO)
    OSBSTD.set("UCDLIX", odheadDLIX)
    queryOSBSTD.readAll(OSBSTD, 4, 10, outDataOSBSTD)
    logger.debug("odheadNTAM = " + odheadNTAM)

    logger.debug("xxxxxxxxxxxxxxxxxxxxxxxxxxx odheadDLIX = " + odheadDLIX)

    DBAction queryMHDISH = database.table("MHDISH").index("00").selection("OQPIST").build()
    DBContainer MHDISH = queryMHDISH.getContainer()
    MHDISH.set("OQCONO", currentCompany)
    MHDISH.set("OQINOU",  1)
    MHDISH.set("OQDLIX",  ODHEAD.get("UADLIX"))
    if(queryMHDISH.read(MHDISH)) {
      if(MHDISH.get("OQPIST") as String != "30"){
        logger.debug("Statut colisage différent de 30 - Index non pris en compte")
        return
      }
    }

    DBAction queryCUGEX1 = database.table("CUGEX1").index("00").selection("F1CHB1").build()
    DBContainer CUGEX1 = queryCUGEX1.getContainer()
    CUGEX1.set("F1CONO", currentCompany)
    CUGEX1.set("F1FILE",  "DROUDI")
    CUGEX1.set("F1PK01",  ODHEAD.get("UAROUT"))
    CUGEX1.set("F1PK02",  String.format("%03d", odheadRODN))
    if(queryCUGEX1.read(CUGEX1)){
      if(CUGEX1.get("F1CHB1") == 1){
        logger.debug("Exclusion départ CUGEX1.CHB1 = 1 - Index non pris en compte")
        writeEXT030()
        return
      }
    }

    logger.debug("Ecriture EXT030, EXT031")
    writeEXT030()
    writeEXT031()
  }
  // write EXT030
  public void writeEXT030() {
    retrieveRouteCutOff()
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXT030").index("00").build()
    DBContainer EXT030 = query.getContainer()
    EXT030.set("EXCONO", currentCompany)
    EXT030.set("EXDIVI", currentDivision)
    EXT030.set("EXDLDT", odheadDLDT)
    EXT030.set("EXCUNO", odheadCUNO)
    if (!query.read(EXT030)) {
      EXT030.set("EXROUT", drodprROUT)
      EXT030.set("EXLILH", droudiLILH)
      EXT030.set("EXLILM", droudiLILM)
      EXT030.set("EXBJNO", jobNumber)
      EXT030.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT030.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT030.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT030.setInt("EXCHNO", 1)
      EXT030.set("EXCHID", program.getUser())
      query.insert(EXT030)
    }
  }
  // write EXT031
  public void writeEXT031() {
    retrieveIndexWeight()
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXT031").index("00").build()
    DBContainer EXT031 = query.getContainer()
    EXT031.set("EXCONO", currentCompany)
    EXT031.set("EXDIVI", currentDivision)
    EXT031.set("EXDLDT", odheadDLDT)
    EXT031.set("EXCUNO", odheadCUNO)
    EXT031.set("EXDLIX", odheadDLIX)
    EXT031.set("EXWHLO", odheadWHLO)
    EXT031.set("EXROUT", odheadROUT)
    EXT031.set("EXRODN", odheadRODN)
    if (!query.read(EXT031)) {
      EXT031.set("EXGWTM", sumGWTM)
      EXT031.set("EXNTAM", odheadNTAM)
      EXT031.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT031.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT031.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT031.setInt("EXCHNO", 1)
      EXT031.set("EXCHID", program.getUser())
      query.insert(EXT031)
    } else {
      query.readLock(EXT031, updateCallBackEXT0312)
    }
  }
  // Retrieve route
  public void retrieveRouteCutOff() {
    logger.debug("retrieveRouteCutOff")
    drodprROUT = ""
    droudiLILH = 0
    droudiLILM = 0
    ocusmaWHLO = ""
    DBAction queryOCUSMA = database.table("OCUSMA").index("00").selection("OKWHLO").build()
    DBContainer OCUSMA = queryOCUSMA.getContainer()
    OCUSMA.set("OKCONO", currentCompany)
    OCUSMA.set("OKCUNO", odheadCUNO)
    if (queryOCUSMA.read(OCUSMA)) {
      logger.debug("retrieveRouteCutOff OCUSMA found")
      ocusmaWHLO = OCUSMA.get("OKWHLO")
      DBAction queryMITWHL = database.table("MITWHL").index("00").selection("MWFACI", "MWSDES").build()
      DBContainer MITWHL = queryMITWHL.getContainer()
      MITWHL.set("MWCONO", currentCompany)
      MITWHL.set("MWWHLO", ocusmaWHLO)
      if(queryMITWHL.read(MITWHL)){
        logger.debug("retrieveRouteCutOff MITWHL found - FACI = " + MITWHL.get("MWFACI"))
        logger.debug("retrieveRouteCutOff MITWHL found - SDES = " + MITWHL.get("MWSDES"))
        DBAction queryDRODPR = database.table("DRODPR").index("00").selection("DOROUT").build()
        DBContainer DRODPR = queryDRODPR.getContainer()
        DRODPR.set("DOCONO", currentCompany)
        DRODPR.set("DODIVI", "")
        DRODPR.set("DOPREX", " 5")
        DRODPR.set("DOEDES", MITWHL.get("MWSDES"))
        DRODPR.set("DOOBV1", odheadCUNO)
        DRODPR.set("DOOBV2", modeLivraison)                                          // CRS881 A CHANGER cf. Greg
        if(queryDRODPR.read(DRODPR)){
          logger.debug("retrieveRouteCutOff DRODPR found")
          drodprROUT = DRODPR.get("DOROUT")
          DBAction queryDROUDI = database.table("DROUDI").index("00").selection("DSLILH","DSLILM").build()
          DBContainer DROUDI = queryDROUDI.getContainer()
          DROUDI.set("DSCONO", currentCompany)
          DROUDI.set("DSROUT", drodprROUT)
          DROUDI.set("DSRODN", 1)
          if (queryDROUDI.read(DROUDI)) {
            logger.debug("retrieveRouteCutOff DROUDI found")
            droudiLILH = DROUDI.get("DSLILH")
            droudiLILM = DROUDI.get("DSLILM")
          }
        }
      }
    }
  }

  // Retrieve customer parameters
  public void retrieveParameters() {
    MVXD = ""
    getCRS881("EXT030", "1", "ModeLivraison", "I", "Header", "ModeLivraison", "", "")
    modeLivraison = MVXD.trim()

    MVXD = ""
    getCRS881("EXT030", "1", "OrderType", "I", "Header", "OrderType", "", "")
    orderType = MVXD.trim()

    MVXD = ""
    getCRS881("EXT030", "1", "SKUFraisAdmin", "I", "Line", "SKUFraisAdmin", "", "")
    SKUFraisAdmin = MVXD.trim()

    MVXD = ""
    getCRS881("EXT030", "1", "SKUFraisTransport", "I", "Line", "SKUFraisTransport", "", "")
    SKUFraisTransport = MVXD.trim()

    logger.debug("Parameters CRS881  **********************************************************************************")
    logger.debug("Mode Livraison = " + modeLivraison)
    logger.debug("Order Type = " + orderType)
    logger.debug("SKU Frais Admin = " + SKUFraisAdmin)
    logger.debug("SKU Frais Transport = " + SKUFraisTransport)

  }

  // Get CRS881
  public void getCRS881(String MSTD, String MVRS, String BMSG, String IBOB, String ELMP, String ELMD, String ELMC, String MBMC){
    DBAction qMBMTRN = database.table("MBMTRN").index("00").selection("TRIDTR").build()
    DBContainer MBMTRN = qMBMTRN.getContainer()
    MBMTRN.set("TRTRQF", "0")
    MBMTRN.set("TRMSTD", MSTD)
    MBMTRN.set("TRMVRS", MVRS)
    MBMTRN.set("TRBMSG", BMSG)
    MBMTRN.set("TRIBOB", IBOB)
    MBMTRN.set("TRELMP", ELMP)
    MBMTRN.set("TRELMD", ELMD)
    MBMTRN.set("TRELMC", ELMC)
    MBMTRN.set("TRMBMC", MBMC)
    if (qMBMTRN.read(MBMTRN)) {
      DBAction qMBMTRD = database.table("MBMTRD").index("00").selection("TDMVXD").build()
      DBContainer MBMTRD = qMBMTRD.getContainer()
      MBMTRD.set("TDCONO", currentCompany)
      MBMTRD.set("TDDIVI", currentDivision)
      MBMTRD.set("TDIDTR", MBMTRN.get("TRIDTR"))
      qMBMTRD.readAll(MBMTRD, 3, 1, outDataMBMTRD)
    }
  }

  // Retrieve MBTRND
  Closure<?> outDataMBMTRD= { DBContainer MBMTRD ->
    MVXD = MBMTRD.get("TDMVXD")
  }

  // Retrieve MBTRND
  Closure<?> outDataMBMTRD2= { DBContainer MBMTRD ->
    excludedOrderType = true
  }

  // Retrieve customer parameters
  public void retrieveCustomerParameters() {
    savedECAR = ""
    DBAction queryOCUSMA = database.table("OCUSMA").index("00").selection("OKECAR").build()
    DBContainer OCUSMA = queryOCUSMA.getContainer()
    OCUSMA.set("OKCONO", currentCompany)
    OCUSMA.set("OKCUNO", ext030CUNO)
    if (queryOCUSMA.read(OCUSMA)) {
      savedECAR = OCUSMA.get("OKECAR")
    }
    logger.debug("Ecar (OKECAR) = " + savedECAR)

    calculationMethod = ""
    fixedChargeAmount = 0
    administrativeFeeAmount = 0
    typeAdministrativeFeeAmount = ""
    freeAmount = 0
    flatRateAmount = 0
    DBAction queryCUGEX1 = database.table("CUGEX1").index("00").selection("F1A030","F1A121","F1A230","F1A830","F1A930").build()
    DBContainer CUGEX1 = queryCUGEX1.getContainer()
    CUGEX1.set("F1CONO", currentCompany)
    CUGEX1.set("F1FILE",  "OCUSMA")
    CUGEX1.set("F1PK01",  ext030CUNO)
    CUGEX1.set("F1PK02",  "")
    if(queryCUGEX1.read(CUGEX1)){
      calculationMethod = CUGEX1.get("F1A030")
      if(CUGEX1.get("F1A121") != "")
        fixedChargeAmount = CUGEX1.get("F1A121") as Double
      if(CUGEX1.get("F1A230") != "") {
        String a230 = CUGEX1.get("F1A230")
        logger.debug(" (A230) = " + a230)
        String firstA230 = a230.substring(0, 1)
        logger.debug(" (first A230) = " + firstA230)
        if (firstA230.trim() == "H") {
          int index = a230.indexOf(" ")
          logger.debug(" (index) = " + index)
          String valueA230 = a230.substring(1, index)
          administrativeFeeAmount = valueA230 as double
          typeAdministrativeFeeAmount = "H"
        }
        if (firstA230.trim() == "M") {
          int index = a230.indexOf(" ")
          String valueA230 = a230.substring(1, index)
          administrativeFeeAmount = valueA230 as double
          typeAdministrativeFeeAmount = "M"
        }
        if (firstA230.trim() != "H"  && firstA230.trim() != "M") {
          administrativeFeeAmount = CUGEX1.get("F1A230") as Double
        }
      }
      if(CUGEX1.get("F1A830") != "")
        freeAmount = CUGEX1.get("F1A830") as Double
      if(CUGEX1.get("F1A930") != "")
        flatRateAmount = CUGEX1.get("F1A930") as Double
    }
    logger.debug("---------- RETRIEVE PARAMETERS ----------------------------------------------------------------------------------------------------")
    logger.debug("calculationMethod (A030) = " + calculationMethod)
    logger.debug("fixedChargeAmount (A121) = " + fixedChargeAmount)
    logger.debug("administrativeFeeAmount (A230) = " + administrativeFeeAmount)
    logger.debug("typeAdministrativeFeeAmount (A230) = " + typeAdministrativeFeeAmount)
    logger.debug("freeAmount (A830) = " + freeAmount)
    logger.debug("flatRateAmount (A930) = " + flatRateAmount)
  }
  // Calculate index weight
  public void retrieveIndexWeight() {
    sumGWTM = 0
    DBAction queryMPTRNS = database.table("MPTRNS").index("03").selection("ORGWTM", "ORNTWM").build()
    DBContainer MPTRNS = queryMPTRNS.getContainer()
    MPTRNS.set("ORCONO", currentCompany)
    MPTRNS.set("ORDLIX", odheadDLIX)
    queryMPTRNS.readAll(MPTRNS, 2, 5, outDataMPTRNS)

    logger.debug("retrieveIndexWeight - sumGWTM = " + sumGWTM)
  }
  // Retrieve MPTRNS
  Closure<?> outDataMPTRNS = { DBContainer MPTRNS ->
    logger.debug("sumGWTM Before = " + sumGWTM)
    sumGWTM += MPTRNS.get("ORGWTM") as Double
    logger.debug("sumGWTM After ORGWTM = " + sumGWTM)
    //sumGWTM += MPTRNS.get("ORNTWM") as Double
    //logger.debug("sumGWTM After ORNTWM = " + sumGWTM)
  }
  // Retrieve EXT030
  Closure<?> outDataEXT030 = { DBContainer EXT030 ->
  }
  // Retrieve EXT030
  Closure<?> outDataEXT0302 = { DBContainer EXT030 ->
    int zgbl = EXT030.get("EXZGBL") as int
    if(zgbl == 1){
      return
    }
    ext030DLDT = EXT030.get("EXDLDT")
    ext030CUNO = EXT030.get("EXCUNO")
    ext030ROUT = EXT030.get("EXROUT")
    ext030LILH = EXT030.get("EXLILH")
    ext030LILM = EXT030.get("EXLILM")

    logger.debug("ext030CUNO = " + ext030CUNO)

    sumZFF1 = 0
    sumZFF2 = 0
    sumZFFO = 0
    sumZFPR = 0
    sumZFPA = 0
    sumNTA1 = 0
    sumNTA2 = 0

    retrieveCustomerParameters()

    // Calculate charge depending on customer calculation method
    logger.debug("---------- CALCULATION METHOD ----------------------------------------------------------------------------------------------------")
    switch (calculationMethod.trim()) {
      case "00":
        method00()
        break
      case "01":
        method01()
        break
      case "02":
        method02()
        break
      case "03":
        method03()
        break
      case "04":
        method04()
        break
      case "05":
        method05()
        break
      default:
        return
    }

    fraisAchatTransport()

    fraisAdministratif()

    logger.debug("MISE A JOUR EXT030 POUR LE CLIENT " + ext030CUNO + " XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction queryEXT0302 = database.table("EXT030").index("00").build()
    DBContainer EXT0302 = queryEXT0302.getContainer()
    EXT0302.set("EXCONO", currentCompany)
    EXT0302.set("EXDIVI", currentDivision)
    EXT0302.set("EXDLDT", ext030DLDT)
    EXT0302.set("EXCUNO", ext030CUNO)
    queryEXT0302.readLock(EXT0302, updateCallBackEXT030)
  }
  // Update EXT030
  Closure<?> updateCallBackEXT030 = { LockedResult lockedResult ->
    logger.debug("Update EXT030 _ EXZCLM = " + calculationMethod)
    logger.debug("Update EXT030 _ EXZFFO = " + sumZFFO)
    logger.debug("Update EXT030 _ EXZFF1 = " + sumZFF1)
    logger.debug("Update EXT030 _ EXNTA1 = " + sumNTA1)
    logger.debug("Update EXT030 _ EXZFF2 = " + sumZFF2)
    logger.debug("Update EXT030 _ EXNTA2 = " + sumNTA2)
    logger.debug("Update EXT030 _ EXZFPR = " + sumZFPR)
    logger.debug("Update EXT030 _ EXZFPA = " + sumZFPA)
    logger.debug("Update EXT030 _ EXZFAD = " + administrativeFeeAmount)
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("EXCHNO")
    lockedResult.set("EXZCLM", calculationMethod)
    lockedResult.set("EXZFFO", sumZFFO)
    lockedResult.set("EXZFF1", sumZFF1)
    lockedResult.set("EXNTA1", sumNTA1)
    lockedResult.set("EXZFF2", sumZFF2)
    lockedResult.set("EXNTA2", sumNTA2)
    lockedResult.set("EXZFPR", sumZFPR)
    lockedResult.set("EXZFPA", sumZFPA)
    lockedResult.set("EXZFAD", administrativeFeeAmount)
    lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("EXCHNO", changeNumber + 1)
    lockedResult.set("EXCHID", program.getUser())
    lockedResult.update()
  }
  // Calculation method 00 - "Non Applicable"
  public void method00() {
    logger.debug("method00")
  }

  // Calculation method 01 - "Groupé"
  public void method01() {
    logger.debug("method01")
    beforeCutoffAmount = 0
    flagBeforeCutoffAmount = false
    afterCutoffAmount = 0
    flagAfterCutoffAmount = false
    DBAction queryEXT031 = database.table("EXT031").index("00").selection("EXDLDT","EXCUNO","EXDLIX","EXWHLO","EXROUT","EXRODN").build()
    DBContainer EXT031 = queryEXT031.getContainer()
    EXT031.set("EXCONO", currentCompany)
    EXT031.set("EXDIVI", currentDivision)
    EXT031.set("EXDLDT", ext030DLDT)
    EXT031.set("EXCUNO", ext030CUNO)
    queryEXT031.readAll(EXT031, 4, 500, outDataEXT031)

    logger.debug("freeAmount = " + freeAmount)
    logger.debug("flagBeforeCutoffAmount = " + flagBeforeCutoffAmount)
    logger.debug("flagAfterCutoffAmount = " + flagAfterCutoffAmount)
    if(flagBeforeCutoffAmount){
      sumNTA1 = beforeCutoffAmount
      sumZFF1 = fixedChargeAmount
    }
    if(flagAfterCutoffAmount){
      sumNTA2 = afterCutoffAmount
      sumZFF2 = fixedChargeAmount
    }

  }

  // Retrieve EXT031
  Closure<?> outDataEXT031 = { DBContainer EXT031 ->
    ext031DLDT = EXT031.get("EXDLDT")
    ext031CUNO = EXT031.get("EXCUNO")
    ext031DLIX = EXT031.get("EXDLIX")
    ext031WHLO = EXT031.get("EXWHLO")
    ext031ROUT = EXT031.get("EXROUT")
    ext031RODN = EXT031.get("EXRODN")

    logger.debug("ext031DLIX = " + ext031DLIX)

    DBAction queryODHEAD = database.table("ODHEAD").index("22").selection("UAORNO","UADIVI","UANTAM").build()
    DBContainer ODHEAD = queryODHEAD.getContainer()
    ODHEAD.set("UACONO", currentCompany)
    ODHEAD.set("UADLIX", ext031DLIX)
    queryODHEAD.readAll(ODHEAD, 2, 20, outDataODHEAD2)
  }
  // Retrieve ODHEAD
  Closure<?> outDataODHEAD2 = { DBContainer ODHEAD ->
    odheadORNO = ODHEAD.get("UAORNO")
    odheadDIVI = ODHEAD.get("UADIVI")

    odheadNTAM = 0
    CUTO = 0

    // Retrieve delivery amount
    DBAction queryEXT033 = database.table("EXT033").index("00").selection("EXNTAM").build()
    DBContainer EXT033 = queryEXT033.getContainer()
    EXT033.set("EXCONO", currentCompany)
    EXT033.set("EXDIVI", odheadDIVI)
    EXT033.set("EXORNO", odheadORNO)
    EXT033.set("EXDLIX", ext031DLIX)
    if(queryEXT033.read(EXT033)){
      odheadNTAM = EXT033.get("EXNTAM")
    }

    logger.debug("odheadNTAM from EXT033 = " + odheadNTAM)

    DBAction queryOOHEAD = database.table("OOHEAD").index("00").selection("OARLHM").build()
    DBContainer OOHEAD = queryOOHEAD.getContainer()
    OOHEAD.set("OACONO", currentCompany)
    OOHEAD.set("OAORNO", odheadORNO)
    if(queryOOHEAD.read(OOHEAD)){
      ooheadRLHM = OOHEAD.get("OARLHM")
      String cutOff = "${ext030LILH}${ext030LILM}"
      logger.debug("ooheadRLHM = " + ooheadRLHM)
      logger.debug("cutOff = " + cutOff)
      if(ooheadRLHM <= (cutOff as Integer)){
        flagBeforeCutoffAmount = true
        beforeCutoffAmount += odheadNTAM
        logger.debug("Alimentation beforeCutoffAmount = " + beforeCutoffAmount)
      } else {
        CUTO = 1
        flagAfterCutoffAmount = true
        afterCutoffAmount += odheadNTAM
        logger.debug("Alimentation afterCutoffAmount = " + afterCutoffAmount)
      }
      updateEXT031()
    }
  }
  // Calculation method 02 - "Tournée"
  public void method02() {
    logger.debug("method02")

    shippingChargeAmount = 0
    savedROUT = ""
    savedRODN = 0
    ext030ROUT = ""
    ext030RODN = 0
    totalWeight = 0
    totalAmount = 0
    counter = 0
    maxLimit = 10
    DBAction queryEXT031 = database.table("EXT031").index("10").selection("EXROUT", "EXRODN", "EXGWTM", "EXNTAM").build()
    DBContainer EXT031 = queryEXT031.getContainer()
    EXT031.set("EXCONO", currentCompany)
    EXT031.set("EXDIVI", currentDivision)
    EXT031.set("EXDLDT", ext030DLDT)
    EXT031.set("EXCUNO", ext030CUNO)
    queryEXT031.readAll(EXT031, 4, 500, outDataEXT031M5)

    calculationCombination()

    sumZFPR += shippingChargeAmount
    logger.debug("sumZFPR = " + sumZFPR)

  }

  // Calculation method 03 - "Forfait hebdo"
  public void method03() {
    logger.debug("method03")

    logger.debug("dldt = " + ext030DLDT)
    dldtWeekNumber = getWeekNumber(ext030DLDT as String)
    logger.debug("dldtWeek = " + dldtWeekNumber)

    String odldt = ext030DLDT
    Integer year = (ext030DLDT / 10000).toInteger()
    Integer monthDay = (ext030DLDT % 10000).toInteger()
    Integer dateMonth = (year - 1) * 10000 + monthDay
    String cDate = dateMonth
    logger.debug("cDate = " + cDate)

    sameWeek = false

    ExpressionFactory expressionWeek = database.getExpressionFactory("EXT030")
    expressionWeek = expressionWeek.ne("EXDLDT", odldt)
    expressionWeek = expressionWeek.and(expressionWeek.ge("EXDLDT", cDate))
    DBAction weekEXT030 = database.table("EXT030").index("10").matching(expressionWeek).selection("EXDLDT").build()
    DBContainer wEXT030 = weekEXT030.getContainer()
    wEXT030.set("EXCONO", currentCompany)
    wEXT030.set("EXDIVI", currentDivision)
    wEXT030.set("EXCUNO", ext030CUNO)
    weekEXT030.readAll(wEXT030, 3, nbMaxRecord, outDataEXT030W)

    if(sameWeek){
      shippingChargeAmount = 0
    } else {
      shippingChargeAmount = flatRateAmount
    }
    sumZFFO = shippingChargeAmount
  }

  // Retrieve EXT030
  Closure<?> outDataEXT030W = { DBContainer EXT030 ->
    previousRGDT = EXT030.get("EXDLDT")
    logger.debug("dldt = " + ext030DLDT)
    logger.debug("previousRGDT = " + previousRGDT)
    if(ext030DLDT!=previousRGDT){
      Integer previousWeekNumber = getWeekNumber(previousRGDT as String)
      logger.debug("dldtWeek = " + dldtWeekNumber)
      logger.debug("previousWeekNumber = " + previousWeekNumber)
      if(dldtWeekNumber == previousWeekNumber){
        sameWeek = true
      }
    }
  }

  // Retrieve EXT030
  Closure<?> outDataEXT030M = { DBContainer EXT030 ->
    previousRGDT = EXT030.get("EXDLDT")
  }

  // Get week number
  private Integer getWeekNumber(String dateStr){
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    LocalDate date = LocalDate.parse(dateStr, formatter)

    WeekFields weekFields = WeekFields.of(Locale.getDefault())
    return date.get(weekFields.weekOfWeekBasedYear())

  }

  // Calculation method 04 - "Forfait mensuel"
  public void method04() {
    logger.debug("method04")

    String odldt = ext030DLDT
    previousRGDT = 0
    Integer month = (ext030DLDT / 100).toInteger()
    Integer dateMonth = month * 100 + 1
    String cDate = dateMonth

    ExpressionFactory expressionMonth = database.getExpressionFactory("EXT030")
    expressionMonth = expressionMonth.ne("EXDLDT", odldt)
    expressionMonth = expressionMonth.and(expressionMonth.ge("EXDLDT", cDate))
    DBAction monthEXT030 = database.table("EXT030").index("10").matching(expressionMonth).selection("EXDLDT").build()
    DBContainer mEXT030 = monthEXT030.getContainer()
    mEXT030.set("EXCONO", currentCompany)
    mEXT030.set("EXDIVI", currentDivision)
    mEXT030.set("EXCUNO", ext030CUNO)
    monthEXT030.readAll(mEXT030, 3, 1, outDataEXT030M)

    if(previousRGDT!=0 && previousRGDT!=ext030DLDT){
      shippingChargeAmount = 0
    } else {
      shippingChargeAmount = flatRateAmount
    }
    sumZFFO = shippingChargeAmount

  }

  // Calculation method 05 - "Frais réel"
  public void method05() {
    logger.debug("method05")

    shippingChargeAmount = 0
    savedROUT = ""
    savedRODN = 0
    ext030ROUT = ""
    ext030RODN = 0
    totalWeight = 0
    totalAmount = 0
    counter = 0
    maxLimit = 10
    DBAction queryEXT031 = database.table("EXT031").index("10").selection("EXROUT", "EXRODN", "EXGWTM", "EXNTAM").build()
    DBContainer EXT031 = queryEXT031.getContainer()
    EXT031.set("EXCONO", currentCompany)
    EXT031.set("EXDIVI", currentDivision)
    EXT031.set("EXDLDT", ext030DLDT)
    EXT031.set("EXCUNO", ext030CUNO)
    queryEXT031.readAll(EXT031, 4, 500, outDataEXT031M5)

    calculationCombination()

    sumZFPR = shippingChargeAmount
    logger.debug("sumZFPR = " + sumZFPR)

  }

  // Calculation "Frais Achat transport"
  public void fraisAchatTransport() {
    logger.debug("Frais Achat Transport")

    shippingChargeAmount = 0
    savedROUT = ""
    savedRODN = 0
    ext030ROUT = ""
    ext030RODN = 0
    counter = 0
    maxLimit = 10
    DBAction queryEXT031 = database.table("EXT031").index("10").selection("EXROUT", "EXRODN", "EXGWTM", "EXNTAM").build()
    DBContainer EXT031 = queryEXT031.getContainer()
    EXT031.set("EXCONO", currentCompany)
    EXT031.set("EXDIVI", currentDivision)
    EXT031.set("EXDLDT", ext030DLDT)
    EXT031.set("EXCUNO", ext030CUNO)
    queryEXT031.readAll(EXT031, 4, 500, outDataEXT031ZFPA)

    calculationCombinationZFPA()

    sumZFPA = shippingChargeAmount
    logger.debug("sumZFPA = " + sumZFPA)

  }



  // Calculation "Frais Achat transport"
  public void fraisAdministratif() {
    logger.debug("Frais Administratif")
    String odldt = ext030DLDT
    if(typeAdministrativeFeeAmount == "H"){
      logger.debug("dldt = " + ext030DLDT)
      dldtWeekNumber = getWeekNumber(ext030DLDT as String)
      logger.debug("dldtWeek = " + dldtWeekNumber)


      Integer year = (ext030DLDT / 10000).toInteger()
      Integer monthDay = (ext030DLDT % 10000).toInteger()
      Integer dateMonth = (year - 1) * 10000 + monthDay
      String cDate = dateMonth
      logger.debug("cDate = " + cDate)

      sameWeek = false

      ExpressionFactory expressionWeek = database.getExpressionFactory("EXT030")
      expressionWeek = expressionWeek.ne("EXDLDT", odldt)
      expressionWeek = expressionWeek.and(expressionWeek.ge("EXDLDT", cDate))
      DBAction weekEXT030 = database.table("EXT030").index("10").matching(expressionWeek).selection("EXDLDT").build()
      DBContainer wEXT030 = weekEXT030.getContainer()
      wEXT030.set("EXCONO", currentCompany)
      wEXT030.set("EXDIVI", currentDivision)
      wEXT030.set("EXCUNO", ext030CUNO)
      weekEXT030.readAll(wEXT030, 3, nbMaxRecord, outDataEXT030W)

      if(sameWeek){
        administrativeFeeAmount = 0
      }
    }
    if(typeAdministrativeFeeAmount == "M"){
      previousRGDT = 0
      Integer month = (ext030DLDT / 100).toInteger()
      Integer dateMonth = month * 100 + 1
      String cDate = dateMonth

      ExpressionFactory expressionMonth = database.getExpressionFactory("EXT030")
      expressionMonth = expressionMonth.ne("EXDLDT", odldt)
      expressionMonth = expressionMonth.and(expressionMonth.ge("EXDLDT", cDate))
      DBAction monthEXT030 = database.table("EXT030").index("10").matching(expressionMonth).selection("EXDLDT").build()
      DBContainer mEXT030 = monthEXT030.getContainer()
      mEXT030.set("EXCONO", currentCompany)
      mEXT030.set("EXDIVI", currentDivision)
      mEXT030.set("EXCUNO", ext030CUNO)
      monthEXT030.readAll(mEXT030, 3, 1, outDataEXT030M)

      if(previousRGDT!=0 && previousRGDT!=ext030DLDT){
        administrativeFeeAmount = 0
      }
    }
  }

  // Retrieve EXT031
  Closure<?> outDataEXT031M5 = { DBContainer EXT031 ->
    ext030ROUT = EXT031.get("EXROUT")
    ext030RODN = EXT031.get("EXRODN")

    logger.debug("Found EXT030 - ext030ROUT = " + ext030ROUT)
    logger.debug("Found EXT030 - ext030RODN = " + ext030RODN)

    if (savedROUT.trim() == "") {
      logger.debug("First record")
      savedROUT = EXT031.get("EXROUT")
      savedRODN = EXT031.get("EXRODN")
      totalWeight = 0
      totalAmount = 0
      retrieveDepartureInformation()
    }

    if (savedROUT.trim() != ext030ROUT.trim() ||
      savedRODN != ext030RODN) {
      if (counter < maxLimit) {
        logger.debug("calculationCombination called in EXT031M5")
        calculationCombination()
        counter++
      }
    }

    logger.debug("Lecture EXT031 - GWTM = " + EXT031.get("EXGWTM"))
    logger.debug("Lecture EXT031 - totalWeight avant ajout = " + totalWeight)

    totalWeight += (double) EXT031.get("EXGWTM")
    totalAmount += (double) EXT031.get("EXNTAM")

    logger.debug("Lecture EXT031 - totalWeight après ajout = " + totalWeight)

  }

  // Retrieve departure information
  public void retrieveDepartureInformation(){
    drtx40 = ""
    DBAction queryDROUTE = database.table("DROUTE").index("00").selection("DRTX40").build()
    DBContainer DROUTE = queryDROUTE.getContainer()
    DROUTE.set("DRCONO", currentCompany)
    DROUTE.set("DRROUT", savedROUT)
    if (queryDROUTE.read(DROUTE)) {
      drtx40 = DROUTE.get("DRTX40")
    }
    logger.debug("DRTX40 = " + drtx40)

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

    sunoName = ""
    DBAction queryCIDMAS = database.table("CIDMAS").index("00").selection("IDSUNM").build()
    DBContainer CIDMAS = queryCIDMAS.getContainer()
    CIDMAS.set("IDCONO", currentCompany)
    CIDMAS.set("IDSUNO", forwardingAgent)
    if (queryCIDMAS.read(CIDMAS)) {
      sunoName = CIDMAS.get("IDSUNM")
    }
    logger.debug("sunoName = " + sunoName)

    mwwhnm = ""
    DBAction queryMITWHL = database.table("MITWHL").index("00").selection("MWWHNM").build()
    DBContainer MITWHL = queryMITWHL.getContainer()
    MITWHL.set("MWCONO", currentCompany)
    MITWHL.set("MWWHLO", ext031WHLO)
    if (queryMITWHL.read(MITWHL)) {
      mwwhnm = MITWHL.get("MWWHNM")
    }
    logger.debug("whloName = " + mwwhnm)

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
    }
    logger.debug("carrierIDChargeCalc = " + carrierIDChargeCalc)
  }

  // Retrieve EXT031
  Closure<?> outDataEXT031ZFPA = { DBContainer EXT031 ->
    ext030ROUT = EXT031.get("EXROUT")
    ext030RODN = EXT031.get("EXRODN")

    logger.debug("Found EXT030 - ext030ROUT = " + ext030ROUT)
    logger.debug("Found EXT030 - ext030RODN = " + ext030RODN)

    if (savedROUT.trim() == "") {
      logger.debug("First record")
      savedROUT = EXT031.get("EXROUT")
      savedRODN = EXT031.get("EXRODN")
      totalWeight = 0
      totalAmount = 0
      retrieveDepartureInformationZFPA()
    }

    if (savedROUT.trim() != ext030ROUT.trim() ||
      savedRODN != ext030RODN) {
      if (counter < maxLimit) {
        logger.debug("calculationCombination called in EXT031ZFPA")
        calculationCombinationZFPA()
        counter++
      }
    }

    logger.debug("Lecture EXT031 - GWTM = " + EXT031.get("EXGWTM"))
    logger.debug("Lecture EXT031 - totalWeight avant ajout = " + totalWeight)

    totalWeight += (double) EXT031.get("EXGWTM")
    totalAmount += (double) EXT031.get("EXNTAM")

    logger.debug("Lecture EXT031 - totalWeight après ajout = " + totalWeight)

  }

  // Retrieve departure information
  public void retrieveDepartureInformationZFPA(){
    forwardingAgent = ""
    carrierIDChargeCalc = ""
    DBAction queryDROUDI = database.table("DROUDI").index("00").selection("DSFWNO", "DSTSID").build()
    DBContainer DROUDI = queryDROUDI.getContainer()
    DROUDI.set("DSCONO", currentCompany)
    DROUDI.set("DSROUT", savedROUT)
    DROUDI.set("DSRODN", savedRODN)
    if (queryDROUDI.read(DROUDI)) {
      forwardingAgent = DROUDI.get("DSFWNO")
      carrierIDChargeCalc = DROUDI.get("DSTSID")
    }
    logger.debug("forwardingAgent = " + forwardingAgent)
    logger.debug("carrierIDChargeCalc = " + carrierIDChargeCalc)

  }

  // Calculation per combination ROUT, RODN
  public void calculationCombination(){
    logger.debug("New combination - Previous combination : " + savedROUT+"/"+savedRODN)
    retrieveDepartureInformation()
    if(departureIsExcluded){
      logger.debug("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! DEPARTURE : " +savedROUT+"/"+savedRODN+ " IS EXCLUDED, NO CHARGE CALCULATION !!!!!!!!!!!!!!!!!!!!!!!")
      return
    }
    retrievePriceByWeight()

    logger.debug("CALCULATION FOR COMBINATION : " + savedROUT+"/"+ " XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    logger.debug("totalWeight = " + totalWeight)
    logger.debug("priceByWeight = " + priceByWeight)

    if (calculationMethod.trim() == "02"){
      shippingChargeAmount += (totalWeight * priceByWeight)
      logger.debug("Add totalWeight x priceByWeight XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
      logger.debug("shippingChargeAmount = " + shippingChargeAmount)
      shippingChargeAmount += flatRateAmount
      logger.debug("Add flat rate XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
      logger.debug("flatRateAmount (A930) = " + flatRateAmount)
      sumZFPR += shippingChargeAmount
      logger.debug("sumZFPR = " + sumZFPR)
      updateEXT032()
      shippingChargeAmount = 0
    } else {
      shippingChargeAmount += (totalWeight * priceByWeight)
      logger.debug("Add totalWeight x priceByWeight XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
      logger.debug("shippingChargeAmount = " + shippingChargeAmount)
      shippingChargeAmount += flatRateAmount
      logger.debug("Add flat rate XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
      logger.debug("flatRateAmount (A930) = " + flatRateAmount)
    }

    logger.debug("TOTAL AMOUNT XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    logger.debug("shippingChargeAmount = " + shippingChargeAmount)

    totalWeight = 0
    totalAmount = 0
    savedROUT = ext030ROUT
    savedRODN = ext030RODN
  }

  // Calculation per combination ROUT, RODN
  public void calculationCombinationZFPA(){
    logger.debug("New combination - Previous combination : " + savedROUT+"/"+savedRODN)
    retrieveDepartureInformationZFPA()
    if(departureIsExcluded){
      logger.debug("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! DEPARTURE : " +savedROUT+"/"+savedRODN+ " IS EXCLUDED, NO CHARGE CALCULATION !!!!!!!!!!!!!!!!!!!!!!!")
      return
    }
    retrievePriceByWeight()

    logger.debug("CALCULATION FOR COMBINATION : " + savedROUT+"/"+ " XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    logger.debug("totalWeight = " + totalWeight)
    logger.debug("priceByWeight = " + priceByWeight)

    shippingChargeAmount += (totalWeight * priceByWeight)
    logger.debug("Add totalWeight x priceByWeight XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    logger.debug("shippingChargeAmount = " + shippingChargeAmount)
    shippingChargeAmount += flatRateAmount
    logger.debug("Add flat rate XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    logger.debug("flatRateAmount (A930) = " + flatRateAmount)

    logger.debug("TOTAL AMOUNT XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    logger.debug("shippingChargeAmount = " + shippingChargeAmount)

    totalWeight = 0
    totalAmount = 0
    savedROUT = ext030ROUT
    savedRODN = ext030RODN
  }

  // Retrieve price by weight
  public void retrievePriceByWeight(){
    priceByWeight = 0

    ExpressionFactory expression = database.getExpressionFactory("MPAGRL")
    expression = expression.eq("AIOBV1", carrierIDChargeCalc)
    expression = expression.and(expression.le("AIFVDT", ext030DLDT as String))
    expression = expression.and(expression.ge("AIUVDT", ext030DLDT as String))
    DBAction queryMPAGRL = database.table("MPAGRL").index("00").matching(expression).selection("AICONO","AISUNO","AIAGNB","AIGRPI","AIOBV1","AIOBV2","AIOBV3","AIOBV4","AIFVDT").build()
    DBContainer MPAGRL = queryMPAGRL.getContainer()
    MPAGRL.set("AICONO", currentCompany)
    MPAGRL.set("AISUNO", forwardingAgent)
    queryMPAGRL.readAll(MPAGRL, 2, 5, outDataMPAGRL)
  }

  // Retrieve MPAGRL
  Closure<?> outDataMPAGRL = { DBContainer MPAGRL ->
    logger.debug("MPAGRL found - Retrieve price by weight --------------------------------------------------------------------------------")
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
    queryMPAGRE.readAll(MPAGRE, 9, 20, outDataMPAGRE)

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
    queryMPAGRE2.readAll(MPAGRE2, 9, 20, outDataMPAGRE)
  }

  // Retrieve MPAGRE
  Closure<?> outDataMPAGRE = { DBContainer MPAGRE ->
    logger.debug("MPAGRE found")
    savedFROP = MPAGRE.get("F1FROP")
    logger.debug("savedFROP = " + savedFROP)
    DBAction queryMPAGRF = database.table("MPAGRF").index("10").selection("F2FRQT","F2FRRA","F2OBV1","F2RBV1","F2RBV2").reverse().build()
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
    if (!queryMPAGRF.readAll(MPAGRF, 12, 10, outDataMPAGRF)) {
      MPAGRF.set("F2RBV1", savedROUT)
      MPAGRF.set("F2RBV2", "")
      if (!queryMPAGRF.readAll(MPAGRF, 12, 10, outDataMPAGRF)) {
        MPAGRF.set("F2RBV1", "")
        MPAGRF.set("F2RBV2", "")
        queryMPAGRF.readAll(MPAGRF, 12, 10, outDataMPAGRF)
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

  // Update EXT031
  private void updateEXT031(){
    DBAction queryEXT031 = database.table("EXT031").index("00").build()
    DBContainer EXT031 = queryEXT031.getContainer()
    EXT031.set("EXCONO", currentCompany)
    EXT031.set("EXDIVI", currentDivision)
    EXT031.set("EXDLDT", ext031DLDT)
    EXT031.set("EXCUNO", ext031CUNO)
    EXT031.set("EXDLIX", ext031DLIX)
    EXT031.set("EXWHLO", ext031WHLO)
    EXT031.set("EXROUT", ext031ROUT)
    EXT031.set("EXRODN", ext031RODN)
    queryEXT031.readLock(EXT031, updateCallBackEXT031)
  }

  // Update EXT031
  Closure<?> updateCallBackEXT031 = { LockedResult lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("EXCHNO")
    if(CUTO==0){
      lockedResult.set("EXCUT1", 1)
    }
    if(CUTO==1){
      lockedResult.set("EXCUT2", 1)
    }
    lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("EXCHNO", changeNumber + 1)
    lockedResult.set("EXCHID", program.getUser())
    lockedResult.update()
  }

  // Update EXT031
  Closure<?> updateCallBackEXT0312 = { LockedResult lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("EXCHNO")
    double NTAM = lockedResult.get("EXNTAM")
    lockedResult.set("EXNTAM", NTAM + odheadNTAM)
    lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("EXCHNO", changeNumber + 1)
    lockedResult.set("EXCHID", program.getUser())
    lockedResult.update()
  }

  // Update EXT032
  private void updateEXT032(){
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction queryEXT032 = database.table("EXT032").index("00").build()
    DBContainer EXT032 = queryEXT032.getContainer()
    EXT032.set("EXCONO", currentCompany)
    EXT032.set("EXDIVI", currentDivision)
    EXT032.set("EXDLDT", ext030DLDT)
    EXT032.set("EXCUNO", ext030CUNO)
    EXT032.set("EXROUT", savedROUT)
    EXT032.set("EXRODN", savedRODN)
    if (!queryEXT032.readLock(EXT032, updateCallBackEXT032)) {
      EXT032.set("EXZFPV", shippingChargeAmount)
      EXT032.set("EXNTAM", totalAmount)
      EXT032.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT032.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT032.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT032.setInt("EXCHNO", 1)
      EXT032.set("EXCHID", program.getUser())
      queryEXT032.insert(EXT032)
    }

  }

  // Update EXT032
  Closure<?> updateCallBackEXT032 = { LockedResult lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("EXCHNO")
    lockedResult.set("EXZFPV", shippingChargeAmount)
    lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("EXCHNO", changeNumber + 1)
    lockedResult.set("EXCHID", program.getUser())
    lockedResult.update()
  }


  // Retrieve EXT030
  Closure<?> outDataEXT0303 = { DBContainer EXT030 ->
    int zgbl = EXT030.get("EXZGBL") as int
    if(zgbl == 1){
      return
    }

    ext030DLDT = EXT030.get("EXDLDT")
    ext030CUNO = EXT030.get("EXCUNO")
    IN60 = false
    txtHead = false

    logger.debug("---------- Creation entête frais ----------------------------------------------------------------------------------------------------")
    logger.debug("ext030CUNO = " + ext030CUNO)

    retrieveCustomerParameters()
    logger.debug("calculation Method = " + calculationMethod)

    double oZFAD = EXT030.get("EXZFAD")
    logger.debug("frais administratif = " + oZFAD)
    sumZFF1 = EXT030.get("EXZFF1")
    logger.debug("ZFF1 = " + sumZFF1)
    sumNTA1 = EXT030.get("EXNTA1")
    logger.debug("NTA1 = " + sumNTA1)
    sumZFF2 = EXT030.get("EXZFF2")
    logger.debug("ZFF2 = " + sumZFF2)
    sumNTA2 = EXT030.get("EXNTA2")
    logger.debug("NTA2 = " + sumNTA2)
    sumZFFO = EXT030.get("EXZFFO")
    logger.debug("ZFFO = " + sumZFFO)
    sumZFPR = EXT030.get("EXZFPR")
    logger.debug("ZFPR = " + sumZFPR)

    newORNO = ""
    newPONR = 0
    newPOSX = 0

    if(oZFAD!=0 || (calculationMethod.trim()=="01" && (sumZFF1!=0 || sumZFF2 !=0))  || (calculationMethod.trim()=="02" && sumZFPR != 0) ||
      (calculationMethod.trim()=="03" && sumZFFO != 0) || (calculationMethod.trim()=="04" && sumZFFO != 0)
      || (calculationMethod.trim()=="05" && sumZFPR != 0)  ){

      executeOIS100MIAddBatchHead(currentCompany as String, ext030CUNO, orderType, ext030DLDT as String)

      logger.debug("---------- Creation ligne frais Transport ----------------------------------------------------------------------------------------------------")
      switch (calculationMethod.trim()) {
        case "00":
          crtLine00()
          break
        case "01":
          crtLine01()
          break
        case "02":
          crtLine02()
          break
        case "03":
          crtLine03()
          break
        case "04":
          crtLine04()
          break
        case "05":
          crtLine05()
          break
        default:
          return
      }

      logger.debug("---------- Creation ligne frais Transport ----------------------------------------------------------------------------------------------------")
      if(oZFAD!=0){
        String oORQT = "1"
        executeOIS100MIAddBatchLine(currentCompany as String, newORNO, SKUFraisAdmin, oORQT, oZFAD as String, "0")
      }

      logger.debug("---------- Confirm ---------------------------------------------------------------------------------------------------------------------------")
      OIS100MIConfirm(newORNO)

    }

    DBAction updEXT030 = database.table("EXT030").index("00").build()
    DBContainer uEXT030 = updEXT030.getContainer()
    uEXT030.set("EXCONO", currentCompany)
    uEXT030.set("EXDIVI", currentDivision)
    uEXT030.set("EXDLDT", ext030DLDT)
    uEXT030.set("EXCUNO", ext030CUNO)
    updEXT030.readLock(uEXT030, updateCallBackEXT030Bl)

  }

  // Manage text
  private void manageText(){
    logger.debug("---------- Creation Text ----------------------------------------------------------------------------------------------------")
    savedROUT = ""
    savedRODN = 0
    ext030ROUT = ""
    ext030RODN = 0
    ext031WHLO = ""
    text240 = ""
    txtHead = false
    txtTotalWeight = 0
    DBAction queryEXT031 = database.table("EXT031").index("10").selection("EXROUT", "EXRODN", "EXDLIX", "EXGWTM", "EXWHLO").build()
    DBContainer EXT031 = queryEXT031.getContainer()
    EXT031.set("EXCONO", currentCompany)
    EXT031.set("EXDIVI", currentDivision)
    EXT031.set("EXDLDT", ext030DLDT)
    EXT031.set("EXCUNO", ext030CUNO)
    queryEXT031.readAll(EXT031, 4, 500, outDataEXT031TX)

    createLineText()
    createLineTextWeight()
  }

  // Manage text 1
  private void manageText1(){
    logger.debug("---------- Creation Text 1 ----------------------------------------------------------------------------------------------------")
    savedROUT = ""
    savedRODN = 0
    ext030ROUT = ""
    ext030RODN = 0
    ext031WHLO = ""
    text240 = ""
    txtHead = false
    txtTotalWeight = 0
    ExpressionFactory expressionTx1 = database.getExpressionFactory("EXT031")
    expressionTx1 = expressionTx1.eq("EXCUT1", "1")
    DBAction queryEXT031 = database.table("EXT031").index("10").matching(expressionTx1).selection("EXROUT", "EXRODN", "EXDLIX", "EXGWTM", "EXWHLO").build()
    DBContainer EXT031 = queryEXT031.getContainer()
    EXT031.set("EXCONO", currentCompany)
    EXT031.set("EXDIVI", currentDivision)
    EXT031.set("EXDLDT", ext030DLDT)
    EXT031.set("EXCUNO", ext030CUNO)
    queryEXT031.readAll(EXT031, 4, 500, outDataEXT031TX)

    createLineText()
    createLineTextWeight()
  }

  // Manage text 2
  private void manageText2(){
    logger.debug("---------- Creation Text 2 ----------------------------------------------------------------------------------------------------")
    savedROUT = ""
    savedRODN = 0
    ext030ROUT = ""
    ext030RODN = 0
    ext031WHLO = ""
    text240 = ""
    txtHead = false
    txtTotalWeight = 0
    ExpressionFactory expressionTx2 = database.getExpressionFactory("EXT031")
    expressionTx2 = expressionTx2.eq("EXCUT2", "1")
    DBAction queryEXT031 = database.table("EXT031").index("10").matching(expressionTx2).selection("EXROUT", "EXRODN", "EXDLIX", "EXGWTM", "EXWHLO").build()
    DBContainer EXT031 = queryEXT031.getContainer()
    EXT031.set("EXCONO", currentCompany)
    EXT031.set("EXDIVI", currentDivision)
    EXT031.set("EXDLDT", ext030DLDT)
    EXT031.set("EXCUNO", ext030CUNO)
    queryEXT031.readAll(EXT031, 4, 500, outDataEXT031TX)

    createLineText()
    createLineTextWeight()
  }



  // Manage text 4
  private void manageText4(String rout, String rodn){
    logger.debug("---------- Creation Text 1 ----------------------------------------------------------------------------------------------------")
    savedROUT = ""
    savedRODN = 0
    ext030ROUT = ""
    ext030RODN = 0
    ext031WHLO = ""
    text240 = ""
    txtHead = false
    txtTotalWeight = 0
    ExpressionFactory expressionTx1 = database.getExpressionFactory("EXT031")
    expressionTx1 = expressionTx1.eq("EXROUT", rout)
    expressionTx1 = expressionTx1.and(expressionTx1.eq("EXRODN", rodn))
    DBAction queryEXT031 = database.table("EXT031").index("10").matching(expressionTx1).selection("EXROUT", "EXRODN", "EXDLIX", "EXGWTM", "EXWHLO").build()
    DBContainer EXT031 = queryEXT031.getContainer()
    EXT031.set("EXCONO", currentCompany)
    EXT031.set("EXDIVI", currentDivision)
    EXT031.set("EXDLDT", ext030DLDT)
    EXT031.set("EXCUNO", ext030CUNO)
    queryEXT031.readAll(EXT031, 4, 100, outDataEXT031TX)

    createLineText()
    createLineTextWeight()
  }

  // Retrieve EXT030
  Closure<?> outDataOSBSTD = { DBContainer OSBSTD ->
    odheadNTAM += OSBSTD.get("UCSAAM") as double
  }

  // No charge
  private void crtLine00(){
    logger.debug("crtLine00")
  }

  // "Groupé"
  private void crtLine01(){
    logger.debug("crtLine01")
    String oDIP1
    String oORQT = "1"

    if(sumZFF1!=0){
      oDIP1 = "0"

      logger.debug("Method 01 - totalAmount 1 = " + sumNTA1)
      logger.debug("Method 02 - freeAmount = " + freeAmount)
      if(sumNTA1 > freeAmount) {
        oDIP1 = "100"
      }
      logger.debug("Method 01 - DIP1 = " + oDIP1)
      executeOIS100MIAddBatchLine(currentCompany as String, newORNO, SKUFraisTransport, oORQT, sumZFF1 as String, oDIP1)
      manageText1()
    }
    if(sumZFF2!=0){
      oDIP1 = "0"

      logger.debug("Method 01 - totalAmount 2 = " + sumNTA2)
      logger.debug("Method 01 - freeAmount = " + freeAmount)
      if(sumNTA2 > freeAmount) {
        oDIP1 = "100"
      }
      logger.debug("Method 01 - DIP1 = " + oDIP1)
      executeOIS100MIAddBatchLine(currentCompany as String, newORNO, SKUFraisTransport, oORQT, sumZFF2 as String, oDIP1)
      manageText2()
    }
  }

  // "Tournée"
  private void crtLine02(){
    logger.debug("crtLine02")
    DBAction queryEXT032 = database.table("EXT032").index("00").selection("EXROUT", "EXRODN", "EXZFPV", "EXNTAM").build()
    DBContainer EXT032 = queryEXT032.getContainer()
    EXT032.set("EXCONO", currentCompany)
    EXT032.set("EXDIVI", currentDivision)
    EXT032.set("EXDLDT", ext030DLDT)
    EXT032.set("EXCUNO", ext030CUNO)
    queryEXT032.readAll(EXT032, 4, 100, outDataEXT032)
  }

  // "Forfait hebdo"
  private void crtLine03(){
    logger.debug("crtLine03")
    String oORQT = "1"

    if(sumZFFO!=0){
      executeOIS100MIAddBatchLine(currentCompany as String, newORNO, SKUFraisTransport, oORQT, sumZFFO as String, "0")
      manageText()
    }
  }

  // "Forfait mensuel"
  private void crtLine04(){
    logger.debug("crtLine04")
    String oORQT = "1"

    if(sumZFFO!=0){
      executeOIS100MIAddBatchLine(currentCompany as String, newORNO, SKUFraisTransport, oORQT, sumZFFO as String, "0")
      manageText()
    }
  }

  // "Frais réel"
  private void crtLine05(){
    logger.debug("crtLine05")
    String oORQT = "1"

    if(sumZFPR!=0){
      executeOIS100MIAddBatchLine(currentCompany as String, newORNO, SKUFraisTransport, oORQT, sumZFPR as String, "0")
      manageText()
    }
  }


  // Retrieve EXT031
  Closure<?> outDataEXT032 = { DBContainer EXT032 ->
    String oROUT = EXT032.get("EXROUT")
    Integer oRODN = EXT032.get("EXRODN")
    double oZFPV = EXT032.get("EXZFPV")
    double oNTAM = EXT032.get("EXNTAM")

    logger.debug("ROUT = " + oROUT)
    logger.debug("RODN = " + oRODN)
    logger.debug("ZFPV = " + oZFPV)
    logger.debug("NTAM = " + oZFPV)

    String oORQT = "1"
    String oDIP1 = "0"

    logger.debug("Method 02 - totalAmount = " + oNTAM)
    logger.debug("Method 02 - freeAmount = " + freeAmount)
    if(oNTAM > freeAmount) {
      oDIP1 = "100"
    }
    logger.debug("Method 02 - DIP1 = " + oDIP1)

    if(oZFPV!=0){
      executeOIS100MIAddBatchLine(currentCompany as String, newORNO, SKUFraisTransport, oORQT, oZFPV as String, oDIP1)
      manageText4(oROUT, oRODN as String)
    }

  }


  // Retrieve EXT031
  Closure<?> outDataEXT031TX = { DBContainer EXT031 ->
    ext030ROUT = EXT031.get("EXROUT")
    ext030RODN = EXT031.get("EXRODN")
    ext031DLIX = EXT031.get("EXDLIX")
    double oGWTM = EXT031.get("EXGWTM") as double

    logger.debug("ROUT = " + ext030ROUT)
    logger.debug("ROUT = " + ext030RODN)
    logger.debug("DLIX = " + ext031DLIX)

    if(savedROUT.trim()==""){
      logger.debug("First record")
      savedROUT = EXT031.get("EXROUT")
      savedRODN = EXT031.get("EXRODN")
      ext031WHLO = EXT031.get("EXWHLO")
      listDLIX = ""
      retrieveDepartureInformation()
    }

    Integer length = 0
    if(listDLIX.trim()!=""){
      length = listDLIX.length()
    }

    if (savedROUT.trim() != ext030ROUT.trim() ||
      savedRODN != ext030RODN ||
      length >= 150) {
      createLineText()
      createLineTextWeight()
      txtTotalWeight = 0
      savedROUT = EXT031.get("EXROUT")
      savedRODN = EXT031.get("EXRODN")
      ext031WHLO = EXT031.get("EXWHLO")
      retrieveDepartureInformation()
    }

    txtTotalWeight = txtTotalWeight + oGWTM

    logger.debug("listDLIX before = " + listDLIX)
    if(listDLIX.trim()!=""){
      listDLIX = listDLIX.trim() + ", " + ext031DLIX
    } else {
      listDLIX = ext031DLIX
    }
    logger.debug("listDLIX after = " + listDLIX)

  }

  // Create line text
  private void createLineText(){
    String dateString = ext030DLDT
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    LocalDate date = LocalDate.parse(dateString, formatter)
    String formatedDate = date.format(DateTimeFormatter.ofPattern("dd.MM.yy"))
    if(!txtHead){
      txtHead = true
      text240 = "Facturation du port pour le(s) BL(s) du " + formatedDate + " : "
      executeOIS100MIAddBatchText(currentCompany as String, newORNO, "2", newPONR as String, newPOSX as String, "2", "CO01", text240)
    }

    text240 = "Pour le dépôt : " + mwwhnm

    executeOIS100MIAddBatchText(currentCompany as String, newORNO, "2", newPONR as String, newPOSX as String, "2", "CO01", text240)

    text240 = listDLIX

    executeOIS100MIAddBatchText(currentCompany as String, newORNO, "2", newPONR as String, newPOSX as String, "2", "CO01", text240)

    listDLIX = ""

    text240 = "Transporteur : " + sunoName

    executeOIS100MIAddBatchText(currentCompany as String, newORNO, "2", newPONR as String, newPOSX as String, "2", "CO01", text240)

  }

  // Create line text weight
  private void createLineTextWeight(){
    String otxtTotalWeight = txtTotalWeight
    text240 =  "Poids total : " + otxtTotalWeight + " kg"

    executeOIS100MIAddBatchText(currentCompany as String, newORNO, "2", newPONR as String, newPOSX as String, "2", "CO01", text240)
  }

  // Execute OIS100MI.AddBatchHead
  private executeOIS100MIAddBatchHead(String CONO, String CUNO, String ORTP, String DLDT){
    logger.debug("OIS100MI AddBatchHead ")
    logger.debug("CONO = "+ CONO+ " / CUNO = "+ CUNO+ " / ORTP = "+ ORTP+ " / DLDT = "+ DLDT)
    Map<String, String> params = ["CONO": CONO, "CUNO": CUNO, "ORTP": ORTP, "RLDT": DLDT,  "ORDT": DLDT, "CUDT": DLDT]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        IN60 = true
        logger.debug("Failed OIS100MI.AddBatchHead: "+ response.errorMessage)
        return
      }
      if (response.ORNO != null)
        newORNO = response.ORNO.trim()
    }
    miCaller.call("OIS100MI", "AddBatchHead", params, handler)
  }

  // Execute OIS100MI.AddBatchLine
  private executeOIS100MIAddBatchLine(String CONO, String ORNO, String ITNO, String ORQT, String SAPR, String DIP1){
    logger.debug("OIS100MI AddBatchLine ")
    logger.debug("CONO = "+ CONO+ " / ORNO = "+ ORNO+ " / ITNO = "+ ITNO+ " / ORQT = "+ ORQT+ " / SAPR = "+ SAPR+ " / DIP1 = "+ DIP1)
    Map<String, String> params = ["CONO": CONO, "ORNO": ORNO, "ITNO": ITNO, "ORQT": ORQT, "SAPR": SAPR, "DIP1": DIP1]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        IN60 = true
        logger.debug("Failed OIS100MI.AddBatchLine: "+ response.errorMessage)
        return
      }
      if (response.PONR != null)
        newPONR = response.PONR as Integer
      if (response.POSX != null)
        newPOSX = response.POSX as Integer
    }
    miCaller.call("OIS100MI", "AddBatchLine", params, handler)
  }

  // Execute OIS100MI.AddBatchText
  private executeOIS100MIAddBatchText(String CONO, String ORNO, String TYPE, String PONR, String POSX, String TYTR, String TXHE, String PARM ){
    logger.debug("OIS100MI AddBatchText ")
    logger.debug("CONO = "+ CONO+ " / ORNO = "+ ORNO+ " / TYPE = "+ TYPE+ " / PONR ="+ PONR+ " / POSX = "+ POSX+ " / TYTR = "+ TYTR+ " / TXHE = "+ TXHE+ " / PARM = "+ PARM)
    Map<String, String> params = ["CONO": CONO, "ORNO": ORNO, "TYPE": TYPE, "PONR": PONR, "POSX": POSX, "TYTR": TYTR, "TXHE": TXHE, "PARM": PARM]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        IN60 = true
        logger.debug("Failed OIS100MI.AddBatchText: "+ response.errorMessage)
        return
      }
    }
    miCaller.call("OIS100MI", "AddBatchText", params, handler)
  }

  // Confirm customer order
  private void OIS100MIConfirm(String ORNO){
    logger.debug("OIS100MI Confirm")
    logger.debug("ORNO = " + ORNO)
    Map<String, String> paramOIS100MIConfirm = ["ORNO": ORNO]
    Closure<?> rOIS100MIConfirm = {Map<String, String> response ->
      if(response.error != null){
        IN60 = true
        logger.debug("Failed OIS100MI.Confirm: "+ response.errorMessage)
        return
      }
    }
    miCaller.call("OIS100MI", "Confirm", paramOIS100MIConfirm, rOIS100MIConfirm)
  }

  // Update EXT030
  Closure<?> updateCallBackEXT030Bl = { LockedResult lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("EXCHNO")
    lockedResult.set("EXZGBL", 1)
    lockedResult.set("EXCRDT", currentCompany)
    lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("EXCHNO", changeNumber + 1)
    lockedResult.set("EXCHID", program.getUser())
    lockedResult.update()
  }

  // Get first parameter
  private String getFirstParameter(){
    logger.debug("rawData = " + rawData)
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    String parameter = rawData.substring(beginIndex, endIndex)
    logger.debug("parameter = " + parameter)
    return parameter
  }
  // Get next parameter
  private String getNextParameter(){
    beginIndex = endIndex + 1
    endIndex = rawDataLength - rawData.indexOf(";") - 1
    rawData = rawData.substring(beginIndex, rawDataLength)
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    String parameter = rawData.substring(beginIndex, endIndex)
    logger.debug("parameter = " + parameter)
    return parameter
  }
  /**
   * Delete records related to the current job from EXTJOB table
   */
  public void deleteEXTJOB(){
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXTJOB").index("00").build()
    DBContainer EXTJOB = query.getContainer()
    EXTJOB.set("EXRFID", batch.getReferenceId().get())
    query.readAllLock(EXTJOB, 1, updateCallBackEXTJOB)
  }
  // updateCallBackEXTJOB :: Delete EXTJOB
  Closure<?> updateCallBackEXTJOB = { LockedResult lockedResult ->
    lockedResult.delete()
  }
  // Check excluded order type
  private boolean excludedOrderType(){
    excludedOrderType = false
    checkCRS881("EXT030", "1", "ExcludedOrderType", "I", "Header", "ExcludedOrderType", "", "")
    logger.debug("excludedOrderType = " + excludedOrderType)
    return excludedOrderType
  }
  // Check CRS881
  public void checkCRS881(String MSTD, String MVRS, String BMSG, String IBOB, String ELMP, String ELMD, String ELMC, String MBMC){
    DBAction qMBMTRN = database.table("MBMTRN").index("00").selection("TRIDTR").build()
    DBContainer MBMTRN = qMBMTRN.getContainer()
    MBMTRN.set("TRTRQF", "0")
    MBMTRN.set("TRMSTD", MSTD)
    MBMTRN.set("TRMVRS", MVRS)
    MBMTRN.set("TRBMSG", BMSG)
    MBMTRN.set("TRIBOB", IBOB)
    MBMTRN.set("TRELMP", ELMP)
    MBMTRN.set("TRELMD", ELMD)
    MBMTRN.set("TRELMC", ELMC)
    MBMTRN.set("TRMBMC", MBMC)
    if (qMBMTRN.read(MBMTRN)) {
      ExpressionFactory expression = database.getExpressionFactory("MBMTRD")
      expression = expression.eq("TDMBMD", odheadORTP)
      DBAction qMBMTRD = database.table("MBMTRD").index("00").matching(expression).selection("TDMVXD").build()
      DBContainer MBMTRD = qMBMTRD.getContainer()
      MBMTRD.set("TDCONO", currentCompany)
      MBMTRD.set("TDDIVI", currentDivision)
      MBMTRD.set("TDIDTR", MBMTRN.get("TRIDTR"))
      qMBMTRD.readAll(MBMTRD, 3, 1, outDataMBMTRD2)
    }
  }
  // Execute EXT030MI.RtvSlsStatAmnt
  private executeEXT030MIRtvSlsStatAmnt(String DIVI, String ORNO, String DLIX){
    logger.debug("EXT030MI RtvSlsStatAmnt ")
    logger.debug("DIVI = "+ DIVI+ " / ORNO = "+ ORNO+ " / DLIX = "+ DLIX)
    Map<String, String> params = ["DIVI": DIVI, "ORNO": ORNO, "DLIX": DLIX]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        IN60 = true
        logger.debug("Failed EXT030MI.RtvSlsStatAmnt: "+ response.errorMessage)
        return
      }
      if (response.NTAM != null)
        odheadNTAM = response.NTAM.trim() as double
    }
    miCaller.call("EXT030MI", "RtvSlsStatAmnt", params, handler)
  }
}
