/****************************************************************************************
 Extension Name: EXT392MI.DeprecRequest
 Type: ExtendM3Transaction
 Script Author: ARENARD
 Date: 2024-10-18
 Description:
 * Depreciation request
 * 5289 – Interrogation décote

 Revision History:
 Name                    Date             Version          Description of Changes
 ARENARD                 2024-10-18       1.0              5289 Interrogation décote
 ARENARD                 2025-09-18       1.1              Standardization of the program header comment block / Extension has been fixed
 ******************************************************************************************/

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

public class DeprecRequest extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer nbMaxRecord = 10000
  private int currentCompany
  private String currentDivision
  private String inCUNO
  private String inORNO
  private Long inDLIX
  private String inITNO
  private Integer inZOCO
  private long daysDifference
  private double decote
  private Integer transactionDate
  private LocalDate currentDate
  private boolean IN60
  private boolean customerOrderNotFound

  public DeprecRequest(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  public void main() {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    currentDate = LocalDate.now()

    currentCompany = (Integer) program.getLDAZD().CONO
    currentDivision = program.getLDAZD().DIVI

    // Check customer number
    inCUNO = ""
    if(mi.in.get("CUNO") != null){
      DBAction queryOCUSMA = database.table("OCUSMA").index("00").build()
      DBContainer OCUSMA = queryOCUSMA.getContainer()
      OCUSMA.set("OKCONO", currentCompany)
      OCUSMA.set("OKCUNO", mi.in.get("CUNO"))
      if (!queryOCUSMA.read(OCUSMA)) {
        mi.error("Code client " + mi.in.get("CUNO") + " n'existe pas")
        return
      }
      inCUNO = mi.in.get("CUNO")
    }

    // NB : Customer orders and delivery indexes are not checked because some do not exist in M3.
    // Some exist only in the EXT395 table containing customer orders and delivery indexes from the old M3 version
    // Customer number
    inORNO = ""
    if(mi.in.get("ORNO") != null) {
      inORNO = mi.in.get("ORNO")
    } else {
      mi.error("Commande est oblligatoire")
      return
    }

    // Delivery index
    inDLIX = 0
    if(mi.in.get("DLIX") != null) {
      inDLIX = mi.in.get("DLIX")
    } else {
      mi.error("Index de livraison est oblligatoire")
      return
    }

    // Check item number
    inITNO = ""
    if(mi.in.get("ITNO") != null){
      DBAction queryMITMAS = database.table("MITMAS").index("00").build()
      DBContainer MITMAS = queryMITMAS.getContainer()
      MITMAS.set("MMCONO", currentCompany)
      MITMAS.set("MMITNO",  mi.in.get("ITNO"))
      if (!queryMITMAS.read(MITMAS)) {
        mi.error("Code article " + mi.in.get("ITNO") + " n'existe pas")
        return
      }
      inITNO = mi.in.get("ITNO")
    }

    // Old company
    if (mi.in.get("ZOCO") != null) {
      inZOCO = mi.in.get("ZOCO")
    } else {
      inZOCO = currentCompany
    }

    // Retrieve transaction date
    retrieveTransactionDate()
    if(customerOrderNotFound){
      mi.error("Commande " + mi.in.get("ORNO") + " n'existe pas")
      return
    }

    // Depreciation calculation
    depreciationCalculation()

    mi.outData.put("ZDC1", decote as String)
    mi.write()
  }

  // Retrieve transaction date
  private void retrieveTransactionDate(){
    transactionDate = 0
    logger.debug("Retrieve transaction date --------------------------------------------------------------------------------------------------------------------")
    DBAction queryOOHEAD = database.table("OOHEAD").index("00").build()
    DBContainer OOHEAD = queryOOHEAD.getContainer()
    OOHEAD.set("OACONO", currentCompany)
    OOHEAD.set("OAORNO", inORNO)
    if(queryOOHEAD.read(OOHEAD)){
      logger.debug("OOHEAD found")
      DBAction queryMHDISH = database.table("MHDISH").index("00").selection("OQTRDT").build()
      DBContainer MHDISH = queryMHDISH.getContainer()
      MHDISH.set("OQCONO", currentCompany)
      MHDISH.set("OQINOU",  1)
      MHDISH.set("OQDLIX",  inDLIX)
      if(queryMHDISH.read(MHDISH)) {
        transactionDate = MHDISH.get("OQTRDT")
        logger.debug("MHDISH found - transactionDate = " + transactionDate)
      }
    }
    logger.debug("Transaction date after OOHEAD - transactionDate = " + transactionDate)
    if(transactionDate == 0){
      logger.debug("Search EXT395")
      DBAction queryEXT395 = database.table("EXT395").index("30").selection("EXORNO","EXPONR","EXDLDT").build()
      DBContainer EXT395 = queryEXT395.getContainer()
      EXT395.set("EXCONO", inZOCO)    //Old company
      EXT395.set("EXORNO", inORNO)
      EXT395.set("EXDLIX", inDLIX)
      EXT395.set("EXITNO", inITNO)
      if(inCUNO == ""){
        if(!queryEXT395.readAll(EXT395, 4, 1, outDataEXT395)){
          logger.debug("EXT395 not found without CUNO")
          customerOrderNotFound = true
        }
      } else {
        EXT395.set("EXCUNO", inCUNO)
        if(!queryEXT395.readAll(EXT395, 5, 1, outDataEXT395)){
          logger.debug("EXT395 not found with CUNO")
          customerOrderNotFound = true
        }
      }
    }
  }

  // Depreciation calculation
  private void depreciationCalculation(){
    logger.debug("Depreciation calculation --------------------------------------------------------------------------------------------------------------------")
    decote = 0
    if(transactionDate>0){
      logger.debug("transactionDate = " + transactionDate)
      logger.debug("currentDate = " + currentDate)

      // Calculating the number of days between transactionDate and currentDate
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
      LocalDate oTRDT = LocalDate.parse(transactionDate as String, formatter)
      daysDifference = ChronoUnit.DAYS.between(oTRDT, currentDate)
      logger.debug("daysDifference = " + daysDifference)

      DBAction qCUGEX1 = database.table("CUGEX1").index("00").selection("F1PK03","F1PK04","F1N096").build()
      DBContainer CUGEX1 = qCUGEX1.getContainer()
      CUGEX1.set("F1CONO", currentCompany)
      CUGEX1.set("F1FILE", "DECOTE")
      CUGEX1.set("F1PK01", currentDivision)
      CUGEX1.set("F1PK02", "1")
      if (!qCUGEX1.readAll(CUGEX1, 4, nbMaxRecord, outDataCUGEX1)){
      }
      logger.debug("decote = " + decote + "------------------------------------------------------------------------------------------------------------------")
    }
  }

  // Retrieve EXT395
  Closure<?> outDataEXT395 = { DBContainer EXT395 ->
    String ORNO = EXT395.get("EXORNO")
    String PONR = EXT395.get("EXPONR")

    transactionDate = EXT395.get("EXDLDT")

    logger.debug("EXT395 found - ORNO = " + ORNO)
    logger.debug("EXT395 found - PONR = " + PONR)
    logger.debug("EXT395 found - transactionDate = " + transactionDate)
  }

  // Retrieve CUGEX1
  Closure<?> outDataCUGEX1 = { DBContainer CUGEX1 ->
    long oPK03 = CUGEX1.get("F1PK03") as long
    long oPK04 = CUGEX1.get("F1PK04") as long
    if(oPK03 <= daysDifference && oPK04 >= daysDifference){
      decote = CUGEX1.get("F1N096")
      return false
    }
  }
}
