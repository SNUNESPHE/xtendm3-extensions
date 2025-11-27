/***************************************************************************************************************************************************************
 Extension Name: EXT035
 Type: ExtendM3Batch
 Script Author: ARENARD
 Date: 20250929
 Description:   5209 - Franco de port
 * Description of script functionality
 Allows to submit batch EXT030 by customer

 Revision History:
 Name                    Date             Version          Description of Changes
 ARENARD                 2025-09-29       1.0              Batch creation
 *****************************************************************************************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import java.time.temporal.WeekFields


public class EXT035 extends ExtendM3Batch {
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
  private String inDLDT
  private String dldt
  private String cuno

  private String currentDivision
  private Integer currentDate

  java.util.Set<String> uniqueCunos = new java.util.LinkedHashSet<String>()

  public EXT035(LoggerAPI logger, UtilityAPI utility,DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles) {
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
    inDLDT = getFirstParameter()

    logger.debug("value inDLDT = " + inDLDT)

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

    uniqueCunos.clear()
    logger.debug("LECTURE ODHEAD, SELECTION DES INDEX XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    ExpressionFactory expression = database.getExpressionFactory("ODHEAD")
    expression = expression.eq("UADLDT", inDLDT)
    expression = expression.and(expression.ge("UAORST", "60"))
    DBAction queryODHEAD = database.table("ODHEAD").index("40").matching(expression).selection("UACUNO","UADLIX").build()
    DBContainer ODHEAD = queryODHEAD.getContainer()
    ODHEAD.set("UACONO", currentCompany)
    ODHEAD.set("UADIVI", currentDivision)
    queryODHEAD.readAll(ODHEAD, 2, 1000, outDataODHEAD)

    // Submit EXT030 for each customer
    for (String cunoValue : uniqueCunos) {
      dldt = inDLDT
      cuno = cunoValue
      logger.debug("Lancement du batch EXT030 pour le client " + cuno + " et la date de livraison " + dldt)
      executeEXT820MISubmitBatch(currentCompany as String, "EXT030", dldt.trim(), cuno.trim(), "", "", "", "", "", "", "")
    }
  }
  // Retrieve ODHEAD and save customers
  Closure<?> outDataODHEAD = { DBContainer ODHEAD ->
    logger.debug("UADLIX = " + ODHEAD.get("UADLIX"))

    String rawCuno = ODHEAD.get("UACUNO")
    if (rawCuno != null) {
      uniqueCunos.add(rawCuno.trim())
    }
  }

  // Execute EXT820MI.SubmitBatch
  private executeEXT820MISubmitBatch(String CONO, String JOID, String P001, String P002, String P003, String P004, String P005, String P006, String P007, String P008, String P009) {
    Map<String, String> parameters = ["CONO": CONO, "JOID": JOID, "P001": P001, "P002": P002, "P003": P003, "P004": P004, "P005": P005, "P006": P006, "P007": P007, "P008": P008, "P009": P009]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
      } else {
      }
    }
    miCaller.call("EXT820MI", "SubmitBatch", parameters, handler)
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
}
