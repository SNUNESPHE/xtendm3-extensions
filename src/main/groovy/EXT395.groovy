/****************************************************************************************
 Extension Name: EXT395
 Type: ExtendM3Transaction
 Script Author: ARENARD
 Date: 2024-11-19
 Description:
 * Delete all EXT395 records for one company
 * 5158 – Création des retours clients

 Revision History:
 Name                    Date             Version          Description of Changes
 ARENARD                 2024-11-19       1.0              5158 Création des retours clients
 ARENARD                 2025-09-18       1.1              Standardization of the program header comment block / Extension has been fixed
 ******************************************************************************************/

import java.time.LocalDate
import java.time.DayOfWeek
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class EXT395 extends ExtendM3Batch {
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private final BatchAPI batch
  private final TextFilesAPI textFiles
  private Integer currentCompany
  private String rawData
  private int rawDataLength
  private int beginIndex
  private int endIndex
  private String jobNumber
  private String inCONO
  private Integer nbMaxRecord = 10000

  public EXT395(LoggerAPI logger, UtilityAPI utility,DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles) {
    this.logger = logger
    this.database = database
    this.program = program
    this.batch = batch
    this.miCaller = miCaller
    this.textFiles = textFiles
    this.utility = utility
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
      // No job data found
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
    inCONO = getFirstParameter()

    if (inCONO != null && inCONO != "") {
      currentCompany = inCONO as Integer
    } else {
      return
    }

    // Delete all EXT395 records for one company
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXT395").index("00").build()
    DBContainer EXT395 = query.getContainer()
    EXT395.set("EXCONO", currentCompany)

    Closure<?> deleteWorkFile = { DBContainer readResult ->
      query.readLock(readResult, { LockedResult lockedResult ->
        lockedResult.delete()
      })
    }

    if(!query.readAll(EXT395, 1, nbMaxRecord, deleteWorkFile)){
    }
  }

  // Get first parameter
  private String getFirstParameter(){
    logger.debug("rawData = " + rawData)
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    // Get parameter
    String parameter = rawData.substring(beginIndex, endIndex)
    logger.debug("parameter = " + parameter)
    return parameter
  }
}
