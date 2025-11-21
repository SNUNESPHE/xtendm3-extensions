/**
 * README
 * This extension is used by Mashup
 * Name : EXT490
 * Description :
 * Date         Changed By   Description
 * 20240819     YJANNIN      5116 - Automatic generation of credit note
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class EXT490 extends ExtendM3Batch {

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
  private boolean IN60
  private String jobNumber
  private Integer nbMaxRecord = 10000
  private Integer currentDate
  private Integer counter

  private String FACI
  private String savedORNO
  private double SAPR
  private double cSAPR
  private long REPN
  private Integer RELI
  private String CUNO
  private String ORTP
  private Integer REOC
  private Integer YEA4
  private String EXIN
  private String RXIN
  private String DIVI
  private String ITNO
  private String REST
  private String RSCD
  private String rscdOCLINE
  private String rscdMITTRA
  private String RIDN
  private boolean decote
  private boolean createdline
  private double TRQT
  private double REQT
  private double REQ3
  private double REQ4
  private double REQ5
  private double REQ7
  private boolean flagReq3
  private boolean flagReq5
  private double reclassCost
  private long attributenumber
  private String savedBMIN

  private boolean xFirst
  private boolean xDebug

  private String LOW
  private String HIGH
  private Integer CLOW
  private Integer CHIGH

  private String inWHLO
  private String inRES1
  private String inRES2
  private String inRES3
  private String inRES4
  private String inRES5

  public EXT490(LoggerAPI logger, UtilityAPI utility,DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles) {
    this.logger = logger
    this.database = database
    this.program = program
    this.batch = batch
    this.miCaller = miCaller
    this.textFiles = textFiles
    this.utility = utility
  }

  public void main() {
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
    inWHLO = getFirstParameter()
    String inCUNF = getNextParameter()
    String inCUNT = getNextParameter()
    inRES1 = getNextParameter()
    inRES2 = getNextParameter()
    inRES3 = getNextParameter()
    inRES4 = getNextParameter()
    inRES5 = getNextParameter()

    currentCompany = (Integer)program.getLDAZD().CONO
    logger.debug("Current Company = " + currentCompany)

    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer

    if(inWHLO == null || inWHLO == "" ){
      logger.debug("Code dépôt est obligatoire")
      return
    }
    if(inCUNF == null || inCUNF == "" ){
      logger.debug("Code client début est obligatoire")
      return
    }
    if(inCUNT == null || inCUNT == "" ){
      logger.debug("Code client fin est obligatoire")
      return
    }

    xDebug = false

    ORTP = " "
    DBAction queryOREPAR = database.table("OREPAR").index("00").selection("OEORTP").build()
    DBContainer OREPAR = queryOREPAR.getContainer()
    OREPAR.set("OECONO",currentCompany)
    OREPAR.set("OEWHLO",inWHLO)
    if (queryOREPAR.read(OREPAR)){
      ORTP = OREPAR.get("OEORTP")
    }
    logger.debug("ORTP = " + ORTP)
    logger.debug("WHLO = " + inWHLO)
    logger.debug("from CUNO = " + inCUNF)
    logger.debug("to CUNO = " + inCUNT)

    if(inCUNF==inCUNT){

      ExpressionFactory expressionOCHEAD1 = database.getExpressionFactory("OCHEAD")
      expressionOCHEAD1 = expressionOCHEAD1.le("OCCRSB", "1")
      DBAction queryOCHEAD10 = database.table("OCHEAD").index("10").matching(expressionOCHEAD1).selection("OCCUNO", "OCFACI", "OCREPN", "OCRESL", "OCCRSB", "OCREOC", "OCEXIN", "OCYEA4").build()
      DBContainer OCHEAD10 = queryOCHEAD10.getContainer()
      OCHEAD10.set("OCCONO", currentCompany)
      OCHEAD10.set("OCWHLO", inWHLO)
      OCHEAD10.set("OCCUNO", inCUNF)
      if(!queryOCHEAD10.readAll(OCHEAD10, 3, nbMaxRecord, outDataOCHEAD10)){
        logger.debug("L'enregistrement OCHEAD10 n'existe pas donc pas de client")
        return
      }
    }
    if(inCUNF!=inCUNT){
      counter = 0
      ExpressionFactory expressionOCHEAD0 = database.getExpressionFactory("OCHEAD")
      expressionOCHEAD0 = expressionOCHEAD0.ge("OCCUNO", inCUNF)
      expressionOCHEAD0 = expressionOCHEAD0.and(expressionOCHEAD0.le("OCCUNO", inCUNT))
      expressionOCHEAD0 = expressionOCHEAD0.and(expressionOCHEAD0.le("OCCRSB", "1"))
      expressionOCHEAD0 = expressionOCHEAD0.and(expressionOCHEAD0.ge("OCRESH", "13"))
      DBAction queryOCHEAD00 = database.table("OCHEAD").index("10").matching(expressionOCHEAD0).selection("OCCUNO", "OCFACI", "OCREPN", "OCRESL", "OCCRSB", "OCREOC", "OCEXIN", "OCYEA4").build()
      DBContainer OCHEAD00 = queryOCHEAD00.getContainer()
      OCHEAD00.set("OCCONO", currentCompany)
      OCHEAD00.set("OCWHLO", inWHLO)
      if(!queryOCHEAD00.readAll(OCHEAD00, 2, nbMaxRecord, outDataOCHEAD00)){
        logger.debug("L'enregistrement OCHEAD00 n'existe pas donc pas de client")
        return
      }
      logger.debug("Lecture OCHEAD00 " + counter + " +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    }

  }

  // outDataOCHEAD10 :: Retrieve EXT080
  Closure<?> outDataOCHEAD10 = { DBContainer OCHEAD10 ->
    logger.debug("Lecture OCHEAD10 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    savedORNO = " "
    DIVI = ""
    FACI = OCHEAD10.get("OCFACI")
    DBAction queryCFACIL = database.table("CFACIL").index("00").selection("CFDIVI").build()
    DBContainer CFACIL = queryCFACIL.createContainer()
    CFACIL.set("CFCONO",currentCompany)
    CFACIL.set("CFFACI", FACI)
    if (queryCFACIL.read(CFACIL)){
      DIVI = CFACIL.getString("CFDIVI")
    }
    CUNO = OCHEAD10.get("OCCUNO")
    REOC = OCHEAD10.get("OCREOC") as Integer
    REPN = OCHEAD10.get("OCREPN") as long
    YEA4 = OCHEAD10.get("OCYEA4") as Integer
    EXIN = OCHEAD10.get("OCEXIN")
    String oRESL = OCHEAD10.get("OCRESL")
    Integer oCRSB = OCHEAD10.get("OCCRSB") as Integer
    logger.debug("FACI = " + FACI)
    logger.debug("DIVI = " + DIVI)
    logger.debug("CUNO = " + CUNO)
    logger.debug("REOC = " + REOC)
    logger.debug("REPN = " + REPN)
    logger.debug("YEA4 = " + YEA4)
    logger.debug("EXIN = " + EXIN)
    logger.debug("RESL = " + oRESL)
    logger.debug("CRSB = " + oCRSB)
    if(oCRSB == 0 || oCRSB == 1){
      lectureOCLINE()
    }
    if(savedORNO != " "){
      OIUPDRST()
    }

  }

  // outDataOCHEAD00 :: Retrieve EXT080
  Closure<?> outDataOCHEAD00 = { DBContainer OCHEAD00 ->
    counter++

    String oREPN = OCHEAD00.get("OCREPN")
    if(oREPN.trim()=="700037309"){
      xDebug = true
    } else {
      xDebug = false
    }
    if(xDebug){
      logger.debug("Lecture OCHEAD00 " + counter + " +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    }
    CUNO = OCHEAD00.get("OCCUNO")
    FACI = OCHEAD00.get("OCFACI")
    REOC = OCHEAD00.get("OCREOC") as Integer
    REPN = OCHEAD00.get("OCREPN") as long
    YEA4 = OCHEAD00.get("OCYEA4") as Integer
    EXIN = OCHEAD00.get("OCEXIN")
    String oRESL = OCHEAD00.get("OCRESL")
    Integer oCRSB = OCHEAD00.get("OCCRSB") as Integer
    if(xDebug) {
      logger.debug("FACI = " + FACI)
      logger.debug("DIVI = " + DIVI)
      logger.debug("CUNO = " + CUNO)
      logger.debug("REOC = " + REOC)
      logger.debug("REPN = " + REPN)
      logger.debug("YEA4 = " + YEA4)
      logger.debug("EXIN = " + EXIN)
      logger.debug("RESL = " + oRESL)
      logger.debug("CRSB = " + oCRSB)
    }
    savedORNO = " "
    DIVI = ""
    DBAction queryCFACIL = database.table("CFACIL").index("00").selection("CFDIVI").build()
    DBContainer CFACIL = queryCFACIL.createContainer()
    CFACIL.set("CFCONO",currentCompany)
    CFACIL.set("CFFACI", FACI)
    if (queryCFACIL.read(CFACIL)){
      DIVI = CFACIL.getString("CFDIVI")
    }
    if(oCRSB == 0 || oCRSB == 1){
      lectureOCLINE()
    }
    if(savedORNO != " "){
      OIUPDRST()
    }
  }

  // Read OCLINE
  public void lectureOCLINE(){
    flagReq3 = false
    flagReq5 = false
    DBAction queryOCLINE = database.table("OCLINE").index("00").selection("ODREPN","ODRELI", "ODITNO", "ODREQ3", "ODREQ4", "ODREQ5", "ODREQ6", "ODREQ7", "ODSAPR", "ODREST", "ODRSCD").build()
    DBContainer OCLINE = queryOCLINE.getContainer()
    OCLINE.set("ODCONO",currentCompany)
    OCLINE.set("ODWHLO",inWHLO)
    OCLINE.set("ODREPN",REPN)
    if (!queryOCLINE.readAll(OCLINE, 3, nbMaxRecord, outDataOCLINE)){

    }
    if(savedORNO != " "){
      OIS100MIConfirm()
    }
    if(flagReq3){
      req3Edition()
    }
    if(flagReq5){
      req5Edition()
    }
  }

  // outDataOCHEAD10 :: Retrieve EXT080
  Closure<?> outDataOCLINE = { DBContainer OCLINE ->
    if(xDebug) {
      logger.debug("Lecture OCLINE +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    }
    REST = OCLINE.get("ODREST")
    if((inRES1==REST && inRES1!=" ") || (inRES2==REST && inRES2!=" ") || (inRES3==REST && inRES3!=" ") ||
      (inRES2==REST && inRES2!=" ") || (inRES3==REST && inRES3!=" ")){
      createdline = false
      RELI = OCLINE.get("ODRELI") as Integer
      rscdOCLINE = OCLINE.get("ODRSCD")
      ITNO = OCLINE.get("ODITNO")
      SAPR = OCLINE.get("ODSAPR") as double
      reclassCost = OCLINE.get("ODUCOS") as double
      REQ3 = OCLINE.get("ODREQ3") as double
      REQ4 = OCLINE.get("ODREQ4") as double
      REQ5 = OCLINE.get("ODREQ5") as double
      double oREQ6 = OCLINE.get("ODREQ6") as double
      REQ7 = OCLINE.get("ODREQ7") as double
      REQT = REQ3 + REQ4 - REQ7
      if(xDebug) {
        logger.debug("REST = " + REST)
        logger.debug("RELI = " + RELI)
        logger.debug("RSCD OCLINE= " + rscdOCLINE)
        logger.debug("ITNO = " + ITNO)
        logger.debug("SAPR = " + SAPR)
        logger.debug("UCOS= " + reclassCost)
        logger.debug("REQ3 = " + REQ3)
        logger.debug("REQ4 = " + REQ4)
        logger.debug("REQ5 = " + REQ5)
        logger.debug("REQ6 = " + oREQ6)
        logger.debug("REQ7 = " + REQ7)
        logger.debug("REQT = " + REQT)
      }
      convertREPNtoRIDN()
      if(xDebug) {
        logger.debug("RIDN = " + RIDN)
      }

      if(REQT>0){
        inzFldOCLINE()
      }
      TRQT = 0
      DBAction queryMITTRA = database.table("MITTRA").index("30").selection("MTTRQT", "MTTRDT", "MTATNB", "MTTRPR", "MTRSCD").build()
      DBContainer MITTRA = queryMITTRA.getContainer()
      MITTRA.set("MTCONO",currentCompany)
      MITTRA.set("MTTTYP",93)
      MITTRA.set("MTRIDN", RIDN)
      MITTRA.set("MTRIDL", RELI)
      if (!queryMITTRA.readAll(MITTRA, 4, nbMaxRecord,  outDataMITTRA)){

      }
      if(xDebug) {
        logger.debug("avant TRQT = " + TRQT)
        logger.debug("REQ7 = " + REQ7)
      }
      TRQT = TRQT - REQ7
      if(xDebug) {
        logger.debug("après TRQT = " + TRQT)
      }
      if(TRQT > 0 && REQT>0){
        DBAction queryCUGEX1 = database.table("CUGEX1").index("00").build()
        DBContainer CUGEX1 = queryCUGEX1.getContainer()
        CUGEX1.set("F1CONO", currentCompany)
        CUGEX1.set("F1FILE", "DECOTE")
        CUGEX1.set("F1PK01", DIVI)
        CUGEX1.set("F1PK02", "2")
        CUGEX1.set("F1PK03", rscdOCLINE)
        CUGEX1.set("F1PK04", rscdMITTRA)
        if (!queryCUGEX1.readAll(CUGEX1, 6, nbMaxRecord, outDataCUGEX1)) {
          decote = false
        }
        if(savedORNO==" "){
          OIS100MIAddBatchHead()
          if(xDebug) {
            logger.debug("ORNO = " + savedORNO)
          }
        }
        calculatedPrice()
        OIS100MIAddBatchLine()
      }
      if(createdline){
        updateOCLINE()
      }
    }
  }

  // convert REPN to RIDN
  private void convertREPNtoRIDN(){
    String oRIDN = REPN
    RIDN = oRIDN.padLeft(10, '0')
  }

  // Update statut
  private void OIUPDRST(){
    if(xDebug) {
      logger.debug("Update Statut")
    }
    xFirst = false
    DIVI = " "
    RXIN = " "
    DBAction queryOCLINE80 = database.table("OCLINE").index("80").selection("ODREST").build()
    DBContainer OCLINE80 = queryOCLINE80.getContainer()
    OCLINE80.set("ODCONO",currentCompany)
    OCLINE80.set("ODWHLO",inWHLO)
    OCLINE80.set("ODREPN",REPN)
    if (!queryOCLINE80.readAll(OCLINE80, 3, nbMaxRecord, outDataOCLINE80)){

    }

    if(xFirst==false){
      LOW = "05"
      HIGH = "05"
      CLOW = 0
      CHIGH = 0
    } else {
      xFirst = false
      DBAction queryOCLINE30 = database.table("OCLINE").index("30").selection("ODCRES").build()
      DBContainer OCLINE30 = queryOCLINE30.getContainer()
      OCLINE30.set("ODCONO",currentCompany)
      OCLINE30.set("ODWHLO",inWHLO)
      OCLINE30.set("ODREPN",REPN)
      if (!queryOCLINE30.readAll(OCLINE30, 3, nbMaxRecord, outDataOCLINE30)){

      }
    }

    if (EXIN != "" && YEA4 != 0){
      DBAction queryCFACIL = database.table("CFACIL").index("00").selection("CFDIVI").build()
      DBContainer CFACIL = queryCFACIL.createContainer()
      CFACIL.set("CFCONO",currentCompany)
      CFACIL.set("CFFACI", FACI)
      if (queryCFACIL.read(CFACIL)){
        DIVI = CFACIL.getString("CFDIVI")
      }

      DBAction queryOINVOH = database.table("OINVOH").index("91").selection("UHRXIN").build()
      DBContainer OINVOH = queryOINVOH.getContainer()
      OINVOH.set("UHCONO",currentCompany)
      OINVOH.set("UHDIVI",DIVI)
      OINVOH.set("UHYEA4",YEA4)
      OINVOH.set("UHEXIN",EXIN)
      queryOINVOH.readAll(OINVOH, 4, 1, {DBContainer recordOINVOH->
        RXIN = recordOINVOH.getString("UHRXIN")
      })
    }

    if(xDebug) {
      logger.debug("Param Update Statut")
      logger.debug("Low = " + LOW)
      logger.debug("High = " + HIGH)
      logger.debug("CLow = " + CLOW)
      logger.debug("CHigh = " + CHIGH)
      logger.debug("RXIN = " + RXIN)
    }

    DBAction updatOCHEAD00 = database.table("OCHEAD").index("00").selection( "OCCHNO").build()
    DBContainer updOCHEAD = updatOCHEAD00.getContainer()
    updOCHEAD.set("OCCONO", currentCompany)
    updOCHEAD.set("OCWHLO", inWHLO)
    updOCHEAD.set("OCREPN", REPN)
    updOCHEAD.set("OCCUNO", CUNO)
    if(!updatOCHEAD00.readLock(updOCHEAD, updateCallBackOCHEAD)){

    }

  }

  //updateCallBackEXT075 : update EXT075 before process 1
  Closure<?> updateCallBackOCHEAD = { LockedResult lockedResult ->
    if(xDebug) {
      logger.debug("Update OCHEAD")
    }
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("OCCHNO")
    lockedResult.set("OCRESL", LOW)
    lockedResult.set("OCRESH", HIGH)
    lockedResult.set("OCCRSB", CLOW)
    lockedResult.set("OCCRSH", CHIGH)
    if(RXIN.trim()!=""){
      lockedResult.set("OCCRSB", 2)
      lockedResult.set("OCCRSH", 2)
    }
    lockedResult.setInt("OCLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)          // A REACTIVER
    lockedResult.setInt("OCCHNO", changeNumber + 1)
    lockedResult.set("OCCHID", program.getUser())
    lockedResult.update()
  }

  // outDataOCLINE80 :: Retrieve OCLINE80
  Closure<?> outDataOCLINE80 = { DBContainer OCLINE80 ->
    if (xDebug) {
      logger.debug("Low Before = " + LOW)
      logger.debug("High Before = " + HIGH)
    }
    if (xFirst == false) {
      LOW = OCLINE80.get("ODREST")
      xFirst = true
    }
    HIGH = OCLINE80.get("ODREST")
    if (xDebug) {
      logger.debug("Low After = " + LOW)
      logger.debug("High After = " + HIGH)
    }
  }

  // outDataOCLINE30 :: Retrieve OCLINE30
  Closure<?> outDataOCLINE30 = { DBContainer OCLINE30 ->
    if(xDebug) {
      logger.debug("CLow Before = " + CLOW)
      logger.debug("CHigh Before = " + CHIGH)
    }
    if(xFirst == false ){
      CLOW = OCLINE30.get("ODCRES") as Integer
      xFirst = true
    }
    CHIGH = OCLINE30.get("ODCRES") as Integer
    if(xDebug) {
      logger.debug("CLow After = " + CLOW)
      logger.debug("CHigh After = " + CHIGH)
    }

  }

  // init field OCLINE
  private void inzFldOCLINE(){
    getUCOSandSAPRFromMITTRA()

  }

  // Get UCOS and SAPR from MITTRA
  private void getUCOSandSAPRFromMITTRA(){
    attributenumber = 0
    RSCD = ""
    if(xDebug) {
      logger.debug("Get UCOS and SAPR from MITTRA +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      logger.debug("REPN = " + REPN)
      logger.debug("RELI = " + RELI)
      logger.debug("ITNO = " + ITNO)
    }
    convertREPNtoRIDN()
    if(xDebug) {
      logger.debug("RIDN = " + RIDN)
    }
    DBAction queryMITTRA40 = database.table("MITTRA").index("40").selection("MTTTYP","MTTRQT",  "MTATNB", "MTTRPR", "MTRSCD").build()
    DBContainer MITTRA40 = queryMITTRA40.getContainer()
    MITTRA40.set("MTCONO",currentCompany)
    MITTRA40.set("MTRORC",3)
    MITTRA40.set("MTRORN", RIDN)
    MITTRA40.set("MTRORL", RELI)
    MITTRA40.set("MTRORX", 0)
    MITTRA40.set("MTWHLO", inWHLO)
    MITTRA40.set("MTITNO", ITNO)
    if (!queryMITTRA40.readAll(MITTRA40, 7, nbMaxRecord, outDataMITTRA40)){

    }
    if(attributenumber==0){
      DBAction queryMITTRA30 = database.table("MITTRA").index("30").selection("MTTRQT",  "MTATNB", "MTTRPR", "MTRSCD").build()
      DBContainer MITTRA30 = queryMITTRA30.getContainer()
      MITTRA30.set("MTCONO",currentCompany)
      MITTRA30.set("MTTTYP",93)
      MITTRA30.set("MTRIDN", RIDN)
      MITTRA30.set("MTRIDL", RELI)
      if (!queryMITTRA30.readAll(MITTRA30, 4, nbMaxRecord, outDataMITTRA30)){

      }
    }

  }

  // outDataMITTRA :: Retrieve MITTRA
  Closure<?> outDataMITTRA = { DBContainer MITTRA ->
    double oTRQT =  MITTRA.get("MTTRQT") as double
    TRQT =  TRQT + oTRQT
    Integer TRDT = MITTRA.get("MTTRDT") as Integer
    rscdMITTRA = MITTRA.get("MTRSCD")
    if(xDebug) {
      logger.debug("Lecture MITTRA +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      logger.debug("TRQT = " + TRQT)
      logger.debug("RSCD MITTRA= " + rscdMITTRA)
    }

    if(REQ3>0 && TRDT == currentDate){
      flagReq3 = true
    }
    if(REQ4>0 && TRDT == currentDate){
      flagReq3 = true
    }
    if(REQ5>0 && TRDT == currentDate){
      flagReq5 = true
    }
  }

  Closure<?> outDataCUGEX1 = { DBContainer CUGEX1 ->
    decote = true
  }

  // outDataMITTRA40 :: Retrieve MITTRA40
  Closure<?> outDataMITTRA40 = { DBContainer MITTRA40 ->
    Integer oTTYP = MITTRA40.get("MTTTYP") as Integer
    double oTRQT =  MITTRA40.get("MTTRQT")
    if(xDebug) {
      logger.debug("Lecture MITTRA40 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      logger.debug("TTYP = " + oTTYP)
      logger.debug("TRQT = " + oTRQT)
      logger.debug("attribute number before = " + attributenumber)
      logger.debug("reclass Cost before = " + reclassCost)
    }
    if(oTTYP == 96 && oTRQT > 0 ){
      attributenumber = MITTRA40.get("MTATNB") as long
      reclassCost = MITTRA40.get("MTTRPR")
    }
    if(xDebug) {
      logger.debug("attribute number after = " + attributenumber)
      logger.debug("reclass Cost after = " + reclassCost)
    }
  }

  // outDataMITTRA30 :: Retrieve MITTRA30
  Closure<?> outDataMITTRA30 = { DBContainer MITTRA30 ->
    double oTRQT =  MITTRA30.get("MTTRQT")
    if(xDebug) {
      logger.debug("Lecture MITTRA30 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      logger.debug("TRQT = " + oTRQT)
      logger.debug("attribute number before = " + attributenumber)
      logger.debug("reclass Cost before = " + reclassCost)
    }
    if(oTRQT > 0 ){
      attributenumber = MITTRA30.get("MTATNB") as long
      reclassCost = MITTRA30.get("MTTRPR")
    }
    if(xDebug) {
      logger.debug("attribute number after = " + attributenumber)
      logger.debug("reclass Cost after = " + reclassCost)
    }
  }

  private void req3Edition(){
    savedBMIN = ""
    String zret = ""

    DBAction queryEXT390 = database.table("EXT390").index("00").selection("EXZRET").build()
    DBContainer EXT390 = queryEXT390.getContainer()
    EXT390.set("EXCONO",currentCompany)
    EXT390.set("EXWHLO",inWHLO)
    EXT390.set("EXREPN",REPN)
    EXT390.set("EXCUNO",CUNO)
    if (queryEXT390.read(EXT390)){
      zret = EXT390.get("EXZRET")
    }

    MNS260MIAddMBMInit( "3", "OPT2", "DOC_RETOUR", "RETOUR", "MBM", "1", "DOC", "ZRET", zret, "REPN", REPN as String, "WHLO", inWHLO)

    if(savedBMIN!=""){
      MNS260MIAddMBMInitLRFld(savedBMIN, "IDM", "1", "AE", "Accounting_Entity")

      MNS260MIAddMBMInitLRFld(savedBMIN, "IDM", "1", "CONO", "Company")

      MNS260MIAddMBMInitLRFld(savedBMIN, "IDM", "1", "DOCTY1", "Accord_de_Retour")

      MNS260MIAddMBMInitLRFld(savedBMIN, "IDM", "1", "DOCTY2", "Notification_de_refus")

      MNS260MIAddMBMInitLRFld(savedBMIN, "IDM", "1", "DOCTY3", "Notification_Avoir")

      MNS260MIAddMBMInitLRFld(savedBMIN, "IDM", "1", "REPN", "Num_Retour_M3")

      MNS260MIPrcMBMInit()
    }
  }

  private void req5Edition(){
    savedBMIN = ""
    String zret = ""

    DBAction queryEXT390 = database.table("EXT390").index("00").selection("EXZRET").build()
    DBContainer EXT390 = queryEXT390.getContainer()
    EXT390.set("EXCONO",currentCompany)
    EXT390.set("EXWHLO",inWHLO)
    EXT390.set("EXREPN",REPN)
    EXT390.set("EXCUNO",CUNO)
    if (queryEXT390.read(EXT390)){
      zret = EXT390.get("EXZRET")
    }

    MNS260MIAddMBMInit( "2", "OPT2", "DOC_RETOUR", "RETOUR", "MBM", "1", "DOC", "ZRET", zret, "REPN", REPN as String, "WHLO", inWHLO)

    if(savedBMIN!=""){
      MNS260MIAddMBMInitLRFld(savedBMIN, "IDM", "1", "AE", "Accounting_Entity")

      MNS260MIAddMBMInitLRFld(savedBMIN, "IDM", "1", "CONO", "Company")

      MNS260MIAddMBMInitLRFld(savedBMIN, "IDM", "1", "DOCTY1", "Accord_de_Retour")

      MNS260MIAddMBMInitLRFld(savedBMIN, "IDM", "1", "DOCTY2", "Notification_de_refus")

      MNS260MIAddMBMInitLRFld(savedBMIN, "IDM", "1", "DOCTY3", "Notification_Avoir")

      MNS260MIAddMBMInitLRFld(savedBMIN, "IDM", "1", "REPN", "Num_Retour_M3")

      MNS260MIPrcMBMInit()
    }
  }

  // Add Iniator
  private void MNS260MIAddMBMInit(String zdono, String objc, String prtf, String prf1, String donr, String arsd, String prf2, String mkf4, String mkv4, String mkf5, String mkv5, String mkf6, String mkv6){
    if(xDebug) {
      logger.debug("MNS260MI AddMBMInit()")
      logger.debug("DONO = " + zdono + " / OBJC = " + objc + " / PRTF = " + prtf + " / PRF1 = " + prf1 + " / DONR = " + donr + " / ARSD = " + arsd + " / PRF2 = " + prf2 + " / MKF4 = " + mkf4 + " / MKV4 = " + mkv4 + " / MKF5 = " + mkf5 + " / MKV5 = " + mkv5 + " / MKF6 = " + mkf6 + " / MKV6 = " + mkv6)
    }
    Map<String, String> paramMNS260MIAddMBMInit = ["DONO": zdono, "OBJC": objc, "PRTF": prtf, "PRF1": prf1, "DONR": donr, "ARSD": arsd, "PRF2": prf2, "MKF4": mkf4, "MKV4": mkv4, "MKF5": mkf5, "MKV5": mkv5, "MKF6": mkf6, "MKV6": mkv6]
    Closure<?> rMNS260MIAddMBMInit = {Map<String, String> response ->
      if(response.error != null){
        IN60 = true
        logger.debug("Failed MNS260MI.AddMBMInit: "+ response.errorMessage)
        return
      }
      if (response.BMIN != null)
        savedBMIN = response.BMIN.trim()
    }
    miCaller.call("MNS260MI", "AddMBMInit", paramMNS260MIAddMBMInit, rMNS260MIAddMBMInit)
  }

  // Add a new field to a list record
  private void MNS260MIAddMBMInitLRFld(String obmin, String file, String ornbr, String oflda, String ofvlu){
    if(xDebug) {
      logger.debug("MNS260MI AddMBMInitLRFld()")
      logger.debug("BMIN = " + obmin + " / FILE = " + file + " / RNBR = " + ornbr + " / FLDA = " + oflda + " / FVLU = " + ofvlu)
    }
    Map<String, String> paramMNS260MIAddMBMInitLRFld = ["BMIN": obmin, "FILE": file, "RNBR": ornbr, "FLDA": oflda, "FVLU": ofvlu]
    Closure<?> rMNS260MIAddMBMInitLRFld = {Map<String, String> response ->
      if(response.error != null){
        IN60 = true
        logger.debug("Failed MNS260MI.AddMBMInitLRFld: "+ response.errorMessage)
        return
      }
    }
    miCaller.call("MNS260MI", "AddMBMInitLRFld", paramMNS260MIAddMBMInitLRFld, rMNS260MIAddMBMInitLRFld)
  }

  // Send Initiator
  private void MNS260MIPrcMBMInit(){
    if(xDebug) {
      logger.debug("MNS260MI PrcMBMInit()")
      logger.debug("BMIN = " + savedBMIN)
    }
    Map<String, String> paramMNS260MIPrcMBMInit = ["BMIN": savedBMIN]
    Closure<?> rMNS260MIPrcMBMInit = {Map<String, String> response ->
      if(response.error != null){
        IN60 = true
        logger.debug("Failed MNS260MI.PrcMBMInit: "+ response.errorMessage)
        return
      }
    }
    miCaller.call("MNS260MI", "PrcMBMInit", paramMNS260MIPrcMBMInit, rMNS260MIPrcMBMInit)
  }



  private void calculatedPrice(){
    if(xDebug) {
      logger.debug("Lecture EXT391 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    }
    double odiscount
    double oZDS1 = 0
    double oZDS2 = 0
    DBAction queryEXT391 = database.table("EXT391").index("00").selection("EXZDC1","EXZDC2").build()
    DBContainer EXT391 = queryEXT391.getContainer()
    EXT391.set("EXCONO",currentCompany)
    EXT391.set("EXWHLO",inWHLO)
    EXT391.set("EXREPN",REPN)
    EXT391.set("EXRELI",RELI)
    if (queryEXT391.read(EXT391)){
      oZDS1 = EXT391.get("EXZDC1")
      oZDS2 = EXT391.get("EXZDC2")
    }
    if(xDebug) {
      logger.debug("decote = " + decote)
      logger.debug("ZDC1 = " + oZDS1)
      logger.debug("ZDC2 = " + oZDS2)
    }
    if(decote){
      odiscount = (1 - (oZDS1/100)) * (1 - (oZDS2/100))
    } else {
      odiscount = (1 - (oZDS1/100))
    }
    cSAPR = SAPR * odiscount
    if(xDebug) {
      logger.debug("Taux decote = " + odiscount)
      logger.debug("SAPR = " + SAPR)
      logger.debug("SAPR calculé = " + cSAPR)
    }
  }

  // Update OCLINE
  private void updateOCLINE(){
    if(xDebug) {
      logger.debug("Update OCLINE +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    }
    DBAction updOCLINE = database.table("OCLINE").index("00").selection("ODREST", "ODCRES", "ODREQ0", "ODREQ1", "ODREQ2", "ODREQ5", "ODREQ7")build()
    DBContainer OCLINE00 = updOCLINE.getContainer()
    OCLINE00.set("ODCONO", currentCompany)
    OCLINE00.set("ODWHLO", inWHLO)
    OCLINE00.set("ODREPN", REPN)
    OCLINE00.set("ODRELI", RELI)
    if(!updOCLINE.readLock(OCLINE00, updateCallBackOCLINE)){}
  }

  // Update EXT081
  Closure<?> updateCallBackOCLINE = { LockedResult lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    String oREST = lockedResult.get("ODREST")
    String WKREST = " "
    boolean WFCRES = false
    int WKCRES = 0
    double changeREQ7 = lockedResult.get("ODREQ7") as double
    double oREQ5 = lockedResult.get("ODREQ5") as double
    double oREQ2 = lockedResult.get("ODREQ2") as double
    double oREQ1 = lockedResult.get("ODREQ1") as double
    double oREQ0 = lockedResult.get("ODREQ0") as double
    double XREQ1
    XREQ1 = oREQ5 + changeREQ7 + REQT
    double XREQ7
    XREQ7 = changeREQ7 + REQT
    if(xDebug) {
      logger.debug("oREQ0 = " + oREQ0)
      logger.debug("oREQ1 = " + oREQ1)
      logger.debug("oREQ2 = " + oREQ2)
      logger.debug("oREQ5 = " + oREQ5)
      logger.debug("oREQ7 = " + changeREQ7)
      logger.debug("XREQ1 = " + XREQ1)
      logger.debug("XREQ7 = " + XREQ7)
    }
    int changeNumber = lockedResult.get("ODCHNO")
    lockedResult.set("ODREQ7", XREQ7)
    if(WKREST==" "){
      if(oREST=="99"){
        WKREST = "99"
      }
    }
    if(WKREST==" "){
      if(oREST=="90"){
        WKREST = "90"
      }
    }
    if(WKREST==" "){
      if(oREQ0 == 0 && oREQ1 == 0){
        WKREST = "05"
      }
    }
    if(WKREST==" "){
      if(oREQ0 == 0 && oREQ1 > 0 && oREQ1==oREQ2){
        WKREST = "22"
      }
    }
    if(WKREST==" "){
      if(oREQ0 == 0 && oREQ1 > 0 && oREQ2==0){
        WKREST = "33"
      }
    }
    if(WKREST==" "){
      if(oREQ0 == 0 && oREQ1 >= 0 && oREQ1>oREQ2){
        WKREST = "23"
      }
    }
    if(WKREST==" "){
      if(oREQ0 > 0 && oREQ1 == 0){
        WKREST = "11"
      }
    }
    if(WKREST==" "){
      if(oREQ0 > 0 && oREQ1 >= oREQ0  && oREQ1==oREQ2){
        WKREST = "22"
      }
    }
    if(WKREST==" "){
      if(oREQ0 > 0 && oREQ1 >= oREQ0  && oREQ1>oREQ2 && oREQ2!=0){
        WKREST = "23"
      }
    }
    if(WKREST==" "){
      if(oREQ0 > 0 && oREQ1 >= oREQ0  && oREQ2==0){
        WKREST = "33"
      }
    }
    if(WKREST==" "){
      if(oREQ0 > 0 && oREQ1==oREQ2){
        WKREST = "12"
      }
    }
    if(WKREST==" "){
      if(oREQ0 > 0 && oREQ1>oREQ2){
        WKREST = "13"
      }
    }
    if(WKREST==" "){
      WKREST = "99"
    }
    if(WFCRES == false){
      if(XREQ7==0){
        WKCRES = 0
        WFCRES = true
      }
    }
    if(WFCRES == false){
      if(XREQ7 > 0 && XREQ7 < oREQ1) {
        WKCRES = 1
        WFCRES = true
      }
    }
    if(WFCRES == false){
      if((XREQ7 > 0 && XREQ7 == oREQ1) || XREQ1 != oREQ1) {
        WKCRES = 2
        WFCRES = true
      }
    }
    if(WFCRES == false){
      WKCRES = 0
    }
    if(xDebug) {
      logger.debug("WKREST = " + WKREST)
      logger.debug("WKCRES = " + WKCRES)
    }
    lockedResult.set("ODREST", WKREST)
    lockedResult.set("ODCRES", WKCRES)
    lockedResult.setInt("ODLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("ODCHNO", changeNumber + 1)
    lockedResult.set("ODCHID", program.getUser())
    lockedResult.update()
  }

  // Add Batch Head
  private void OIS100MIAddBatchHead(){
    if(xDebug) {
      logger.debug("OIS100MI AddBatchHead")
      logger.debug("CUNO = " + CUNO + " / ORTP = " + ORTP + " / FACI = " + FACI + " / CUOR = " + REPN)
    }
    Map<String, String> paramOIS100MIAddBatchHead = ["CUNO": CUNO, "ORTP": ORTP, "FACI": FACI, "CUOR": REPN as String]
    Closure<?> rOIS100MIAddBatchHead = {Map<String, String> response ->
      if(response.error != null){
        IN60 = true
        logger.debug("Failed OIS100MI.AddBatchHead: "+ response.errorMessage)
        return
      }
      if (response.ORNO != null)
        savedORNO = response.ORNO.trim()
    }
    miCaller.call("OIS100MI", "AddBatchHead", paramOIS100MIAddBatchHead, rOIS100MIAddBatchHead)
  }

  // Add Batch Line
  private void OIS100MIAddBatchLine(){
    double wORQT = 0 - REQT
    if(xDebug) {
      logger.debug("OIS100MI AddBatchLine")
      logger.debug("REQT = " + REQT)
      logger.debug("wREQT = " + wORQT)
      logger.debug("ORNO = " + savedORNO + " / ITNO = " + ITNO + " / WHLO = " + inWHLO + " / SAPR = " + cSAPR + " / CUOR = " + REPN + " / ORQT = " + wORQT + " / RSCD = " + rscdMITTRA)
    }
    Map<String, String> paramOIS100MIAddBatchLine = ["ORNO": savedORNO, "ITNO": ITNO, "WHLO": inWHLO, "SAPR": cSAPR as String, "CUOR": REPN as String, "ORQT": wORQT as String, "RSCD": rscdMITTRA]
    Closure<?> rOIS100MIAddBatchLine = {Map<String, String> response ->
      if(response.error != null){
        IN60 = true
        logger.debug("Failed OIS100MI.AddBatchLine: "+ response.errorMessage)
        return
      }
      createdline = true
    }
    miCaller.call("OIS100MI", "AddBatchLine", paramOIS100MIAddBatchLine, rOIS100MIAddBatchLine)
  }


  // Confirm customer order
  private void OIS100MIConfirm(){
    if(xDebug) {
      logger.debug("OIS100MI Confirm")
      logger.debug("ORNO = " + savedORNO)
    }
    Map<String, String> paramOIS100MIConfirm = ["ORNO": savedORNO]
    Closure<?> rOIS100MIConfirm = {Map<String, String> response ->
      if(response.error != null){
        IN60 = true
        logger.debug("Failed OIS100MI.Confirm: "+ response.errorMessage)
        return
      }
    }
    miCaller.call("OIS100MI", "Confirm", paramOIS100MIConfirm, rOIS100MIConfirm)
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
