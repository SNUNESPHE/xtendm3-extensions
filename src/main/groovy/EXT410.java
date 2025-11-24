/****************************************************************************************
 Extension Name: EXT410
 Type: ExtendM3Transaction
 Script Author: YJANNIN
 Date: 2024-07-30
 Description:
 * Gestion de la planification des vagues de prélèvements (picking) utilisée par Mashup.

 Revision History:
 Name          Date        Version   Description of Changes
 YJANNIN       2024-07-30  1.0       Ticket 5148 – Mise en place de la gestion de la planification des vagues de prélèvements (création initiale).
 ARENARD       2025-08-26  1.1       Correctifs sur l’extension (stabilité/bugs).
 ARENARD       2025-10-06  1.2       ReadAll limits have been significantly reduced to optimize performance
 ARENARD       2025-10-14  1.3       Generic fields have been removed
 ARENARD       2025-11-06  1.4       executeMWS410MIConnectShipment added
 ******************************************************************************************/

import java.time.LocalDate
import java.time.DayOfWeek
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.time.ZonedDateTime
import java.time.ZoneId

public class EXT410 extends ExtendM3Batch {

    private final MIAPI mi
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

    private String inCFIN
    private String inWHLO
    private String inDLIX

    private String weekDay
    private String timeDay

    private boolean foundCUGEX1
    private boolean split
    private String cutOff
    private Integer NADL
    private boolean singleRouteCutoff
    private String svROUT

    private String WHLO
    private String CONA
    private String svPISE
    private String mitaloPISE
    private String TLIX
    private Integer svZNVT
    private String svPLRI
    private String DEV0

    private String BJNO
    private String FILE
    private String PK01
    private String PK02
    private String PK03
    private String PK04
    private String PK05
    private Integer INOU
    private long DLIX
    private String ROUT
    private Integer ZNVT
    private String A130
    private String MVXD
    private String currentDivision

    private String svFILE
    private String svPK01
    private String svPK02
    private String svPK03
    private String svPK04
    private String svPK05

    private double maximumVolume
    private double maximumWeight
    private double maximumLine
    private double maximumIndex
    private double totalVolume
    private double totalWeight
    private double totalLine
    private double totalIndex
    private String errorMsg

    public EXT410(LoggerAPI logger, UtilityAPI utility,DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles) {
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


    // Perform actual job
    private performActualJob(Optional<String> data) {
        if (!data.isPresent()) {
            logger.debug("Job reference Id ${batch.getReferenceId().get()} is passed but data was not found")
            return
        }
        rawData = data.get()
        logger.debug("Début performActualJob")
        inCFIN = getFirstParameter()
        inWHLO = getNextParameter()
        inDLIX = getNextParameter()

        logger.debug("inCFIN = " + inCFIN)
        logger.debug("inWHLO = " + inWHLO)
        logger.debug("inDLIX = " + inDLIX)

        currentCompany = (Integer)program.getLDAZD().CONO
        currentDivision = program.getLDAZD().DIVI

        BJNO = jobNumber
        logger.debug("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX jobNumber = " + jobNumber)
        ZNVT = 0

        LocalDate date = LocalDate.now()
        DayOfWeek dayOfWeek = date.getDayOfWeek()
        weekDay = getBinaryDay(dayOfWeek)
        logger.debug("week day = " + weekDay)

        LocalTime time = LocalTime.now()
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HHmm")
        timeDay = time.format(formatter)
        logger.debug("time day = " + timeDay)

        ZoneId zoneId = ZoneId.of("Europe/Paris")
        ZonedDateTime zonedTime = ZonedDateTime.now(zoneId)

        timeDay = zonedTime.format(formatter)
        logger.debug("time day Paris = " + timeDay)

        createWorkFile()

        splitIndex()

        traitementLibre()

        traitementRetention()

        initEXT410()

        initEXT411()

        initEXT412()

        initEXT413()
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

    // Fonction pour obtenir le format binaire
    String getBinaryDay(DayOfWeek day) {
        switch (day) {
            case DayOfWeek.MONDAY:
                return "1000000"
            case DayOfWeek.TUESDAY:
                return "0100000"
            case DayOfWeek.WEDNESDAY:
                return "0010000"
            case DayOfWeek.THURSDAY:
                return "0001000"
            case DayOfWeek.FRIDAY:
                return "0000100"
            case DayOfWeek.SATURDAY:
                return "0000010"
            case DayOfWeek.SUNDAY:
                return "0000001"
            default:
                return "0000000"
        }

    }

    // Traitement retention
    public void traitementRetention() {
        ZNVT = 0
        traitementVague()

        traitementFinHoraire()

        DBAction liberationEXT411 = database.table("EXT411").index("00").selection("EXZPMF", "EXZPM1", "EXZPM2", "EXZPM3", "EXZPM4", "EXZPM5", "EXZNVT").build()
        DBContainer EXT411 = liberationEXT411.getContainer()
        EXT411.set("EXCONO",currentCompany)
        EXT411.set("EXBJNO", BJNO)
        liberationEXT411.readAll(EXT411, 2, 100, outDataLiberation)

        DBAction queryEXT413 = database.table("EXT413").index("00").selection("EXPLRI","EXDLIX").build()
        DBContainer EXT413 = queryEXT413.getContainer()
        EXT413.set("EXBJNO", BJNO)
        EXT413.set("EXCONO",currentCompany)
        queryEXT413.readAll(EXT413, 2, nbMaxRecord, outDataPrtByWave)
    }

    //outDataLiberation : Liberation
    Closure<?> outDataLiberation = { DBContainer EXT411 ->
            logger.debug("EXT411.EXZPMF = " + EXT411.get("EXZPMF"))
            logger.debug("EXT411.EXZPM1 = " + EXT411.get("EXZPM1"))
            logger.debug("EXT411.EXZPM2 = " + EXT411.get("EXZPM2"))
            logger.debug("EXT411.EXZPM3 = " + EXT411.get("EXZPM3"))
            logger.debug("EXT411.EXZPM4 = " + EXT411.get("EXZPM4"))
            logger.debug("EXT411.EXZPM5 = " + EXT411.get("EXZPM5"))
            logger.debug("EXT411.EXZNVT = " + EXT411.get("EXZNVT"))

            svZNVT = 0
            svPLRI = "0"
            DBAction relByWaveEXT410 = database.table("EXT410").index("10").selection("EXINOU", "EXDLIX", "EXZNVT").build()
            DBContainer EXT410 = relByWaveEXT410.getContainer()
            EXT410.set("EXBJNO", BJNO)
            EXT410.set("EXZPMF", EXT411.get("EXZPMF"))
            EXT410.set("EXZPM1", EXT411.get("EXZPM1"))
            EXT410.set("EXZPM2", EXT411.get("EXZPM2"))
            EXT410.set("EXZPM3", EXT411.get("EXZPM3"))
            EXT410.set("EXZPM4", EXT411.get("EXZPM4"))
            EXT410.set("EXZPM5", EXT411.get("EXZPM5"))
            EXT410.set("EXZNVT", EXT411.get("EXZNVT"))
            relByWaveEXT410.readAll(EXT410, 8, 100, outDataRelByWave)
    }

    //outDataRelByWave : Liberation
    Closure<?> outDataRelByWave = { DBContainer EXT410 ->
            logger.debug("xxxxx")
            logger.debug("EXT410.EXZNVT = " + EXT410.get("EXZNVT"))
            logger.debug("EXT410.EXDLIX = " + EXT410.get("EXDLIX"))
            Integer oZNVT = EXT410.get("EXZNVT") as Integer
            String oDLIX = EXT410.get("EXDLIX")
            String oPLRI
        if(svZNVT!=oZNVT){
        oPLRI = " "
        svPLRI = "0"
        logger.debug("DLIX = " + oDLIX)
        logger.debug("executeEXT410MIRelForPickByWav 1 - DLIX = " + oDLIX)
        logger.debug("executeEXT410MIRelForPickByWav 1 - PLRI = " + oPLRI)
        executeEXT410MIRelForPickByWav(oDLIX, oPLRI)
        logger.debug("PLRI = " + svPLRI)

        LocalDateTime timeOfCreation = LocalDateTime.now()
        DBAction addQuery = database.table("EXT413").index("00").build()
        DBContainer EXT413 = addQuery.getContainer()
        EXT413.set("EXBJNO", BJNO)
        EXT413.set("EXCONO",currentCompany)
        EXT413.set("EXPLRI",svPLRI as Long)
        if (!addQuery.read(EXT413)) {
            EXT413.set("EXDLIX",oDLIX as Long)
            EXT413.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
            EXT413.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
            EXT413.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
            EXT413.setInt("EXCHNO", 1)
            EXT413.set("EXCHID", program.getUser())
            addQuery.insert(EXT413)
        }
    }
        if(svZNVT==oZNVT){
        oPLRI = svPLRI
        logger.debug("DLIX = " + oDLIX)
        logger.debug("PLRI = " + svPLRI)
        logger.debug("executeEXT410MIRelForPickByWav 2 - DLIX = " + oDLIX)
        logger.debug("executeEXT410MIRelForPickByWav 2 - PLRI = " + oPLRI)
        executeEXT410MIRelForPickByWav(oDLIX, oPLRI)
    }
    svZNVT = oZNVT
}

// Print by wave
Closure<?> outDataPrtByWave = { DBContainer EXT413 ->
        String prtPLRI = EXT413.get("EXPLRI")
        String prtDLIX = EXT413.get("EXDLIX")
        Integer prtINOU = 1

        logger.debug("Mode rétention - Edition LP - executeMWS420MIPrtPickingList - PLRI = " + prtPLRI)
        logger.debug("Mode rétention - Edition LP - executeMWS420MIPrtPickingList - DLIX = " + prtDLIX)
        prtPLRI = prtPLRI.trim()
        prtPLRI = prtPLRI.padLeft(10)


        DEV0 = ""
        executeMNS205MIGet(prtINOU, prtDLIX)

        executeMWS420MIPrtPickingList(prtDLIX,"",prtPLRI,"2", DEV0, "1")
}

// Execute EXT410MI RelForPickByWav
private executeEXT410MIRelForPickByWav(String pDLIX, String pPLRI){
    logger.debug("EXT410MI RelForPickByWav")
    String oCONO = currentCompany
    logger.debug("CONO = "+ oCONO+ " / DLIX = "+ pDLIX+ " / PLRI = "+ pPLRI)
    Map<String, String> paramEXT410MI = ["CONO": oCONO , "DLIX": pDLIX, "PLRI": pPLRI]
    Closure<?> handlerEXT410MI = { Map<String, String> response ->
    if (response.error != null) {
        IN60 = true
        logger.debug("EXT410MI RelForPickByWav error")
        return
    }
    if (response.PLRI != null && response.PLRI.trim() != "")
        svPLRI = response.PLRI.trim()
        }
    miCaller.call("EXT410MI", "RelForPickByWav", paramEXT410MI, handlerEXT410MI)
}

// Execute MWS420MI PrtPickingList
private executeMWS420MIPrtPickingList(String pDLIX, String pPLSX, String pPLRI, String pCOPL, String pDEV, String pCOPY){
    logger.debug("MWS420MI PrtPickingList")
    String oCONO = currentCompany
    logger.debug("CONO = " + oCONO + " / DLIX = " + pDLIX + " / PLSX = " + pPLSX + " / PLRI = " + pPLRI + " / COPL = " + pCOPL+ " / DEV0 = " + pDEV + " / pCOPY = " + pCOPY)
    Map<String, String> paramMWS420MI = ["CONO": oCONO , "DLIX": pDLIX, "PLSX": pPLSX, "PLRI": pPLRI, "COPL": pCOPL, "DEV0": pDEV, "COPY": pCOPY]
    Closure<?> handlerMWS420MI = { Map<String, String> response ->
    if (response.error != null) {
        IN60 = true
        logger.debug("MWS420MI PrtPickingList error")
        return
    } else {
        logger.debug("MWS420MI PrtPickingList - OK")
    }
        }
    miCaller.call("MWS420MI", "PrtPickingList", paramMWS420MI, handlerMWS420MI)
}

// Execute MNS205MI Get
private executeMNS205MIGet(Integer oINOU, String oDLIX){
    logger.debug("MNS205MI Get")
    logger.debug("INOU = " + oINOU)
    logger.debug("DLIX = " + oDLIX)
    String oCONO = currentCompany

    MVXD = ""
    getCRS881("EXT410", "1", "Utilisateur", "I", "DeliveryIndex", "Utilisateur", "", "")
    String USID = MVXD.trim()

    logger.debug("USID = " + USID)

    String mhdishWHLO = ""
    DBAction queryMHDISH = database.table("MHDISH").index("00").selection("OQWHLO").build()
    DBContainer MHDISH = queryMHDISH.getContainer()
    MHDISH.set("OQCONO", currentCompany)
    MHDISH.set("OQINOU", oINOU)
    MHDISH.set("OQDLIX", oDLIX as long)
    if(queryMHDISH.read(MHDISH)){
        mhdishWHLO = MHDISH.get("OQWHLO")
    }
    logger.debug("mhdish WHLO = " + mhdishWHLO)

    String mhpichSLTP = ""
    DBAction queryMHPICH = database.table("MHPICH").index("00").selection("PISLTP").build()
    DBContainer MHPICH = queryMHPICH.getContainer()
    MHPICH.set("PICONO", currentCompany)
    MHPICH.set("PIDLIX", oDLIX as long)
    MHPICH.set("PIPLSX", 1)
    if(queryMHPICH.read(MHPICH)){
        mhpichSLTP = MHPICH.get("PISLTP")
    }
    logger.debug("mhpich SLTP = " + mhpichSLTP)
    String DEVD = mhdishWHLO.trim() + mhpichSLTP.trim()
    logger.debug("DEVD = " + DEVD)

    logger.debug("DIVI ="+ currentDivision + " / PRTF ="+ "MWS435PF"+ " / USID ="+ USID+ " / DEVD ="+ DEVD+ " / MEDC ="+ "*PRT"+ " / SEQN ="+ "1")
    Map<String, String> paramMNS205MI = ["DIVI": currentDivision , "PRTF": "MWS435PF", "USID": USID, "DEVD": DEVD, "MEDC": "*PRT", "SEQN": "1"]
    Closure<?> handlerMNS205MI = { Map<String, String> response ->
    if (response.error != null) {
        IN60 = true
        logger.debug("MNS205MI Get error")
        return
    } else {
        logger.debug("MNS205MI Get - OK")
        if (response.DEV != null) {
            DEV0 = response.DEV.trim()
            logger.debug("DEV0 = " + DEV0)
        }
    }
        }
    miCaller.call("MNS205MI", "Get", paramMNS205MI, handlerMNS205MI)
}


// traitement Fin Horaire
public void traitementFinHoraire() {
    ZNVT++
    logger.debug("traitementFinHoraire - ZNVT++ - ZNVT = " + ZNVT)
    singleRouteCutoff = false
    logger.debug("traitement Fin Horaire")
    ExpressionFactory expressionFinHoraireEXT410 = database.getExpressionFactory("EXT410")
    expressionFinHoraireEXT410 = expressionFinHoraireEXT410.eq("EXZFIN", "1")
    DBAction finHoraireEXT410 = database.table("EXT410").index("20").matching(expressionFinHoraireEXT410).selection("EXZPMF","EXZPM1", "EXZPM2", "EXZPM3", "EXZPM4", "EXZPM5", "EXPISE", "EXINOU", "EXDLIX", "EXROUT").build()
    DBContainer EXT410 = finHoraireEXT410.getContainer()
    EXT410.set("EXBJNO", BJNO)
    EXT410.set("EXZMOD", "HLD")
    finHoraireEXT410.readAll(EXT410, 2, 100, outDataFinHoraire)
}

// traitement vague
public void traitementVague() {
    logger.debug("traitement par Vague")
    svFILE = ""
    svPK01 = ""
    svPK02 = ""
    svPK03 = ""
    svPK04 = ""
    svPK05 = ""
    svPISE = ""
    maximumVolume = 0
    maximumWeight = 0
    maximumLine = 0
    maximumIndex = 0
    totalVolume = 0
    totalWeight = 0
    totalLine = 0
    totalIndex = 0
    singleRouteCutoff = false

    ExpressionFactory expressionVagueEXT410 = database.getExpressionFactory("EXT410")
    expressionVagueEXT410 = expressionVagueEXT410.eq("EXZFIN", "0")
    DBAction vagueEXT410 = database.table("EXT410").index("20").matching(expressionVagueEXT410).selection("EXZPMF", "EXZPM1", "EXZPM2", "EXZPM3", "EXZPM4", "EXZPM5", "EXPISE", "EXINOU", "EXDLIX", "EXROUT").build()
    DBContainer EXT410 = vagueEXT410.getContainer()
    EXT410.set("EXBJNO", BJNO)
    EXT410.set("EXZMOD", "HLD")
    vagueEXT410.readAll(EXT410, 2, 100, outDataVague)
}

//outDataVague : Vague
Closure<?> outDataVague = { DBContainer EXT410 ->
        String oFILE = EXT410.get("EXZPMF")
        String oPK01 = EXT410.get("EXZPM1")
        String oPK02 = EXT410.get("EXZPM2")
        String oPK03 = EXT410.get("EXZPM3")
        String oPK04 = EXT410.get("EXZPM4")
        String oPK05 = EXT410.get("EXZPM5")
        String oPISE = EXT410.get("EXPISE")
        Integer oINOU = EXT410.get("EXINOU") as Integer
long oDLIX = EXT410.get("EXDLIX") as long
String oROUT = EXT410.get("EXROUT")

        logger.debug("outDataVague EXT410.PK01 = " + oPK01)
        logger.debug("outDataVague EXT410.PK02 = " + oPK02)
        logger.debug("outDataVague EXT410.PK03 = " + oPK03)
        logger.debug("outDataVague EXT410.PK04 = " + oPK04)
        logger.debug("outDataVague EXT410.PK05 = " + oPK05)
        logger.debug("outDataVague EXT410.PISE = " + oPISE)
        logger.debug("outDataVague EXT410.DLIX = " + oDLIX)

        if (svPK01!=oPK01 || svPK02!=oPK02 || svPK03!=oPK03 || svPK04!=oPK04 || svPK05!=oPK05 || svPISE!=oPISE){
        logger.debug("outDataVague Rupture combinaison")
svFILE = oFILE
        svPK01 = oPK01
svPK02 = oPK02
        svPK03 = oPK03
svPK04 = oPK04
        svPK05 = oPK05
svPISE = oPISE

ZNVT++

DBAction queryCUGEX1 = database.table("CUGEX1").index("00").selection("F1N096", "F1N196", "F1N296", "F1N396", "F1A030").build()
DBContainer qCUGEX1 = queryCUGEX1.getContainer()
            qCUGEX1.set("F1CONO", currentCompany)
            qCUGEX1.set("F1FILE", svFILE)
            qCUGEX1.set("F1PK01", svPK01)
            qCUGEX1.set("F1PK02", svPK02)
            qCUGEX1.set("F1PK03", svPK03)
            qCUGEX1.set("F1PK04", svPK04)
            qCUGEX1.set("F1PK05", svPK05)
            qCUGEX1.set("F1PK06", "")
            qCUGEX1.set("F1PK07", "")
            qCUGEX1.set("F1PK08", "")
            if (queryCUGEX1.read(qCUGEX1)) {
maximumVolume = qCUGEX1.get("F1N096")
maximumWeight =  qCUGEX1.get("F1N196")
maximumLine =  qCUGEX1.get("F1N296")
maximumIndex =  qCUGEX1.get("F1N396")
                logger.debug("clear totaux")
totalVolume = 0
totalWeight = 0
totalLine = 0
totalIndex = 0
svROUT = oROUT
        singleRouteCutoff = false
                if(qCUGEX1.get("F1A030") == "0"){
singleRouteCutoff = true
        }
        }
        logger.debug("Paramétrage CUGEX1 pour Vague")
            logger.debug("Maximum volume (F1N096) = " + maximumVolume)
            logger.debug("Maximum Weight (F1N196) = " + maximumWeight)
            logger.debug("Maximum Line (F1N296) = " + maximumLine)
            logger.debug("Maximum Index (F1N396) = " + maximumIndex)
            logger.debug("single Route Cutoff (F1A030) = " + singleRouteCutoff)
        }

                if(singleRouteCutoff==true && svROUT != oROUT){
ZNVT++
        logger.debug("outDataVague singleRouteCutoff - ZNVT++ - ZNVT = " + ZNVT)
totalVolume = 0
totalWeight = 0
totalLine = 0
totalIndex = 0
        }

DBAction queryMHDISH = database.table("MHDISH").index("00").selection("OQVOL3", "OQGRWE").build()
DBContainer MHDISH = queryMHDISH.getContainer()
        MHDISH.set("OQCONO", currentCompany)
        MHDISH.set("OQINOU", oINOU)
        MHDISH.set("OQDLIX", oDLIX)
        if(queryMHDISH.read(MHDISH)){
totalIndex++
        logger.debug("outDataVague MHDISH.VOL3 = " + MHDISH.get("OQVOL3"))
        logger.debug("outDataVague MHDISH.GRWE = " + MHDISH.get("OQGRWE"))
totalVolume += MHDISH.get("OQVOL3") as double
totalWeight += MHDISH.get("OQGRWE") as double
        NADL = 0
DBAction queryMHDISL = database.table("MHDISL").index("00").selection("URRIDN", "URRIDL", "URRIDX").build()
DBContainer MHDISL = queryMHDISL.getContainer()
            MHDISL.set("URCONO", currentCompany)
            MHDISL.set("URDLIX", oDLIX)
            queryMHDISL.readAll(MHDISL, 2, 20, outDataMHDISL)
totalLine += NADL
        }

                logger.debug("Update EXT410.ZNVT = " + ZNVT)
DBAction updateEXT410 = database.table("EXT410").index("00").build()
DBContainer uEXT410 = updateEXT410.getContainer()
        uEXT410.set("EXBJNO", BJNO)
        uEXT410.set("EXZPMF", oFILE)
        uEXT410.set("EXZPM1", oPK01)
        uEXT410.set("EXZPM2", oPK02)
        uEXT410.set("EXZPM3", oPK03)
        uEXT410.set("EXZPM4", oPK04)
        uEXT410.set("EXZPM5", oPK05)
        uEXT410.set("EXINOU", oINOU)
        uEXT410.set("EXDLIX", oDLIX)
        updateEXT410.readLock(uEXT410, updatCallBackEXT410)

        logger.debug("ZNVT = " + ZNVT)
        logger.debug("Test valeur pour Vague")
        logger.debug("totalIndex = " + totalIndex)
        logger.debug("totalWeight = " + totalWeight)
        logger.debug("totalVolume = " + totalVolume)
        logger.debug("totalLine = " + totalLine)
        logger.debug("xxx")
        logger.debug("maximumIndex = " + maximumIndex)
        logger.debug("maximumWeight = " + maximumWeight)
        logger.debug("maximumVolume = " + maximumVolume)
        logger.debug("maximumLine = " + maximumLine)

        if((totalIndex>=maximumIndex && maximumIndex!= 0) ||
        (totalWeight>=maximumWeight && maximumWeight!= 0) ||
        (totalVolume>=maximumVolume && maximumVolume!= 0) ||
        (totalLine>=maximumLine && maximumLine!= 0)) {
        logger.debug("Write EXT411 1")
LocalDateTime timeOfCreation = LocalDateTime.now()
DBAction addQuery = database.table("EXT411").index("00").selection("EXCHID","EXCHNO").build()
DBContainer EXT411 = addQuery.getContainer()
            EXT411.set("EXBJNO", BJNO)
            EXT411.set("EXCONO",currentCompany)
            EXT411.set("EXZPMF",oFILE)
            EXT411.set("EXZPM1",oPK01)
            EXT411.set("EXZPM2",oPK02)
            EXT411.set("EXZPM3",oPK03)
            EXT411.set("EXZPM4",oPK04)
            EXT411.set("EXZPM5",oPK05)
            EXT411.set("EXZNVT",ZNVT)
            if (!addQuery.read(EXT411)) {
        logger.debug("Write EXT411 2")
                EXT411.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT411.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        EXT411.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT411.setInt("EXCHNO", 1)
                EXT411.set("EXCHID", program.getUser())
        addQuery.insert(EXT411)
            }
ZNVT++
        logger.debug("outDataVague - ZNVT++ - ZNVT = " + ZNVT)
totalVolume = 0
totalWeight = 0
totalLine = 0
totalIndex = 0
        }
svROUT = oROUT
    }

//outDataFinHoraire : Fin Horaire
Closure<?> outDataFinHoraire = { DBContainer EXT410 ->
        String oFILE = EXT410.get("EXZPMF")
        String oPK01 = EXT410.get("EXZPM1")
        String oPK02 = EXT410.get("EXZPM2")
        String oPK03 = EXT410.get("EXZPM3")
        String oPK04 = EXT410.get("EXZPM4")
        String oPK05 = EXT410.get("EXZPM5")
        String oPISE = EXT410.get("EXPISE")
        Integer oINOU = EXT410.get("EXINOU") as Integer
long oDLIX = EXT410.get("EXDLIX") as long
String oROUT = EXT410.get("EXROUT")

        if (svFILE!=oFILE || svPK01!=oPK01 || svPK02!=oPK02 || svPK03!=oPK03 || svPK04!=oPK04 || svPK05!=oPK05 || svPISE!=oPISE){
svFILE = oFILE
        svPK01 = oPK01
svPK02 = oPK02
        svPK03 = oPK03
svPK04 = oPK04
        svPK05 = oPK05
svPISE = oPISE
DBAction queryCUGEX1 = database.table("CUGEX1").index("00").selection("F1A030").build()
DBContainer qCUGEX1 = queryCUGEX1.getContainer()
            qCUGEX1.set("F1CONO", currentCompany)
            qCUGEX1.set("F1FILE", svFILE)
            qCUGEX1.set("F1PK01", svPK01)
            qCUGEX1.set("F1PK02", svPK02)
            qCUGEX1.set("F1PK03", svPK03)
            qCUGEX1.set("F1PK04", svPK04)
            qCUGEX1.set("F1PK05", svPK05)
            qCUGEX1.set("F1PK06", "")
            qCUGEX1.set("F1PK07", "")
            qCUGEX1.set("F1PK08", "")
            if (queryCUGEX1.read(qCUGEX1)) {
ZNVT++
        logger.debug("outDataFinHoraire 1 - ZNVT++ - ZNVT = " + ZNVT)
svROUT = oROUT
        singleRouteCutoff = false
                if(qCUGEX1.get("F1A030") == "0"){
singleRouteCutoff = true
        }
        }
        }

        if(singleRouteCutoff==true && svROUT != oROUT){
ZNVT++
        logger.debug("outDataFinHoraire singleRouteCutoff - ZNVT++ - ZNVT = " + ZNVT)
        }

DBAction updateEXT410 = database.table("EXT410").index("00").build()
DBContainer uEXT410 = updateEXT410.getContainer()
        uEXT410.set("EXBJNO", BJNO)
        uEXT410.set("EXZPMF", oFILE)
        uEXT410.set("EXZPM1", oPK01)
        uEXT410.set("EXZPM2", oPK02)
        uEXT410.set("EXZPM3", oPK03)
        uEXT410.set("EXZPM4", oPK04)
        uEXT410.set("EXZPM5", oPK05)
        uEXT410.set("EXINOU", oINOU)
        uEXT410.set("EXDLIX", oDLIX)
        updateEXT410.readLock(uEXT410, updatCallBackEXT410)

LocalDateTime timeOfCreation = LocalDateTime.now()
DBAction addQuery = database.table("EXT411").index("00").selection("EXCHID","EXCHNO").build()
DBContainer EXT411 = addQuery.getContainer()
        EXT411.set("EXBJNO", BJNO)
        EXT411.set("EXCONO",currentCompany)
        EXT411.set("EXZPMF",oFILE)
        EXT411.set("EXZPM1",oPK01)
        EXT411.set("EXZPM2",oPK02)
        EXT411.set("EXZPM3",oPK03)
        EXT411.set("EXZPM4",oPK04)
        EXT411.set("EXZPM5",oPK05)
        EXT411.set("EXZNVT",ZNVT)
        if (!addQuery.read(EXT411)) {
        EXT411.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT411.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        EXT411.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT411.setInt("EXCHNO", 1)
            EXT411.set("EXCHID", program.getUser())
        addQuery.insert(EXT411)
        }
svROUT = oROUT
    }

// Retrieve MHDISL
Closure<?> outDataMHDISL = { DBContainer MHDISL ->
        logger.debug("Retrive MHDISL for OOLINE")
        logger.debug("URRIDN = " + MHDISL.get("URRIDN"))
        logger.debug("URRIDL = " + MHDISL.get("URRIDL"))
        logger.debug("URRIDX = " + MHDISL.get("URRIDX"))
        DBAction queryOOLINE = database.table("OOLINE").index("00").selection("OBALQT").build()
        DBContainer OOLINE = queryOOLINE.getContainer()
        OOLINE.set("OBCONO", currentCompany)
        OOLINE.set("OBORNO", MHDISL.get("URRIDN"))
        OOLINE.set("OBPONR", MHDISL.get("URRIDL"))
        OOLINE.set("OBPOSX", MHDISL.get("URRIDX"))
        if(queryOOLINE.read(OOLINE)){
Double oALQT = OOLINE.get("OBALQT")
            logger.debug("Retrive OOLINE")
            logger.debug("OBALQT = " + oALQT)
            logger.debug("NADL before add = " + NADL)
            if(oALQT > 0){
NADL++
        }
        logger.debug("NADL after add = " + NADL)
        }
                }

// Update EXT410
Closure<?> updatCallBackEXT410 = { LockedResult lockedResult ->
        LocalDateTime timeOfCreation = LocalDateTime.now()
int changeNumber = lockedResult.get("EXCHNO")
        lockedResult.set("EXZNVT", ZNVT)
        lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        lockedResult.setInt("EXCHNO", changeNumber + 1)
        lockedResult.set("EXCHID", program.getUser())
        lockedResult.update()
    }

// Update EXT410
Closure<?> updatCallBackEXT4102 = { LockedResult lockedResult ->
        LocalDateTime timeOfCreation = LocalDateTime.now()
int changeNumber = lockedResult.get("EXCHNO")
        lockedResult.set("EXPISE", mitaloPISE)
        lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        lockedResult.setInt("EXCHNO", changeNumber + 1)
        lockedResult.set("EXCHID", program.getUser())
        lockedResult.update()
    }

// Work split Index
public void traitementLibre() {
    DBAction libreEXT410 = database.table("EXT410").index("20").selection("EXINOU","EXDLIX").build()
    DBContainer EXT410 = libreEXT410.getContainer()
    EXT410.set("EXBJNO", BJNO)
    EXT410.set("EXZMOD", "JIT")
    libreEXT410.readAll(EXT410, 2, 100, outDataLibre)
}

//outDataLibre : Libération
Closure<?> outDataLibre = { DBContainer EXT410 ->
        String oDLIX = EXT410.get("EXDLIX")
        Integer oINOU = EXT410.get("EXINOU")

        executeMWS410MIRelForPick(oDLIX)

        DEV0 = ""
        executeMNS205MIGet(oINOU, oDLIX)

        logger.debug("Mode fil de l'eau - Edition LP - executeMWS420MIPrtPickingList - PLRI = " + "")
        logger.debug("Mode fil de l'eau - Edition LP - executeMWS420MIPrtPickingList - DLIX = " + oDLIX)
        executeMWS420MIPrtPickingList(oDLIX,"1","","", DEV0, "1")
}

// Execute MWS410MI RelForPick
private executeMWS410MIRelForPick(String pDLIX){
    logger.debug("MWS410MI RelForPick")
    String oCONO = currentCompany
    logger.debug("CONO = "+ oCONO+ " / DLIX = "+ pDLIX+ " / OLUP = "+ "1")
    Map<String, String> paramMWS410MI = ["CONO": oCONO, "DLIX": pDLIX, "OLUP": "1"]
    Closure<?> handler = { Map<String, String> response ->
    if (response.error != null) {
        IN60 = true
        return
    }
        }
    miCaller.call("MWS410MI", "RelForPick", paramMWS410MI, handler)
}

// Work split Index
public void splitIndex() {
    logger.debug("Début split Index")
    DBAction workIndexQuery = database.table("EXT410").index("00").selection("EXZPMF", "EXZPM1", "EXZPM2", "EXZPM3", "EXZPM4", "EXZPM5", "EXINOU", "EXDLIX").build()
    DBContainer EXT410 = workIndexQuery.getContainer()
    EXT410.set("EXBJNO", BJNO)
    workIndexQuery.readAll(EXT410, 1, 100, outDataWorkIndexQuery)
}

//outDataWorkIndexQuery : Retrieve MITALO
Closure<?> outDataWorkIndexQuery = { DBContainer EXT410 ->
        split = false
        svPISE = ""
        mitaloPISE = ""
        FILE = EXT410.get("EXZPMF")
        PK01 = EXT410.get("EXZPM1")
        PK02 = EXT410.get("EXZPM2")
        PK03 = EXT410.get("EXZPM3")
        PK04 = EXT410.get("EXZPM4")
        PK05 = EXT410.get("EXZPM5")
        INOU = EXT410.get("EXINOU") as Integer
        DLIX = EXT410.get("EXDLIX") as long

        logger.debug("DLIX = " + DLIX)

DBAction queryMHDISL = database.table("MHDISL").index("00").selection("URRORC", "URRIDN", "URRIDL", "URRIDX").build()
DBContainer MHDISL = queryMHDISL.getContainer()
        MHDISL.set("URCONO",currentCompany)
        MHDISL.set("URDLIX", DLIX)
        queryMHDISL.readAll(MHDISL, 2, 20, outDataCheckMHDISL)

        if(split==true){
        logger.debug("Execute Split DLIX")

DBAction queryCUGEX1 = database.table("CUGEX1").index("00").selection("F1A130").build()
DBContainer qCUGEX1 = queryCUGEX1.getContainer()
            qCUGEX1.set("F1CONO", currentCompany)
            qCUGEX1.set("F1FILE", FILE)
            qCUGEX1.set("F1PK01", PK01)
            qCUGEX1.set("F1PK02", PK02)
            qCUGEX1.set("F1PK03", PK03)
            qCUGEX1.set("F1PK04", PK04)
            qCUGEX1.set("F1PK05", PK05)
            qCUGEX1.set("F1PK06", "")
            qCUGEX1.set("F1PK07", "")
            qCUGEX1.set("F1PK08", "")
            if (queryCUGEX1.read(qCUGEX1)) {
A130 = qCUGEX1.get("F1A130")
            }

                    MHDISL.set("URCONO",currentCompany)
            MHDISL.set("URDLIX", DLIX)
            queryMHDISL.readAll(MHDISL, 2, 20, outDataSplit)

svPISE = ""
TLIX = ""
DBAction workSplitQuery = database.table("EXT412").index("00").selection("EXDLIX", "EXPISE", "EXRIDN", "EXRIDL", "EXRIDX").build()
DBContainer EXT412 = workSplitQuery.getContainer()
            EXT412.set("EXCONO",currentCompany)
            EXT412.set("EXBJNO", BJNO)
            EXT412.set("EXDLIX", DLIX)
            workSplitQuery.readAll(EXT412, 3, 20, outDataWorkSplitQuery)
        } else {
DBAction updateEXT410 = database.table("EXT410").index("00").build()
DBContainer uEXT410 = updateEXT410.getContainer()
            uEXT410.set("EXBJNO", BJNO)
            uEXT410.set("EXZPMF", FILE)
            uEXT410.set("EXZPM1", PK01)
            uEXT410.set("EXZPM2", PK02)
            uEXT410.set("EXZPM3", PK03)
            uEXT410.set("EXZPM4", PK04)
            uEXT410.set("EXZPM5", PK05)
            uEXT410.set("EXINOU", INOU)
            uEXT410.set("EXDLIX", DLIX)
            updateEXT410.readLock(uEXT410, updatCallBackEXT4102)
        }
                }

//outDataWorkSplitQuery : Retrieve EXT412
Closure<?> outDataWorkSplitQuery = { DBContainer EXT412 ->
        LocalDateTime timeOfCreation = LocalDateTime.now()
        logger.debug("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
        String oPISE = EXT412.get("EXPISE")
        String oDLIX = EXT412.get("EXDLIX")
        String oRIDN = EXT412.get("EXRIDN")
        String oRIDL = EXT412.get("EXRIDL")
        String oRIDX = EXT412.get("EXRIDX")
        if(svPISE == ""){
svPISE = oPISE
        }
                logger.debug("EXT412.PISE = " + oPISE)
        logger.debug("EXT412.DLIX = " + oDLIX)
        logger.debug("EXT412.RIDN = " + oRIDN)
        logger.debug("EXT412.RIDL = " + oRIDL)
        logger.debug("EXT412.RIDX = " + oRIDX)
        logger.debug("SV PISE = " + svPISE)
        if(svPISE!=oPISE){
TLIX = ""
        logger.info("executeMWS411MIMoveDelLn 1 - DLIX = " + oDLIX)
            logger.info("executeMWS411MIMoveDelLn 1 - RIDN = " + oRIDN)
            logger.info("executeMWS411MIMoveDelLn 1 - RIDL = " + oRIDL)
            logger.info("executeMWS411MIMoveDelLn 1 - RIDX = " + oRIDX)
            logger.info("executeMWS411MIMoveDelLn 1 - TLIX = " + TLIX)
executeMWS411MIMoveDelLn(oDLIX, oRIDN, oRIDL, oRIDX, TLIX)
            if(TLIX!=""){
Integer connOriginalIndex = 0
DBAction queryMHDISH = database.table("MHDISH").index("00").selection("OQCONN").build()
DBContainer MHDISH = queryMHDISH.getContainer()
                MHDISH.set("OQCONO", currentCompany)
                MHDISH.set("OQINOU", INOU)
                MHDISH.set("OQDLIX", oDLIX as long)
                if(queryMHDISH.read(MHDISH)){
// After the split, connecting the new index to the original index shipment
connOriginalIndex = MHDISH.get("OQCONN")
                    logger.info("executeMWS410MIConnectShipment - DLIX = " + TLIX)
                    logger.info("executeMWS410MIConnectShipment - CONN = " + connOriginalIndex)
                    logger.info("executeMWS410MIConnectShipment - INOU = " + INOU)
executeMWS410MIConnectShipment(TLIX, connOriginalIndex as String, INOU as String)
                }
                        logger.debug("Split Index - Nouvel index TLIX = " + TLIX)
DBAction addQuery = database.table("EXT410").index("00").selection("EXZMOD","EXCHID","EXCHNO").build()
DBContainer EXT410 = addQuery.getContainer()
                EXT410.set("EXBJNO", BJNO)
                EXT410.set("EXZPMF",FILE)
                EXT410.set("EXZPM1",PK01)
                EXT410.set("EXZPM2",PK02)
                EXT410.set("EXZPM3",PK03)
                EXT410.set("EXZPM4",PK04)
                EXT410.set("EXZPM5",PK05)
                EXT410.set("EXINOU",INOU)
                EXT410.set("EXDLIX",TLIX as long)
                if (!addQuery.read(EXT410)) {
        EXT410.set("EXCONO",currentCompany)
                    EXT410.set("EXZMOD", A130)
                    EXT410.set("EXPISE", oPISE)
                    EXT410.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT410.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        EXT410.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT410.setInt("EXCHNO", 1)
                    EXT410.set("EXCHID", program.getUser())
        addQuery.insert(EXT410)
                }
                        }
                        } else {
                        if(TLIX!="") {
        logger.debug("executeMWS411MIMoveDelLn 2 - DLIX = " + oDLIX)
                logger.debug("executeMWS411MIMoveDelLn 2 - RIDN = " + oRIDN)
                logger.debug("executeMWS411MIMoveDelLn 2 - RIDL = " + oRIDL)
                logger.debug("executeMWS411MIMoveDelLn 2 - RIDX = " + oRIDX)
                logger.debug("executeMWS411MIMoveDelLn 2 - TLIX = " + TLIX)
executeMWS411MIMoveDelLn(oDLIX, oRIDN, oRIDL, oRIDX, TLIX)
            }
                    }
svPISE = oPISE
    }

// Execute MWS411MI MoveDelLn
private executeMWS411MIMoveDelLn(String pDLIX, String pRIDN, String pRIDL, String pRIDX, String pTLIX){
    logger.debug("MWS411MI MoveDelLn")
    String oCONO = currentCompany
    logger.debug("CONO = "+ oCONO+ " / DLIX = "+ pDLIX+ " / RORC = "+ "3"+ " / RIDN ="+ pRIDN+ " / RIDL = "+ pRIDL+ " / RIDX = "+ pRIDX+ " / TLIX = "+ pTLIX)
    Map<String, String> paramMWS411MI = ["CONO": oCONO, "DLIX": pDLIX, "RORC": "3", "RIDN": pRIDN, "RIDL": pRIDL, "RIDX": pRIDX, "TLIX": pTLIX]
    Closure<?> handler = { Map<String, String> response ->
    if (response.error != null) {
        IN60 = true
        return
    }
    if (response.TLIX != null)
        TLIX = response.TLIX.trim()
    logger.debug("After executeMWS411MIMoveDelLn - TLIX = " + TLIX)
        }
    miCaller.call("MWS411MI", "MoveDelLn", paramMWS411MI, handler)
}

// Execute MWS410MI ConnectShipment
private executeMWS410MIConnectShipment(String DLIX, String CONN, String INOU){
    logger.debug("MWS410MI ConnectShipment")
    String CONO = currentCompany
    logger.debug("CONO = "+ CONO+ " / DLIX = "+ DLIX+ " / CONN ="+ CONN+ " / INOU = "+ INOU)
    Map<String, String> paramMWS410MI = ["CONO": CONO, "DLIX": DLIX, "CONN": CONN, "INOU": INOU]
    Closure<?> handler = { Map<String, String> response ->
    if (response.error != null) {
        IN60 = true
        errorMsg = ("Failed MWS410MI.ConnectShipment: " + response.errorMessage)
        logger.debug("MWS410MI.ConnectShipment error : " + errorMsg)
        return
    }
        }
    miCaller.call("MWS410MI", "ConnectShipment", paramMWS410MI, handler)
}

//outDataCheckMHDISL : Retrieve MITALO
Closure<?> outDataCheckMHDISL = { DBContainer MHDISL ->
        String oRIDN = MHDISL.get("URRIDN")
        Integer oRIDL = MHDISL.get("URRIDL") as Integer
        Integer oRIDX = MHDISL.get("URRIDX") as Integer
        logger.debug("Read MHDISL")
        logger.debug("URRIDN = " + oRIDN)
        logger.debug("URRIDL = " + oRIDL)
        logger.debug("URRIDX = " + oRIDX)
        DBAction queryMITALO = database.table("MITALO").index("10").selection("MQPISE", "MQRIDN", "MQRIDL", "MQRIDX").build()
        DBContainer MITALO = queryMITALO.getContainer()
        MITALO.set("MQCONO",currentCompany)
        MITALO.set("MQTTYP", 31)
        MITALO.set("MQRIDN", oRIDN)
        MITALO.set("MQRIDO", 0)
        MITALO.set("MQRIDL",oRIDL)
        MITALO.set("MQRIDX",oRIDX)
        queryMITALO.readAll(MITALO, 6, 5, outDataCheckMITALO)
}

//outDataCheckMITALO : Retrieve MITALO
Closure<?> outDataCheckMITALO = { DBContainer MITALO ->
        String oPISE = MITALO.get("MQPISE")
        mitaloPISE = MITALO.get("MQPISE")
        logger.debug("Read MITALO")
        logger.debug("MQPISE = " + oPISE)
        if(oPISE!=svPISE && svPISE != ""){
split = true
        }
svPISE = oPISE
    }

//outDataSplit : Retrieve MHDISL
Closure<?> outDataSplit = { DBContainer MHDISL ->
        String oRIDN = MHDISL.get("URRIDN")
        Integer oRIDL = MHDISL.get("URRIDL") as Integer
        Integer oRIDX = MHDISL.get("URRIDX") as Integer
        logger.debug("Retrive MHDISL for Split")
        logger.debug("URRIDN = " + oRIDN)
        logger.debug("URRIDL = " + oRIDL)
        logger.debug("URRIDX = " + oRIDX)
        DBAction queryMITALO = database.table("MITALO").index("10").selection("MQPISE", "MQRIDN", "MQRIDL", "MQRIDX").build()
        DBContainer MITALO = queryMITALO.getContainer()
        MITALO.set("MQCONO",currentCompany)
        MITALO.set("MQTTYP", 31)
        MITALO.set("MQRIDN", oRIDN)
        MITALO.set("MQRIDO", 0)
        MITALO.set("MQRIDL",oRIDL)
        MITALO.set("MQRIDX",oRIDX)
        queryMITALO.readAll(MITALO, 6, 5, outDataSplitMITALO)
}

//outDataSplitMITALO : Retrieve MITALO
Closure<?> outDataSplitMITALO = { DBContainer MITALO ->
        LocalDateTime timeOfCreation = LocalDateTime.now()
        DBAction addSplitQuery = database.table("EXT412").index("00").selection("EXCHID","EXCHNO").build()
        DBContainer EXT412 = addSplitQuery.getContainer()
        EXT412.set("EXBJNO", BJNO)
        EXT412.set("EXCONO",currentCompany)
        EXT412.set("EXDLIX",DLIX)
        EXT412.set("EXPISE",MITALO.get("MQPISE"))
        EXT412.set("EXRIDN",MITALO.get("MQRIDN"))
        EXT412.set("EXRIDL",MITALO.get("MQRIDL")) as Integer
        EXT412.set("EXRIDX",MITALO.get("MQRIDX")) as Integer
        if (!addSplitQuery.read(EXT412)) {
        EXT412.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT412.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        EXT412.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT412.setInt("EXCHNO", 1)
            EXT412.set("EXCHID", program.getUser())
        addSplitQuery.insert(EXT412)
        }
                }

// create Work File EXT410
public void createWorkFile() {
    logger.debug("Create Work File")
    ExpressionFactory expressionMHDISH = database.getExpressionFactory("MHDISH")
    expressionMHDISH = expressionMHDISH.eq("OQTTYP", "31")
    if(inWHLO != null && inWHLO.trim() != "") {
        expressionMHDISH = expressionMHDISH.and(expressionMHDISH.eq("OQWHLO", inWHLO))
    }
    if(inDLIX != null && inDLIX.trim() != "") {
        expressionMHDISH = expressionMHDISH.and(expressionMHDISH.eq("OQDLIX", inDLIX))
    }
    DBAction indexQuery = database.table("MHDISH").index("91").matching(expressionMHDISH).selection("OQINOU", "OQDLIX", "OQWHLO", "OQCONA", "OQROUT").build()
    DBContainer MHDISH = indexQuery.getContainer()
    MHDISH.set("OQCONO", currentCompany)
    logger.debug("current Company = " + currentCompany)
    if(!indexQuery.readAll(MHDISH, 1, 100, outDataIndexQuery)){
        logger.debug("L'enregistrement MHDISH n'existe pas donc pas de traitement")
        return
    }

}

//outDataIndexQuery : Retrieve MHDISH91
Closure<?> outDataIndexQuery = { DBContainer MHDISH ->

        DLIX = MHDISH.get("OQDLIX") as long
        INOU = MHDISH.get("OQINOU") as Integer
WHLO = MHDISH.get("OQWHLO")
CONA = MHDISH.get("OQCONA")
ROUT = MHDISH.get("OQROUT")
        logger.debug("WHLO = " + WHLO)
        logger.debug("DLIX = " + DLIX)
checkMHDISH()

    }

// check MIDISH
public void checkMHDISH() {
    foundCUGEX1 = false

    logger.debug("Priorite 1 ")
    logger.debug("WHLO = " + WHLO)
    logger.debug("CONA = " + CONA)
    logger.debug("time day = " + timeDay)
    ExpressionFactory expressionCUGEX1 = database.getExpressionFactory("CUGEX1")
    expressionCUGEX1 = expressionCUGEX1.le("F1PK04", timeDay)
    expressionCUGEX1 = expressionCUGEX1.and(expressionCUGEX1.ge("F1PK05", timeDay))
    DBAction prioriteQuery = database.table("CUGEX1").index("00").matching(expressionCUGEX1).selection("F1FILE", "F1PK01", "F1PK02", "F1PK03", "F1PK04", "F1PK05", "F1A130", "F1A230").build()
    DBContainer CUGEX1 = prioriteQuery.getContainer()
    CUGEX1.set("F1CONO",currentCompany)
    CUGEX1.set("F1FILE","SLOT_CUNO")
    CUGEX1.set("F1PK01",WHLO)
    CUGEX1.set("F1PK02",CONA)
    CUGEX1.set("F1PK03",weekDay)
    prioriteQuery.readAll(CUGEX1, 5, 1, outDataCUGEX1)

    if(foundCUGEX1 == true){
        logger.debug("Found priority 1 ")
        return
    }

    logger.debug("Priorite 2 ")
    logger.debug("ROUT = " + ROUT)
    CUGEX1.set("F1CONO",currentCompany)
    CUGEX1.set("F1FILE","SLOT_ROUT")
    CUGEX1.set("F1PK01",WHLO)
    CUGEX1.set("F1PK02",ROUT)
    CUGEX1.set("F1PK03",weekDay)
    prioriteQuery.readAll(CUGEX1, 5, 1, outDataCUGEX1)

    if(foundCUGEX1 == true){
        logger.debug("Found priority 2 ")
        return
    }

    DBAction cutOffQuery = database.table("DROUDI").index("00").selection("DSLILH", "DSLILM").build()
    DBContainer DROUDI = cutOffQuery.getContainer()
    DROUDI.set("DSCONO",currentCompany)
    DROUDI.set("DSROUT",ROUT)
    DROUDI.set("DSRODN",1)
    cutOffQuery.readAll(DROUDI, 3, 1, outDataDROUDI)

    if(foundCUGEX1 == true){
        logger.debug("Found priority 3 ")
        return
    }

    logger.debug("Priority not found ")
}


// outDataDROUDI : Retrieve DROUDI
Closure<?> outDataDROUDI = { DBContainer DROUDI ->
        String oLLIH= DROUDI.get("DSLILH")
        String oLLIM = DROUDI.get("DSLILM")
        cutOff = oLLIH.trim() + oLLIM.trim()


        logger.debug("Priorite 3 ")
        logger.debug("cutoff = " + cutOff)
        ExpressionFactory expressionCUGEX1 = database.getExpressionFactory("CUGEX1")
        expressionCUGEX1 = expressionCUGEX1.le("F1PK04", timeDay)
        expressionCUGEX1 = expressionCUGEX1.and(expressionCUGEX1.ge("F1PK05", timeDay))
        DBAction prioriteQuery = database.table("CUGEX1").index("00").matching(expressionCUGEX1).selection("F1PK01", "F1PK02", "F1PK03", "F1PK04", "F1PK05", "F1A130", "F1A230").build()
        DBContainer CUGEX1 = prioriteQuery.getContainer()
        CUGEX1.set("F1CONO",currentCompany)
        CUGEX1.set("F1FILE","SLOT_COFF")
        CUGEX1.set("F1PK01",WHLO)
        CUGEX1.set("F1PK02",cutOff)
        CUGEX1.set("F1PK03",weekDay)
        prioriteQuery.readAll(CUGEX1, 5, 1, outDataCUGEX1)
}

// outDataCUGEX1 : Retrieve CUGEX1
Closure<?> outDataCUGEX1 = { DBContainer CUGEX1 ->
        String oA230 = CUGEX1.get("F1A230")
        String oPK05 = CUGEX1.get("F1PK05")
        logger.debug("A230 = " + oA230)
        if(oA230.trim().equals("20")){
Integer oZFIN = retriveZFIN(oPK05)
LocalDateTime timeOfCreation = LocalDateTime.now()
foundCUGEX1 = true
DBAction addQuery = database.table("EXT410").index("00").selection("EXZMOD","EXCHID","EXCHNO").build()
DBContainer EXT410 = addQuery.getContainer()
            EXT410.set("EXBJNO", BJNO)
            EXT410.set("EXZPMF",CUGEX1.get("F1FILE"))
        EXT410.set("EXZPM1",CUGEX1.get("F1PK01"))
        EXT410.set("EXZPM2",CUGEX1.get("F1PK02"))
        EXT410.set("EXZPM3",CUGEX1.get("F1PK03"))
        EXT410.set("EXZPM4",CUGEX1.get("F1PK04"))
        EXT410.set("EXZPM5",CUGEX1.get("F1PK05"))
        EXT410.set("EXINOU",INOU)
            EXT410.set("EXDLIX",DLIX)
            if (!addQuery.read(EXT410)) {
        EXT410.set("EXCONO",currentCompany)
                EXT410.set("EXZMOD", CUGEX1.get("F1A130"))
        EXT410.set("EXZFIN", oZFIN)
                EXT410.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT410.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        EXT410.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT410.setInt("EXCHNO", 1)
                EXT410.set("EXCHID", program.getUser())
        addQuery.insert(EXT410)
            }
                    }
                    }

/** Retrieve ZFIN
 * @param PK05 : End of time slot
 * @return 1 if current time is in the time slot else 0
 */
private Integer retriveZFIN(String PK05){
    MVXD = ""
    getCRS881("EXT410", "1", "FinTrancheHoraire", "I", "DeliveryIndex", "FinTrancheHoraire", "", "")
    String numberMinutes = MVXD.trim()
    if(numberMinutes.trim() == ""){
        return 0
    }

    String endTimeSlot = PK05.trim()

    Integer endTimeSlotInt = endTimeSlot.toInteger()
    Integer numberMinutesInt = numberMinutes.toInteger()

    Integer hours = (endTimeSlotInt / 100).toInteger()
    Integer minutes = (endTimeSlotInt % 100).toInteger()

    Integer totalMinutes = hours * 60 + minutes - numberMinutesInt

    Integer newHours = (totalMinutes / 60).toInteger()
    Integer newMinutes = (totalMinutes % 60).toInteger()

    String startEndOfTimeSlot = String.format("%02d%02d", newHours, newMinutes)

    if(timeDay>=startEndOfTimeSlot){
        return 1
    }
    return 0
}

/** Call CRS881 to retrieve MVXD
 * @param MSTD : Program name
 * @param MVRS : Program version
 * @param BMSG : Business message
 * @param IBOB : Input/Output type
 * @param ELMP : Element name for input
 * @param ELMD : Element name for input
 * @param ELMC : Element name for input
 * @param MBMC : Element name for output
 */
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

// Delete EXT410
public void initEXT410() {
    DBAction query = database.table("EXT410").index("00").build()
    DBContainer EXT410 = query.getContainer()
    EXT410.set("EXBJNO", BJNO)

    Closure<?> deleteWorkFile = { DBContainer readResult ->
            query.readLock(readResult, { LockedResult lockedResult ->
                    lockedResult.delete()
            })
    }

    query.readAll(EXT410, 1, nbMaxRecord, deleteWorkFile)
}

// Delete EXT411
public void initEXT411() {
    DBAction query = database.table("EXT411").index("00").build()
    DBContainer EXT411 = query.getContainer()
    EXT411.set("EXBJNO", BJNO)
    EXT411.set("EXCONO", currentCompany)

    Closure<?> deleteWorkFile = { DBContainer readResult ->
            query.readLock(readResult, { LockedResult lockedResult ->
                    lockedResult.delete()
            })
    }

    query.readAll(EXT411, 2, nbMaxRecord, deleteWorkFile)
}

// Delete EXT412
public void initEXT412() {
    DBAction query = database.table("EXT412").index("00").build()
    DBContainer EXT412 = query.getContainer()
    EXT412.set("EXBJNO", BJNO)
    EXT412.set("EXCONO", currentCompany)

    Closure<?> deleteWorkFile = { DBContainer readResult ->
            query.readLock(readResult, { LockedResult lockedResult ->
                    lockedResult.delete()
            })
    }

    query.readAll(EXT412, 2, nbMaxRecord, deleteWorkFile)
}

// Delete EXT413
public void initEXT413() {
    DBAction query = database.table("EXT413").index("00").build()
    DBContainer EXT413 = query.getContainer()
    EXT413.set("EXBJNO", BJNO)
    EXT413.set("EXCONO", currentCompany)

    Closure<?> deleteWorkFile = { DBContainer readResult ->
            query.readLock(readResult, { LockedResult lockedResult ->
                    lockedResult.delete()
            })
    }

    query.readAll(EXT413, 2, nbMaxRecord, deleteWorkFile)
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
