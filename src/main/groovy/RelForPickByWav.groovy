/**
 * README
 * This extension is used by scheduler
 *
 * Name : EXT410MI.RelForPickByWav
 * Description : Release a delivery index for picking and update picking list and allocations with wave number
 * Date         Changed By   Description
 * 20240619     ARNREN       5148- Gestion planification des vagues de prélèments
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class RelForPickByWav extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private int currentCompany
  private int olup
  private long dlix
  private int pgrs
  private String PLRI

  public RelForPickByWav(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    // Check item
    dlix = mi.in.get("DLIX") as long
    if(mi.in.get("DLIX") != null){
      DBAction query = database.table("MHDISH").index("00").selection("OQDLIX", "OQPGRS").build()
      DBContainer MHDISH = query.getContainer()
      MHDISH.set("OQCONO", currentCompany)
      MHDISH.set("OQINOU", 1)
      MHDISH.set("OQDLIX", dlix)
      if (!query.read(MHDISH)) {
        mi.error("Index " + dlix + " n'existe pas")
        return
      } else {
        pgrs = MHDISH.get("OQPGRS").toString() as int
        if(pgrs >= 50) {
          mi.error("Index " + dlix + " déjà libéré pour prélèvement")
          return
        }
      }
    } else {
      mi.error("Index est obligatoire")
      return
    }
    PLRI = ""
    if(mi.in.get("PLRI") != null){
      PLRI = mi.in.get("PLRI")
    }

    // Release for picking
    olup = 1
    executeMWS410MIRelForPick(currentCompany+"", dlix+"", olup+"")

    DBAction query = database.table("MHDISH").index("00").selection("OQDLIX", "OQPGRS").build()
    DBContainer MHDISH = query.getContainer()
    MHDISH.set("OQCONO", currentCompany)
    MHDISH.set("OQINOU", 1)
    MHDISH.set("OQDLIX", dlix)
    if (query.read(MHDISH)) {
      pgrs = MHDISH.get("OQPGRS").toString() as int
      if(pgrs >= 50) {
        if(PLRI == "") {
          // Retrieve wave number
          executeCRS165MIRtvNextNumber("13", "W")
        }
        PLRI = PLRI.trim()
        PLRI = PLRI.padLeft(10)
        DBAction queryMHPICH = database.table("MHPICH").index("00").selection("PIPLRI").build()
        DBContainer MHPICH = queryMHPICH.getContainer()
        MHPICH.set("PICONO", currentCompany)
        MHPICH.set("PIDLIX", dlix)
        MHPICH.set("PIPLSX", 1)
        if (!queryMHPICH.readLock(MHPICH, updateMHPICH)) {
        }
        DBAction queryMITALO = database.table("MITALO").index("30").selection("MQPLRI").build()
        DBContainer MITALO = queryMITALO.getContainer()
        MITALO.set("MQCONO", currentCompany)
        MITALO.set("MQRIDI", dlix)
        if (queryMITALO.readAllLock(MITALO, 2, updateMITALO)) {

        }
      }
    }

    mi.outData.put("PLRI", PLRI)
    mi.write()
  }
  // Update MHPICH
  Closure<?> updateMHPICH = { LockedResult lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("PICHNO")
    lockedResult.set("PIPLRI", PLRI)
    lockedResult.setInt("PILMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("PICHNO", changeNumber + 1)
    lockedResult.set("PICHID", program.getUser())
    lockedResult.update()
  }

  // Update MITALO
  Closure<?> updateMITALO = { LockedResult lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("MQCHNO")
    lockedResult.set("MQPLRI", PLRI)
    lockedResult.setInt("MQLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("MQCHNO", changeNumber + 1)
    lockedResult.set("MQCHID", program.getUser())
    lockedResult.update()
  }

  // Release for picking
  private executeMWS410MIRelForPick(String CONO, String DLIX, String OLUP){
    Map<String, String> parameters = ["CONO": CONO, "DLIX": DLIX, "OLUP": OLUP]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
      }
    }
    miCaller.call("MWS410MI", "RelForPick", parameters, handler)
  }

  // Execute CRS165MI.RtvNextNumber
  private executeCRS165MIRtvNextNumber(String NBTY, String NBID){
    Map<String, String> parameters = ["NBTY": NBTY, "NBID": NBID]
    Closure<?> handler = { Map<String, String> response ->
      PLRI = response.NBNR.trim()

      if (response.error != null) {
        return mi.error("Failed CRS165MI.RtvNextNumber: "+ response.errorMessage)
      }
    }
    miCaller.call("CRS165MI", "RtvNextNumber", parameters, handler)
  }
}
