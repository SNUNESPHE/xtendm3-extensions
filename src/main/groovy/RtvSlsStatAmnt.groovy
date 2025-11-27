/****************************************************************************************
 Extension Name: EXT030MI.RtvSlsStatAmnt
 Type: ExtendM3Transaction
 Script Author: ARENARD
 Date: 2025-10-03
 Description:
 * Retrieve sales statistic amount
 * 5209 - Franco de port

 Revision History:
 Name                    Date             Version          Description of Changes
 ARENARD                 2025-10-03       1.0              Creation
 ARENARD                 2025-10-14       1.1              EXT033 insertion added
 ******************************************************************************************/
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
public class RtvSlsStatAmnt extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private int currentCompany
  private String currentDivision
  private double ntam = 0.0

  public RtvSlsStatAmnt(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  public void main() {
    currentCompany = (Integer)program.getLDAZD().CONO

    // Get division
    if (mi.in.get("DIVI") != null) {
      currentDivision = mi.in.get("DIVI")
    } else {
      currentDivision = program.getLDAZD().DIVI
    }

    // Check order number
    if(mi.in.get("ORNO") != null && mi.in.get("ORNO") != "") {
      DBAction queryOOHEAD = database.table("OOHEAD").index("00").build()
      DBContainer OOHEAD = queryOOHEAD.getContainer()
      OOHEAD.set("OACONO", currentCompany)
      OOHEAD.set("OAORNO", mi.in.get("ORNO"))
      if(!queryOOHEAD.read(OOHEAD)){
        mi.error("La commande " + mi.in.get("ORNO") + " n'existe pas")
        return
      }
    } else {
      mi.error("Le N° de commande est obligatoire")
      return
    }

    // Check delivery number
    if(mi.in.get("DLIX") != null && mi.in.get("DLIX") != "") {
      DBAction queryMHDISH = database.table("MHDISH").index("00").build()
      DBContainer MHDISH = queryMHDISH.getContainer()
      MHDISH.set("OQCONO", currentCompany)
      MHDISH.set("OQINOU", 1)
      MHDISH.set("OQDLIX", mi.in.get("DLIX"))
      if (!queryMHDISH.read(MHDISH)) {
        long dlix = mi.in.get("DLIX")
        mi.error("Index de livraison " + dlix + " n'existe pas")
        return
      }
    } else {
      mi.error("Index de livraison est obligatoire")
      return
    }

    DBAction queryOSBSTD = database.table("OSBSTD").index("00").selection("UCSAAM").build()
    DBContainer OSBSTD = queryOSBSTD.getContainer()
    OSBSTD.set("UCCONO", currentCompany)
    OSBSTD.set("UCDIVI", currentDivision)
    OSBSTD.set("UCORNO", mi.in.get("ORNO"))
    OSBSTD.set("UCDLIX", mi.in.get("DLIX"))
    queryOSBSTD.readAll(OSBSTD, 4, 500, outDataOSBSTD)

    mi.outData.put("NTAM", ntam as String)
    mi.write()

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction queryEXT033 = database.table("EXT033").index("00").build()
    DBContainer EXT033 = queryEXT033.getContainer()
    EXT033.set("EXCONO", currentCompany)
    EXT033.set("EXDIVI", currentDivision)
    EXT033.set("EXORNO", mi.in.get("ORNO"))
    EXT033.set("EXDLIX", mi.in.get("DLIX"))
    if (!queryEXT033.read(EXT033)) {
      EXT033.set("EXNTAM", ntam)
      EXT033.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT033.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT033.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT033.setInt("EXCHNO", 1)
      EXT033.set("EXCHID", program.getUser())
      queryEXT033.insert(EXT033)
    }
  }

  // Retrieve OSBSTD
  Closure<?> outDataOSBSTD = { DBContainer OSBSTD ->
    ntam += OSBSTD.get("UCSAAM") as double
  }
}
