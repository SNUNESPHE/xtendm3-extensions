/****************************************************************************************
 Extension Name: EXT090MI.GetEntitlmtInfo
 Type: ExtendM3Transaction
 Script Author: ARENARD
 Date: 2024-09-11
 Description:
 * Retrieve records from the EXT090 table
 * 5158 – Création des retours clients

 Revision History:
 Name                    Date             Version          Description of Changes
 ARENARD                 2024-09-11       1.0              5158 Création des retours clients
 ARENARD                 2025-09-18       1.1              Standardization of the program header comment block
 ******************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class GetEntitlmtInfo extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  public GetEntitlmtInfo(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
  }

  public void main() {
    Integer currentCompany
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    DBAction query = database.table("EXT090").index("00").selection("EXQTRC","EXQTRA","EXQREC","EXQREA","EXQTNC","EXQTNA","EXRGDT","EXRGTM","EXLMDT","EXCHNO","EXCHID").build()
    DBContainer EXT090 = query.getContainer()
    EXT090.set("EXCONO", currentCompany)
    EXT090.set("EXENNO",  mi.in.get("ENNO"))
    if(query.read(EXT090)) {
      String QTRC = EXT090.get("EXQTRC")
      String QTRA = EXT090.get("EXQTRA")
      String QREC = EXT090.get("EXQREC")
      String QREA = EXT090.get("EXQREA")
      String QTNC = EXT090.get("EXQTNC")
      String QTNA = EXT090.get("EXQTNA")
      String entryDate = EXT090.get("EXRGDT")
      String entryTime = EXT090.get("EXRGTM")
      String changeDate = EXT090.get("EXLMDT")
      String changeNumber = EXT090.get("EXCHNO")
      String changedBy = EXT090.get("EXCHID")
      mi.outData.put("QTRC", QTRC)
      mi.outData.put("QTRA", QTRA)
      mi.outData.put("QREC", QREC)
      mi.outData.put("QREA", QREA)
      mi.outData.put("QTNC", QTNC)
      mi.outData.put("QTNA", QTNA)
      mi.outData.put("RGDT", entryDate)
      mi.outData.put("RGTM", entryTime)
      mi.outData.put("LMDT", changeDate)
      mi.outData.put("CHNO", changeNumber)
      mi.outData.put("CHID", changedBy)
      mi.write()
    } else {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
