/****************************************************************************************
 Extension Name: EXT018MI/GetItem
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELo
 Date: 2025-01-22
 Description:
 * Get item from the table EXT018. 
    
 Revision History:
 Name                       Date             Version          Description of Changes
 Tovonirina ANDRIANARIELO   2025-01-22       1.0              Initial Release
******************************************************************************************/
public class GetItem extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  // all the input variables
  private int inCONO // Company
  private String inDIVI //Division    
  private String inFACI // Facility
  private String inITNO // Item number
  private String inFILE // File
    
  public GetItem(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
    this.mi = mi
    this.program = program
    this.database = database
  }

  public void main() {
    // validate input variables
    if (!validateInputVariables()) {
      return
    }
    int maxRecords = 10000
    int nrOfKeys = 1
    DBAction query = database.table("EXT018")
      .index("00")
      .selection("EXCONO", "EXDIVI", "EXFACI", "EXITNO", "EXFILE","EXDAT1","EXRGDT","EXRGTM","EXCHID","EXCHNO","EXLMDT")
      .build()

    DBContainer container = query.getContainer()
    container.set("EXCONO", inCONO) 
    container.set("EXDIVI", inDIVI) 
    container.set("EXFACI", inFACI)
    container.set("EXITNO", inITNO)
    container.set("EXFILE", inFILE)
    if(query.read(container)){
      mi.outData.put("CONO", container.get("EXCONO") as String)
      mi.outData.put("DIVI", container.get("EXDIVI") as String)
      mi.outData.put("FILE", container.get("EXFILE") as String)
      mi.outData.put("FACI", container.get("EXFACI") as String)
      mi.outData.put("ITNO", container.get("EXITNO") as String)
      mi.outData.put("DAT1", container.get("EXDAT1") as String)
      mi.outData.put("RGDT", container.get("EXRGDT") as String)
      mi.outData.put("RGTM", container.get("EXRGTM") as String)
      mi.outData.put("CHID", container.get("EXCHID") as String)
      mi.outData.put("CHNO", container.get("EXCHNO") as String)
      mi.outData.put("LMDT", container.get("EXLMDT") as String)
      mi.write()
    }else{
      mi.error("No data found")
    }
  }

  /**
   * @description - Validates input variables
   * @params -
   * @returns - true/false
   */
  boolean validateInputVariables() {
    // Handling Company
    if (!mi.in.get('CONO')) {
      inCONO = (Integer) program.getLDAZD().CONO
    } else {
      inCONO = mi.in.get('CONO') as int
    }
    // Handling Division
    if (!mi.in.get('DIVI')) {
      mi.error("La société est obligatoire")
      return false
    }else{
      inDIVI = mi.in.get('DIVI') as String
    }
    // Handling Facility
    if (!mi.in.get('FACI')) {
      mi.error("L'établissement est obligatoire")
      return false
    }else{
      inFACI = mi.in.get('FACI') as String
    }
    // Handling Item number
    if(!mi.in.get('ITNO')) {
      mi.error("Le numéro d'article est obligatoire")
      return false
    }else{
      inITNO = mi.in.get('ITNO') as String
    }
    //handling FILE
    if(!mi.in.get('FILE')) {
      mi.error("Le tableau est obligatoire")
      return false
    }else{
      inFILE = mi.in.get('FILE') as String
    }
    return true
  }
}
