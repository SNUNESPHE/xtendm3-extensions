/****************************************************************************************
 Extension Name: EXT019MI/GetItem
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELo
 Date: 2025-01-22
 Description:
 * Get item from the table EXT019. 
    
 Revision History:
 Name                       Date             Version          Description of Changes
 Tovonirina ANDRIANARIELO   2025-01-22       1.0              Initial Release
 Tovonirina ANDRIANARIELO   2025-09-08       1.1              XtendM3 validation
******************************************************************************************/
public class GetItem extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  // all the input variables
  private int inCONO // Company
  private String inDIVI  //Division  
  private String inWHLO // Warehouse
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
    int nrOfKeys = 1
    DBAction query = database.table("EXT019")
      .index("00")
      .selection("EXCONO", "EXWHLO", "EXITNO", "EXFILE","EXDAT1","EXDAT2","EXIDDT", "EXODDT","EXRGDT","EXRGTM","EXCHID","EXCHNO","EXLMDT")
      .build()

    DBContainer container = query.getContainer()
    container.set("EXCONO", inCONO)
    container.set("EXDIVI", inDIVI)
    container.set("EXWHLO", inWHLO)
    container.set("EXITNO", inITNO)
    container.set("EXFILE", inFILE)
    if(query.read(container)){
      mi.outData.put("CONO", container.get("EXCONO") as String)
      mi.outData.put("FILE", container.get("EXFILE") as String)
      mi.outData.put("WHLO", container.get("EXWHLO") as String)
      mi.outData.put("ITNO", container.get("EXITNO") as String)
      mi.outData.put("DAT1", container.get("EXDAT1") as String)
      mi.outData.put("DAT2", container.get("EXDAT2") as String)
      mi.outData.put("IDDT", container.get("EXIDDT") as String)
      mi.outData.put("ODDT", container.get("EXODDT") as String)
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
      mi.error('La société est obligatoire')
      return false
    }else{
      inDIVI = mi.in.get('DIVI') as String
    }
    // Handling Warehouse
    if (!mi.in.get('WHLO')) {
      mi.error("Le dépôt est obligatoire")
      return false
    }else{
      inWHLO = mi.in.get('WHLO') as String
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
