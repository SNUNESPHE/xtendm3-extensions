/****************************************************************************************
 * Extension Name: EXT370MI.DelLineOA
 * Type : Transaction API
 * Script Author:Ya'Sin Figuelia
 * Date: 2024-09-03
 * 
 * Description: Delete line in FGINLI on Inverse cloture OA
 * 
 * Revision History:
 * Name                           Date            Version               Description of Changes
 * Ya'Sin Figuelia                2024-09-03 1.0  Initial Release
 * Ya'Sin Figuelia                2024-10-23      1.1                   Update code according to the validation process
 * ANDRIANARIVELO Tovonirina      2025-09-04      1.2                   Review for validation
 * ANDRIANARIVELO Tovonirina      2025-11-17      1.3                   Update code according to the validation process
 ******************************************************************************************/
public class DelLineOA extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program

  private int inCONO // Company
  private String inDIVI // Division
  private String inSUNO // Supplier
  private  String inPUNO // Purchase Order
  private String inSINO // Supplier number
  private int inINYR // Invoice Year
  private int inPNLI // PO Line
  private int inPNLS // PO Line sub no
  private int inREPN // Receiving number
  private int inRELP // Receipt type
  private int inINLP // Inv line type

  public DelLineOA(MIAPI mi,DatabaseAPI database, ProgramAPI program) {
    this.mi = mi
    this.database = database
    this.program = program
  
  }

  public void main() {
    //Initialize input data
    inCONO = mi.in.get("CONO") as Integer == null ? program.LDAZD.get("CONO") as Integer : mi.in.get("CONO") as Integer
    inDIVI = mi.inData.get("DIVI") == null ? "" :  mi.inData.get("DIVI").trim()
    inSUNO = mi.inData.get("SUNO").trim()
    inSINO = mi.inData.get("SINO").trim()
    inINYR = mi.in.get("INYR") ? (Integer)mi.in.get("INYR") : 0
    inPUNO = mi.inData.get("PUNO").trim()
    inPNLI = (Integer)mi.in.get("PNLI")
    inPNLS = (Integer)mi.in.get("PNLS") 
    inREPN = (Integer)mi.in.get("REPN") 
    inRELP = (Integer)mi.in.get("RELP")
    inINLP = (Integer)mi.in.get("INLP")
    //Select record in FGINLI
    DBAction dbaFGINLI = database.table("FGINLI").index("00").build()
    DBContainer conFGINLI = dbaFGINLI.getContainer()
    conFGINLI.set("F5CONO", inCONO)
    if(inDIVI){    
      conFGINLI.set("F5DIVI", inDIVI)
    }
    conFGINLI.set("F5SUNO", inSUNO)
    conFGINLI.set("F5SINO", inSINO)
    conFGINLI.set("F5INYR", inINYR) 
    conFGINLI.set("F5PUNO", inPUNO)
    conFGINLI.set("F5PNLI", inPNLI)
    conFGINLI.set("F5PNLS", inPNLS)
    conFGINLI.set("F5REPN", inREPN)
    conFGINLI.set("F5RELP", inRELP)
    conFGINLI.set("F5INLP", inINLP)
  
    dbaFGINLI.readLock(conFGINLI, { 
      LockedResult lockedResult ->
        lockedResult.delete()
      })
  }
}