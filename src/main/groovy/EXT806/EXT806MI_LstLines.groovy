/************************************************************************************************************************************************
Extension Name: EXT806MI.LstLines
Type: ExtendM3Transaction
Script Author: NRAOEL
Date: 2024-10-22
Description:
* List lines from EXT806
Revision History:
Name          Date        Version   Description of Changes
NRAOEL        2024-10-22  1.0       Initial Release
NRAOEL        2025-11-27  1.1       Correction after validation submission
**************************************************************************************************************************************************/

public class LstLines extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  public int inCONO // Company
  public String inDIVI // Division
  public String inSTAT // Status
  private String inCOMP
  private String inDAT1
  private String inDAT2
  private int maxRecords //10000

  public LstLines(MIAPI mi, ProgramAPI program, DatabaseAPI database, MICallerAPI miCaller) {
    this.mi = mi
    this.program = program
    this.database = database
    this.miCaller = miCaller
  }

  public void main() {
    maxRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000 ? 10000 : mi.getMaxRecords()

    //input
    inCONO = program.LDAZD.CONO
    if (!mi.inData.get("CONO").isBlank()) {
      inCONO = mi.in.get("CONO")
    }
    inDIVI = mi.inData.get("DIVI").isBlank() ? program.LDAZD.get("DIVI") : mi.inData.get("DIVI").trim()
    inCOMP = mi.inData.get("COMP").isBlank() ? "" : mi.inData.get("COMP").trim()
    inDAT1 = mi.inData.get("DAT1").isBlank() ? "" : mi.inData.get("DAT1").trim()
    inDAT2 = mi.inData.get("DAT2").isBlank() ? "" : mi.inData.get("DAT2").trim()
    
    ExpressionFactory expression = database.getExpressionFactory("EXT806")
    if (mi.inData.get("STAT").isBlank()) {
      expression = expression.eq("EXSTAT", "10").or(expression.gt("EXSTAT", "10"))
    } else {
      inSTAT = mi.inData.get("STAT")
      expression = expression.eq("EXSTAT", inSTAT)
    }
    
    if (!inCOMP.equals("")) {
      expression = addExpression(expression, expression.eq("COMP", inCOMP.toString()))
    }

    if (!inDAT1.equals("")) {
      expression = addExpression(expression, expression.ge("RGDT", inDAT1))
    }
    if (!inDAT2.equals("")) {
      expression = addExpression(expression, expression.le("RGDT", inDAT2))
    }
    

    DBAction dbaEXT806 = database.table("EXT806").index("00").selection("EXVONO", "EXVSER", "EXTYLI", "EXPYNO", "EXACDT", "EXCINO", "EXINYR", "EXCUCD", "EXCUAM", "EXPYCD", "EXDUDT", "EXVTXT", "EXORNO", "EXSTAT", "EXCOMP", "EXDATE", "EXAIT1", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID", "EXLMTS", "EXNCRE", "EXRMNO", "EXPRCD", "EXOPNO", "EXLINO", "EXACSO", "EXBKAC", "EXCONM", "EXCCD6", "EXCORG", "EXCUNM", "EXCOR2", "EXCUA1", "EXCUA2", "EXPONO", "EXTOWN", "EXCSCD", "EXPHNO", "EXACRF", "EXPYCU").matching(expression).build()
    DBContainer conEXT806 = dbaEXT806.getContainer()

    conEXT806.set("EXCONO", inCONO)

    Closure < ? > listRecords = {
      DBContainer data ->
      mi.outData.put("CONO", data.("EXCONO").toString())
      mi.outData.put("DIVI", data.("EXDIVI").toString())
      mi.outData.put("YEA4", data.("EXYEA4").toString())
      mi.outData.put("JRNO", data.("EXJRNO").toString())
      mi.outData.put("JSNO", data.("EXJSNO").toString())
      mi.outData.put("VONO", data.("EXVONO").toString())
      mi.outData.put("VSER", data.("EXVSER").toString())
      mi.outData.put("TYLI", data.("EXTYLI").toString())
      mi.outData.put("PYNO", data.("EXPYNO").toString())
      mi.outData.put("ACDT", data.("EXACDT").toString())
      mi.outData.put("CINO", data.("EXCINO").toString())
      mi.outData.put("INYR", data.("EXINYR").toString())
      mi.outData.put("CUCD", data.("EXCUCD").toString())
      mi.outData.put("CUAM", data.("EXCUAM").toString())
      mi.outData.put("PYCD", data.("EXPYCD").toString())
      mi.outData.put("DUDT", data.("EXDUDT").toString())
      mi.outData.put("VTXT", data.("EXVTXT").toString())
      mi.outData.put("ORNO", data.("EXORNO").toString())
      mi.outData.put("STAT", data.("EXSTAT").toString())
      mi.outData.put("COMP", data.("EXCOMP").toString())
      mi.outData.put("DATE", data.("EXDATE").toString())
      mi.outData.put("AIT1", data.("EXAIT1").toString())
      mi.outData.put("RGDT", data.("EXRGDT").toString())
      mi.outData.put("RGTM", data.("EXRGTM").toString())
      mi.outData.put("LMDT", data.("EXLMDT").toString())
      mi.outData.put("CHNO", data.("EXCHNO").toString())
      mi.outData.put("CHID", data.("EXCHID").toString())
      mi.outData.put("LMTS", data.("EXLMTS").toString())
      mi.outData.put("NCRE", data.("EXNCRE").toString())
      mi.outData.put("RMNO", data.("EXRMNO").toString())
      mi.outData.put("PRCD", data.("EXPRCD").toString())
      mi.outData.put("OPNO", data.("EXOPNO").toString())
      mi.outData.put("LINO", data.("EXLINO").toString())
      mi.outData.put("ACSO", data.("EXACSO").toString())
      mi.outData.put("BKAC", data.("EXBKAC").toString())
      mi.outData.put("CONM", data.("EXCONM").toString())
      mi.outData.put("CCD6", data.("EXCCD6").toString())
      mi.outData.put("CORG", data.("EXCORG").toString())
      mi.outData.put("CUNM", data.("EXCUNM").toString())
      mi.outData.put("COR2", data.("EXCOR2").toString())
      mi.outData.put("CUA1", data.("EXCUA1").toString())
      mi.outData.put("CUA2", data.("EXCUA2").toString())
      mi.outData.put("PONO", data.("EXPONO").toString())
      mi.outData.put("TOWN", data.("EXTOWN").toString())
      mi.outData.put("CSCD", data.("EXCSCD").toString())
      mi.outData.put("PHNO", data.("EXPHNO").toString())
      mi.outData.put("ACRF", data.("EXACRF").toString())
      mi.outData.put("PYCU", data.("EXPYCU").toString())
      mi.write()
    }
    if (!dbaEXT806.readAll(conEXT806, 1, maxRecords, listRecords)) {
      mi.error("Record(s) does not exist.")
      return
    }
  }
  
  /**
   * @ExpressionFactory - Concat expressions 
   * @params - expression
   * @returns - expressions
   */
  ExpressionFactory addExpression(ExpressionFactory current, ExpressionFactory newExpr) {
    return current == null ? newExpr : current.and(newExpr)
  }

}