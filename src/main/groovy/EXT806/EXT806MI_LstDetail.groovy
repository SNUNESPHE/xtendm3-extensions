/************************************************************************************************************************************************
Extension Name: EXT806MI.LstDetail
Type: ExtendM3Transaction
Script Author: NRAOEL
Date: 2024-10-22
Description:
* List Lines from EXT810

Revision History:
Name          Date        Version   Description of Changes
NRAOEL        2025-11-28  1.0       Initial Release
NRAOEL        2025-12-16  1.1       Correction after validation submission
**************************************************************************************************************************************************/

public class LstDetail extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database

  public int inCONO // Company
  public String inDIVI // Division
  public String inPYNO //Payer
  public String inCIN1
  public String inCIN2
  public int maxRecords

  public LstDetail(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
    this.mi = mi
    this.program = program
    this.database = database
  }

  public void main() {
    inCONO = !mi.inData.get("CONO").isBlank() ? mi.in.get("CONO") as Integer : program.LDAZD.get("CONO") as Integer
    inDIVI = mi.inData.get("DIVI").isBlank() ? program.LDAZD.get("DIVI") : mi.inData.get("DIVI").trim()
    inPYNO = mi.inData.get("PYNO").isBlank() ? "" : mi.inData.get("PYNO").trim()
    inCIN1 = mi.inData.get("CIN1").isBlank() ? "" : mi.inData.get("CIN1").trim()
    inCIN2 = mi.inData.get("CIN2").isBlank() ? "" : mi.inData.get("CIN2").trim()
    maxRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() > 10000 ? 10000 : mi.getMaxRecords()
    
    ExpressionFactory expressionFactory = database.getExpressionFactory("EXT810")
    ExpressionFactory expression = expressionFactory.ne("ASGN", "")

    if (inCIN1 != null && !inCIN1.trim().isEmpty()) {
      expression = addExpression(expression, expressionFactory.ge("CINO", inCIN1))
    }

    if (inCIN2 != null && !inCIN2.trim().isEmpty()) {
      expression = addExpression(expression, expressionFactory.le("CINO", inCIN2))
    }

    DBAction dbaEXT810 = database.table("EXT810")
      .index("00")
      .selection("EXCUA1", "EXCUA2", "EXCUA3", "EXCUA4", "EXACDT", "EXASGN")
      .matching(expression)
      .build()

    DBContainer conEXT810 = dbaEXT810.getContainer()
    conEXT810.set("EXCONO", inCONO)
    conEXT810.set("EXDIVI", inDIVI)
    conEXT810.set("EXPYNO", inPYNO)

    Closure<?> listRecords = { DBContainer data ->

      mi.outData.put("CONO", inCONO.toString())
      mi.outData.put("DIVI", inDIVI)
      mi.outData.put("ASGN", data.get("EXASGN").toString())
      mi.outData.put("PYNO", data.get("EXPYNO").toString())
      mi.outData.put("CUA1", data.get("EXCUA1").toString())
      mi.outData.put("CUA2", data.get("EXCUA2").toString())
      mi.outData.put("CUA3", data.get("EXCUA3").toString())
      mi.outData.put("CUA4", data.get("EXCUA4").toString())
      mi.outData.put("CINO", data.get("EXCINO").toString())
      mi.outData.put("ACDT", data.get("EXACDT").toString())

      mi.write()
    }

    if (!dbaEXT810.readAll(conEXT810, 3, maxRecords, listRecords)) {
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
