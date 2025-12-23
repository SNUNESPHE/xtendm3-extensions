/************************************************************************************************************************************************
Extension Name: EXT806MI.LstControle
Type: ExtendM3Transaction
Script Author: NRAOEL
Date: 2024-10-22
Description:
* List Lines from EXT806

Revision History:
Name          Date        Version   Description of Changes
NRAOEL        2024-10-22  1.0       Initial Release
NRAOEL        2025-11-27  1.1       Correction after validation submission
**************************************************************************************************************************************************/

public class LstControle extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  public int inCONO //Company
  public int maxRecords
  public String inDIVI //Division
  private String inCUN1 //First Payer
  private String inCUN2 //Last Payer
  private String inACD1 //First accounting date
  private String inACD2 //Last accounting date
  private String inDUD1 //First due date
  private String inDUD2 //Last due date
  private String inAIT1 //First account
  private String inAIT2 //Last account
  private String inBKA1 //First bank account
  private String inBKA2 //Last bank account
  private String inTYL1 //Card type
  private String inTYL2 //Card type
  private String inSTT1 //Status
  private String inSTT2 //Status
  private float inCUAM //Accounting dim

  public String ccd6 //Code cédant

  public LstControle(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
    this.mi = mi
    this.program = program
    this.database = database
  }

  public void main() {
    maxRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000 ? 10000 : mi.getMaxRecords()

    if (!mi.inData.get("CONO").isBlank()) {
      inCONO = mi.in.get("CONO") as Integer
    } else {
      inCONO = program.LDAZD.get("CONO") as Integer
    }
    inDIVI = mi.inData.get("DIVI").isBlank() ? program.LDAZD.get("DIVI") : mi.inData.get("DIVI").trim()
    inCUN1 = mi.inData.get("CUN1").isBlank() ? "" : mi.inData.get("CUN1").trim()
    inCUN2 = mi.inData.get("CUN2").isBlank() ? "" : mi.inData.get("CUN2").trim()
    inACD1 = mi.inData.get("ACD1").isBlank() ? "" : mi.inData.get("ACD1").trim()
    inACD2 = mi.inData.get("ACD2").isBlank() ? "" : mi.inData.get("ACD2").trim()
    inDUD1 = mi.inData.get("DUD1").isBlank() ? "" : mi.inData.get("DUD1").trim()
    inDUD2 = mi.inData.get("DUD2").isBlank() ? "" : mi.inData.get("DUD2").trim()
    inAIT1 = mi.inData.get("AIT1").isBlank() ? "" : mi.inData.get("AIT1").trim()
    inAIT2 = mi.inData.get("AIT2").isBlank() ? "" : mi.inData.get("AIT2").trim()
    inBKA1 = mi.inData.get("BKA1").isBlank() ? "" : mi.inData.get("BKA1").trim()
    inBKA2 = mi.inData.get("BKA2").isBlank() ? "" : mi.inData.get("BKA2").trim()
    inTYL1 = mi.inData.get("TYL1").isBlank() ? "" : mi.inData.get("TYL1").trim()
    inTYL2 = mi.inData.get("TYL2").isBlank() ? "" : mi.inData.get("TYL2").trim()
    inSTT1 = mi.inData.get("STT1").isBlank() ? "" : mi.inData.get("STT1").trim()
    inSTT2 = mi.inData.get("STT2").isBlank() ? "" : mi.inData.get("STT2").trim()
    inCUAM = mi.in.get("CUAM") as Integer == null ? 0 : mi.in.get("CUAM") as Integer

    ccd6 = readCMNDIV("CCD6")
    if (ccd6.trim().equals("")) {
      mi.error("La societe n'a pas de code cedant")
      return
    }

    ExpressionFactory expressionFactory = database.getExpressionFactory("EXT806")
    ExpressionFactory expression = expressionFactory.gt("TYLI", "100")

    if (!inTYL1.equals("")) {
      expression = addExpression(expression, expressionFactory.ge("TYLI", inTYL1))
    }
    if (!inTYL2.equals("")) {
      expression = addExpression(expression, expressionFactory.le("TYLI", inTYL2))
    }

    if (!inSTT1.equals("")) {
      expression = addExpression(expression, expressionFactory.ge("STAT", inSTT1))
    }
    if (!inSTT2.equals("")) {
      expression = addExpression(expression, expressionFactory.le("STAT", inSTT2))
    }

    if (!inCUN1.equals("")) {
      expression = addExpression(expression, expressionFactory.ge("PYNO", inCUN1))
    }
    if (!inCUN2.equals("")) {
      expression = addExpression(expression, expressionFactory.le("PYNO", inCUN2))
    }

    if (!inACD1.equals("")) {
      expression = addExpression(expression, expressionFactory.ge("ACDT", inACD1))
    }
    if (!inACD2.equals("")) {
      expression = addExpression(expression, expressionFactory.le("ACDT", inACD2))
    }

    if (!inDUD1.equals("")) {
      expression = addExpression(expression, expressionFactory.ge("DUDT", inDUD1))
    }
    if (!inDUD2.equals("")) {
      expression = addExpression(expression, expressionFactory.le("DUDT", inDUD2))
    }

    if (!inAIT1.equals("")) {
      expression = addExpression(expression, expressionFactory.ge("AIT1", inAIT1))
    }
    if (!inAIT2.equals("")) {
      expression = addExpression(expression, expressionFactory.le("AIT1", inAIT2))
    }

    if (!inBKA1.equals("")) {
      expression = addExpression(expression, expressionFactory.ge("BKAC", inBKA1))
    }
    if (!inBKA2.equals("")) {
      expression = addExpression(expression, expressionFactory.le("BKAC", inBKA2))
    }

    DBAction dbaEXT806 = database.table("EXT806").index("00").selection("EXVONO", "EXVSER", "EXTYLI", "EXPYNO", "EXACDT", "EXCINO", "EXINYR", "EXCUCD", "EXCUAM", "EXPYCD", "EXDUDT", "EXVTXT", "EXORNO", "EXSTAT", "EXCOMP", "EXDATE", "EXAIT1", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHID", "EXCHNO", "EXLMTS", "EXNCRE", "EXRMNO", "EXPRCD", "EXOPNO", "EXLINO", "EXACSO", "EXBKAC", "EXCORG", "EXCOR2", "EXCUNM", "EXCUA1", "EXCUA2", "EXPONO", "EXTOWN", "EXCSCD", "EXVRNO").matching(expression).build()
    DBContainer conEXT806 = dbaEXT806.getContainer()
    conEXT806.set("EXCONO", inCONO)
    conEXT806.set("EXDIVI", inDIVI)

    Closure < ? > listRecords = {
      DBContainer data ->
      float cuam = Math.abs(Float.parseFloat(data.("EXCUAM").toString()))

      if (inCUAM <= cuam) {
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
        mi.outData.put("CHID", data.("EXCHID").toString())
        mi.outData.put("CHNO", data.("EXCHNO").toString())
        mi.outData.put("LMTS", data.("EXLMTS").toString())
        mi.outData.put("NCRE", data.("EXNCRE").toString())
        mi.outData.put("RMNO", data.("EXRMNO").toString())
        mi.outData.put("PRCD", data.("EXPRCD").toString())
        mi.outData.put("OPNO", data.("EXOPNO").toString())
        mi.outData.put("LINO", data.("EXLINO").toString())
        mi.outData.put("ACSO", data.("EXACSO").toString())
        mi.outData.put("BKAC", data.("EXBKAC").toString())
        mi.outData.put("CORG", data.("EXCORG").toString())
        mi.outData.put("COR2", data.("EXCOR2").toString())
        mi.outData.put("CUNM", data.("EXCUNM").toString())
        mi.outData.put("CUA1", data.("EXCUA1").toString())
        mi.outData.put("CUA2", data.("EXCUA2").toString())
        mi.outData.put("PONO", data.("EXPONO").toString())
        mi.outData.put("TOWN", data.("EXTOWN").toString())
        mi.outData.put("CSCD", data.("EXCSCD").toString())
        mi.outData.put("VRNO", data.("EXVRNO").toString())

        mi.write()
      }

    }
    if (!dbaEXT806.readAll(conEXT806, 2, maxRecords, listRecords)) {
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

  /**
   * @readCMNDIV - get lines from CMNDIV    
   * @params - outBound
   * @returns - conm, ccd6
   */
  String readCMNDIV(String outBound) {
    DBAction query = database.table("CMNDIV")
      .index("00")
      .selection("CCCONM", "CCCCD6")
      .build()
    DBContainer container = query.getContainer()
    container.set("CCCONO", inCONO)
    container.set("CCDIVI", inDIVI)

    String result = ""
    query.readAll(container, 2, 1, {
      DBContainer container1 ->
      if (outBound.equals("CCD6")) {
        result = container1.get("CCCCD6").toString()
      } else if (outBound.equals("CONM")) {
        result = container1.get("CCCONM").toString()
      }
    })
    return result
  }
}