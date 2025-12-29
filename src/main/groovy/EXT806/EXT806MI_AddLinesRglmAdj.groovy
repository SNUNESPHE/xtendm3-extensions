/************************************************************************************************************************************************
Extension Name: EXT806MI.AddLinesRglmAdj
Type: ExtendM3Transaction
Script Author: NRAOEL
Date: 2024-10-22
Description:
* Add Lines to the dynamic table EXT806 (virement, lettrage, impayee, remboursement, cheque/effet/prelevement avant remise)

Revision History:
Name          Date        Version   Description of Changes
NRAOEL        2024-10-22  1.0       Initial Release
NRAOEL        2025-11-27  1.1       Correction after validation submission
**************************************************************************************************************************************************/

import java.util.ArrayList
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

public class AddLinesRglmAdj extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  public int inCONO //Company
  public String inDIVI //Division
  private int inYEA4 //Year
  private int inJRNO //Journal number
  private int inJSNO //Journal sequence
  public int maxRecords //1000
  public String compteCollectifClient = "" //Client collective account number
  public String compteDedieFactor = "" //Bank account Factor
  public String conm = "" //Company name
  public String ccd6 = "" //Company number

  private ArrayList < HashMap < String, String >> output = new ArrayList < > () //Array containing accounting entries
  private ArrayList < HashMap < String, String >> output103104 = new ArrayList < > () //Array containing accounting entries
  private List < String > exclusionList //List of accounts number
  private List < String > account3 //List of accounts number
  private List < String > reglement //List of invoice payments
  private List < String > domTom //List country

  public AddLinesRglmAdj(MIAPI mi, ProgramAPI program, DatabaseAPI database, MICallerAPI miCaller) {
    this.mi = mi
    this.program = program
    this.database = database
    this.miCaller = miCaller
  }

  public void main() {
    maxRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 1000 ? 1000 : mi.getMaxRecords()

    // Initialisation
    if (!mi.inData.get("CONO").isBlank()) {
      inCONO = mi.in.get("CONO") as Integer
    } else {
      inCONO = program.LDAZD.get("CONO") as Integer
    }
    inDIVI = mi.inData.get("DIVI").isBlank() ? program.LDAZD.get("DIVI") : mi.inData.get("DIVI").trim()
    inYEA4 = mi.in.get("YEA4") as Integer == null ? 0 : mi.in.get("YEA4") as Integer
    inJRNO = mi.in.get("JRNO") as Integer == null ? 0 : mi.in.get("JRNO") as Integer
    inJSNO = mi.in.get("JSNO") as Integer == null ? 0 : mi.in.get("JSNO") as Integer

    conm = readCMNDIV("CONM")
    ccd6 = readCMNDIV("CCD6")

    if (ccd6.trim().equals("")) { // If the company don't have CCD6 field, the process is stopped
      mi.error("La societe n'a pas de code cedant")
      return
    }

    exclusionList = getCompteCUGEX("0")
    account3 = getCompteCUGEX("3")

    compteCollectifClient = getCUGEX1("1")
    compteDedieFactor = getCUGEX1("2")
    if (!compteDedieFactor.trim().equals("") && !compteCollectifClient.trim().equals("")) {
      reglement = lstCUGEX3("FEID")
      domTom = lstCUGEX3("CSCD")

      readFGLEDG()
      readFSLEDG()
      addCustomFields()
      combine()
      render()
    }
  }

  /**
   * @readFGLEDG - get fields from FGLEDG   
   * @params
   * @returns
   */
  void readFGLEDG() {
    ExpressionFactory expression = database.getExpressionFactory("FGLEDG")
    expression = expression.eq("EGAIT1", compteCollectifClient)

    DBAction query = database.table("FGLEDG")
      .index("00")
      .selection("EGVONO", "EGVSER", "EGACDT", "EGCUCD", "EGCUAM", "EGVTXT", "EGAIT1", "EGFEID", "EGTRCD", "EGDBCR")
      .matching(expression)
      .build()
    DBContainer container = query.getContainer()
    container.set("EGCONO", inCONO)
    container.set("EGDIVI", inDIVI)
    container.set("EGYEA4", inYEA4)
    container.set("EGJRNO", inJRNO)
    container.set("EGJSNO", inJSNO)

    HashMap < String, String > tmp
    query.readAll(container, 5, 1, { DBContainer container1 ->
      String gexi = getFGLEDX(Integer.parseInt(container1.get("EGYEA4").toString()), Integer.parseInt(container1.get("EGJRNO").toString()), Integer.parseInt(container1.get("EGJSNO").toString()), 500) //Check if the line has already been processed
      String feid = container1.get("EGFEID").toString() //Check if the line is an invoice payment
      if (
        gexi.trim().isBlank() && 
        reglement.contains(feid)) {
        tmp = new HashMap < String, String > ()
        tmp.put("CCD6", ccd6)
        tmp.put("CONM", conm)
        tmp.put("CONO", container1.get("EGCONO").toString())
        tmp.put("DIVI", container1.get("EGDIVI").toString())
        tmp.put("YEA4", container1.get("EGYEA4").toString())
        tmp.put("JRNO", container1.get("EGJRNO").toString())
        tmp.put("JSNO", container1.get("EGJSNO").toString())
        tmp.put("VONO", container1.get("EGVONO").toString())
        tmp.put("VSER", container1.get("EGVSER").toString())
        tmp.put("ACDT", container1.get("EGACDT").toString())
        tmp.put("CUCD", container1.get("EGCUCD").toString())
        tmp.put("CUAM", container1.get("EGCUAM").toString())
        tmp.put("VTXT", container1.get("EGVTXT").toString())
        tmp.put("AIT1", container1.get("EGAIT1").toString())
        tmp.put("DBCR", container1.get("EGDBCR").toString())
        tmp.put("FEID", feid)
        tmp.put("EGTRCD", container1.get("EGTRCD").toString())

        output.add(tmp)
      }
    })
  }

  /**
   * @readFSLEDG - get fields from FSLEDG - Join FGLEDG and FSLEDG
   * @params
   * @returns
   */
  void readFSLEDG() {
    ExpressionFactory expression = database.getExpressionFactory("FSLEDG")
    expression = expression
      .lt("ESACDT", getDATE().toString())
      .and(expression.eq("ESTRCD", "10").or(expression.eq("ESTRCD", "20")).or(expression.eq("ESTRCD", "21")))

    for (int i = 0; i < output.size(); i++) {
      DBAction query = database.table("FSLEDG")
        .index("00")
        .matching(expression)
        .selection("ESPYNO", "ESPYCD", "ESPYTP", "ESCINO", "ESINYR", "ESACDT", "ESTRCD", "ESDUDT")
        .build()
      DBContainer container = query.getContainer()
      container.set("ESCONO", Integer.parseInt(output.get(i).get("CONO")))
      container.set("ESDIVI", output.get(i).get("DIVI"))
      container.set("ESYEA4", Integer.parseInt(output.get(i).get("YEA4")))
      container.set("ESJRNO", Integer.parseInt(output.get(i).get("JRNO")))
      container.set("ESJSNO", Integer.parseInt(output.get(i).get("JSNO")))

      if (query.read(container)) {
        output.get(i).put("PYNO", container.get("ESPYNO").toString())
        output.get(i).put("PYCD", container.get("ESPYCD").toString())
        output.get(i).put("PYTP", container.get("ESPYTP").toString())
        output.get(i).put("CINO", container.get("ESCINO").toString())
        output.get(i).put("INYR", container.get("ESINYR").toString())
        output.get(i).put("DUDT", container.get("ESDUDT").toString())
        output.get(i).put("ESACDT", container.get("ESACDT").toString())
        output.get(i).put("ESTRCD", container.get("ESTRCD").toString())
        output.get(i).put("EXST", "OK")
      } else {
        output.get(i).put("EXST", "KO")
      }
    }
  }

  /**
   * @getOCUSMA - get fields from OCUSMA and store them
   * @params - cuno
   * @returns - the list of expected fields from OCUSMA
   */
  HashMap < String, String > getOCUSMA(String cuno) {
    DBAction query = database.table("OCUSMA")
      .index("00")
      .selection("OKCORG", "OKCOR2", "OKCUNM", "OKCUA1", "OKCUA2", "OKPONO", "OKTOWN", "OKCSCD", "OKPHNO", "OKACRF", "OKVRNO")
      .build()
    DBContainer container = query.getContainer()
    container.set("OKCONO", inCONO)
    container.set("OKCUNO", cuno)

    HashMap < String, String > result = new HashMap < > ()
    query.readAll(container, 2, 1, { DBContainer container1 ->
      result.put("CORG", container1.get("OKCORG").toString())
      result.put("COR2", container1.get("OKCOR2").toString())
      result.put("CUNM", container1.get("OKCUNM").toString())
      result.put("CUA1", container1.get("OKCUA1").toString())
      result.put("CUA2", container1.get("OKCUA2").toString())
      result.put("PONO", container1.get("OKPONO").toString())
      result.put("TOWN", container1.get("OKTOWN").toString())
      result.put("CSCD", container1.get("OKCSCD").toString())
      result.put("PHNO", container1.get("OKPHNO").toString())
      result.put("ACRF", container1.get("OKACRF").toString())
      result.put("VRNO", container1.get("OKVRNO").toString())
    })
    return result
  }

  /**
   * @getCUGEX3 - get F3TX40's field from CUGEVM
   * @params - cono, file, cuer, fldi, al30
   * @returns - tx40 description's field from CUGEVM
   */
  String getCUGEX3(int cono, String file, String cuer, String fldi, String al30) {
    DBAction query = database.table("CUGEVM")
      .index("00")
      .selection("F3TX40")
      .build()
    DBContainer container = query.getContainer()
    container.set("F3CONO", cono)
    container.set("F3FILE", file)
    container.set("F3CUER", cuer)
    container.set("F3FLDI", fldi)
    container.set("F3AL30", al30)

    String result = ""
    query.readAll(container, 5, 1, { DBContainer container1 ->
      result = container1.get("F3TX40").toString().trim()
    })
    return result
  }

  /**
   * @lstCUGEX3 - List fields from CUGEVM
   * @params - 
   * @returns - CSCD/FEID
   */
  List<String> lstCUGEX3(String cuer) {
    ExpressionFactory expressionFactory = database.getExpressionFactory("CUGEVM")
    ExpressionFactory expression = null

    DBAction query

    if ("FEID".equals(cuer)) {
        expression = expressionFactory.eq("F3TX40", "2")
        query = database.table("CUGEVM")
                        .index("00")
                        .selection("F3TX40")
                        .matching(expression)
                        .build()
    } else {
        query = database.table("CUGEVM")
                        .index("00")
                        .selection("F3TX40")
                        .build()
    }

    DBContainer container = query.getContainer()
    container.set("F3CONO", inCONO)
    container.set("F3FILE", "CUGEX3")
    container.set("F3CUER", cuer)
    container.set("F3FLDI", "F3A030")

    int nbrOfRecords = 50
    List<String> results = new ArrayList<>()
    query.readAll(container, 4, nbrOfRecords, { DBContainer container1 ->
        if (container1.get("F3AL30") != null) {
            results.add(container1.get("F3AL30").toString().trim())
        }
    })

    return results
}

  /**
   * @getCUGEX1 - Read Field from CUGEX1
   * @params - chb1
   * @returns - pk03 : account number
   */
  String getCUGEX1(String chb1) {
    ExpressionFactory expression = database.getExpressionFactory("CUGEX1")
    expression = expression.eq("F1CHB1", chb1).and(expression.eq("F1PK02", "1"))

    DBAction query = database.table("CUGEX1")
      .index("00")
      .selection("F1PK03")
      .matching(expression)
      .build()
    DBContainer container = query.getContainer()
    container.set("F1CONO", inCONO)
    container.set("F1FILE", "FCHACC")

    String result = ""
    query.readAll(container, 2, 1, { DBContainer container1 ->
      result = container1.get("F1PK03").toString().trim()
    })
    return result
  }

  /**
   * @getPYCU - get PYCU field from CUGEX1    
   * @params - pycd : payment method
   * @returns - pycu : cugex payment method
   */
  String getPYCU(String pycd) {
    ExpressionFactory expression = database.getExpressionFactory("CUGEX1")
    expression = expression.eq("F1PK02", "PYCD").and(expression.eq("F1PK03", pycd))

    DBAction query = database.table("CUGEX1")
      .index("00")
      .matching(expression)
      .selection("F1A130")
      .build()
    DBContainer container = query.getContainer()
    container.set("F1CONO", inCONO)
    container.set("F1FILE", "CSYTAB")

    String result = ""
    query.readAll(container, 2, 1, { DBContainer container1 ->
      result = container1.get("F1A130").toString().trim()
    })
    return result
  }

  /**
   * @readCMNDIV - get ccd6, conm from CMNDIV    
   * @params
   * @returns
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
    query.readAll(container, 2, 1, { DBContainer container1 ->
      if (outBound.equals("CCD6")) {
        result = container1.get("CCCCD6").toString()
      } else if (outBound.equals("CONM")) {
        result = container1.get("CCCONM").toString()
      }
    })
    return result
  }

  /**
   * @getORNO - REG05 : get order number from FSLEDX
   * @params - yea4, jrno, jsno
   * @returns - orno
   */
  String getORNO(int yea4, int jrno, int jsno) {
    return getFSLEDX(yea4, jrno, jsno, 203)
  }

  /**
   * @getFSLEDX - Read SEXI from FSLEDX
   * @params - yea4, jrno, jsno, sexn
   * @returns - sexi field from FSLEDX
   */
  String getFSLEDX(int yea4, int jrno, int jsno, int sexn) {
    DBAction query = database.table("FSLEDX")
      .index("00")
      .selection("ESSEXI")
      .build()
    DBContainer container = query.getContainer()
    container.set("ESCONO", inCONO)
    container.set("ESDIVI", inDIVI)
    container.set("ESYEA4", yea4)
    container.set("ESJRNO", jrno)
    container.set("ESJSNO", jsno)
    container.set("ESSEXN", sexn)

    String result = ""
    query.readAll(container, 6, 1, { DBContainer container1 ->
      result = container1.get("ESSEXI").toString()
    })
    return result
  }

  /**
   * @getFGLEDX - get GEXI field from FGLEDX
   * @params - yea4, jrno, jsno, gexn
   * @returns - gexi 
   */
  String getFGLEDX(int yea4, int jrno, int jsno, int gexn) {
    ExpressionFactory expression = database.getExpressionFactory("FGLEDX")
    expression = expression.eq("EGTXID", "9999999999999")
    
    DBAction query = database.table("FGLEDX")
      .index("00")
      .selection("EGGEXI")
      .matching(expression)
      .build()
    DBContainer container = query.getContainer()
    container.set("EGCONO", inCONO)
    container.set("EGDIVI", inDIVI)
    container.set("EGYEA4", yea4)
    container.set("EGJRNO", jrno)
    container.set("EGJSNO", jsno)
    container.set("EGGEXN", gexn)

    String result = ""
    query.readAll(container, 6, 1, { DBContainer container1 ->
      result = container1.get("EGGEXI").toString()
    })
    return result
  }

  /**
   * @getCKNO - REG10 : get CKNO from FSLEDX
   * @params - yea4, jrno, jsno, gexn
   * @returns -  ckno : check number
   */
  String getCKNO(int yea4, int jrno, int jsno) {
    return getFSLEDX(yea4, jrno, jsno, 214)
  }

  /**
   * @getSTMN - REG10 : get "Relevé" number from FSLEDX
   * @params - yea4, jrno, jsno, gexn
   * @returns -  "Relevé" number
   */
  String getSTMN(int yea4, int jrno, int jsno) {
    return getFSLEDX(yea4, jrno, jsno, 209)
  }

  /**
   * @getEFNO - REG10 : "Effet" number
   * @params - yea4, jrno, jsno, gexn
   * @returns -  "Effet" number from FSLEDX
   */
  String getEFNO(int yea4, int jrno, int jsno) {
    return getFSLEDX(yea4, jrno, jsno, 213)
  }

  /**
   * @getSTAT - REG06 : Status
   * @params - 
   * @returns - 10
   */
  int getSTAT() {
    return 10
  }

  /**
   * @getTYLI - REG03 : Line Type
   * @params - cugex3, egtrcd, estrcd, ait1, cuam
   * @returns - 103|0
   */
  int getTYLI(String cugex3, String estrcd, String ait1, float cuam) {
    if (cugex3.equals("2") && (estrcd.equals("10") || estrcd.equals("20") || estrcd.equals("21"))) {
      return 103
    } else {
      return 0
    }
  }

  /**
   * @getPRCD - REG09
   * @params - 
   * @returns - 1|2
   */
  int getPRCD(String ait1) {
    if (ait1.trim().equals(compteCollectifClient)) {
      return 1
    }
    return 2
  }

  /**
   * @getPYCL - Get payment class
   * @params - pytp : Payment Type
   * @returns - CTPARM field from CSYTAB
   */
  String getPYCL(String pytp) {
    DBAction query = database.table("CSYTAB")
      .index("00")
      .selection("CTPARM")
      .build()
    DBContainer container = query.getContainer()
    container.set("CTCONO", inCONO)
    container.set("CTSTCO", "PYTP")
    container.set("CTSTKY", pytp)

    String result = ""
    query.readAll(container, 4, 1, { DBContainer container1 ->
      String ctparmValue = container1.get("CTPARM").toString()
      if (ctparmValue.length() > 1) {
        result = String.valueOf(ctparmValue.charAt(1))
      }
    })
    return result
  }

  /**
   * @getBAI1NAI1 - REG12, REG13
   * @params - pyno, drfn, inyr
   * @returns - DMBAI1, DMNAI1 field from FDRFMA
   */
  String getBAI1NAI1(String pyno, String drfn, String inyr, String outBound) {
    ExpressionFactory expression = database.getExpressionFactory("FDRFMA")
    expression = expression.eq("DMINYR", inyr)

    DBAction query = database.table("FDRFMA")
      .index("00")
      .matching(expression)
      .selection("DMBAI1", "DMNAI1")
      .build()
    DBContainer container = query.getContainer()
    container.set("DMCONO", inCONO)
    container.set("DMDIVI", inDIVI)
    container.set("DMPYNO", pyno)
    container.set("DMDRFN", drfn)

    String result = ""
    query.readAll(container, 4, 1, { DBContainer container1 ->
      if (outBound.equals("BAI1")) {
        result = container1.get("DMBAI1").toString()
      } else if (outBound.equals("NAI1")) {
        result = container1.get("DMNAI1").toString()
      }
    })
    return result
  }

  /**
   * @getCompteCUGEX - get account number from CUGEX1
   * @params - 
   * @returns - pk03 : account number exclusionList
   */
  List < String > getCompteCUGEX(String val) {
    ExpressionFactory expression = database.getExpressionFactory("CUGEX1")
    
    if (val.equals("3")){
      expression = expression.eq("F1PK02", "1").and(expression.eq("F1CHB1", "3"))
    } else if (val.equals("0")){
      expression = expression.eq("F1PK02", "1")
    }
    
    DBAction query = database.table("CUGEX1")
      .index("00")
      .selection("F1PK03")
      .matching(expression)
      .build()
    DBContainer container = query.getContainer()
    container.set("F1CONO", inCONO)
    container.set("F1FILE", "FCHACC")

    int nbrOfRecords = 50
    List < String > results = new ArrayList < > ()
    query.readAll(container, 2, nbrOfRecords, { DBContainer container1 ->
      if (container1.get("F1PK03") != null) {
        results.add(container1.get("F1PK03").toString().trim())
      }
    })
    return results
  }

  /**
   * @getBKAC - get BKAC (virement)
   * @params - jrno, yea4
   * @returns - ait1 : bank account
   */
  String getBKAC(int jrno, int yea4, String vono) {
    ExpressionFactory expression = database.getExpressionFactory("FGLEDG")
    expression = expression.eq("EGVONO", vono).and(expression.like("EGAIT1", "5%"))

    DBAction query = database.table("FGLEDG")
      .index("00")
      .selection("EGAIT1")
      .matching(expression)
      .build()
    DBContainer container = query.getContainer()
    container.set("EGCONO", inCONO)
    container.set("EGDIVI", inDIVI)
    container.set("EGYEA4", yea4)
    container.set("EGJRNO", jrno)

    String result = ""
    query.readAll(container, 4, 1, { DBContainer container1 ->
      result = container1.get("EGAIT1").toString().trim()
    })
    return result
  }

  /**
   * @getCINO - get EGGEXI from FGLEDX
   * @params - jrno, jsno, yea4
   * @returns - cino : Invoice Number
   */
  String getCINO(int jrno, int jsno, int yea4) {
    DBAction query = database.table("FGLEDX")
      .index("00")
      .selection("EGGEXI")
      .build()
    DBContainer container = query.getContainer()
    container.set("EGCONO", inCONO)
    container.set("EGDIVI", inDIVI)
    container.set("EGYEA4", yea4)
    container.set("EGJRNO", jrno)
    container.set("EGJSNO", jsno)
    container.set("EGGEXN", 15)

    String result = ""
    query.readAll(container, 6, 1, { DBContainer container1 ->
      String rawValue = container1.get("EGGEXI").toString().trim()
        String[] parts = rawValue.split("\\s+") 

        if (parts.length > 1) {
            result = String.join(" ", java.util.Arrays.copyOf(parts, parts.length - 1))
        } else {
            result = rawValue
        }
    })
    return result
  }

  /**
   * @add103104Virement - REG03 : create 103 104 (Virement), list associated lines
   * @params - tyli, ait1, jrno, vono, yea4, vser, feid, cino, pycl, trcd, jsno, cuam, lineNumber
   * @returns - array
   */
  void add103104Virement(int lineNumber) {
    String tyli = output.get(lineNumber).get("TYLI")
    String ait1 = output.get(lineNumber).get("AIT1")
    int jrno = Integer.parseInt(output.get(lineNumber).get("JRNO"))
    String vono = output.get(lineNumber).get("VONO")
    int yea4 = Integer.parseInt(output.get(lineNumber).get("YEA4"))
    String pycl = output.get(lineNumber).get("PYCL")
    String dbcr = output.get(lineNumber).get("DBCR").trim()
    String cino = output.get(lineNumber).get("CINO").trim()

    if (tyli.equals("103") && ait1.trim().equals(compteCollectifClient) && (pycl.trim().equals("3") || pycl.trim().equals("0")) && dbcr.equals("C")) {
      
      ExpressionFactory expCheck = database.getExpressionFactory("FGLEDG")
      expCheck = expCheck.eq("EGVONO", vono).and(expCheck.like("EGAIT1", "5%")).and(expCheck.eq("EGDBCR", "D"))

      DBAction queryCheck = database.table("FGLEDG")
        .index("00")
        .selection("EGAIT1")
        .matching(expCheck)
        .build()
      DBContainer conCheck = queryCheck.getContainer()
      conCheck.set("EGCONO", inCONO)
      conCheck.set("EGDIVI", inDIVI)
      conCheck.set("EGYEA4", yea4)
      conCheck.set("EGJRNO", jrno)

      boolean found512115 = false
      boolean foundbank = false

      queryCheck.readAll(conCheck, 4, 1, { DBContainer container1 ->
        String ait1Value = container1.get("EGAIT1").toString().trim()
        foundbank = true
        if (ait1Value.equals(compteDedieFactor)) {
          found512115 = true
        }
      })

      ExpressionFactory expAll = database.getExpressionFactory("FGLEDG")
      expAll = expAll.eq("EGVONO", vono)

      DBAction queryAll = database.table("FGLEDG")
        .index("00")
        .selection("EGVONO", "EGVSER", "EGACDT", "EGCUCD", "EGCUAM", "EGVTXT", "EGAIT1", "EGFEID", "EGTRCD","EGDBCR")
        .matching(expAll)
        .build()
      DBContainer conAll = queryAll.getContainer()
      conAll.set("EGCONO", inCONO)
      conAll.set("EGDIVI", inDIVI)
      conAll.set("EGYEA4", yea4)
      conAll.set("EGJRNO", jrno)
      queryAll.readAll(conAll, 4, maxRecords, { DBContainer container1 ->
        String ait1Value = container1.get("EGAIT1").toString().trim()
        String trcd = container1.get("EGTRCD").toString().trim()
        if (!exclusionList.contains(ait1Value) && foundbank && (trcd.equals("10") || trcd.equals("20") || trcd.equals("21"))) {
          HashMap < String, String > tmp1 = new HashMap < > ()
          HashMap < String, String > tmp2 = new HashMap < > ()
          tmp1.putAll(output.get(lineNumber))

          int currentYea4 = Integer.parseInt(container1.get("EGYEA4").toString())
          int currentJrno = Integer.parseInt(container1.get("EGJRNO").toString())
          int currentJsno = Integer.parseInt(container1.get("EGJSNO").toString())

          tmp1.put("CONO", container1.get("EGCONO").toString())
          tmp1.put("DIVI", container1.get("EGDIVI").toString())
          tmp1.put("YEA4", String.valueOf(currentYea4))
          tmp1.put("JRNO", String.valueOf(currentJrno))
          tmp1.put("JSNO", String.valueOf(currentJsno))
          tmp1.put("VONO", container1.get("EGVONO").toString())
          tmp1.put("VSER", container1.get("EGVSER").toString())
          tmp1.put("ACDT", container1.get("EGACDT").toString())
          tmp1.put("CUCD", container1.get("EGCUCD").toString())
          tmp1.put("CUAM", container1.get("EGCUAM").toString())
          tmp1.put("VTXT", container1.get("EGVTXT").toString())
          tmp1.put("AIT1", container1.get("EGAIT1").toString())
          tmp1.put("FEID", container1.get("EGFEID").toString())

          String currentCino = getCINO(currentJrno, currentJsno, currentYea4).trim()
          if (!currentCino.equals("")) {
            tmp1.put("CINO", currentCino)
          }

          tmp2.putAll(tmp1)
          tmp2.put("TYLI", "104")
          tmp2.put("CUAM", String.valueOf(-Float.parseFloat(tmp1.get("CUAM"))))

          if (found512115) {
            output103104.add(tmp1)
            output103104.add(tmp2)
          } else if (!found512115) {
            if (currentCino.equals(cino) && (trcd.equals("10") || trcd.equals("20") || trcd.equals("21"))) {
              output103104.add(tmp1)
              output103104.add(tmp2)
            }
          }
        }
      })
    }
  }

  /**
   * @add104Impaye - REG03 : create 103 104 (Impayè), list associated lines
   * @params - tyli, ait1, jrno, vono, yea4, vser, feid, cino, pycl, trcd, jsno, cuam, lineNumber
   * @returns - array
   */
  void add104Impaye(int lineNumber) {
    String tyli = output.get(lineNumber).get("TYLI")
    String ait1 = output.get(lineNumber).get("AIT1")
    int jrno = Integer.parseInt(output.get(lineNumber).get("JRNO"))
    String vono = output.get(lineNumber).get("VONO")
    int yea4 = Integer.parseInt(output.get(lineNumber).get("YEA4"))
    String vser = output.get(lineNumber).get("VSER")
    String feid = output.get(lineNumber).get("FEID")
    String pycl = output.get(lineNumber).get("PYCL")
    String dbcr = output.get(lineNumber).get("DBCR").trim()

    if (tyli.equals("103") &&
      ait1.trim().equals(compteCollectifClient) &&
      (pycl.trim().equals("0") || pycl.trim().equals("2") || pycl.trim().equals("3") || pycl.trim().equals("4")) && dbcr.equals("D")) {

      ExpressionFactory expCheck = database.getExpressionFactory("FGLEDG")
      expCheck = expCheck.eq("EGVONO", vono).and(expCheck.like("EGAIT1", "5%")).and(expCheck.eq("EGDBCR", "C"))

      DBAction queryCheck = database.table("FGLEDG")
        .index("00")
        .selection("EGAIT1")
        .matching(expCheck)
        .build()
      DBContainer conCheck = queryCheck.getContainer()
      conCheck.set("EGCONO", inCONO)
      conCheck.set("EGDIVI", inDIVI)
      conCheck.set("EGYEA4", yea4)
      conCheck.set("EGJRNO", jrno)

      boolean foundBank = false
      boolean found512115 = false

      queryCheck.readAll(conCheck, 4, 1, { DBContainer container1 ->
        String ait1Value = container1.get("EGAIT1").toString().trim()
        foundBank = true
        if (ait1Value.equals(compteDedieFactor)) {
          found512115 = true
        }
      })
      
      ExpressionFactory expAll = database.getExpressionFactory("FGLEDG")
      expAll = expAll.eq("EGVONO", vono).and(expAll.eq("EGVSER", vser)).and(expAll.eq("EGFEID", feid))

      DBAction queryAll = database.table("FGLEDG")
        .index("00")
        .selection("EGVONO", "EGVSER", "EGACDT", "EGCUCD", "EGCUAM", "EGVTXT", "EGAIT1", "EGFEID", "EGTRCD","EGDBCR")
        .matching(expAll)
        .build()
      DBContainer conAll = queryAll.getContainer()
      conAll.set("EGCONO", inCONO)
      conAll.set("EGDIVI", inDIVI)
      conAll.set("EGYEA4", yea4)
      conAll.set("EGJRNO", jrno)
      if (foundBank && found512115) {
        queryAll.readAll(conAll, 4, maxRecords, { DBContainer container1 ->
          String ait1Value = container1.get("EGAIT1").toString().trim()
          String trcd = container1.get("EGTRCD").toString().trim()
          if (found512115 && !exclusionList.contains(ait1Value) && (trcd.equals("10") || trcd.equals("20") || trcd.equals("21"))) {
            HashMap < String, String > tmp1 = new HashMap < > ()
            HashMap < String, String > tmp2 = new HashMap < > ()
            tmp1.putAll(output.get(lineNumber))

            int currentYea4 = Integer.parseInt(container1.get("EGYEA4").toString())
            int currentJrno = Integer.parseInt(container1.get("EGJRNO").toString())
            int currentJsno = Integer.parseInt(container1.get("EGJSNO").toString())

            tmp1.put("CONO", container1.get("EGCONO").toString())
            tmp1.put("DIVI", container1.get("EGDIVI").toString())
            tmp1.put("YEA4", String.valueOf(currentYea4))
            tmp1.put("JRNO", String.valueOf(currentJrno))
            tmp1.put("JSNO", String.valueOf(currentJsno))
            tmp1.put("VONO", container1.get("EGVONO").toString())
            tmp1.put("VSER", container1.get("EGVSER").toString())
            tmp1.put("ACDT", container1.get("EGACDT").toString())
            tmp1.put("CUCD", container1.get("EGCUCD").toString())
            tmp1.put("CUAM", container1.get("EGCUAM").toString())
            tmp1.put("VTXT", container1.get("EGVTXT").toString())
            tmp1.put("AIT1", container1.get("EGAIT1").toString())
            tmp1.put("FEID", container1.get("EGFEID").toString())

            String currentCino = getCINO(currentJrno, currentJsno, currentYea4).trim()
            if (!currentCino.trim().equals("")) {
              tmp1.put("CINO", currentCino)
            }

            tmp2.putAll(tmp1)
            tmp2.put("TYLI", "104")
            tmp2.put("CUAM", String.valueOf(-Float.parseFloat(tmp1.get("CUAM"))))

            output103104.add(tmp1)
            output103104.add(tmp2)
          }
        })
      } else if (foundBank && !found512115) {
        output.get(lineNumber).put("TYLI", "104")
      }
    }
  }

  /**
   * @getRAI1 - get RAI1 from FCHKMA
   * @params - pyno, ckno, bkid
   * @returns - rmnb : Remittance Number
   */
  String getRAI1(String pyno, String ckno, String bkid) {
    DBAction query = database.table("FCHKMA")
      .index("00")
      .selection("FYRAI1")
      .build()
    DBContainer container = query.getContainer()
    container.set("FYCONO", inCONO)
    container.set("FYDIVI", inDIVI)
    container.set("FYPYNO", pyno)
    container.set("FYCKNO", ckno.trim())
    container.set("FYBKID", bkid.trim())

    String result = ""
    query.readAll(container, 5, 1, { DBContainer container1 ->
      result = container1.get("FYRAI1").toString().trim()
    })
    return result
  }

  /**
   * @add103104ChequeAvantRemise - REG03 : create 103 104 (Chèque avant remise), list associated lines
   * @params - tyli, ait1, jrno, vono, yea4, vser, feid, cino, pycl, trcd, jsno, cuam, lineNumber
   * @returns - array
   */
  void add103104ChequeAvantRemise(int lineNumber) {
    String tyli = output.get(lineNumber).get("TYLI")
    String ait1 = output.get(lineNumber).get("AIT1")
    int jrno = Integer.parseInt(output.get(lineNumber).get("JRNO"))
    String vono = output.get(lineNumber).get("VONO")
    int yea4 = Integer.parseInt(output.get(lineNumber).get("YEA4"))
    String vser = output.get(lineNumber).get("VSER")
    String feid = output.get(lineNumber).get("FEID")
    String pycl = output.get(lineNumber).get("PYCL")
    String dbcr = output.get(lineNumber).get("DBCR").trim()
    String cino = output.get(lineNumber).get("CINO").trim()

    if (tyli.equals("103") && ait1.trim().equals(compteCollectifClient) && pycl.trim().equals("2") && dbcr.equals("C")) {
      String jsnb = ""

      ExpressionFactory expCheck = database.getExpressionFactory("FGLEDG")
      expCheck = expCheck.eq("EGVONO", vono).and(expCheck.eq("EGVSER", vser)).and(expCheck.eq("EGFEID", feid)).and(expCheck.like("EGAIT1", "5%")).and(expCheck.eq("EGTRCD", "10").or(expCheck.eq("EGTRCD", "20")).or(expCheck.eq("EGTRCD", "21")))

      DBAction queryCheck = database.table("FGLEDG")
        .index("00")
        .selection("EGAIT1")
        .matching(expCheck)
        .build()
      DBContainer conCheck = queryCheck.getContainer()
      conCheck.set("EGCONO", inCONO)
      conCheck.set("EGDIVI", inDIVI)
      conCheck.set("EGYEA4", yea4)
      conCheck.set("EGJRNO", jrno)

      String acso103104 = ""
      queryCheck.readAll(conCheck, 4, 1, { DBContainer container1 ->
        String ait1Value = container1.get("EGAIT1").toString().trim()
        jsnb = container1.get("EGJSNO").toString().trim()
        acso103104 = ait1Value
        output.get(lineNumber).put("ACSO", ait1Value)
      })

      ExpressionFactory expAll = database.getExpressionFactory("FGLEDG")
      expAll = expAll.eq("EGVONO", vono).and(expAll.eq("EGVSER", vser)).and(expAll.eq("EGFEID", feid)).and(expAll.eq("EGTRCD", "10").or(expAll.eq("EGTRCD", "20")).or(expAll.eq("EGTRCD", "21")))

      DBAction queryAll = database.table("FGLEDG")
        .index("00")
        .selection("EGVONO", "EGVSER", "EGACDT", "EGCUCD", "EGCUAM", "EGVTXT", "EGAIT1", "EGFEID", "EGTRCD")
        .matching(expAll)
        .build()
      DBContainer conAll = queryAll.getContainer()
      conAll.set("EGCONO", inCONO)
      conAll.set("EGDIVI", inDIVI)
      conAll.set("EGYEA4", yea4)
      conAll.set("EGJRNO", jrno)
      queryAll.readAll(conAll, 4, maxRecords, { DBContainer container1 ->
        String ait1Value = container1.get("EGAIT1").toString().trim()
        if (!exclusionList.contains(ait1Value) || ait1Value.equals(compteCollectifClient)) {
          HashMap < String, String > tmp1 = new HashMap < > ()
          HashMap < String, String > tmp2 = new HashMap < > ()
          tmp1.putAll(output.get(lineNumber))

          int currentYea4 = Integer.parseInt(container1.get("EGYEA4").toString())
          int currentJrno = Integer.parseInt(container1.get("EGJRNO").toString())
          int currentJsno = Integer.parseInt(container1.get("EGJSNO").toString())

          String currentTrcd = container1.get("EGTRCD").toString()

          tmp1.put("CONO", container1.get("EGCONO").toString())
          tmp1.put("DIVI", container1.get("EGDIVI").toString())
          tmp1.put("YEA4", String.valueOf(currentYea4))
          tmp1.put("JRNO", String.valueOf(currentJrno))
          tmp1.put("JSNO", String.valueOf(currentJsno))
          tmp1.put("VONO", container1.get("EGVONO").toString())
          tmp1.put("VSER", container1.get("EGVSER").toString())
          tmp1.put("ACDT", container1.get("EGACDT").toString())
          tmp1.put("CUCD", container1.get("EGCUCD").toString())
          tmp1.put("CUAM", container1.get("EGCUAM").toString())
          tmp1.put("VTXT", container1.get("EGVTXT").toString())
          tmp1.put("AIT1", container1.get("EGAIT1").toString())
          tmp1.put("FEID", container1.get("EGFEID").toString())
          tmp1.put("TRCD", currentTrcd)

          tmp1.put("ACSO", acso103104)
          tmp1.put("TYLI", "103")

          String currentCino = getCINO(currentJrno, currentJsno, currentYea4).trim()
          if (!currentCino.trim().equals("")) {
            tmp1.put("CINO", currentCino)
          }

          tmp2.putAll(tmp1)
          tmp2.put("TYLI", "104")
          tmp2.put("CUAM", String.valueOf(-Float.parseFloat(tmp1.get("CUAM"))))

          if (tmp1.get("AIT1").trim().equals(compteCollectifClient)) {
            tmp1.put("TUPD", "OK")
          }

          if (currentCino.equals(cino) && currentTrcd.trim().equals("21")) {
            output103104.add(tmp1)
            output103104.add(tmp2)
            tmp1.put("TUPD", "OK")
          }

          DBAction insertAction = database.table("EXT807").build()
          DBContainer ext807Container = insertAction.getContainer()
          
          LocalDateTime dateTime = LocalDateTime.now()
          int entryDate = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
          int entryTime = dateTime.format(DateTimeFormatter.ofPattern("HHmmss")).toInteger()

          ext807Container.set("EXCONO", Integer.parseInt(tmp1.get("CONO")))
          ext807Container.set("EXDIVI", tmp1.get("DIVI"))
          ext807Container.set("EXYEA4", Integer.parseInt(tmp1.get("YEA4")))
          ext807Container.set("EXJRNO", Integer.parseInt(tmp1.get("JRNO")))
          ext807Container.set("EXJSNO", Integer.parseInt(tmp1.get("JSNO")))
          ext807Container.set("EXVONO", Integer.parseInt(tmp1.get("VONO")))
          ext807Container.set("EXVSER", tmp1.get("VSER"))
          ext807Container.set("EXACDT", Integer.parseInt(tmp1.get("ACDT")))
          ext807Container.set("EXCUCD", tmp1.get("CUCD"))
          ext807Container.set("EXCUAM", Double.parseDouble(tmp1.get("CUAM")))
          ext807Container.set("EXVTXT", tmp1.get("VTXT"))
          ext807Container.set("EXAIT1", tmp1.get("AIT1"))
          ext807Container.set("EXFEID", tmp1.get("FEID"))
          ext807Container.set("EXTRCD", Integer.parseInt(tmp1.get("TRCD")))
          ext807Container.set("EXACSO", tmp1.get("ACSO"))
          ext807Container.set("EXSTAT", tmp1.get("STAT"))
          ext807Container.set("EXEXST", tmp1.get("EXST"))
          ext807Container.set("EXTYLI", tmp1.get("TYLI"))
          ext807Container.set("EXDMTM", tmp1.get("DMTM"))
          ext807Container.set("EXPYCU", tmp1.get("PYCU"))
          ext807Container.set("EXCINO", tmp1.get("CINO"))
          ext807Container.set("EXPYNO", tmp1.get("PYNO"))
          ext807Container.set("EXPYCD", tmp1.get("PYCD"))
          ext807Container.set("EXINYR", Integer.parseInt(tmp1.get("INYR")))
          ext807Container.set("EXDUDT", Integer.parseInt(tmp1.get("DUDT")))
          ext807Container.set("EXORNO", tmp1.get("ORNO"))
          ext807Container.set("EXPYCL", Integer.parseInt(tmp1.get("PYCL")))
          ext807Container.set("EXRMNO", Integer.parseInt(tmp1.get("RMNO")))
          ext807Container.set("EXNCRE", tmp1.get("NCRE"))
          ext807Container.set("EXACSO", tmp1.get("ACSO"))
          ext807Container.set("EXBKAC", tmp1.get("BKAC"))
          ext807Container.set("EXSTAT", tmp1.get("STAT"))
          ext807Container.set("EXDATE", Integer.parseInt(tmp1.get("DATE")))
          ext807Container.set("EXPRCD", tmp1.get("PRCD"))
          ext807Container.set("EXCONM", tmp1.get("CONM"))
          ext807Container.set("EXCCD6", tmp1.get("CCD6"))
          ext807Container.set("EXACRF", tmp1.get("ACRF"))
          String customerInfo = tmp1.get("CORG") + "~" + tmp1.get("COR2") + "~" + tmp1.get("CUNM")
          ext807Container.set("EXCINF", customerInfo)
          String customerAdresses = tmp1.get("CUA1") + "~" + tmp1.get("CUA2") + "~" + tmp1.get("PONO") + "~" + tmp1.get("TOWN") + "~" + tmp1.get("CSCD") + "~" + tmp1.get("PHNO")
          ext807Container.set("EXCADR", customerAdresses)
          ext807Container.set("EXTUPD", tmp1.get("TUPD"))
          ext807Container.set("EXVRNO", tmp1.get("VRNO"))
          ext807Container.set("EXRGDT", entryDate)
          ext807Container.set("EXRGTM", entryTime)
          ext807Container.set("EXLMDT", entryDate)
          ext807Container.set("EXCHID", program.getUser())
          ext807Container.set("EXCHNO", 1)
          ext807Container.set("EXJSNB", Integer.parseInt(jsnb))

          insertAction.insert(ext807Container) // Add lines to Ext807 for further processing
        }
      })
    }
  }
  
  /**
   * @getNewDudt - get DUDT from FSLEDG
   * @params - yea4, jrno, jsno
   * @returns - dudt field from FSLEDG
   */
  String getNewDudt(int yea4, int jrno, int jsno) {
    ExpressionFactory expression = database.getExpressionFactory("FSLEDG")
    expression = expression.eq("ESTRCD", "10")
    
    DBAction query = database.table("FSLEDG")
      .index("00")
      .selection("ESDUDT")
      .matching(expression)
      .build()
    DBContainer container = query.getContainer()
    container.set("ESCONO", inCONO)
    container.set("ESDIVI", inDIVI)
    container.set("ESYEA4", yea4)
    container.set("ESJRNO", jrno)
    container.set("ESJSNO", jsno)

    String result = ""
    query.readAll(container, 5, 1, { DBContainer container1 ->
      result = container1.get("ESDUDT").toString()
    })
    return result
  }

  /**
   * @add103104EffetPrelevementAvantRemise - REG03 : create 103 104 (Effet, prelevement), list associated lines
   * @params - tyli, ait1, jrno, vono, yea4, vser, feid, cino, pycl, trcd, jsno, cuam, lineNumber
   * @returns - array
   */
  void add103104EffetPrelevementAvantRemise(int lineNumber) {
    String tyli = output.get(lineNumber).get("TYLI")
    String jsnb = output.get(lineNumber).get("JSNO")
    String ait1 = output.get(lineNumber).get("AIT1")
    int jrno = Integer.parseInt(output.get(lineNumber).get("JRNO"))
    String vono = output.get(lineNumber).get("VONO")
    int yea4 = Integer.parseInt(output.get(lineNumber).get("YEA4"))
    String vser = output.get(lineNumber).get("VSER")
    String feid = output.get(lineNumber).get("FEID")
    String pycl = output.get(lineNumber).get("PYCL")
    String dbcr = output.get(lineNumber).get("DBCR").trim()
    String cino = output.get(lineNumber).get("CINO").trim()

    if (tyli.equals("103") && ait1.trim().equals(compteCollectifClient) && (pycl.trim().equals("4") || pycl.trim().equals("5"))) {
      String newDudt = ""
      
      ExpressionFactory expCheck = database.getExpressionFactory("FGLEDG")
      expCheck = expCheck.eq("EGVONO", vono).and(expCheck.eq("EGVSER", vser)).and(expCheck.eq("EGFEID", feid)).and(expCheck.in("EGAIT1", account3.toArray(new String[0]))).and(expCheck.eq("EGTRCD", "10").or(expCheck.eq("EGTRCD", "20")).or(expCheck.eq("EGTRCD", "21")))

      DBAction queryCheck = database.table("FGLEDG")
        .index("00")
        .selection()
        .matching(expCheck)
        .build()
      DBContainer conCheck = queryCheck.getContainer()
      conCheck.set("EGCONO", inCONO)
      conCheck.set("EGDIVI", inDIVI)
      conCheck.set("EGYEA4", yea4)
      conCheck.set("EGJRNO", jrno)
      
      queryCheck.readAll(conCheck, 4, 1, { DBContainer container1 ->
        int currentYea4 = Integer.parseInt(container1.get("EGYEA4").toString())
        int currentJrno = Integer.parseInt(container1.get("EGJRNO").toString())
        int currentJsno = Integer.parseInt(container1.get("EGJSNO").toString())
        if (pycl.equals("4")) {
          newDudt = getNewDudt(currentYea4, currentJrno, currentJsno)
          output.get(lineNumber).put("DUDT", newDudt)
        }
      })
      
      ExpressionFactory expAll = database.getExpressionFactory("FGLEDG")
      expAll = expAll.eq("EGVONO", vono).and(expAll.eq("EGVSER", vser)).and(expAll.eq("EGFEID", feid)).and(expAll.eq("EGTRCD", "10").or(expAll.eq("EGTRCD", "20")).or(expAll.eq("EGTRCD", "21")))

      DBAction queryAll = database.table("FGLEDG")
        .index("00")
        .selection("EGVONO", "EGVSER", "EGACDT", "EGCUCD", "EGCUAM", "EGVTXT", "EGAIT1", "EGFEID", "EGTRCD")
        .matching(expAll)
        .build()
      DBContainer conAll = queryAll.getContainer()
      conAll.set("EGCONO", inCONO)
      conAll.set("EGDIVI", inDIVI)
      conAll.set("EGYEA4", yea4)
      conAll.set("EGJRNO", jrno)
      
      if (dbcr.equals("C")){
      queryAll.readAll(conAll, 4, maxRecords, { DBContainer container1 ->
        String ait1Value = container1.get("EGAIT1").toString().trim()
        if (!exclusionList.contains(ait1Value) || ait1Value.equals(compteCollectifClient)) {
          HashMap < String, String > tmp1 = new HashMap < > ()
          HashMap < String, String > tmp2 = new HashMap < > ()
          tmp1.putAll(output.get(lineNumber))

          int currentYea4 = Integer.parseInt(container1.get("EGYEA4").toString())
          int currentJrno = Integer.parseInt(container1.get("EGJRNO").toString())
          int currentJsno = Integer.parseInt(container1.get("EGJSNO").toString())

          String currentTrcd = container1.get("EGTRCD").toString()

          tmp1.put("CONO", container1.get("EGCONO").toString())
          tmp1.put("DIVI", container1.get("EGDIVI").toString())
          tmp1.put("YEA4", String.valueOf(currentYea4))
          tmp1.put("JRNO", String.valueOf(currentJrno))
          tmp1.put("JSNO", String.valueOf(currentJsno))
          tmp1.put("VONO", container1.get("EGVONO").toString())
          tmp1.put("VSER", container1.get("EGVSER").toString())
          tmp1.put("ACDT", container1.get("EGACDT").toString())
          tmp1.put("CUCD", container1.get("EGCUCD").toString())
          tmp1.put("CUAM", container1.get("EGCUAM").toString())
          tmp1.put("VTXT", container1.get("EGVTXT").toString())
          tmp1.put("AIT1", container1.get("EGAIT1").toString())
          tmp1.put("FEID", container1.get("EGFEID").toString())
          tmp1.put("FNCN", container1.get("EGFNCN").toString())
          tmp1.put("TRCD", currentTrcd)

          tmp1.put("TYLI", "103")

          String currentCino = getCINO(currentJrno, currentJsno, currentYea4).trim()
          if (!currentCino.trim().equals("")) {
            tmp1.put("CINO", currentCino)
          }

          tmp2.putAll(tmp1)
          tmp2.put("TYLI", "104")
          tmp2.put("CUAM", String.valueOf(-Float.parseFloat(tmp1.get("CUAM"))))

          if (tmp1.get("AIT1").trim().equals(compteCollectifClient)) {
            tmp1.put("TUPD", "OK")
          }

          if (currentCino.equals(cino) && currentTrcd.trim().equals("21")) {
            output103104.add(tmp1)
            output103104.add(tmp2)
            tmp1.put("TUPD", "OK")
          }

          DBAction insertAction = database.table("EXT807").build()
          DBContainer ext807Container = insertAction.getContainer()
          
          LocalDateTime dateTime = LocalDateTime.now()
          int entryDate = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
          int entryTime = dateTime.format(DateTimeFormatter.ofPattern("HHmmss")).toInteger()
          
          ext807Container.set("EXCONO", Integer.parseInt(tmp1.get("CONO")))
          ext807Container.set("EXDIVI", tmp1.get("DIVI"))
          ext807Container.set("EXYEA4", Integer.parseInt(tmp1.get("YEA4")))
          ext807Container.set("EXJRNO", Integer.parseInt(tmp1.get("JRNO")))
          ext807Container.set("EXJSNO", Integer.parseInt(tmp1.get("JSNO")))
          ext807Container.set("EXVONO", Integer.parseInt(tmp1.get("VONO")))
          ext807Container.set("EXVSER", tmp1.get("VSER"))
          ext807Container.set("EXACDT", Integer.parseInt(tmp1.get("ACDT")))
          ext807Container.set("EXCUCD", tmp1.get("CUCD"))
          ext807Container.set("EXCUAM", Double.parseDouble(tmp1.get("CUAM")))
          ext807Container.set("EXVTXT", tmp1.get("VTXT"))
          ext807Container.set("EXAIT1", tmp1.get("AIT1"))
          ext807Container.set("EXFEID", tmp1.get("FEID"))
          ext807Container.set("EXTRCD", Integer.parseInt(tmp1.get("TRCD")))
          ext807Container.set("EXACSO", tmp1.get("ACSO"))
          ext807Container.set("EXSTAT", tmp1.get("STAT"))
          ext807Container.set("EXEXST", tmp1.get("EXST"))
          ext807Container.set("EXTYLI", tmp1.get("TYLI"))
          ext807Container.set("EXDMTM", tmp1.get("DMTM"))
          ext807Container.set("EXPYCU", tmp1.get("PYCU"))
          ext807Container.set("EXCINO", tmp1.get("CINO"))
          ext807Container.set("EXPYNO", tmp1.get("PYNO"))
          ext807Container.set("EXPYCD", tmp1.get("PYCD"))
          ext807Container.set("EXINYR", Integer.parseInt(tmp1.get("INYR")))
          ext807Container.set("EXDUDT", Integer.parseInt(tmp1.get("DUDT")))
          ext807Container.set("EXORNO", tmp1.get("ORNO"))
          ext807Container.set("EXPYCL", Integer.parseInt(tmp1.get("PYCL")))
          ext807Container.set("EXRMNO", Integer.parseInt(tmp1.get("RMNO")))
          ext807Container.set("EXNCRE", tmp1.get("NCRE"))
          ext807Container.set("EXACSO", tmp1.get("ACSO"))
          ext807Container.set("EXBKAC", tmp1.get("BKAC"))
          ext807Container.set("EXSTAT", tmp1.get("STAT"))
          ext807Container.set("EXDATE", Integer.parseInt(tmp1.get("DATE")))
          ext807Container.set("EXPRCD", tmp1.get("PRCD"))
          ext807Container.set("EXCONM", tmp1.get("CONM"))
          ext807Container.set("EXCCD6", tmp1.get("CCD6"))
          ext807Container.set("EXACRF", tmp1.get("ACRF"))
          String customerInfo = tmp1.get("CORG") + "~" + tmp1.get("COR2") + "~" + tmp1.get("CUNM")
          ext807Container.set("EXCINF", customerInfo)
          String customerAdresses = tmp1.get("CUA1") + "~" + tmp1.get("CUA2") + "~" + tmp1.get("PONO") + "~" + tmp1.get("TOWN") + "~" + tmp1.get("CSCD") + "~" + tmp1.get("PHNO")
          ext807Container.set("EXCADR", customerAdresses)
          ext807Container.set("EXTUPD", tmp1.get("TUPD"))
          ext807Container.set("EXVRNO", tmp1.get("VRNO"))
          ext807Container.set("EXRGDT", entryDate)
          ext807Container.set("EXRGTM", entryTime)
          ext807Container.set("EXLMDT", entryDate)
          ext807Container.set("EXCHID", program.getUser())
          ext807Container.set("EXCHNO", 1)
          if (tmp1.get("TRCD").trim().equals("21")) {
            ext807Container.set("EXJSNB", Integer.parseInt(jsnb))
          }

          insertAction.insert(ext807Container) // Add lines to Ext807 for further processing
        }
      })
      }
    }
  }

  /**
   * @add104Lettrage - REG03 : create 103 104 (lettrage), list associated lines
   * @params - tyli, ait1, jrno, vono, yea4, vser, feid, cino, pycl, trcd, jsno, cuam, lineNumber
   * @returns - array
   */
  void add104Lettrage(int lineNumber) {
    String tyli = output.get(lineNumber).get("TYLI")
    String ait1 = output.get(lineNumber).get("AIT1")
    int jrno = Integer.parseInt(output.get(lineNumber).get("JRNO"))
    String vono = output.get(lineNumber).get("VONO")
    int yea4 = Integer.parseInt(output.get(lineNumber).get("YEA4"))
    String pycl = output.get(lineNumber).get("PYCL")

    if (tyli.equals("103") && ait1.trim().equals(compteCollectifClient) && (pycl.trim().equals("3") || pycl.trim().equals("0"))) {
      ExpressionFactory expression = database.getExpressionFactory("FGLEDG")
      expression = expression.eq("EGVONO", vono).and(expression.like("EGAIT1", "5%"))

      DBAction query = database.table("FGLEDG")
        .index("00")
        .selection()
        .matching(expression)
        .build()
      DBContainer container = query.getContainer()
      container.set("EGCONO", inCONO)
      container.set("EGDIVI", inDIVI)
      container.set("EGYEA4", yea4)
      container.set("EGJRNO", jrno)

      boolean foundBank = false
      query.readAll(container, 4, 1, { DBContainer container1 ->
          foundBank = true
      })
      if (!foundBank) {
        output.get(lineNumber).put("TYLI", "104")
      }
    }
  }

  /**
   * @combine - Combine output + output103104
   * @params - 
   * @returns -
   */
  void combine() {
    output.addAll(output103104)
  }

  /**
   * @addCustomFields - Add custom fields
   * @params - 
   * @returns -
   */
  void addCustomFields() {
    for (int i = 0; i < output.size(); i++) {
      if (output.get(i).get("EXST").equals("OK")) {
        output.get(i).put("TUPD", "")

        output.get(i).put("ORNO", getORNO(Integer.parseInt(output.get(i).get("YEA4")), Integer.parseInt(output.get(i).get("JRNO")), Integer.parseInt(output.get(i).get("JSNO"))))
        String pycl = getPYCL(output.get(i).get("PYTP"))
        output.get(i).put("PYCL", pycl)
        
        //CR : Regul DUDT
        if(pycl.equals("0") || pycl.equals("3") || pycl.equals("2") || pycl.equals("5")){
          output.get(i).put("DUDT", output.get(i).get("ESACDT"))
        }
        
        String efno = ""
        String remittance = "0"
        String ckno = ""
        String rawCkno = ""
        String stmn = ""
        String bkid = ""

        if (pycl.trim().equals("2")) {
          rawCkno = getCKNO(Integer.parseInt(output.get(i).get("YEA4")), Integer.parseInt(output.get(i).get("JRNO")), Integer.parseInt(output.get(i).get("JSNO")))
          output.get(i).put("NCRE", rawCkno)
          output.get(i).put("RMNO", remittance)

          int spaceIndex = rawCkno.indexOf(" ")

          if (spaceIndex != -1) {
            bkid = rawCkno.substring(0, spaceIndex)
            ckno = rawCkno.substring(spaceIndex + 1)
          } else {
            bkid = rawCkno
            ckno = rawCkno
          }

          output.get(i).put("ACSO", getRAI1(output.get(i).get("PYNO"), ckno, bkid))
          output.get(i).put("BKAC", getBAI1NAI1(output.get(i).get("PYNO"), output.get(i).get("NCRE"), output.get(i).get("YEA4"), "BAI1"))
        } else if (pycl.trim().equals("4")) {
          efno = getEFNO(Integer.parseInt(output.get(i).get("YEA4")), Integer.parseInt(output.get(i).get("JRNO")), Integer.parseInt(output.get(i).get("JSNO")))
          output.get(i).put("NCRE", efno)
          output.get(i).put("RMNO", remittance)

          output.get(i).put("ACSO", getBAI1NAI1(output.get(i).get("PYNO"), output.get(i).get("NCRE"), output.get(i).get("YEA4"), "NAI1"))
          output.get(i).put("BKAC", getBAI1NAI1(output.get(i).get("PYNO"), output.get(i).get("NCRE"), output.get(i).get("YEA4"), "BAI1"))
        } else {
          stmn = getSTMN(Integer.parseInt(output.get(i).get("YEA4")), Integer.parseInt(output.get(i).get("JRNO")), Integer.parseInt(output.get(i).get("JSNO")))
          output.get(i).put("NCRE", stmn)
          output.get(i).put("RMNO", remittance)
          output.get(i).put("ACSO", getBAI1NAI1(output.get(i).get("PYNO"), output.get(i).get("NCRE"), output.get(i).get("YEA4"), "NAI1"))
          output.get(i).put("BKAC", getBKAC(Integer.parseInt(output.get(i).get("JRNO")), Integer.parseInt(output.get(i).get("YEA4")), output.get(i).get("VONO")))
        }

        output.get(i).put("STAT", getSTAT().toString())

        String cugex3 = getCUGEX3(Integer.parseInt(output.get(i).get("CONO")), "CUGEX3", "FEID", "F3A030", output.get(i).get("FEID"))

        String tyli = getTYLI(cugex3, output.get(i).get("ESTRCD"), output.get(i).get("AIT1"), Float.parseFloat(output.get(i).get("CUAM"))).toString()
        output.get(i).put("TYLI", tyli)

        output.get(i).put("DATE", getDATE().toString())
        output.get(i).put("PRCD", getPRCD(output.get(i).get("AIT1")).toString())

        HashMap < String, String > ocusma = getOCUSMA(output.get(i).get("PYNO"))
        if (ocusma != null && !ocusma.isEmpty()) {
          output.get(i).put("ACRF", ocusma.get("ACRF"))
          output.get(i).put("CORG", ocusma.get("CORG"))
          output.get(i).put("COR2", ocusma.get("COR2"))
          output.get(i).put("CUNM", ocusma.get("CUNM"))
          output.get(i).put("CUA1", ocusma.get("CUA1"))
          output.get(i).put("CUA2", ocusma.get("CUA2"))
          output.get(i).put("PONO", ocusma.get("PONO"))
          output.get(i).put("TOWN", ocusma.get("TOWN"))
          String cscd = ocusma.get("CSCD")
          output.get(i).put("CSCD", cscd)
          if (domTom.contains(cscd.trim())){
            String dmtm = "OK-" + pycl
            output.get(i).put("DMTM", dmtm)
          } else {
            String dmtm = "KO-" + pycl
            output.get(i).put("DMTM", dmtm)
          }
          output.get(i).put("PHNO", ocusma.get("PHNO"))
          output.get(i).put("VRNO", ocusma.get("VRNO"))
        }

        String cugexPaiement = getPYCU(output.get(i).get("PYCD"))
        output.get(i).put("PYCU", cugexPaiement)

        if (output.get(i).get("TYLI").equals("103") && !ocusma.get("ACRF").trim().equals("CL-FA")) {
          output.get(i).put("CORG", "999999998")
          output.get(i).put("COR2", "00019")
          output.get(i).put("CUA1", "18 RUE HOCHE")
          output.get(i).put("CUA2", " ")
          output.get(i).put("PONO", "92988")
          output.get(i).put("TOWN", "PARIS LA DEFENSE")
          output.get(i).put("CSCD", "FR")
          if (domTom.contains("FR")){
            String dmtm = "OK-" + pycl
            output.get(i).put("DMTM", dmtm)
          } else {
            String dmtm = "KO-" + pycl
            output.get(i).put("DMTM", dmtm)
          }
        }

        add103104Virement(i)
        add104Impaye(i)
        add104Lettrage(i)
        add103104ChequeAvantRemise(i)
        add103104EffetPrelevementAvantRemise(i)
      }
    }
  }

  /**
   * @getDATE - REG08 : get current date
   * @params - 
   * @returns - current date
   */
  int getDATE() {
    LocalDateTime currentDate = LocalDateTime.now()
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    String formattedDate = currentDate.format(formatter)

    return Integer.parseInt(formattedDate)
  }

  /**
   * @render - add array's lines in EXT806
   * @params - 
   * @returns -
   */
  void render() {
    for (int i = 0; i < output.size(); i++) {
      if (output.get(i).get("EXST").equals("OK") && Integer.parseInt(output.get(i).get("TYLI")) > 0) {

        DBAction dbaEXT806 = database.table("EXT806").index("00").build()
        DBContainer conEXT806 = dbaEXT806.createContainer()

        LocalDateTime dateTime = LocalDateTime.now()
        int entryDate = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
        int entryTime = dateTime.format(DateTimeFormatter.ofPattern("HHmmss")).toInteger()

        conEXT806.set("EXCONO", Integer.parseInt(output.get(i).get("CONO")))
        conEXT806.set("EXDIVI", output.get(i).get("DIVI"))
        conEXT806.set("EXYEA4", Integer.parseInt(output.get(i).get("YEA4")))
        conEXT806.set("EXJRNO", Integer.parseInt(output.get(i).get("JRNO")))
        conEXT806.set("EXJSNO", Integer.parseInt(output.get(i).get("JSNO")))
        conEXT806.set("EXVONO", Integer.parseInt(output.get(i).get("VONO")))
        conEXT806.set("EXVSER", output.get(i).get("VSER"))
        conEXT806.set("EXTYLI", output.get(i).get("TYLI"))
        conEXT806.set("EXPYNO", output.get(i).get("PYNO"))
        conEXT806.set("EXACDT", Integer.parseInt(output.get(i).get("ACDT")))
        conEXT806.set("EXCINO", output.get(i).get("CINO"))
        conEXT806.set("EXINYR", Integer.parseInt(output.get(i).get("INYR")))
        conEXT806.set("EXCUCD", output.get(i).get("CUCD"))
        conEXT806.set("EXCUAM", Double.parseDouble(output.get(i).get("CUAM")))
        conEXT806.set("EXPYCD", output.get(i).get("PYCD"))
        conEXT806.set("EXDMTM", output.get(i).get("DMTM"))
        conEXT806.set("EXDUDT", Integer.parseInt(output.get(i).get("DUDT")))
        conEXT806.set("EXVTXT", output.get(i).get("VTXT"))
        conEXT806.set("EXORNO", output.get(i).get("ORNO"))
        conEXT806.set("EXSTAT", output.get(i).get("STAT"))
        conEXT806.set("EXDATE", Integer.parseInt(output.get(i).get("DATE")))
        conEXT806.set("EXAIT1", output.get(i).get("AIT1"))
        conEXT806.set("EXRGDT", entryDate)
        conEXT806.set("EXRGTM", entryTime)
        conEXT806.set("EXLMDT", entryDate)
        conEXT806.set("EXCHNO", 1)
        conEXT806.set("EXCHID", program.getUser())
        conEXT806.set("EXNCRE", output.get(i).get("NCRE"))
        conEXT806.set("EXRMNO", Integer.parseInt(output.get(i).get("RMNO")))
        conEXT806.set("EXPRCD", output.get(i).get("PRCD"))
        conEXT806.set("EXACSO", output.get(i).get("ACSO"))
        conEXT806.set("EXBKAC", output.get(i).get("BKAC"))
        conEXT806.set("EXCONM", output.get(i).get("CONM"))
        conEXT806.set("EXCCD6", output.get(i).get("CCD6"))
        conEXT806.set("EXCORG", output.get(i).get("CORG"))
        conEXT806.set("EXCUNM", output.get(i).get("CUNM"))
        conEXT806.set("EXCOR2", output.get(i).get("COR2"))
        conEXT806.set("EXCUA1", output.get(i).get("CUA1"))
        conEXT806.set("EXCUA2", output.get(i).get("CUA2"))
        conEXT806.set("EXPONO", output.get(i).get("PONO"))
        conEXT806.set("EXTOWN", output.get(i).get("TOWN"))
        conEXT806.set("EXCSCD", output.get(i).get("CSCD"))
        conEXT806.set("EXPHNO", output.get(i).get("PHNO"))
        conEXT806.set("EXACRF", output.get(i).get("ACRF"))
        conEXT806.set("EXPYCU", output.get(i).get("PYCU"))
        conEXT806.set("EXVRNO", output.get(i).get("VRNO"))

        dbaEXT806.insert(conEXT806)

        mi.outData.put("RSLT", "OK")
        mi.write()
      }
    }
  }
}
