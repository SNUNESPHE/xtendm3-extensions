/************************************************************************************************************************************************
Extension Name: EXT806MI.AddLinesFacAv
Type: ExtendM3Transaction
Script Author: NRAOEL
Date: 2024-10-22
Description:
* Add Lines to the dynamic table EXT806 (Facture et avoir)

Revision History:
Name          Date        Version   Description of Changes
NRAOEL        2024-10-22  1.0       Initial Release
NRAOEL        2025-11-27  1.1       Correction after validation submission
**************************************************************************************************************************************************/

import java.util.ArrayList
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

public class AddLinesFacAv extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  private int inCONO //Company
  private String inDIVI //Division
  private int inYEA4 //Year
  private int inJRNO //Journal number
  private int inJSNO //Journal sequence
  public int maxRecords //10000

  public String compteCollectifClient = "" //accountNumber
  public String compteDedieFactor = "" //bankAccountNumber
  public String conm = "" //Company name
  public String ccd6 = "" //Company number

  private ArrayList < HashMap < String, String >> output = new ArrayList < > () //Array containing accounting entries
  private List < String > factureAvoir //List of invoice and credit
  private List < String > domTom //List country

  public AddLinesFacAv(MIAPI mi, ProgramAPI program, DatabaseAPI database, MICallerAPI miCaller) {
    this.mi = mi
    this.program = program
    this.database = database
    this.miCaller = miCaller
  }

  public void main() {
    maxRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 5000 ? 5000 : mi.getMaxRecords()

    //Initialization
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

    compteCollectifClient = getCUGEX1("1")
    compteDedieFactor = getCUGEX1("2")

    if (!compteDedieFactor.trim().equals("") && !compteCollectifClient.trim().equals("")) {
      factureAvoir = lstCUGEX3("FEID")
      domTom = lstCUGEX3("CSCD")

      readFGLEDG()
      readFSLEDG()
      addCustomFields()
      render()
    }
  }

  /**
   * @readFGLEDG - get fields from FGLEDG   
   * @params
   * @returns - array
   */
  void readFGLEDG() {
    ExpressionFactory expression = database.getExpressionFactory("FGLEDG")
    expression = expression.eq("EGAIT1", compteCollectifClient)

    DBAction query = database.table("FGLEDG")
      .index("00")
      .selection("EGVONO", "EGVSER", "EGACDT", "EGCUCD", "EGCUAM", "EGVTXT", "EGAIT1", "EGFEID", "EGFNCN", "EGTRCD", "EGVDSC")
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
      String feid = container1.get("EGFEID").toString() //Check if the line is an invoice or a credit
      if (gexi.trim().isBlank() && factureAvoir.contains(feid)) {
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
        tmp.put("FEID", feid)
        tmp.put("FNCN", container1.get("EGFNCN").toString())
        tmp.put("EGTRCD", container1.get("EGTRCD").toString())
        tmp.put("EGVDSC", container1.get("EGVDSC").toString())

        output.add(tmp)
      }
    })
  }

  /**
   * @readFSLEDG - get fields from FSLEDG - Join FGLEDG and FSLEDG
   * @params
   * @returns - array
   */
  void readFSLEDG() {
    ExpressionFactory expression = database.getExpressionFactory("FSLEDG")
    expression = expression
      .lt("ESACDT", getDATE().toString())
      .and(expression.eq("ESTRCD", "10").or(expression.eq("ESTRCD", "20")).or(expression.eq("ESTRCD", "21")))

    for (int i = 0; i < output.size(); i++) { //Join FGLEDG in FSLEDG
      DBAction query = database.table("FSLEDG")
        .index("00")
        .matching(expression)
        .selection("ESPYNO", "ESPYCD", "ESPYTP", "ESCINO", "ESINYR", "ESDUDT", "ESTRCD")
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
        output.get(i).put("ESTRCD", container.get("ESTRCD").toString())
        output.get(i).put("EXST", "OK")
      } else {
        output.get(i).put("EXST", "KO")
      }
    }
  }

  /**
   * @getOCUSMA - get all lines from OCUSMA  
   * @params - cono, cuno
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
   * @getCUGEX3 - Read F3TX40 from CUGEVM
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
   * @LstCUGEX3 - List fields from CUGEVM
   * @params - 
   * @returns - CSCD/FEID
   */
  List < String > lstCUGEX3(String cuer) {
    ExpressionFactory expressionFactory = database.getExpressionFactory("CUGEVM")
    ExpressionFactory expression = null

    DBAction query

    if ("FEID".equals(cuer)) {
      expression = expressionFactory.eq("F3TX40", "1")
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

    List < String > results = new ArrayList < > ()
    query.readAll(container, 4, maxRecords, { DBContainer container1 ->
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
   * @getPYCU - get PYCU from CUGEX1    
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
    query.readAll(container, 2, maxRecords, { DBContainer container1 ->
      if (outBound.equals("CCD6")) {
        result = container1.get("CCCCD6").toString()
      } else if (outBound.equals("CONM")) {
        result = container1.get("CCCONM").toString()
      }
    })
    return result
  }

  /**
   * @getORNO - REG05
   * @params - yea4, jrno, jsno
   * @returns - ORNO field from FSLEDG
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
   * @getFGLEDX - Read GEXI from FGLEDX
   * @params - yea4, jrno, jsno, gexn
   * @returns - gexi field from FGLEDX
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
   * @returns - 101|102|0
   */
  int getTYLI(String cugex3, String egtrcd, String estrcd, String ait1, float cuam) {
    if (cugex3.equals("1") && egtrcd.equals("10") && ait1.trim().equals(compteCollectifClient)) {
      return cuam > 0 ? 101 : 102
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
   * @addCustomFields - Add custom fields
   * @params - 
   * @returns -
   */
  void addCustomFields() {
    for (int i = 0; i < output.size(); i++) {
      if (output.get(i).get("EXST").equals("OK")) {

        output.get(i).put("ORNO", getORNO(Integer.parseInt(output.get(i).get("YEA4")), Integer.parseInt(output.get(i).get("JRNO")), Integer.parseInt(output.get(i).get("JSNO"))))
        String pycl = getPYCL(output.get(i).get("PYTP"))
        output.get(i).put("PYCL", pycl)

        output.get(i).put("STAT", getSTAT().toString())

        String cugex3 = getCUGEX3(Integer.parseInt(output.get(i).get("CONO")), "CUGEX3", "FEID", "F3A030", output.get(i).get("FEID"))

        String tyli = getTYLI(cugex3, output.get(i).get("EGTRCD"), output.get(i).get("ESTRCD"), output.get(i).get("AIT1"), Float.parseFloat(output.get(i).get("CUAM"))).toString()
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
          if (domTom.contains(cscd.trim())) {
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
          if (domTom.contains("FR")) {
            String dmtm = "OK-" + pycl
            output.get(i).put("DMTM", dmtm)
          } else {
            String dmtm = "KO-" + pycl
            output.get(i).put("DMTM", dmtm)
          }
        }
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
        conEXT806.set("EXPRCD", output.get(i).get("PRCD"))
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