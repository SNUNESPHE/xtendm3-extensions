/************************************************************************************************************************************************
Extension Name: EXT806MI.AddUpdLines
Type: ExtendM3Transaction
Script Author: NRAOEL
Date: 2024-10-22
Description:
* Add Lines to the dynamic table EXT806 (cheque/effet/prelevement apres remise)

Revision History:
Name          Date        Version   Description of Changes
NRAOEL        2024-10-22  1.0       Initial Release
NRAOEL        2025-11-27  1.1       Correction after validation submission
**************************************************************************************************************************************************/

import java.util.ArrayList
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

public class AddUpdLines extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  public int inCONO //Company
  public String inDIVI //Division
  private int inYEA4 //Year
  private int inJRNO //Journal number
  private int inJSNO //Journal sequence
  public int maxRecords //10000

  public String compteCollectifClient = "" //accountNumber
  public String compteDedieFactor = "" //bankAccountNumber

  private ArrayList < HashMap < String, String >> output103104 = new ArrayList < > () //Array containing accounting entries

  public AddUpdLines(MIAPI mi, ProgramAPI program, DatabaseAPI database, MICallerAPI miCaller) {
    this.mi = mi
    this.program = program
    this.database = database
    this.miCaller = miCaller
  }

  public void main() {
    maxRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 5000 ? 5000 : mi.getMaxRecords()

    // Initialization
    if (!mi.inData.get("CONO").isBlank()) {
      inCONO = mi.in.get("CONO") as Integer
    } else {
      inCONO = program.LDAZD.get("CONO") as Integer
    }
    inDIVI = mi.inData.get("DIVI").isBlank() ? program.LDAZD.get("DIVI") : mi.inData.get("DIVI").trim()
    inYEA4 = mi.in.get("YEA4") as Integer == null ? 0 : mi.in.get("YEA4") as Integer
    inJRNO = mi.in.get("JRNO") as Integer == null ? 0 : mi.in.get("JRNO") as Integer
    inJSNO = mi.in.get("JSNO") as Integer == null ? 0 : mi.in.get("JSNO") as Integer

    compteCollectifClient = getCUGEX1("1")
    compteDedieFactor = getCUGEX1("2")

    if (!compteDedieFactor.trim().equals("") && !compteCollectifClient.trim().equals("")) {
      add103104ChequeApresRemise()
      add103104EffetApresRemise()
      render()
    }
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
    return result.trim()
  }

  /**
   * @checkCKNO - get EGGEXI from FGLEDX
   * @params - jrno, jsno, yea4
   * @returns - ckno : Check Number
   */
  String checkCKNO(int yea4, int jrno, int jsno) {
    ExpressionFactory expression = database.getExpressionFactory("FGLEDX")
    expression = expression.eq("EGGEXN", "2")

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

    String result = ""
    query.readAll(container, 5, 1, { DBContainer container1 ->
      result = container1.get("EGGEXI").toString().trim()
    })
    return result
  }

  /**
   * @checkPYNO - get EGGEXI from FGLEDX
   * @params - jrno, jsno, yea4
   * @returns - pyno : Payer
   */
  String checkPYNO(int yea4, int jrno, int jsnb) {
    ExpressionFactory expression = database.getExpressionFactory("FGLEDX")
    expression = expression.eq("EGGEXN", "8")

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
    container.set("EGJSNO", jsnb)

    String result = ""
    query.readAll(container, 5, 1, { DBContainer container1 ->
      result = container1.get("EGGEXI").toString().trim()
    })
    return result
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
   * @checkFYRMNB - get FYRMNB from FCHKMA
   * @params - pyno, ckno, bkid
   * @returns - rmnb : Remittance Number
   */
  String checkFYRMNB(String pyno, String ckno, String bkid) {
    ExpressionFactory expression = database.getExpressionFactory("FCHKMA")
    expression = expression.eq("FYPYRS", "90")

    DBAction query = database.table("FCHKMA")
      .index("00")
      .selection("FYRMNB")
      .matching(expression)
      .build()
    DBContainer container = query.getContainer()
    container.set("FYCONO", inCONO)
    container.set("FYDIVI", inDIVI)
    container.set("FYPYNO", pyno)
    container.set("FYCKNO", ckno.trim())
    container.set("FYBKID", bkid.trim())

    String result = ""
    query.readAll(container, 5, 1, { DBContainer container1 ->
      result = container1.get("FYRMNB").toString().trim()
    })
    return result
  }

  /**
   * @checkAIT1FromFARREM - get R1BKI2 from FARREM
   * @params - jrno, yea4, rmnb
   * @returns - ait1 : Account Number
   */
  String checkAIT1FromFARREM(String rmnb) {
    ExpressionFactory expression = database.getExpressionFactory("FARREM")
    expression = expression.eq("ERRSTA", "11")

    DBAction query = database.table("FARREM")
      .index("00")
      .selection("ERBKI2")
      .matching(expression)
      .build()
    DBContainer container = query.getContainer()
    container.set("ERCONO", inCONO)
    container.set("ERDIVI", inDIVI)
    container.set("ERRMNB", Integer.parseInt(rmnb))

    String result = ""
    query.readAll(container, 3, 1, { DBContainer container1 ->
      result = container1.get("ERBKI2").toString().trim()
    })
    return result
  }

  /**
   * @checkBankFromCBANAC - get BCAIT1 from CBANAC
   * @params - bkid : Bank Id
   * @returns - ait1 : Account Number
   */
  String checkBankFromCBANAC(String bkid) {
    DBAction query = database.table("CBANAC")
      .index("30")
      .selection("BCAIT1")
      .build()
    DBContainer container = query.getContainer()
    container.set("BCCONO", inCONO)
    container.set("BCDIVI", inDIVI)
    container.set("BCBKID", bkid)

    String result = ""
    query.readAll(container, 3, 1, { DBContainer container1 ->
      result = container1.get("BCAIT1").toString().trim()
    })
    return result
  }

  /**
   * @add103104ChequeApresRemise - REG03 : create 103 104 Cheque (apres la remise en banque), list associated lines
   * @params - 
   * @returns - 
   */
  void add103104ChequeApresRemise() {
    ExpressionFactory expression = database.getExpressionFactory("EXT807")
    expression = expression.ne("EXTUPD", "KO").and(expression.eq("EXPYCL", "2"))

    DBAction query = database.table("EXT807")
      .index("00")
      .selectAllFields()
      .matching(expression)
      .build()
    DBContainer container = query.getContainer()
    container.set("EXCONO", inCONO)
    container.set("EXDIVI", inDIVI)
    container.set("EXYEA4", inYEA4)
    container.set("EXJRNO", inJRNO)
    container.set("EXJSNO", inJSNO)

    query.readAll(container, 5, maxRecords, { DBContainer container1 ->
      HashMap < String, String > tmp1 = new HashMap < > ()
      HashMap < String, String > tmp2 = new HashMap < > ()

      int ext807Yea4 = Integer.parseInt(container1.get("EXYEA4").toString())
      int ext807Jrno = Integer.parseInt(container1.get("EXJRNO").toString())
      int ext807Jsno = Integer.parseInt(container1.get("EXJSNO").toString())
      int ext807Jsnb = Integer.parseInt(container1.get("EXJSNB").toString())
      String ext807Tupd = container1.get("EXTUPD").toString()

      String bkac103104 = ""

      String pyno = checkPYNO(ext807Yea4, ext807Jrno, ext807Jsnb)
      String rawCkno = checkCKNO(ext807Yea4, ext807Jrno, ext807Jsno)
      String ckno = ""
      String bkidFCHKMA = ""

      int spaceIndex = rawCkno.indexOf(" ")

      if (spaceIndex != -1) {
        bkidFCHKMA = rawCkno.substring(0, spaceIndex)
        ckno = rawCkno.substring(spaceIndex + 1)
      } else {
        bkidFCHKMA = rawCkno
        ckno = rawCkno
      }

      String fyrmnb = ""
      String bkid = ""
      String ait1Remise = ""
      boolean found512115 = false
      if (!pyno.trim().equals("") && !ckno.trim().equals("")) {
        fyrmnb = checkFYRMNB(pyno, ckno, bkidFCHKMA)
        if (!fyrmnb.trim().equals("")) {
          bkid = checkAIT1FromFARREM(fyrmnb)
          if (!bkid.trim().equals("")) {
            ait1Remise = checkBankFromCBANAC(bkid.trim())
            if (ait1Remise.trim().equals(compteDedieFactor)) {
              found512115 = true
              bkac103104 = ait1Remise
            } else {
              updateLines103104(ext807Yea4, ext807Jrno, ext807Jsno, fyrmnb, ait1Remise, "103") // Update remittance number and bank account number 
              updateLines103104(ext807Yea4, ext807Jrno, ext807Jsno, fyrmnb, ait1Remise, "104") // Update remittance number and bank account number 
              updateLinesExt807(ext807Yea4, ext807Jrno, ext807Jsno) // Mark that the line has already been processed
            }
          }
        }
      }

      if (found512115 && ext807Tupd.trim().equals("OK")) {
        updateLines103104(ext807Yea4, ext807Jrno, ext807Jsno, fyrmnb, bkac103104, "103") // Update remittance number and bank account number if TUPD(to update) = "OK"
        updateLines103104(ext807Yea4, ext807Jrno, ext807Jsno, fyrmnb, bkac103104, "104") // Update remittance number and bank account number if TUPD(to update) = "OK"
        updateLinesExt807(ext807Yea4, ext807Jrno, ext807Jsno) // Mark that the line has already been processed
      } else if (found512115 && ext807Tupd.trim().equals("")) { // Add lines 103 104 in output array if TUPD(to update) = ""
      
        tmp1.put("CONO", container1.get("EXCONO").toString())
        tmp1.put("DIVI", container1.get("EXDIVI").toString())
        tmp1.put("YEA4", container1.get("EXYEA4").toString())
        tmp1.put("JRNO", container1.get("EXJRNO").toString())
        tmp1.put("JSNO", container1.get("EXJSNO").toString())
        tmp1.put("VONO", container1.get("EXVONO").toString())
        tmp1.put("VSER", container1.get("EXVSER").toString())
        tmp1.put("TYLI", container1.get("EXTYLI").toString())
        tmp1.put("PYNO", container1.get("EXPYNO").toString())
        tmp1.put("ACDT", container1.get("EXACDT").toString())
        tmp1.put("CINO", container1.get("EXCINO").toString())
        tmp1.put("INYR", container1.get("EXINYR").toString())
        tmp1.put("CUCD", container1.get("EXCUCD").toString())
        tmp1.put("CUAM", container1.get("EXCUAM").toString())
        tmp1.put("PYCD", container1.get("EXPYCD").toString())
        tmp1.put("DUDT", container1.get("EXDUDT").toString())
        tmp1.put("VTXT", container1.get("EXVTXT").toString())
        tmp1.put("ORNO", container1.get("EXORNO").toString())
        tmp1.put("STAT", container1.get("EXSTAT").toString())
        tmp1.put("COMP", container1.get("EXCOMP").toString())
        tmp1.put("DATE", container1.get("EXDATE").toString())
        tmp1.put("AIT1", container1.get("EXAIT1").toString())
        tmp1.put("CHID", container1.get("EXCHID").toString())
        tmp1.put("PRCD", container1.get("EXPRCD").toString())
        tmp1.put("NCRE", container1.get("EXNCRE").toString())
        tmp1.put("RMNO", container1.get("EXRMNO").toString())
        tmp1.put("ACSO", container1.get("EXACSO").toString())
        tmp1.put("BKAC", container1.get("EXBKAC").toString())
        
        String customerInfo = container1.get("EXCINF").toString()
        String[] parts = customerInfo.split("~")
        String corg = parts.length > 0 ? parts[0] : ""
        String cor2 = parts.length > 1 ? parts[1] : ""
        String cunm = parts.length > 2 ? parts[2] : ""

        tmp1.put("CORG", corg)
        tmp1.put("COR2", cor2)
        tmp1.put("CUNM", cunm)
        
        String customerAdresses = container1.get("EXCADR").toString()
        String[] addrParts = customerAdresses.split("~")
        String cua1 = addrParts.length > 0 ? addrParts[0] : ""
        String cua2 = addrParts.length > 1 ? addrParts[1] : ""
        String pono = addrParts.length > 2 ? addrParts[2] : ""   
        String town = addrParts.length > 3 ? addrParts[3] : ""
        String cscd = addrParts.length > 4 ? addrParts[4] : ""
        String phno = addrParts.length > 5 ? addrParts[5] : ""

        tmp1.put("CUA1", cua1)
        tmp1.put("CUA2", cua2)
        tmp1.put("PONO", pono)
        tmp1.put("TOWN", town)
        tmp1.put("CSCD", cscd)
        tmp1.put("PHNO", phno)
        tmp1.put("CCD6", container1.get("EXCCD6").toString())
        tmp1.put("CONM", container1.get("EXCONM").toString())
        tmp1.put("DMTM", container1.get("EXDMTM").toString())
        tmp1.put("ACRF", container1.get("EXACRF").toString())
        tmp1.put("PYCU", container1.get("EXPYCU").toString())
        tmp1.put("FEID", container1.get("EXFEID").toString())
        tmp1.put("TRCD", container1.get("EXTRCD").toString())
        tmp1.put("UPD", container1.get("EXTUPD").toString())
        tmp1.put("PYCL", container1.get("EXPYCL").toString())
        tmp1.put("VRNO", container1.get("EXVRNO").toString())
        tmp1.put("EXST", container1.get("EXEXST").toString())

        tmp2.putAll(tmp1)
        tmp2.put("TYLI", "104")
        tmp2.put("CUAM", String.valueOf(-Float.parseFloat(tmp1.get("CUAM"))))

        if (!fyrmnb.trim().equals("")) {
          tmp1.put("RMNO", fyrmnb)
          tmp2.put("RMNO", fyrmnb)
        }

        if (!bkac103104.trim().equals("")) {
          tmp1.put("BKAC", bkac103104)
          tmp2.put("BKAC", bkac103104)
        }

        output103104.add(tmp1)
        output103104.add(tmp2)

        updateLinesExt807(ext807Yea4, ext807Jrno, ext807Jsno) // Mark that the line has already been processed
      }
    })
  }

  /**
   * @getEffectNumber - Get Effect Number 
   * @params - yea4, jrno
   * @returns - 
   */
  String getEffectNumber(int yea4, int jrno, int jsno) {
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
    container.set("ESSEXN", 213)

    String result = ""
    query.readAll(container, 5, 1, {
      DBContainer container1 ->
      result = container1.get("ESSEXI").toString().trim()
    })
    return result
  }

  /**
   * @getBankEffectRemittance - get R1BKI2, R1RMNB from FARRED
   * @params - yea4, rmbe, efno
   * @returns - bki2 : Bank Id
   */
  String getBankEffectRemittance(String yea4, String efno, String pyno, String outBound) {
    ExpressionFactory expression = database.getExpressionFactory("FARRED")
    expression = expression.eq("R1YEA4", yea4).and(expression.eq("R1PYNO", pyno))

    DBAction query = database.table("FARRED")
      .index("80")
      .selection("R1BKI2", "R1RMNB")
      .matching(expression)
      .build()
    DBContainer container = query.getContainer()
    container.set("R1CONO", inCONO)
    container.set("R1DIVI", inDIVI)
    container.set("R1CINO", efno)

    String result = ""
    query.readAll(container, 3, 1, { DBContainer container1 ->
      if (outBound.equals("RMNB")) {
        result = container1.get("R1RMNB").toString().trim()
      } else if (outBound.equals("BKI2")) {
        result = container1.get("R1BKI2").toString().trim()
      }
    })
    return result
  }

  /**
   * @add103104EffetApresRemise - REG03 : create 103 104 Effet (apres la remise en banque), list associated lines
   * @params - 
   * @returns - 
   */
  void add103104EffetApresRemise() {
    ExpressionFactory expression = database.getExpressionFactory("EXT807")
    expression = expression.ne("EXTUPD", "KO").and((expression.eq("EXPYCL", "4")).or(expression.eq("EXPYCL", "5")))

    DBAction query = database.table("EXT807")
      .index("00")
      .selectAllFields()
      .matching(expression)
      .build()
    DBContainer container = query.getContainer()
    container.set("EXCONO", inCONO)
    container.set("EXDIVI", inDIVI)
    container.set("EXYEA4", inYEA4)
    container.set("EXJRNO", inJRNO)
    container.set("EXJSNO", inJSNO)

    query.readAll(container, 5, maxRecords, { DBContainer container1 ->
      String trcd = container1.get("EXTRCD").toString().trim()
      HashMap < String, String > tmp1 = new HashMap < > ()
      HashMap < String, String > tmp2 = new HashMap < > ()

      int ext807Yea4 = Integer.parseInt(container1.get("EXYEA4").toString())
      int ext807Jrno = Integer.parseInt(container1.get("EXJRNO").toString())
      int ext807Jsno = Integer.parseInt(container1.get("EXJSNO").toString())
      int ext807Jsnb = Integer.parseInt(container1.get("EXJSNB").toString())
      String ext807Tupd = container1.get("EXTUPD").toString()
      String pyno = container1.get("EXPYNO").toString()

      String bkac103104 = ""
      String bkid = ""
      String ait1Remise = ""
      String validation = ""
      String efno = ""
      String rmbe = ""

      if (trcd.equals("21")) {
        efno = getEffectNumber(ext807Yea4, ext807Jrno, ext807Jsnb)
      } else {
        efno = getEffectNumber(ext807Yea4, ext807Jrno, ext807Jsno)
      }

      boolean found512115 = false
      if (!efno.trim().equals("")) {
        rmbe = getBankEffectRemittance(ext807Yea4.toString(), efno, pyno, "RMNB")
        bkid = getBankEffectRemittance(ext807Yea4.toString(), efno, pyno, "BKI2")
        if (!rmbe.trim().equals("") && !bkid.trim().equals("")) {
          validation = checkAIT1FromFARREM(rmbe)
          if (!validation.trim().equals("")) {
            ait1Remise = checkBankFromCBANAC(bkid.trim())
            if (ait1Remise.trim().equals(compteDedieFactor)) {
              found512115 = true
              bkac103104 = ait1Remise
            } else {
              updateLines103104(ext807Yea4, ext807Jrno, ext807Jsno, rmbe, ait1Remise, "103") // Update remittance number and bank account number
              updateLines103104(ext807Yea4, ext807Jrno, ext807Jsno, rmbe, ait1Remise, "104") // Update remittance number and bank account number
              updateLinesExt807(ext807Yea4, ext807Jrno, ext807Jsno) // Mark that the line has already been processed
            }
          }
        }
      }

      if (found512115 && ext807Tupd.trim().equals("OK")) {
        updateLines103104(ext807Yea4, ext807Jrno, ext807Jsno, rmbe, bkac103104, "103") // Update remittance number and bank account number if TUPD(to update) = "OK"
        updateLines103104(ext807Yea4, ext807Jrno, ext807Jsno, rmbe, bkac103104, "104") // Update remittance number and bank account number if TUPD(to update) = "OK"
        updateLinesExt807(ext807Yea4, ext807Jrno, ext807Jsno) // Mark that the line has already been processed
      } else if (found512115 && ext807Tupd.trim().equals("")) { // Add lines 103 104 in output array if TUPD(to update) = ""
        tmp1.put("CONO", container1.get("EXCONO").toString())
        tmp1.put("DIVI", container1.get("EXDIVI").toString())
        tmp1.put("YEA4", container1.get("EXYEA4").toString())
        tmp1.put("JRNO", container1.get("EXJRNO").toString())
        tmp1.put("JSNO", container1.get("EXJSNO").toString())
        tmp1.put("VONO", container1.get("EXVONO").toString())
        tmp1.put("VSER", container1.get("EXVSER").toString())
        tmp1.put("TYLI", container1.get("EXTYLI").toString())
        tmp1.put("PYNO", container1.get("EXPYNO").toString())
        tmp1.put("ACDT", container1.get("EXACDT").toString())
        tmp1.put("CINO", container1.get("EXCINO").toString())
        tmp1.put("INYR", container1.get("EXINYR").toString())
        tmp1.put("CUCD", container1.get("EXCUCD").toString())
        tmp1.put("CUAM", container1.get("EXCUAM").toString())
        tmp1.put("PYCD", container1.get("EXPYCD").toString())
        tmp1.put("DUDT", container1.get("EXDUDT").toString())
        tmp1.put("VTXT", container1.get("EXVTXT").toString())
        tmp1.put("ORNO", container1.get("EXORNO").toString())
        tmp1.put("STAT", container1.get("EXSTAT").toString())
        tmp1.put("COMP", container1.get("EXCOMP").toString())
        tmp1.put("DATE", container1.get("EXDATE").toString())
        tmp1.put("AIT1", container1.get("EXAIT1").toString())
        tmp1.put("CHID", container1.get("EXCHID").toString())
        tmp1.put("PRCD", container1.get("EXPRCD").toString())
        tmp1.put("NCRE", container1.get("EXNCRE").toString())
        tmp1.put("RMNO", container1.get("EXRMNO").toString())
        tmp1.put("ACSO", container1.get("EXACSO").toString())
        tmp1.put("BKAC", container1.get("EXBKAC").toString())
        String customerInfo = container1.get("EXCINF").toString()
        String[] parts = customerInfo.split("~")
        String corg = parts.length > 0 ? parts[0] : ""
        String cor2 = parts.length > 1 ? parts[1] : ""
        String cunm = parts.length > 2 ? parts[2] : ""

        tmp1.put("CORG", corg)
        tmp1.put("COR2", cor2)
        tmp1.put("CUNM", cunm)
        
        String customerAdresses = container1.get("EXCADR").toString()
        String[] addrParts = customerAdresses.split("~")
        String cua1 = addrParts.length > 0 ? addrParts[0] : ""
        String cua2 = addrParts.length > 1 ? addrParts[1] : ""
        String pono = addrParts.length > 2 ? addrParts[2] : ""   
        String town = addrParts.length > 3 ? addrParts[3] : ""
        String cscd = addrParts.length > 4 ? addrParts[4] : ""
        String phno = addrParts.length > 5 ? addrParts[5] : ""

        tmp1.put("CUA1", cua1)
        tmp1.put("CUA2", cua2)
        tmp1.put("PONO", pono)
        tmp1.put("TOWN", town)
        tmp1.put("CSCD", cscd)
        tmp1.put("PHNO", phno)
        tmp1.put("CCD6", container1.get("EXCCD6").toString())
        tmp1.put("CONM", container1.get("EXCONM").toString())
        tmp1.put("DMTM", container1.get("EXDMTM").toString())
        tmp1.put("ACRF", container1.get("EXACRF").toString())
        tmp1.put("PYCU", container1.get("EXPYCU").toString())
        tmp1.put("FEID", container1.get("EXFEID").toString())
        tmp1.put("TRCD", container1.get("EXTRCD").toString())
        tmp1.put("UPD", container1.get("EXTUPD").toString())
        tmp1.put("PYCL", container1.get("EXPYCL").toString())
        tmp1.put("VRNO", container1.get("EXVRNO").toString())
        tmp1.put("EXST", container1.get("EXEXST").toString())

        if (!rmbe.trim().equals("")) {
          tmp1.put("RMNO", rmbe)
        }

        if (!bkac103104.trim().equals("")) {
          tmp1.put("BKAC", bkac103104)
        }

        tmp2.putAll(tmp1)
        tmp2.put("TYLI", "104")
        tmp2.put("CUAM", String.valueOf(-Float.parseFloat(tmp1.get("CUAM"))))

        output103104.add(tmp1)
        output103104.add(tmp2)

        updateLinesExt807(ext807Yea4, ext807Jrno, ext807Jsno) //Mark that the line has already been processed
      }
    })
  }

  /**
   * @updateLines103104 - Update remittance and bank account number
   * @params - 
   * @returns -
   */
  void updateLines103104(int ext807Yea4ToExt806, int ext807JrnoToExt806, int ext807JsnoToExt806, String fyrmnb, String bkac, String tyli) {
      DBAction query = database.table("EXT806")
                      .index("00")
                      .build()
      DBContainer container = query.getContainer()
      container.set("EXCONO", inCONO)
      container.set("EXDIVI", inDIVI)
      container.set("EXYEA4", ext807Yea4ToExt806)
      container.set("EXJRNO", ext807JrnoToExt806)
      container.set("EXJSNO", ext807JsnoToExt806)
      container.set("EXTYLI", tyli)
      
      query.readLock(container, { LockedResult lockedResult ->
      lockedResult.set("EXRMNO", Integer.parseInt(fyrmnb))
      lockedResult.set("EXBKAC", bkac)
      lockedResult.update()
    })
  }

  /**
   * @updateLinesExt807 -
   * @params - 
   * @returns - 
   */
  void updateLinesExt807(int yea4, int jrno, int jsno) {
    DBAction query = database.table("EXT807")
      .index("00")
      .build()
    DBContainer container = query.getContainer()
    container.set("EXCONO", inCONO)
    container.set("EXDIVI", inDIVI)
    container.set("EXJRNO", jrno)
    container.set("EXJSNO", jsno)
    container.set("EXYEA4", yea4)

    query.readLock(container, { LockedResult lockedResult ->
      lockedResult.setString("EXTUPD", "KO")
      lockedResult.update()
    })
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
    for (int i = 0; i < output103104.size(); i++) {
      if ("OK".equals(String.valueOf(output103104.get(i).get("EXST")).trim()) && Integer.parseInt(output103104.get(i).get("TYLI")) > 0) {
        DBAction dbaEXT806 = database.table("EXT806").index("00").build()
        DBContainer conEXT806 = dbaEXT806.createContainer()

        LocalDateTime dateTime = LocalDateTime.now()
        int entryDate = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
        int entryTime = dateTime.format(DateTimeFormatter.ofPattern("HHmmss")).toInteger()

        conEXT806.set("EXCONO", Integer.parseInt(output103104.get(i).get("CONO")))
        conEXT806.set("EXDIVI", output103104.get(i).get("DIVI"))
        conEXT806.set("EXYEA4", Integer.parseInt(output103104.get(i).get("YEA4")))
        conEXT806.set("EXJRNO", Integer.parseInt(output103104.get(i).get("JRNO")))
        conEXT806.set("EXJSNO", Integer.parseInt(output103104.get(i).get("JSNO")))
        conEXT806.set("EXVONO", Integer.parseInt(output103104.get(i).get("VONO")))
        conEXT806.set("EXVSER", output103104.get(i).get("VSER"))
        conEXT806.set("EXTYLI", output103104.get(i).get("TYLI"))
        conEXT806.set("EXPYNO", output103104.get(i).get("PYNO"))
        conEXT806.set("EXACDT", Integer.parseInt(output103104.get(i).get("ACDT")))
        conEXT806.set("EXCINO", output103104.get(i).get("CINO"))
        conEXT806.set("EXINYR", Integer.parseInt(output103104.get(i).get("INYR")))
        conEXT806.set("EXCUCD", output103104.get(i).get("CUCD"))
        conEXT806.set("EXCUAM", Double.parseDouble(output103104.get(i).get("CUAM")))
        conEXT806.set("EXPYCD", output103104.get(i).get("PYCD"))
        conEXT806.set("EXDMTM", output103104.get(i).get("DMTM"))
        conEXT806.set("EXDUDT", Integer.parseInt(output103104.get(i).get("DUDT")))
        conEXT806.set("EXVTXT", output103104.get(i).get("VTXT"))
        conEXT806.set("EXORNO", output103104.get(i).get("ORNO"))
        conEXT806.set("EXSTAT", output103104.get(i).get("STAT"))
        conEXT806.set("EXDATE", Integer.parseInt(output103104.get(i).get("DATE")))
        conEXT806.set("EXAIT1", output103104.get(i).get("AIT1"))
        conEXT806.set("EXRGDT", entryDate)
        conEXT806.set("EXRGTM", entryTime)
        conEXT806.set("EXLMDT", entryDate)
        conEXT806.set("EXCHNO", 1)
        conEXT806.set("EXCHID", program.getUser())
        conEXT806.set("EXNCRE", output103104.get(i).get("NCRE"))
        conEXT806.set("EXRMNO", Integer.parseInt(output103104.get(i).get("RMNO")))
        conEXT806.set("EXPRCD", output103104.get(i).get("PRCD"))
        conEXT806.set("EXACSO", output103104.get(i).get("ACSO"))
        conEXT806.set("EXBKAC", output103104.get(i).get("BKAC"))
        conEXT806.set("EXCONM", output103104.get(i).get("CONM"))
        conEXT806.set("EXCCD6", output103104.get(i).get("CCD6"))
        conEXT806.set("EXCORG", output103104.get(i).get("CORG"))
        conEXT806.set("EXCUNM", output103104.get(i).get("CUNM"))
        conEXT806.set("EXCOR2", output103104.get(i).get("COR2"))
        conEXT806.set("EXCUA1", output103104.get(i).get("CUA1"))
        conEXT806.set("EXCUA2", output103104.get(i).get("CUA2"))
        conEXT806.set("EXPONO", output103104.get(i).get("PONO"))
        conEXT806.set("EXTOWN", output103104.get(i).get("TOWN"))
        conEXT806.set("EXCSCD", output103104.get(i).get("CSCD"))
        conEXT806.set("EXPHNO", output103104.get(i).get("PHNO"))
        conEXT806.set("EXACRF", output103104.get(i).get("ACRF"))
        conEXT806.set("EXPYCU", output103104.get(i).get("PYCU"))
        conEXT806.set("EXVRNO", output103104.get(i).get("VRNO"))

        dbaEXT806.insert(conEXT806)

        mi.outData.put("RSLT", "OK")
        mi.write()
      }
    }
  }
}