/****************************************************************************************
Extension Name: EXT370MI.AddLineOA
Type : Transaction API
Script Author:Ya'Sin Figuelia
Date:  2024-09-03

Description:  Add line in FGINLI on cloture OA

Revision History:
  Name                           Date          Version              Description of Changes
  Ya'Sin Figuelia                2024-09-03    1.0                  Initial Release
  Ya'Sin Figuelia                2024-10-23    1.1                  Update code according to the validation process
  ANDRIANARIVELO Tovonirina      2025-09-04    1.2                  Review for validation
  ANDRIANARIVELO Tovonirina      2025-11-17    1.3                  Update code according to the validation process
******************************************************************************************/
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.LocalDate

public class AddLineOA extends ExtendM3Transaction {

  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private int inCONO     //Company
  private String inDIVI  //Division
  private String inSUNO  //Supplier
  private String inSINO  //Supplier number
  private  String inPUNO //Purchase Order number
  private int inPNLI     //PO Line
  private int inPNLS     //PO Line sub no
  private int inREPN     //Receiving number
  private int inRELP     //Receipt type
  private double inIVQT  //Invoice qty
  private double inIVQA  //Inv qty alt U/M
  private double inIVOC  //Inv price pur
  private double inIVNA  //Inv net amount
  private double inIVDI  //Invoiced disc
  private String inPUUN  //PO U/M
  private String inPPUN  //Purch price U/M
  private int inPUCD     //Purch price qty
  private double inIVCW  //Invoiced C/W
  private double inSERA  //Rcdv exch rate
  private int inVTCD     //VAT code

  public AddLineOA(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
    this.mi = mi
    this.database = database
    this.program = program
  }

  public void main() {
    //Initialize input data
    LocalDateTime dateTime = LocalDateTime.now()
    int entryDate = dateTime.format(DateTimeFormatter.ofPattern('yyyyMMdd')).toInteger()
    int entryTime = dateTime.format(DateTimeFormatter.ofPattern('HHmmss')).toInteger()
    inCONO = mi.in.get('CONO') as Integer == null ? program.LDAZD.get('CONO') as Integer : mi.in.get('CONO') as Integer
    inSUNO = mi.inData.get('SUNO').trim()
    inPUNO = mi.inData.get('PUNO').trim()
    inDIVI = mi.inData.get('DIVI') == null ? '' :  mi.inData.get('DIVI').trim()
    inSINO = mi.inData.get('SINO') == null ? '' :  mi.inData.get('SINO').trim()
    inPNLI = (mi.in.get('PNLI') == null) ? 0 : mi.in.get('PNLI') as Integer
    inPNLS = (mi.in.get('PNLS') == null) ? 0 : mi.in.get('PNLS') as Integer
    inREPN = (mi.in.get('REPN') == null) ? 0 : mi.in.get('REPN') as Integer
    inRELP = (mi.in.get('RELP') == null) ? 0 : mi.in.get('RELP') as Integer

    inIVQT = ((mi.in.get('IVQT') == null) ? 0 : mi.in.get('IVQT')) as double
    inIVQA = ((mi.in.get('IVQA') == null) ? 0 : mi.in.get('IVQA')) as double
    inIVOC = ((mi.in.get('IVOC') == null) ? 0 : mi.in.get('IVOC')) as double
    inIVNA = ((mi.in.get('IVNA') == null) ? 0 : mi.in.get('IVNA')) as double
    inIVDI = ((mi.in.get('IVDI') == null) ? 0 : mi.in.get('IVDI')) as double

    inPUUN = mi.inData.get('PUUN') == null ? '' :  mi.inData.get('PUUN').trim()
    inPPUN = mi.inData.get('PPUN') == null ? '' :  mi.inData.get('PPUN').trim()
    inPUCD = ((mi.in.get('PUCD') == null) ? 0 : mi.in.get('PUCD')) as Integer
    inIVCW = ((mi.in.get('IVCW') == null) ? 0 : mi.in.get('IVCW')) as double
    inSERA = ((mi.in.get('SERA') == null) ? 0 : mi.in.get('SERA')) as double
    inVTCD = ((mi.in.get('VTCD') == null) ? 0 : mi.in.get('VTCD')) as Integer
    //Check input Validation
    if (checkInputValid()) {
      //Select record if FGINLI
      DBAction dbaFGINLI = database.table('FGINLI').index('00').build()
      DBContainer conFGINLI = dbaFGINLI.getContainer()
      conFGINLI.set('F5CONO', inCONO)
      if (inDIVI) {
        conFGINLI.set('F5DIVI', inDIVI)
      }
      conFGINLI.set('F5SUNO', inSUNO)
      conFGINLI.set('F5SINO', inSINO)
      conFGINLI.set('F5INYR', 0)
      conFGINLI.set('F5PUNO', inPUNO)
      conFGINLI.set('F5PNLI', inPNLI)
      conFGINLI.set('F5PNLS', inPNLS)
      conFGINLI.set('F5REPN', inREPN)
      conFGINLI.set('F5RELP', inRELP)
      conFGINLI.set('F5INLP', 3)
      //Check if the record already exist
      if (dbaFGINLI.read(conFGINLI)) {
        mi.error('Record already exist')
        return
      }else {
        conFGINLI = dbaFGINLI.createContainer()
        conFGINLI.set('F5CONO', inCONO)
        if (inDIVI) {
          conFGINLI.set('F5DIVI', inDIVI)
        }
        conFGINLI.set('F5SUNO', inSUNO)
        conFGINLI.set('F5SINO', inSINO)
        conFGINLI.set('F5INYR', 0)
        conFGINLI.set('F5PUNO', inPUNO)
        conFGINLI.set('F5PNLI', inPNLI)
        conFGINLI.set('F5PNLS', inPNLS)
        conFGINLI.set('F5REPN', inREPN)
        conFGINLI.set('F5RELP', inRELP)
        conFGINLI.set('F5INLP', 3)
        conFGINLI.set('F5INS1', '3')

        conFGINLI.set('F5INS2', '3')

        conFGINLI.set('F5INS3', '3')

        conFGINLI.set('F5INS4', '3')

        conFGINLI.set('F5INS5', '3')

        conFGINLI.set('F5VRCD', '10')
        conFGINLI.set('F5SERS', 0)
        conFGINLI.set('F5ACDT', 0)
        conFGINLI.set('F5IMST', 0)

        conFGINLI.set('F5DNQT', 0.0)
        conFGINLI.set('F5DNQA', 0.0)
        conFGINLI.set('F5DNPR', 0.0)
        conFGINLI.set('F5DNCM', 0.0)
        conFGINLI.set('F5IMDT', 0)
        conFGINLI.set('F5RPQT', 0.0)
        conFGINLI.set('F5RPQA', 0.0)
        conFGINLI.set('F5RCAC', 0.0)
        conFGINLI.set('F5ICAC', 0.0)

        if (inIVQT) {
          conFGINLI.set('F5IVQT', inIVQT)
        }
        if (inIVQA) {
          conFGINLI.set('F5IVQA', inIVQA)
        }
        if (inIVOC) {
          conFGINLI.set('F5IVOC', inIVOC)
        }
        if (inIVNA) {
          conFGINLI.set('F5IVNA', inIVNA)
        }
        if (inIVDI) {
          conFGINLI.set('F5IVDI', inIVDI)
        }
        if (inPUUN) {
          conFGINLI.set('F5PUUN', inPUUN)
        }
        if (inPPUN) {
          conFGINLI.set('F5PPUN', inPPUN)
        }
        if (inPUCD) {
          conFGINLI.set('F5PUCD', inPUCD)
        }
        if (inIVCW) {
          conFGINLI.set('F5IVCW', inIVCW)
        }
      
          conFGINLI.set('F5ADDG', 0.0)
       
          conFGINLI.set('F5IVLC', 0)
        
        if (inSERA) {
          conFGINLI.set('F5SERA', inSERA)
        }

        if (inVTCD) {
          conFGINLI.set('F5VTCD', inVTCD)
        }
      
        conFGINLI.set('F5LMDT', entryDate)
        conFGINLI.set('F5RGDT', entryDate)
        conFGINLI.set('F5RGTM', entryTime)
        conFGINLI.set('F5CHNO', 0)
        conFGINLI.set('F5CHID', program.getUser())
        //Insert record in FGINLI
        if(dbaFGINLI.insert(conFGINLI)){
           mi.outData.put("RSLT","OK");
           mi.write();
        }
      }
    }
  }

  /**
  * @description - Validates input fields
  * @params -
  * @returns - true/false
  */
  Boolean checkInputValid() {
    return valideSUNO() && valideSINO() && validePUUN_PPUN_VTCD() && validePUNO_PNLI_PNLS_REPN_RELP()
  }

/**
   * @description - Validate SUNO
   * @params -
   * @returns - true/false
   */
  Boolean valideSUNO() {
    DBAction dbCIDMAS = database.table('CIDMAS').index('00').build()
    DBContainer conCIDMAS = dbCIDMAS.getContainer()
    conCIDMAS.set('IDCONO', inCONO)
    conCIDMAS.set('IDSUNO', inSUNO)
    if (!dbCIDMAS.read(conCIDMAS)) {
      mi.error('Supplier number ' + inSUNO + ' does not exist  in Company ' + inCONO)
      return false
    }
    return true
  }

  /**
    * @Description: Validate SINO
    * @params: -
    * @Output : true/false
    */
  Boolean valideSINO() {
    if (!inSUNO.isEmpty()) {
      DBAction dbFGINLI = database.table('FGINLI').index('00').build()
      DBContainer conFGINLI = dbFGINLI.getContainer()
      conFGINLI.set('F5CONO', inCONO)
      conFGINLI.set('F5SUNO', inSUNO)
      conFGINLI.set('F5SINO', inSINO)
      if (dbFGINLI.read(conFGINLI)) {
        mi.error('Supplier invoice number ' + inSINO + ' already exists for supplier ' + inSUNO + ' in Company ' + inCONO)
        return false
      }
    }
    return true
  }

  /**
    * @Description: Validate PUUN, PPUN, PUCD, VTCD
    * @params: -
    * @Output : true/false
    */
  Boolean validePUUN_PPUN_VTCD() {
    DBAction dbaMPLINE = database.table('MPLINE').index('00').selection('IBPUUN', 'IBPPUN', 'IBVTCD').build()
    DBContainer conMPLINE = dbaMPLINE.getContainer()
    conMPLINE.set('IBCONO', inCONO)
    conMPLINE.set('IBPUNO', inPUNO)
    conMPLINE.set('IBPNLI', inPNLI)
    conMPLINE.set('IBPNLS', inPNLS)
    if (!dbaMPLINE.read(conMPLINE)) {
      mi.error('Record does not exist in MPLINE')
      return false
    }

    return true
  }

  /**
   *  @Description:Validate PUNO, PNLI, PNLS, REPN, RELP
   *  @params: -
   *  @Output : true/false
   */
  Boolean validePUNO_PNLI_PNLS_REPN_RELP() {
    DBAction dbaFGRECL = database.table('FGRECL').index('00').build()
    DBContainer conFGRECL = dbaFGRECL.getContainer()
    conFGRECL.set('F2CONO', inCONO)
    conFGRECL.set('F2DIVI', inDIVI)
    conFGRECL.set('F2PUNO', inPUNO)
    conFGRECL.set('F2PNLI', inPNLI)
    conFGRECL.set('F2PNLS', inPNLS)
    conFGRECL.set('F2REPN', inREPN)
    conFGRECL.set('F2RELP', inRELP)
    if (!dbaFGRECL.read(conFGRECL)) {
      mi.error(
          'The line containing ' + ' Division: ' + inDIVI.toString() + ',Purchase order: ' + inPUNO.toString() + ',PO Line:' + inPNLI.toString() + ',PO Line sub no:' + inPNLS.toString() + ',Receiving number:' + inREPN.toString() + ',Receipt type:' + inRELP.toString() + ' does not exist in Company ' + inCONO
            )
      return false
    }
    return true
  }



}
