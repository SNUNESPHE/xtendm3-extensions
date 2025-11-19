/****************************************************************************************
Extension Name: EXT370MI.ClotureOA
Type : Transaction API
Script Author:Ya'Sin Figuelia
Date:  2024-09-03

Description:  Cloture OA

Revision History:
  Name                           Date          Version              Description of Changes
  Ya'Sin Figuelia                2024-09-03    1.0                  Initial Release
  Ya'Sin Figuelia                2024-10-23    1.1                  Update code according to the validation process
  Ya'Sin Figuelia                2024-11-15    1.2                  Add update status PUST in MPHEAD
  ANDRIANARIVELO Tovonirina      2025-09-04    1.2                  Review for validation
  ANDRIANARIVELO Tovonirina      2025-11-17    1.3                  Update code according to the validation process
******************************************************************************************/
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class ClotureOA extends ExtendM3Transaction {

  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private final MICallerAPI miCaller
  private final ExtensionAPI extension
  
  private int inCONO    //Company
  private String inPUNO //Purchase Order
  private int inPNLI    //PO Line
  private int inPNLS    //PO Line sub no
  private String inSUNO //Supplier
  private String inDIVI //Division
  private int inREPN    //Receiving no
  private int inRELP    //Receipt type
  private double outRCAC //Rcvd cost amount
  private double outRPQT //Reported Qty
  private double outRPQA //Reported Qty
  private String outSINO //Fournisseur fact
  private String outIVOC //Inv price pur
  private String outIVNA //Inv net amount
  private String outIVDI //Invoiced disc
  private String outPUUN //PO U/M
  private String outPPUN //Purch price U/M
  private int outPUCD    //Purch price qty
  private double outIVCW //Invoiced C/W
  private double outSERA //Rcdv exch rate
  private int outVTCD    //VAT code
  private int nbRecord   //number of record
  private int nbRecordPUST85 // number of record having PUST equal to 85
  private int maxRecords //Nombre d'enregistrement maximum
  private boolean isAllLineOk // check if all line is OK

  public ClotureOA(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
  }

  public void main() {
    //Initialize variable
    maxRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 1000 ? 1000 : mi.getMaxRecords()
    LocalDateTime dateTime = LocalDateTime.now()
    int entryDate = dateTime.format(DateTimeFormatter.ofPattern('yyyyMMdd')).toInteger()
    nbRecord = 0
    nbRecordPUST85 = 0
    isAllLineOk = true
    inCONO = mi.in.get('CONO') as Integer == null ? program.LDAZD.get('CONO') as Integer : mi.in.get('CONO') as Integer
    inDIVI = mi.inData.get('DIVI') == null ? '' :  mi.inData.get('DIVI').trim()
    inSUNO = mi.inData.get('SUNO').trim()
    inPUNO = mi.inData.get('PUNO').trim()
    inPNLI = mi.in.get('PNLI') as Integer
    inPNLS = mi.in.get('PNLS') as Integer
    inREPN = mi.in.get('REPN') as Integer
    inRELP = mi.in.get('RELP') as Integer
    //Select record
    DBAction dbaFGRECL = database.table('FGRECL').index('00').selection('F2RCAC', 'F2RPQT', 'F2RPQA', 'F2IVQT', 'F2RPQA', 'F2SCDC', 'F2SCOC', 'F2IVCW', 'F2SERA').build()
    DBContainer conFGRECL = dbaFGRECL.getContainer()
    conFGRECL.set('F2CONO', inCONO)
    if (inDIVI) {
      conFGRECL.set('F2DIVI', inDIVI)
    }
    conFGRECL.set('F2PUNO', inPUNO)
    conFGRECL.set('F2PNLI', inPNLI)
    conFGRECL.set('F2PNLS', inPNLS)
    conFGRECL.set('F2REPN', inREPN)
    conFGRECL.set('F2RELP', inRELP)
      
    outRCAC = conFGRECL.get('F2RCAC').toString() as double
    outRPQT = conFGRECL.get('F2RPQT').toString() as double
    outRPQA = conFGRECL.get('F2RPQA').toString() as double
    outIVDI = conFGRECL.get('F2SCDC').toString()
    outIVNA = conFGRECL.get('F2SCOC').toString()
    outIVCW = conFGRECL.get('F2IVCW').toString() as double
    outSERA = conFGRECL.get('F2SERA').toString() as double
    outIVOC = outRCAC

    //Update record in FGRECL
    dbaFGRECL.readLock(conFGRECL, { LockedResult lockedResult ->
      lockedResult.set('F2IMST', 9)
      lockedResult.set('F2IMDT', entryDate)
      lockedResult.set('F2ICAC', outRCAC)
      lockedResult.set('F2IVQT', outRPQT)
      lockedResult.set('F2IVQA', outRPQA)
      lockedResult.update()
    })
    //Select record from MPLINE
    DBAction dbaMPLINE = database.table('MPLINE').index('00').selection('IBPUUN', 'IBPPUN', 'IBPUCD', 'IBVTCD').build()
    DBContainer conMPLINE = dbaMPLINE.getContainer()
    conMPLINE.set('IBPUNO', inPUNO)
    conMPLINE.set('IBPNLI', inPNLI)
    conMPLINE.set('IBPNLS', inPNLS)
    conMPLINE.set('IBCONO', inCONO)

    //Get record from MPLINE
    outPUUN = conMPLINE.get('IBPUUN').toString()
    outPPUN = conMPLINE.get('IBPPUN').toString()
    outPUCD = conMPLINE.get('IBPUCD').toString() as Integer
    outVTCD = conMPLINE.get('IBVTCD').toString() as Integer //CVATPC
    //Update  Status PUST to 85 in MPLINE
    dbaMPLINE.readLock(conMPLINE, { LockedResult lockedResult ->
      lockedResult.set('IBIVQA', outRPQA)
      lockedResult.set('IBIDAT', entryDate)
      lockedResult.set('IBPUST', '85')
      lockedResult.set('IBPUSL', '85')
      lockedResult.set('IBLMDT', entryDate)
      lockedResult.set('IBCHNO', (Integer)lockedResult.get('IBCHNO') + 1)
      lockedResult.set('IBCHID', program.getUser())
      lockedResult.update()
    })
    //Update  Status PUST to 85 in MPHEAD
    updateStatusMPHEAD(entryDate)
    DBAction query = database.table('FGINLI')
      .index('00')
      .build()
    DBContainer container = query.getContainer()
    container.set('F5CONO', inCONO)
    container.set('F5PUNO', inPUNO)
    query.readAll(container, 2, maxRecords, releasedItemProcessor)
    //Compute SINO
    outSINO = nbRecord + 1
    outSINO = "0000${outSINO}"
    
    EXT370MIApiCall()
      
    
  }
  /**
   *  @Description: Get the number of record in FGINLI from the PUNO
   *  @params:
   *  @Output:
   */
  Closure<?> releasedItemProcessor = { DBContainer container ->
    nbRecord = nbRecord + 1
  }
    /**
   *  @Description: Call AddLineOA transaction from EXT370MI
   *  @params: records
   *  @Output:
   */
  void EXT370MIApiCall() {
   
    Map<String, String> paras =  [ 'CONO':"${inCONO}".toString(),
    'DIVI':"${inDIVI}".toString(), 'SUNO':"${inSUNO}".toString(),
    'SINO':"${outSINO}".toString(), 'PUNO':"${inPUNO}".toString(),
    'PNLI':"${inPNLI}".toString(), 'PNLS':"${inPNLS}".toString(),
    'REPN':"${inREPN}".toString(), 'RELP':"${inRELP}".toString(),
    'IVQT':"${outRPQT}".toString(), 'IVQA':"${outRPQA}".toString(),
    'IVOC':"${outIVNA}".toString(),'IVNA':"${outIVOC}".toString(),
    'IVDI':"${outIVDI}".toString(), 'PUUN':"${outPUUN}".toString(),
    'PPUN':"${outPPUN}".toString(),'PUCD':"${outPUCD}".toString(),
    'IVCW':"${outIVCW}".toString(),'SERA':"${outSERA}".toString(),
    'VTCD':"${outVTCD}".toString()]


    miCaller.call('EXT370MI', 'AddLineOA', paras, {})
  }
   /**
   *  @Description: Update Status PUST to 85 in MPHEAD
   *  @params:
   *  @Output:
   */
  void updateStatusMPHEAD(int entryDate) {
    //Select record in MPHEAD
    DBAction dbaMPHEAD = database.table('MPHEAD').index('00').build()
    DBContainer conMPHEAD = dbaMPHEAD.getContainer()
    conMPHEAD.set('IACONO', inCONO)
    conMPHEAD.set('IAPUNO', inPUNO)
    //update Status
    dbaMPHEAD.readLock(conMPHEAD, { LockedResult lockedResult ->
      lockedResult.set('IAPUST', '85')
      if (checkAllLinePUST85()) {
        lockedResult.set('IAPUSL', '85')
      }
      lockedResult.set('IALMDT', entryDate)
      lockedResult.set('IACHNO', (Integer)lockedResult.get('IACHNO') + 1)
      lockedResult.set('IACHID', program.getUser())
      lockedResult.update()
    })
    
  }
  /**
   *  @Description: Check all line status low is 85
   *  @params:
   *  @Output:
   */
  boolean checkAllLinePUST85() {
    DBAction dbaMPLINE = database.table('MPLINE').index('00').
    selection('IBPUST').build()
    DBContainer conMPLINE = dbaMPLINE.getContainer()
    conMPLINE.set('IBCONO', inCONO)
    conMPLINE.set('IBPUNO', inPUNO)
    if (!dbaMPLINE.readAll(conMPLINE, 2, maxRecords, callbackMPLINE)) {
      mi.error('Record does not exist YES')
      return
    }
    else {
      if (nbRecordPUST85 == 1) {
        return true
      }
      else if (nbRecordPUST85 > 1) {
        return isAllLineOk
      }
    }
  }
  Closure<?> callbackMPLINE = { DBContainer container ->
    if (container.get('IBPUST').toString().trim() != '85') {
      isAllLineOk = false
    }
    nbRecordPUST85 = nbRecordPUST85 + 1
  }

}
