/****************************************************************************************
Extension Name: EXT370MI.ClotureInvOA
Type : Transaction API
Script Author:Ya'Sin Figuelia
Date:  2024-09-03

Description:  Inverse cloture OA

Revision History:
  Name                           Date          Version              Description of Changes
  Ya'Sin Figuelia                2024-09-03    1.0                  Initial Release
  Ya'Sin Figuelia                2024-10-23    1.1                  Update code according to the validation process
  Ya'Sin Figuelia                2024-11-15    1.2                  Add update status PUST in MPHEAD
  ANDRIANARIVELO Tovonirina      2025-09-04    1.2                  Review for validation
  ANDRIANARIVELO Tovonirina      2025-11-17    1.3                  Update code according to the validation process
  ANDRIANARIVELO Tovonirina      2025-12-01    1.4                  Update code according to the validation process
******************************************************************************************/
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter



public class ClotureInvOA extends ExtendM3Transaction {

  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private final MICallerAPI miCaller
  
  private int inCONO      //Company
  private String inDIVI   //Division
  private String inPUNO   //Purchase Order
  private String inSUNO   //Supplier
  private int inPNLI      //PO Line
  private int inPNLS      //PO Line sub no
  private int inREPN      //Receiving number
  private int inRELP      //Receipt type
  private String outSINO  //Supplier number
  private String outINYR  //invoice year
  private String outINLP  // Inv line type
  private boolean isAllLineOk // check if all line is OK

  public ClotureInvOA(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
  }
  public void main() {
    isAllLineOk = false
    //Initialize input data
    LocalDateTime dateTime = LocalDateTime.now()
    int entryDate = dateTime.format(DateTimeFormatter.ofPattern('yyyyMMdd')).toInteger()
    int maxRecords =  mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 1000 ? 1000 : mi.getMaxRecords()
    inCONO = mi.in.get('CONO') as Integer == null ? program.LDAZD.get('CONO') as Integer : mi.in.get('CONO') as Integer
    inDIVI = mi.inData.get('DIVI') == null ? '' :  mi.inData.get('DIVI').trim()
    inSUNO = mi.inData.get('SUNO').trim()
    inPUNO = mi.inData.get('PUNO').trim()
    inPNLI = mi.in.get('PNLI') as Integer
    inPNLS = mi.in.get('PNLS') as Integer
    inREPN = mi.in.get('REPN') as Integer
    inRELP = mi.in.get('RELP') as Integer

    //Select record in FGRECL
    DBAction dbaFGRECL = database.table('FGRECL').index('00').build()
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

    //Update Status IMST to 0 , Invoice match date to 0 , Invoice qty to 0
    dbaFGRECL.readLock(conFGRECL, { LockedResult lockedResult ->
      lockedResult.set('F2IMST', 0)
      lockedResult.set('F2IMDT', 0)
      lockedResult.set('F2ICAC', 0.0)
      lockedResult.set('F2IVQT', 0.0)
      lockedResult.set('F2IVQA', 0.0)
      lockedResult.set('F2LMDT', entryDate)
      lockedResult.set('F2CHNO', (Integer)lockedResult.get('F2CHNO') + 1)
      lockedResult.set('F2CHID', program.getUser())
      lockedResult.update()
    })

    //Select record in MPLINE
    DBAction dbaMPLINE = database.table('MPLINE').index('00').build()
    DBContainer conMPLINE = dbaMPLINE.getContainer()
    conMPLINE.set('IBCONO', inCONO)
    conMPLINE.set('IBPUNO', inPUNO)
    conMPLINE.set('IBPNLI', inPNLI)
    conMPLINE.set('IBPNLS', inPNLS)

    //Update Status PUST to 75 , Invoice date to 0, Inv qty to 0
    dbaMPLINE.readLock(conMPLINE, { LockedResult lockedResult ->
      lockedResult.set('IBIVQA', 0.0)
      lockedResult.set('IBIDAT', 0)
      lockedResult.set('IBPUST', '75')
      lockedResult.set('IBLMDT', entryDate)
      lockedResult.set('IBCHNO', (Integer)lockedResult.get('IBCHNO') + 1)
      lockedResult.set('IBCHID', program.getUser())
      lockedResult.update()
    })

    //Update Status PUSL and PUST to 75
    updateStatusMphead(entryDate)
    DBAction dbaFGINLI = database.table('FGINLI')
      .index('20')
      .selection('F5SINO', 'F5INYR', 'F5INLP')
      .build()
    DBContainer conFGINLI = dbaFGINLI.getContainer()
    conFGINLI.set('F5CONO', inCONO)
    conFGINLI.set('F5PUNO', inPUNO)
    conFGINLI.set('F5PNLI', inPNLI)
    conFGINLI.set('F5PNLS', inPNLS)

    if (!dbaFGINLI.readAll(conFGINLI, 4, maxRecords, callback)) {
      mi.error('Record does not exist in FGINLI')
      return
    }
  }
  
  /**
   *  @Description: Get SINO ,INYR, INLP And call EXT370MI.DelLineOA
   *  @params: records
   *  @Output:
   */
  Closure<?> callback = { DBContainer container ->
    //Get SINO, INYR , INLP
    outSINO = container.get('F5SINO').toString()
    outINYR = container.get('F5INYR').toString()
    outINLP = container.get('F5INLP').toString()
    //Call API to delete record in FGINLI
    ext370miApiCall()
  }
  
  /**
   *  @Description: Call DelLineOA transaction from EXT370MI
   *  @params: records
   *  @Output:
   */
  void ext370miApiCall() {
    Map<String, String> paras =  [
      'CONO':"${inCONO}".toString(), 'DIVI':"${inDIVI}".toString(),
      'SUNO':"${inSUNO}".toString(),'SINO':"${outSINO}".toString(),
      'PUNO':"${inPUNO}".toString(), 'INYR':"${outINYR}".toString(),
      'PNLI':"${inPNLI}".toString(), 'PNLS':"${inPNLS}".toString(),
      'REPN':"${inREPN}".toString(), 'RELP':"${inRELP}".toString(),
      'INLP':"${outINLP}".toString()]
 
    miCaller.call('EXT370MI', 'DelLineOA', paras, {})
  }

  /**
   *  @Description: Update Status PUST to 0 in MPHEAD
   *  @params:
   *  @Output:
   */
  void updateStatusMphead(int entryDate) {
    //Select record in MPHEAD
    DBAction dbaMPHEAD = database.table('MPHEAD').index('00').build()
    DBContainer conMPHEAD = dbaMPHEAD.getContainer()
    conMPHEAD.set('IACONO', inCONO)
    conMPHEAD.set('IAPUNO', inPUNO)
    //update Status
    dbaMPHEAD.readLock(conMPHEAD, { LockedResult lockedResult ->
      //check if the is line PUST  equal 85
      if (!checkIfLinePust85()) {
        lockedResult.set('IAPUST', '75')
      }
      lockedResult.set('IAPUSL', '75')
      lockedResult.set('IALMDT', entryDate)
      lockedResult.set('IACHNO', (Integer)lockedResult.get('IACHNO') + 1)
      lockedResult.set('IACHID', program.getUser())
      lockedResult.update()
    })
  }
  
   /**
   *  @Description: Check line status high 85 exist
   *  @params:
   *  @Output:
   */
  boolean checkIfLinePust85() {
    int maxRecords =  mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 1000 ? 1000 : mi.getMaxRecords()

    DBAction dbaMPLINE = database.table('MPLINE').index('00')
    .selection('IBPUST').build()
    DBContainer conMPLINE = dbaMPLINE.getContainer()
    conMPLINE.set('IBCONO', inCONO)
    conMPLINE.set('IBPUNO', inPUNO)

    if (!dbaMPLINE.readAll(conMPLINE, 2, maxRecords, callbackMPLINE)) {
      mi.error('Record does not exist YES')
      return
    }
    else {
      return isAllLineOk
    }
  }

  Closure<?> callbackMPLINE = { DBContainer container ->
    if (container.get('IBPUST').toString().trim() == '85') {
      isAllLineOk = true
    }
  }

}
