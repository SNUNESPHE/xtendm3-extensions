{"programModules":{"EXT017MI":{"program":"EXT017MI","triggers":{},"transactions":{"UpdItem":{"sourceUuid":"b51e7e01-d0e1-4911-ab9f-88bec04993f7","name":"UpdItem","program":"EXT017MI","description":"Update item in EXT017","active":true,"multi":false,"modified":1763460265657,"modifiedBy":"NRAOEL","outputFields":[],"inputFields":[{"name":"CONO","description":"Company","length":3,"mandatory":false,"type":"N","refField":null},{"name":"FILE","description":"Table","length":10,"mandatory":false,"type":"A","refField":null},{"name":"DIVI","description":"Division","length":3,"mandatory":false,"type":"A","refField":null},{"name":"ITNO","description":"Item number","length":15,"mandatory":true,"type":"A","refField":null},{"name":"DAT1","description":"Date 1st placed in stock","length":8,"mandatory":false,"type":"N","refField":null},{"name":"DAT2","description":"Date on which inventory became zero","length":8,"mandatory":false,"type":"N","refField":null},{"name":"CONT","description":"A counter of the number of deposits with stocks of the article","length":15,"mandatory":false,"type":"N","refField":null},{"name":"CRAT","description":"Coverage rate","length":15,"mandatory":false,"type":"N","refField":null},{"name":"RRAT","description":"Rotation rate","length":15,"mandatory":false,"type":"N","refField":null}],"utilities":[],"market":"ALL"}},"batches":{},"advancedPrograms":{}}},"utilities":{},"sources":{"b51e7e01-d0e1-4911-ab9f-88bec04993f7":{"uuid":"b51e7e01-d0e1-4911-ab9f-88bec04993f7","updated":1765879511723,"updatedBy":"NRAOEL","created":1763460259782,"createdBy":"NRAOEL","apiVersion":"0.15","beVersion":"16.0.0.20251027120843.5","language":"GROOVY","codeHash":"E47DACE831622EF697F60AAB1067BAEAD893C3AE24E38D367E840AAB6EF959F7","code":"LyoqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioNCiBFeHRlbnNpb24gTmFtZTogRVhUMDE3TUkvVXBkSXRlbQ0KIFR5cGU6IEV4dGVuZE0zVHJhbnNhY3Rpb24NCiBTY3JpcHQgQXV0aG9yOiBUb3ZvbmlyaW5hIEFORFJJQU5BUklWRUxvDQogRGF0ZTogMjAyNS0wMS0yMg0KIERlc2NyaXB0aW9uOg0KICogVXBkYXRlIGl0ZW0gZnJvbSB0aGUgdGFibGUgRVhUMDE3LiANCiAgICANCiBSZXZpc2lvbiBIaXN0b3J5Og0KIE5hbWUgICAgICAgICAgICAgICAgICAgICAgIERhdGUgICAgICAgICAgICAgVmVyc2lvbiAgICAgICAgICBEZXNjcmlwdGlvbiBvZiBDaGFuZ2VzDQogVG92b25pcmluYSBBTkRSSUFOQVJJRUxPICAgMjAyNS0wMS0yMiAgICAgICAxLjAgICAgICAgICAgICAgIEluaXRpYWwgUmVsZWFzZQ0KIFRvdm9uaXJpbmEgQU5EUklBTkFSSUVMTyAgIDIwMjUtMTEtMTcgICAgICAgMi4wICAgICAgICAgICAgICBBZGQgQ1JBVCAmIFJSQVQNCioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKi8NCmltcG9ydCBqYXZhLnRpbWUuTG9jYWxEYXRlDQppbXBvcnQgamF2YS50aW1lLmZvcm1hdC5EYXRlVGltZUZvcm1hdHRlcg0KaW1wb3J0IGphdmEudGltZS5mb3JtYXQuRGF0ZVRpbWVQYXJzZUV4Y2VwdGlvbg0KDQpwdWJsaWMgY2xhc3MgVXBkSXRlbSBleHRlbmRzIEV4dGVuZE0zVHJhbnNhY3Rpb24gew0KICBwcml2YXRlIGZpbmFsIE1JQVBJIG1pDQogIHByaXZhdGUgZmluYWwgUHJvZ3JhbUFQSSBwcm9ncmFtDQogIHByaXZhdGUgZmluYWwgRGF0YWJhc2VBUEkgZGF0YWJhc2UNCiAgLy8gYWxsIHRoZSBpbnB1dCB2YXJpYWJsZXMNCiAgcHJpdmF0ZSBpbnQgaW5DT05PDQogIHByaXZhdGUgU3RyaW5nIGluRElWSQ0KICBwcml2YXRlIFN0cmluZyBpbklUTk8NCiAgcHJpdmF0ZSBTdHJpbmcgaW5GSUxFDQogIHByaXZhdGUgaW50IGluREFUMQ0KICBwcml2YXRlIGludCBpbkRBVDINCiAgcHJpdmF0ZSBpbnQgaW5DT05UDQogIHByaXZhdGUgZmxvYXQgaW5DUkFUDQogIHByaXZhdGUgZmxvYXQgaW5SUkFUDQogIA0KICBwdWJsaWMgVXBkSXRlbShNSUFQSSBtaSwgUHJvZ3JhbUFQSSBwcm9ncmFtLCBEYXRhYmFzZUFQSSBkYXRhYmFzZSkgew0KICAgIHRoaXMubWkgPSBtaQ0KICAgIHRoaXMucHJvZ3JhbSA9IHByb2dyYW0NCiAgICB0aGlzLmRhdGFiYXNlID0gZGF0YWJhc2UNCiAgfQ0KDQogIHB1YmxpYyB2b2lkIG1haW4oKSB7DQogICAgLy8gdmFsaWRhdGUgaW5wdXQgdmFyaWFibGVzDQogICAgaWYgKCF2YWxpZGF0ZUlucHV0VmFyaWFibGVzKCkpIHsNCiAgICAgIHJldHVybg0KICAgIH0NCiAgICBpbnQgbWF4UmVjb3JkcyA9IDEwMDAwDQogICAgLy8gZ2V0IHRoZSBjdXJyZW50IGRhdGUNCiAgICBMb2NhbERhdGUgY3VycmVudERhdGUgPSBMb2NhbERhdGUubm93KCkNCiAgICBEYXRlVGltZUZvcm1hdHRlciBmb3JtYXR0ZXIgPSBEYXRlVGltZUZvcm1hdHRlci5vZlBhdHRlcm4oJ3l5eXlNTWRkJykNCiAgICBTdHJpbmcgZm9ybWF0dGVkRGF0ZSA9IGN1cnJlbnREYXRlLmZvcm1hdChmb3JtYXR0ZXIpDQoNCiAgICBEQkFjdGlvbiBxdWVyeSA9IGRhdGFiYXNlLnRhYmxlKCJFWFQwMTciKQ0KICAgICAgLmluZGV4KCIwMCIpDQogICAgICAuYnVpbGQoKQ0KICAgIERCQ29udGFpbmVyIGNvbnRhaW5lciA9IHF1ZXJ5LmdldENvbnRhaW5lcigpICAgIA0KICAgIGNvbnRhaW5lci5zZXQoIkVYQ09OTyIsIGluQ09OTykNCiAgICBpZihpbkRJVkkpew0KICAgICAgY29udGFpbmVyLnNldCgiRVhESVZJIiwgaW5ESVZJKQ0KICAgIH0NCiAgICBjb250YWluZXIuc2V0KCJFWElUTk8iLCBpbklUTk8pDQogICAgY29udGFpbmVyLnNldCgiRVhGSUxFIiwgaW5GSUxFKQ0KICAgIA0KICAgIHF1ZXJ5LnJlYWRMb2NrKGNvbnRhaW5lciwgeyBMb2NrZWRSZXN1bHQgbG9ja2VkUmVzdWx0IC0+DQogICAgDQogICAgICBpZiAoaW5EQVQxKSB7DQogICAgICAgIGxvY2tlZFJlc3VsdC5zZXQoIkVYREFUMSIsIGluREFUMS5lcXVhbHMoIj8iKSA/ICIiIDogaW5EQVQxKQ0KICAgICAgfQ0KICAgICAgDQogICAgICBpZiAoaW5EQVQyKSB7DQogICAgICAgIGxvY2tlZFJlc3VsdC5zZXQoIkVYREFUMiIsIGluREFUMi5lcXVhbHMoIj8iKSA/ICIiIDogaW5EQVQyKQ0KICAgICAgfQ0KICAgICAgICANCiAgICAgIGlmIChpbkNPTlQpIHsNCiAgICAgICAgbG9ja2VkUmVzdWx0LnNldCgiRVhDT05UIiwgaW5DT05ULmVxdWFscygiPyIpID8gIiIgOiBpbkNPTlQpDQogICAgICB9DQogICAgICANCiAgICAgIGxvY2tlZFJlc3VsdC5zZXQoIkVYQ1JBVCIsIGluQ1JBVC5lcXVhbHMoIj8iKSA/ICIiIDogaW5DUkFUKQ0KICAgICAgDQogICAgICBsb2NrZWRSZXN1bHQuc2V0KCJFWFJSQVQiLCBpblJSQVQuZXF1YWxzKCI/IikgPyAiIiA6IGluUlJBVCkNCg0KICAgICAgDQogICAgICBsb2NrZWRSZXN1bHQuc2V0KCJFWExNRFQiLCBJbnRlZ2VyLnBhcnNlSW50KGZvcm1hdHRlZERhdGUpKQ0KICAgICAgbG9ja2VkUmVzdWx0LnNldCgiRVhDSElEIiwgcHJvZ3JhbS5nZXRVc2VyKCkpDQogICAgICBsb2NrZWRSZXN1bHQuc2V0KCJFWENITk8iLCAoaW50KSBsb2NrZWRSZXN1bHQuZ2V0KCdFWENITk8nKSArIDEpDQoNCiAgICAgIGxvY2tlZFJlc3VsdC51cGRhdGUoKQ0KICAgIH0pDQogICAgDQogIH0NCg0KICAvKioNCiAgICogQGRlc2NyaXB0aW9uIC0gVmFsaWRhdGVzIGlucHV0IHZhcmlhYmxlcw0KICAgKiBAcGFyYW1zIC0NCiAgICogQHJldHVybnMgLSB0cnVlL2ZhbHNlDQogICAqLw0KICBib29sZWFuIHZhbGlkYXRlSW5wdXRWYXJpYWJsZXMoKSB7DQogICAgLy8gSGFuZGxpbmcgQ29tcGFueQ0KICAgIGlmICghbWkuaW4uZ2V0KCdDT05PJykpIHsNCiAgICAgIGluQ09OTyA9IChJbnRlZ2VyKSBwcm9ncmFtLmdldExEQVpEKCkuQ09OTw0KICAgIH0gZWxzZSB7DQogICAgICBpbkNPTk8gPSBtaS5pbi5nZXQoJ0NPTk8nKSBhcyBpbnQNCiAgICB9DQogICAgLy8gSGFuZGxpbmcgRGl2aXNpb24NCiAgICBpZiAoIW1pLmluRGF0YS5nZXQoJ0RJVkknKSkgew0KICAgICAgaW5ESVZJID0gcHJvZ3JhbS5nZXRMREFaRCgpLkRJVkkNCiAgICB9IGVsc2Ugew0KICAgICAgaW5ESVZJID0gbWkuaW5EYXRhLmdldCgnRElWSScpDQogICAgfQ0KICAgIC8vIEhhbmRsaW5nIEl0ZW0gbnVtYmVyDQogICAgaWYoIW1pLmluLmdldCgnSVROTycpKSB7DQogICAgICBtaS5lcnJvcigiTGUgbnVtw6lybyBkJ2FydGljbGUgZXN0IG9ibGlnYXRvaXJlIikNCiAgICAgIHJldHVybiBmYWxzZQ0KICAgIH1lbHNlIHsNCiAgICAgIGluSVROTyA9IG1pLmluLmdldCgnSVROTycpDQogICAgICBpZighdmFsaWRhdGVJdGVtTnVtYmVyKGluQ09OTywgaW5JVE5PKSkgew0KICAgICAgICByZXR1cm4gZmFsc2UNCiAgICAgIH0NCiAgICB9DQogICAgLy9oYW5kbGluZyBGSUxFDQogICAgaWYoIW1pLmluLmdldCgnRklMRScpKSB7DQogICAgICBpbkZJTEUgPSAiTUlUTUFTIg0KICAgIH1lbHNlIHsNCiAgICAgIGluRklMRSA9IG1pLmluLmdldCgnRklMRScpDQogICAgfQ0KICAgIC8vaGFuZGxpbmcgREFUMSBpcyAgb3B0aW9uYWwNCiAgICBpZihtaS5pbi5nZXQoJ0RBVDEnKSkgew0KICAgICAgaW5EQVQxID0gbWkuaW4uZ2V0KCdEQVQxJykNCiAgICAgIGlmKCF2YWxpZGF0ZURhdGVGb3JtYXQoaW5EQVQxKSkgew0KICAgICAgICBtaS5lcnJvcigiTGUgZm9ybWF0IGRlIGxhIGRhdGUgZGUgcHJlbWllciByw6lhcHByb3Zpc2lvbm5lbWVudCBkb2l0IMOqdHJlIFlZWVlNTUREIikNCiAgICAgICAgcmV0dXJuIGZhbHNlDQogICAgICB9DQogICAgfQ0KICAgIC8vaGFuZGxpbmcgREFUMiBpcyAgb3B0aW9uYWwNCiAgICBpZihtaS5pbi5nZXQoJ0RBVDInKSkgew0KICAgICAgaW5EQVQyID0gbWkuaW4uZ2V0KCdEQVQyJykNCiAgICAgIGlmKCF2YWxpZGF0ZURhdGVGb3JtYXQoaW5EQVQyKSkgew0KICAgICAgICBtaS5lcnJvcigiTGUgZm9ybWF0IGRlIGxhIGRhdGUgb8O5IGxlIHN0b2NrIGRldmllbnMgesOpcm8gZG9pdCDDqnRyZSBZWVlZTU1ERCIpDQogICAgICAgIHJldHVybiBmYWxzZQ0KICAgICAgfQ0KICAgIH0NCiAgICAvL2hhbmRsaW5nIENPTlQgaXMgIG9wdGlvbmFsDQogICAgaWYobWkuaW4uZ2V0KCdDT05UJykpIHsNCiAgICAgIGluQ09OVCA9IG1pLmluLmdldCgnQ09OVCcpDQogICAgfQ0KICAgIC8vaGFuZGxpbmcgQ1JBVCBpcyAgb3B0aW9uYWwNCiAgICBpZihtaS5pbi5nZXQoJ0NSQVQnKSkgew0KICAgICAgaW5DUkFUID0gbWkuaW4uZ2V0KCdDUkFUJykNCiAgICB9DQogICAgLy9oYW5kbGluZyBSUkFUIGlzICBvcHRpb25hbA0KICAgIGlmKG1pLmluLmdldCgnUlJBVCcpKSB7DQogICAgICBpblJSQVQgPSBtaS5pbi5nZXQoJ1JSQVQnKQ0KICAgIH0NCiAgICByZXR1cm4gdHJ1ZQ0KICB9DQogIA0KICAvKioNCiAgICogQGRlc2NyaXB0aW9uIC0gVmFsaWRhdGVzIGl0ZW0gbnVtYmVyDQogICAqIEBwYXJhbXMgLQ0KICAgKiBAcmV0dXJucyAtIHRydWUvZmFsc2UNCiAgICovDQogIGJvb2xlYW4gdmFsaWRhdGVJdGVtTnVtYmVyKGludCBzQ09OTywgU3RyaW5nIHNJVE5PKSB7DQogICAgLy8gY2hlY2sgaWYgdGhlIGl0ZW0gbnVtYmVyIGlzIHZhbGlkDQogICAgREJBY3Rpb24gcmVhZFF1ZXJ5ID0gZGF0YWJhc2UudGFibGUoIk1JVE1BUyIpDQogICAgICAuaW5kZXgoIjAwIikNCiAgICAgIC5zZWxlY3Rpb24oIk1NSVROTyIpDQogICAgICAuYnVpbGQoKQ0KICAgIERCQ29udGFpbmVyIHJlYWRDb250YWluZXIgPSByZWFkUXVlcnkuZ2V0Q29udGFpbmVyKCkNCiAgICByZWFkQ29udGFpbmVyLnNldCgiTU1DT05PIiwgc0NPTk8pDQogICAgcmVhZENvbnRhaW5lci5zZXQoIk1NSVROTyIsIHNJVE5PKQ0KICAgIGlmIChyZWFkUXVlcnkucmVhZChyZWFkQ29udGFpbmVyKSkgew0KICAgICAgcmV0dXJuIHRydWUNCiAgICB9IGVsc2Ugew0KICAgICAgbWkuZXJyb3IoIkxlIG51bcOpcm8gZCdhcnRpY2xlICR7c0lUTk99IG4nZXhpc3RlIHBhcyBzdXIgbGEgY29tcGFnbmllICR7c0NPTk99IikNCiAgICAgIHJldHVybiBmYWxzZQ0KICAgIH0NCiAgfQ0KDQogICAvKioNCiAgICAqIEBkZXNjcmlwdGlvbiAtIENoZWNrIGlmIGRhdGUgaXMgdmFsaWQNCiAgICAqIEBwYXJhbXMgLSBkYXRlLGZvcm1hdA0KICAgICogQHJldHVybnMgLSBib29sZWFuDQogICAgKi8gDQogIHB1YmxpYyBib29sZWFuIHZhbGlkYXRlRGF0ZUZvcm1hdChpbnQgaW5wdXQpew0KICAgIGlmKGlucHV0ID09IG51bGwgfHwgaW5wdXQgPT0gMCl7DQogICAgICByZXR1cm4gdHJ1ZQ0KICAgIH0NCiAgICBTdHJpbmcgZGF0ZSA9IGlucHV0LnRvU3RyaW5nKCkNCiAgICB0cnkgew0KICAgICAgTG9jYWxEYXRlLnBhcnNlKGRhdGUsIERhdGVUaW1lRm9ybWF0dGVyLm9mUGF0dGVybigieXl5eU1NZGQiKSkNCiAgICAgIHJldHVybiB0cnVlDQogICAgfSBjYXRjaCAoRGF0ZVRpbWVQYXJzZUV4Y2VwdGlvbiBlKSB7DQogICAgICByZXR1cm4gZmFsc2UNCiAgICB9DQogIH0NCn0NCg=="}}/****************************************************************************************
 Extension Name: EXT017MI/UpdItem
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELo
 Date: 2025-01-22
 Description:
 * Update item from the table EXT017. 
    
 Revision History:
 Name                       Date             Version          Description of Changes
 Tovonirina ANDRIANARIELO   2025-01-22       1.0              Initial Release
 Tovonirina ANDRIANARIELO   2025-11-17       2.0              Add CRAT & RRAT
******************************************************************************************/
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

public class UpdItem extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  // all the input variables
  private int inCONO
  private String inDIVI
  private String inITNO
  private String inFILE
  private int inDAT1
  private int inDAT2
  private int inCONT
  private float inCRAT
  private float inRRAT
  
  public UpdItem(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
    this.mi = mi
    this.program = program
    this.database = database
  }

  public void main() {
    // validate input variables
    if (!validateInputVariables()) {
      return
    }
    int maxRecords = 10000
    // get the current date
    LocalDate currentDate = LocalDate.now()
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern('yyyyMMdd')
    String formattedDate = currentDate.format(formatter)

    DBAction query = database.table("EXT017")
      .index("00")
      .build()
    DBContainer container = query.getContainer()    
    container.set("EXCONO", inCONO)
    if(inDIVI){
      container.set("EXDIVI", inDIVI)
    }
    container.set("EXITNO", inITNO)
    container.set("EXFILE", inFILE)
    
    query.readLock(container, { LockedResult lockedResult ->
    
      if (inDAT1) {
        lockedResult.set("EXDAT1", inDAT1.equals("?") ? "" : inDAT1)
      }
      
      if (inDAT2) {
        lockedResult.set("EXDAT2", inDAT2.equals("?") ? "" : inDAT2)
      }
        
      if (inCONT) {
        lockedResult.set("EXCONT", inCONT.equals("?") ? "" : inCONT)
      }
      
      lockedResult.set("EXCRAT", inCRAT.equals("?") ? "" : inCRAT)
      
      lockedResult.set("EXRRAT", inRRAT.equals("?") ? "" : inRRAT)

      
      lockedResult.set("EXLMDT", Integer.parseInt(formattedDate))
      lockedResult.set("EXCHID", program.getUser())
      lockedResult.set("EXCHNO", (int) lockedResult.get('EXCHNO') + 1)

      lockedResult.update()
    })
    
  }

  /**
   * @description - Validates input variables
   * @params -
   * @returns - true/false
   */
  boolean validateInputVariables() {
    // Handling Company
    if (!mi.in.get('CONO')) {
      inCONO = (Integer) program.getLDAZD().CONO
    } else {
      inCONO = mi.in.get('CONO') as int
    }
    // Handling Division
    if (!mi.inData.get('DIVI')) {
      inDIVI = program.getLDAZD().DIVI
    } else {
      inDIVI = mi.inData.get('DIVI')
    }
    // Handling Item number
    if(!mi.in.get('ITNO')) {
      mi.error("Le numéro d'article est obligatoire")
      return false
    }else {
      inITNO = mi.in.get('ITNO')
      if(!validateItemNumber(inCONO, inITNO)) {
        return false
      }
    }
    //handling FILE
    if(!mi.in.get('FILE')) {
      inFILE = "MITMAS"
    }else {
      inFILE = mi.in.get('FILE')
    }
    //handling DAT1 is  optional
    if(mi.in.get('DAT1')) {
      inDAT1 = mi.in.get('DAT1')
      if(!validateDateFormat(inDAT1)) {
        mi.error("Le format de la date de premier réapprovisionnement doit être YYYYMMDD")
        return false
      }
    }
    //handling DAT2 is  optional
    if(mi.in.get('DAT2')) {
      inDAT2 = mi.in.get('DAT2')
      if(!validateDateFormat(inDAT2)) {
        mi.error("Le format de la date où le stock deviens zéro doit être YYYYMMDD")
        return false
      }
    }
    //handling CONT is  optional
    if(mi.in.get('CONT')) {
      inCONT = mi.in.get('CONT')
    }
    //handling CRAT is  optional
    if(mi.in.get('CRAT')) {
      inCRAT = mi.in.get('CRAT')
    }
    //handling RRAT is  optional
    if(mi.in.get('RRAT')) {
      inRRAT = mi.in.get('RRAT')
    }
    return true
  }
  
  /**
   * @description - Validates item number
   * @params -
   * @returns - true/false
   */
  boolean validateItemNumber(int sCONO, String sITNO) {
    // check if the item number is valid
    DBAction readQuery = database.table("MITMAS")
      .index("00")
      .selection("MMITNO")
      .build()
    DBContainer readContainer = readQuery.getContainer()
    readContainer.set("MMCONO", sCONO)
    readContainer.set("MMITNO", sITNO)
    if (readQuery.read(readContainer)) {
      return true
    } else {
      mi.error("Le numéro d'article ${sITNO} n'existe pas sur la compagnie ${sCONO}")
      return false
    }
  }

   /**
    * @description - Check if date is valid
    * @params - date,format
    * @returns - boolean
    */ 
  public boolean validateDateFormat(int input){
    if(input == null || input == 0){
      return true
    }
    String date = input.toString()
    try {
      LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyyMMdd"))
      return true
    } catch (DateTimeParseException e) {
      return false
    }
  }
}
