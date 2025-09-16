/****************************************************************************************
 Extension Name: EXT017MI/LstItemByLot
 Type: ExtendM3Transaction
 Script Author: Tovonirina ANDRIANARIVELo
 Date: 2025-04-15
 Description:
 * List items by lot from the table EXT017. 
    
 Revision History:
 Name                       Date             Version          Description of Changes
 Tovonirina ANDRIANARIELO   2025-01-22       1.0              Initial Release
 Tovonirina ANDRIANARIELO   2025-06-17       2.0              Retrieve rotation and couverture rates in this transaction
 Tovonirina ANDRIANARIELO   2025-07-02       2.1              Change logic to retrieve rotation and couverture rate
 Tovonirina ANDRIANARIELO   2025-07-15       2.2              Prioritize the 12 months with sold in the rotation rules
 Tovonirina ANDRIANARIELO   2025-07-17       2.3              new rules for the calculation of couverture rate
 Tovonirina ANDRIANARIELO   2025-07-08       2.4              XtendM3 validation
******************************************************************************************/
public class LstItemByLot extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  // all the input variables
  private int inCONO    // Company
  private String inDIVI // Division
  private String inFILE // File
  // limit the number of records to be read
  private int inNUMB // Number of records to be read
  private String inFROM // From ITNO
  private String inITTO   // To ITNO
  // new map to store rotation rules
  private Map<String, String[]> rotationRules = [:]
  private Map<String, String[]> couvertureRules = [:]
  // Maps to store bulk-fetched MITSTA and MITBAL data
  private Map<String, List<Map<String, Object>>> mitstaMap = [:]
  private Map<String, List<Map<String, Double>>> mitbalMap = [:]

  public LstItemByLot(MIAPI mi, ProgramAPI program, DatabaseAPI database) {
    this.mi = mi
    this.program = program
    this.database = database
  }

  public void main() {
    // validate input variables
    if (!validateInputVariables()) {
      return
    }
    // Get rotation rules for the division
    getRotationRules(inCONO, inDIVI)
    // Get couverture rules for the division
    getCouvertureRules(inCONO, inDIVI)
    //limit the number of records to be read
    int maxRecords = inNUMB <= 0 || inNUMB >= 5000 ? 5000: inNUMB
    int nrOfKeys = 3
    ExpressionFactory expression = database.getExpressionFactory("EXT017")
    expression = expression.ge("EXITNO", inFROM).and(expression.le("EXITNO", inITTO)) 
    DBAction query = database.table("EXT017")
      .index("00")
      .matching(expression)
      .selection("EXDIVI", "EXITNO","EXDAT1","EXDAT2", "EXRGDT", "EXCONO", "EXFILE", "EXLMDT", "EXRGTM", "EXCHID", "EXCHNO")
      .build()

    DBContainer container = query.getContainer()
    container.set("EXCONO", inCONO)
    container.set("EXFILE", inFILE)
    container.set("EXDIVI", inDIVI)

    // Collect all EXT017 items first
    List<Map<String, String>> ext017Items = []
    boolean found = query.readAll(container, nrOfKeys, maxRecords, { DBContainer dbContainer ->
      Map<String, String> item = [
        "EXCONO": dbContainer.get("EXCONO") as String,
        "EXDIVI": dbContainer.get("EXDIVI") as String,
        "EXFILE": dbContainer.get("EXFILE") as String,
        "EXITNO": dbContainer.get("EXITNO") as String,
        "EXDAT1": dbContainer.get("EXDAT1") as String,
        "EXDAT2": dbContainer.get("EXDAT2") as String,
        "EXLMDT": dbContainer.get("EXLMDT") as String,
        "EXRGDT": dbContainer.get("EXRGDT") as String,
        "EXRGTM": dbContainer.get("EXRGTM") as String,
        "EXCHID": dbContainer.get("EXCHID") as String,
        "EXCHNO": dbContainer.get("EXCHNO") as String
      ]
      ext017Items.add(item)
    })
    if (!found || ext017Items.isEmpty()) {
      mi.error("No data found")
      return
    }

    // Collect all item numbers
    Set<String> itemNumbers = ext017Items.collect { it["EXITNO"] as String }.toSet()

    // Bulk fetch MITSTA and MITBAL data for all items
    fetchMITSTARecords(itemNumbers, inCONO)
    fetchMITBALRecords(itemNumbers, inCONO)

    // Now process each EXT017 item
    for (Map<String, String> item : ext017Items) {
      outData(item)
    }
  }

  void outData(Map<String, String> item) {
    String itemNumber = item["EXITNO"] as String
    getMonthSold(itemNumber, inCONO)
    double rotationRate = getRotationRate(itemNumber)
    double couvertureRate = getCouvertureRate(inCONO, itemNumber)
    mi.outData.put("CONO", item["EXCONO"] as String)
    mi.outData.put("DIVI", item["EXDIVI"] as String)
    mi.outData.put("FILE", item["EXFILE"] as String)
    mi.outData.put("ITNO", itemNumber)
    mi.outData.put("DAT1", item["EXDAT1"] as String)
    mi.outData.put("DAT2", item["EXDAT2"] as String)
    mi.outData.put("LMDT", item["EXLMDT"] as String)
    mi.outData.put("RGDT", item["EXRGDT"] as String)
    mi.outData.put("RGTM", item["EXRGTM"] as String)
    mi.outData.put("RRAT", rotationRate as String)
    mi.outData.put("CRAT", couvertureRate as String)
    mi.outData.put("CHID", item["EXCHID"] as String)
    mi.outData.put("CHNO", item["EXCHNO"] as String)
    mi.write()
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
    if (mi.in.get('DIVI')!= null) {
      String divi = mi.in.get('DIVI')
      inDIVI = !divi.isEmpty()?divi:null
    }else{
      mi.error('DIVI is mandatory')
      return false
    }

    // Handling FROM Item number
    if(mi.in.get('FROM') == null) {
      mi.error('FROM ITNO is mandatory')
      return false
    }else {
      inFROM = mi.in.get('FROM') as String
    }
    // Handling TO Item number
    if(mi.in.get('ITTO') == null) {
      mi.error('TO ITNO is mandatory')
      return false
    }else {
      inITTO = mi.in.get('ITTO') as String
    }
    // Handling Number of records to be read
    if(mi.in.get('NUMB') == null) {
      mi.error('Number of records to be read is mandatory')
      return false
    }else {
      inNUMB = mi.in.get('NUMB') as int
    }
    //handling FILE
    if(mi.in.get('FILE') != null) {
      inFILE = mi.in.get('FILE') as String
    }else {
      inFILE = "MITMAS"
    }
    return true
  }
  
  /**
   * @description - get the sold during the last months
   * @params - item number, month number
   * @returns - void
   */
  void getRotationRules(int company, String division){
    DBAction query = database.table("EXT015")
      .index("00")
      .selection("EXMNTH", "EXRATE")
      .build()

    DBContainer container = query.getContainer()
    container.set("EXCONO", company)
    container.set("EXDIVI", division)
    // maximum number of rotation rules is 20
    int nrOfRecords = 20
    query.readAll(container, 2, nrOfRecords, { DBContainer dbContainer ->
      int length = rotationRules.size()
      // only add the month not equal to 0
      if (dbContainer.get("EXMNTH") == null || dbContainer.get("EXMNTH").toString().equals("0")) {
        return
      }
      rotationRules.put(dbContainer.get("EXMNTH") as String, [dbContainer.get("EXMNTH") as String, dbContainer.get("EXRATE") as String, null] as String[])
      length++
    })
  }


  /**
   * @description - get the sold during the last months
   * @params - item number, month number
   * @returns - void
   */
  void getCouvertureRules(int company, String division){
    DBAction query = database.table("EXT016")
      .index("00")
      .selection("EXTRCH", "EXRATE")
      .build()
    DBContainer container = query.getContainer()
    container.set("EXCONO", company)
    container.set("EXDIVI", division)
    // maximum number of couverture rules is 50
    int nrOfRecords = 50
    query.readAll(container, 2, nrOfRecords, { DBContainer dbContainer ->
      int length = couvertureRules.size()
      // only add the month not equal to 0
      if (dbContainer.get("EXTRCH") == null || dbContainer.get("EXTRCH").toString().equals("0")) {
        return
      }
      couvertureRules.put(String.valueOf(length), [dbContainer.get("EXTRCH") as String, dbContainer.get("EXRATE") as String] as String[])
      length++
    })
  }


  /**
   * @description - get the sold during the last months
   * @params - item number, month number
   * @returns - true/false
   */
  void getMonthSold(String itemNumber, int company) {
    java.time.LocalDate now = java.time.LocalDate.now()
    Map<String, Double> totals = [:]
    for (String key : rotationRules.keySet()) {
      String[] values = rotationRules.get(key)
      totals.put(values[0] as String, 0.0 as double)
    }
    boolean has12Months = rotationRules.findAll { entry -> entry.value[0] == "12" }.size() > 0
    if (!has12Months) {
      rotationRules.put("12", ["12", "0", null] as String[])
      totals.put("12" as String, 0.0 as double)
    }
    int maxMonth = rotationRules.values().collect { it[0] as int }.max() ?: 0
    List<Map<String, Object>> mitstaRecords = mitstaMap[itemNumber] ?: []
    for (Map<String, Object> rec : mitstaRecords) {
      String cyp6 = rec["MHCYP6"] as String
      double soqt = rec["MHSOQT"] as double
      if (!cyp6.contains("00")) {
        for (String key : rotationRules.keySet()) {
          String[] values = rotationRules.get(key)
          double tempQty = totals.get(values[0]) ?: 0.0 as double
          int month = values[0] as int
          if (cyp6.toInteger() >= now.minusMonths(month).format(java.time.format.DateTimeFormatter.ofPattern("yyyyMM")).toInteger() && now.minusMonths(1).format(java.time.format.DateTimeFormatter.ofPattern("yyyyMM")).toInteger() >= cyp6.toInteger()) {
            totals[month.toString()] = (tempQty + soqt)
          }
        }
      }
    }
    for (String key : rotationRules.keySet()) {
      String[] values = rotationRules.get(key)
      if (totals.containsKey(values[0])) {
        values[2] = String.valueOf(totals[values[0]] >= 0 ? totals[values[0]] : 0)
      } else {
        values[2] = "0.0"
      }
      rotationRules.put(key, values)
    }
  }

  /**
   * @description - get item quantity in hand
   * @params - item number
   * @returns - void
   */
  double getQuantityInHand(int company, String itemNumber) {
    List<Map<String, Double>> mitbalRecords = mitbalMap[itemNumber] ?: []
    double quantityInHand = 0.0
    // Use Groovy's sum method for better performance and readability
    for (Map<String, Double> record : mitbalRecords) {
      double qty = record["MBSTQT"] ?: 0
      if (qty > 0) {
      quantityInHand += qty
      }
    }
    return quantityInHand
  }

  // Bulk fetch MITSTA records for all items and store in mitstaMap
  void fetchMITSTARecords(Set<String> itemNumbers, int company) {
    mitstaMap.clear()
    if (itemNumbers.isEmpty()) return
    // For simplicity, fetch all MITSTA records for these items (could be optimized further)
    for (String itemNumber : itemNumbers) {
      List<Map<String, Object>> records = []
      DBAction query = database.table("MITSTA")
        .index("10")
        .selection("MHCYP6", "MHSOQT")
        .build()
      DBContainer container = query.getContainer()
      container.set("MHCONO", company)
      container.set("MHITNO", itemNumber)
      int nrOfRecords = 100
      query.readAll(container, 2, nrOfRecords, { DBContainer rec ->
        records.add([
          "MHCYP6": rec.get("MHCYP6"),
          "MHSOQT": rec.get("MHSOQT")
        ])
      })
      mitstaMap[itemNumber] = records
    }
  }

  // Bulk fetch MITBAL records for all items and store in mitbalMap
  void fetchMITBALRecords(Set<String> itemNumbers, int company) {
    mitbalMap.clear()
    if (itemNumbers.isEmpty()) return
    for (String itemNumber : itemNumbers) {
      List<Map<String, Double>> records = []
      DBAction query = database.table("MITBAL")
        .index("10")
        .selection("MBSTQT")
        .build()
      DBContainer container = query.getContainer()
      container.set("MBCONO", company)
      container.set("MBITNO", itemNumber)
      int nrOfRecords = 20
      query.readAll(container, 2, nrOfRecords, { DBContainer rec ->
        records.add([
          "MBSTQT": rec.getDouble("MBSTQT")
        ])
      })
      mitbalMap[itemNumber] = records
    }
  }

  /**
   * @description - get the rotation rate for the item number
   * @params - item number
   * @returns - double
   */
  double getRotationRate(String itemNumber) {
    // get the maximum month without sales
    double rotationRate = 0.0
    // sold quantity for the last twelve months
    String soldQty = rotationRules.get("12")[2]
    if(soldQty != null && Double.parseDouble(soldQty)>0.0){
      rotationRate=0.0
      return rotationRate
    }
    String maxMonth = "0"
    for(String key : rotationRules.keySet()){
      String[] values = rotationRules.get(key)
      if(values[2] != null && Double.parseDouble(values[2]) == 0.0) {
        if (Integer.parseInt(values[0]) > Integer.parseInt(maxMonth)) {
          maxMonth = values[0]
          rotationRate = Double.parseDouble(values[1])
        }
      }
    }
    return rotationRate
  }

  /**
   * @description - get the couverture rate for the item number
   * @params - item number
   * @returns - void
   */
  double getCouvertureRate(int company,String itemNumber) {
    double qtyInHand = getQuantityInHand(company, itemNumber)
    double couvertureRate = 0.0
    // the calculated palier number
    int calculatedPalierNb = 0
    // Get the last 12 months sold quantity
    double soldQty = (rotationRules.entrySet()
    .find { entry -> entry.value[0] == "12" }?.value[2] ?: "0").toDouble()
    // Calculate the maximum tranche
    double division = soldQty != 0 ? qtyInHand / soldQty : 0
    int maxTRCH = 0
    // add 1 to the maximum tranche if sold quantity is an integer
    // or if it is not an integer, calculate the ceiling of the division
    // to ensure we have enough tranches to cover the quantity in hand
    if (soldQty != 0) {
      if (division == (int) division) {
        calculatedPalierNb = (int) division + 1
      } else {
        calculatedPalierNb = (int) Math.ceil(division)
      }
    }
    if (calculatedPalierNb < 1) {
      return 0.0
    }
    if(calculatedPalierNb > couvertureRules.size()) {
      maxTRCH = couvertureRules.size()
    }else{
      maxTRCH=calculatedPalierNb

    }
    // Prepare a list of filtered and sorted entries in one step
    List<Map.Entry<String, String[]>> filteredEntries = couvertureRules.entrySet()
        .sort { Map.Entry<String, String[]> a, Map.Entry<String, String[]> b ->
          a.value[0].toInteger() <=> b.value[0].toInteger()
        }

    double sumOfRates = 0.0
    double lastRATE = 0.0
    int size = filteredEntries.size()
    if (size > 0) {
        // Sum rates from the second to the penultimate tranche (if more than 2 tranches)
        for (int i = 0;  i < maxTRCH - 1;  i++) {
            sumOfRates += Double.parseDouble(filteredEntries.get(i).getValue()[1])
        }
        // Use the last rate for the remainder
        lastRATE = Double.parseDouble(filteredEntries.get(maxTRCH-1).getValue()[1])
    }
    if(qtyInHand != 0) {
      if(calculatedPalierNb > maxTRCH){
        double couvRate = (soldQty * sumOfRates + (qtyInHand - (soldQty * (maxTRCH-1))) * lastRATE) / qtyInHand
        couvertureRate = new java.math.BigDecimal(couvRate).setScale(2, java.math.RoundingMode.HALF_UP).doubleValue()
      }else{
        double couvRate = (soldQty * sumOfRates + (qtyInHand % soldQty) * lastRATE) / qtyInHand
        couvertureRate = new java.math.BigDecimal(couvRate).setScale(2, java.math.RoundingMode.HALF_UP).doubleValue()
      }
    }
    return couvertureRate
  }
}
