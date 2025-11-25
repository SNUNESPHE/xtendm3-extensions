/**
 * README
 * This extension is used by scheduler
 *
 * Name : EXT040MI.AddTranspInfo
 * Description : Add transport info
 * Date         Changed By   Description
 * 20241105     ARENARD      5004 - Modèle étiquette transport - GEODIS
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddTranspInfo extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private int currentCompany
  public String inZPRC = ""    // Product code
  public String inZHAA = ""    // Handling agency
  public String inZCUC = ""    // Customer code
  public String inZHUT = ""    // Handling unit type
  public String inZWGT = ""    // Weight type
  public String inZRCC = ""    // Recipient country code
  public String inZRGZ = ""    // Recipient geographic zone
  public String inZRPC = ""    // Recipient postal code
  public String inZLLL = ""    // Locality label
  public String inZPRN = " "   // Production network
  public String inZLDP = " "   // Loading priority
  public String inZDAC = ""    // Delivery agency code
  public String inZEHC = ""    // Exit hub agency code
  public String inZDIR = ""    // Direction / Routing
  public String inZRCD = ""    // Recipient code
  public String inZRNM = ""    // Recipient name
  public String inZRA1 = ""    // Recipient address 1
  public String inZRA2 = ""    // Recipient address 2

  public AddTranspInfo(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    // Product code
    if (mi.in.get("ZPRC") != null) {
      inZPRC = mi.in.get("ZPRC")
    }

    // Handling agency
    if (mi.in.get("ZHAA") != null) {
      inZHAA = mi.in.get("ZHAA")
    }

    // Customer code
    if (mi.in.get("ZCUC") != null) {
      inZCUC = mi.in.get("ZCUC")
    }

    // Handling unit type
    if (mi.in.get("ZHUT") != null) {
      inZHUT = mi.in.get("ZHUT")
    }

    // Weight type
    if (mi.in.get("ZWGT") != null) {
      inZWGT = mi.in.get("ZWGT")
    }

    // Recipient country code
    if (mi.in.get("ZRCC") != null) {
      inZRCC = mi.in.get("ZRCC")
    }

    // Recipient geographic zone
    if (mi.in.get("ZRGZ") != null) {
      inZRGZ = mi.in.get("ZRGZ")
    }

    // Recipient postal code
    if (mi.in.get("ZRPC") != null) {
      inZRPC = mi.in.get("ZRPC")
    }

    // Locality label
    if (mi.in.get("ZLLL") != null) {
      inZLLL = mi.in.get("ZLLL")
    }

    // Production network
    if (mi.in.get("ZPRN") != null) {
      inZPRN = mi.in.get("ZPRN")
    }

    // Loading priority
    if (mi.in.get("ZLDP") != null) {
      inZLDP = mi.in.get("ZLDP")
    }

    // Delivery agency code
    if (mi.in.get("ZDAC") != null) {
      inZDAC = mi.in.get("ZDAC")
    }

    // Exit hub agency code
    if (mi.in.get("ZEHC") != null) {
      inZEHC = mi.in.get("ZEHC")
    }

    // Direction / Routing
    if (mi.in.get("ZDIR") != null) {
      inZDIR = mi.in.get("ZDIR")
    }

    // Recipient code
    if (mi.in.get("ZRCD") != null) {
      inZRCD = mi.in.get("ZRCD")
    }

    // Recipient name
    if (mi.in.get("ZRNM") != null) {
      inZRNM = mi.in.get("ZRNM")
    }

    // Recipient address 1
    if (mi.in.get("ZRA1") != null) {
      inZRA1 = mi.in.get("ZRA1")
    }

    // Recipient address 2
    if (mi.in.get("ZRA2") != null) {
      inZRA2 = mi.in.get("ZRA2")
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction queryEXT040 = database.table("EXT040").index("00").build()
    DBContainer EXT040 = queryEXT040.getContainer()
    EXT040.set("EXCONO", currentCompany)
    EXT040.set("EXZPRC", inZPRC)        // Product code
    EXT040.set("EXZHAA", inZHAA)        // Handling agency
    EXT040.set("EXZCUC", inZCUC)        // Customer code
    EXT040.set("EXZHUT", inZHUT)        // Handling unit type
    EXT040.set("EXZWGT", inZWGT)        // Weight type
    EXT040.set("EXZRCC", inZRCC)        // Recipient country code
    EXT040.set("EXZRGZ", inZRGZ)        // Recipient geographic zone
    EXT040.set("EXZRPC", inZRPC)        // Recipient postal code
    if (!queryEXT040.read(EXT040)) {
      EXT040.set("EXZLLL", inZLLL)    // Locality label
      EXT040.set("EXZPRN", inZPRN)    // Production network
      EXT040.set("EXZLDP", inZLDP)    // Loading priority
      EXT040.set("EXZDAC", inZDAC)    // Delivery agency code
      EXT040.set("EXZEHC", inZEHC)    // Exit hub agency code
      EXT040.set("EXZDIR", inZDIR)    // Direction / Routing
      EXT040.set("EXZRCD", inZRCD)    // Recipient code
      EXT040.set("EXZRNM", inZRNM)    // Recipient name
      EXT040.set("EXZRA1", inZRA1)    // Recipient address 1
      EXT040.set("EXZRA2", inZRA2)    // Recipient address 2
      EXT040.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT040.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT040.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT040.setInt("EXCHNO", 1)
      EXT040.set("EXCHID", program.getUser())
      queryEXT040.insert(EXT040)
    } else {
      mi.error("Enregistrement existe déjà")
      return
    }
  }
}
