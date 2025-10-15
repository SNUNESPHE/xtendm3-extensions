/**
 * README
 * This extension is used by scheduler
 *
 * Name : EXT010MI.UpdDepreciation
 * Description : Update depreciation
 * Date         Changed By   Description
 * 20240910     ARNREN       5190 - Ecran de gestion de la décote
 * 20241204     ARNREN       MMS850MI.AddReclass handling added*/
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdDepreciation extends ExtendM3Transaction {
    private final MIAPI mi
    private final DatabaseAPI database
    private final LoggerAPI logger
    private final MICallerAPI miCaller
    private final ProgramAPI program
    private final UtilityAPI utility
    private int currentCompany
    private String inWHLO
    private String inITNO
    private Integer inRGDT
    private Integer inRGTM
    private Integer inTMSX
    private String divi
    private Integer ttyp
    private Integer repn
    private String rscdMITTRA
    private String ridn
    private Integer ridl
    private String pk03
    private String rscdOCLINE
    private double sapr
    private Integer n096
    private Integer nbMaxRecord = 10000
    private boolean consignedItem
    private boolean consignedReasonCode
    private String cri1
    private String whsl
    private String stas
    private String trqt

    public UpdDepreciation(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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

        // Check warehouse
        if (mi.in.get("WHLO") != null && mi.in.get("WHLO") != "") {
            DBAction queryMITWHL = database.table("MITWHL").index("00").selection("MWDIVI").build()
            DBContainer MITWHL = queryMITWHL.getContainer()
            MITWHL.set("MWCONO", currentCompany)
            MITWHL.set("MWWHLO", mi.in.get("WHLO"))
            if (!queryMITWHL.read(MITWHL)) {
                mi.error("Dépôt " + mi.in.get("WHLO") + " n'existe pas")
                return
            } else {
                divi = MITWHL.get("MWDIVI")
            }
            inWHLO = mi.in.get("WHLO")
        }

        // Check item
        if (mi.in.get("ITNO") != null && mi.in.get("ITNO") != "") {
            DBAction queryMITMAS = database.table("MITMAS").index("00").selection("MMCRI1").build()
            DBContainer MITMAS = queryMITMAS.getContainer()
            MITMAS.set("MMCONO", currentCompany)
            MITMAS.set("MMITNO", mi.in.get("ITNO"))
            if (!queryMITMAS.read(MITMAS)) {
                mi.error("Code article " + mi.in.get("ITNO") + " n'existe pas")
                return
            } else {
                cri1 = MITMAS.get("MMCRI1")
                if (cri1.trim() != "") {
                    consignedItem = true
                }
            }
            inITNO = mi.in.get("ITNO")
        }

        // Check entry date
        if (mi.in.get("RGDT") != null && mi.in.get("RGDT") != "") {
            if (!utility.call("DateUtil", "isDateValid", mi.in.get("RGDT"), "yyyyMMdd")) {
                mi.error("Date de saisie " + mi.in.get("RGDT") + " est invalide")
                return
            }
            inRGDT = mi.in.get("RGDT") as Integer
        }

        // Entry time
        inRGTM = mi.in.get("RGTM") as Integer

        // Time suffix
        inTMSX = mi.in.get("TMSX") as Integer

        // Retrieve MITTRA
        DBAction queryMITTRA = database.table("MITTRA").index("00").selection("MTTTYP", "MTREPN", "MTRSCD", "MTRIDN", "MTRIDL", "MTWHSL", "MTSTAS", "MTTRQT").build()
        DBContainer MITTRA = queryMITTRA.getContainer()
        MITTRA.set("MTCONO", currentCompany)
        MITTRA.set("MTWHLO", inWHLO)
        MITTRA.set("MTITNO", inITNO)
        MITTRA.set("MTRGDT", inRGDT)
        MITTRA.set("MTRGTM", inRGTM)
        MITTRA.set("MTTMSX", inTMSX)
        if (queryMITTRA.read(MITTRA)) {

            rscdMITTRA = MITTRA.get("MTRSCD")
            ridn = MITTRA.get("MTRIDN")
            ridl = MITTRA.get("MTRIDL")
            trqt = MITTRA.get("MTTRQT")

            logger.debug("rscdMITTRA = " + rscdMITTRA)
            logger.debug("ridn = " + ridn)
            logger.debug("ridl = " + ridl)

            whsl = MITTRA.get("MTWHSL")
            stas = MITTRA.get("MTSTAS")

            // Retrieve OCLINE
            DBAction queryOCLINE = database.table("OCLINE").index("00").selection("ODRSCD", "ODSAPR").build()
            DBContainer OCLINE = queryOCLINE.getContainer()
            OCLINE.set("ODCONO", currentCompany)
            OCLINE.set("ODWHLO", inWHLO)
            OCLINE.set("ODREPN", ridn as Long)
            OCLINE.set("ODRELI", ridl)
            if (!queryOCLINE.read(OCLINE)) {
                return
            } else {
                rscdOCLINE = OCLINE.get("ODRSCD")
                sapr = OCLINE.get("ODSAPR")
            }

            reclassification()

            // Check transaction type
            ttyp = MITTRA.get("MTTTYP")
            if (ttyp != 93) return

            logger.debug("TTYP OK - ttyp = " + ttyp)

            // Check receiving number
            repn = MITTRA.get("MTREPN")
            if (repn == 0) return

            logger.debug("REPN OK - repn = " + repn)

            // Retrieve CUGEX1
            DBAction queryCUGEX1 = database.table("CUGEX1").index("30").selection("F1PK03").build()
            DBContainer CUGEX1 = queryCUGEX1.getContainer()
            CUGEX1.set("F1CONO", currentCompany)
            CUGEX1.set("F1FILE", "DECOTE")
            CUGEX1.set("F1PK01", divi)
            CUGEX1.set("F1PK02", "2")
            CUGEX1.set("F1PK04", rscdMITTRA)
            if (!queryCUGEX1.readAll(CUGEX1, 5, nbMaxRecord, outDataCUGEX1)) {
                return
            }
            logger.debug("CUGEX1 1 OK - pk03 = " + pk03)

            logger.debug("OCLINE OK - rscdOCLINE = " + rscdOCLINE)
            logger.debug("OCLINE OK - sapr = " + sapr)

            if (rscdOCLINE.trim() != pk03.trim()) return

            logger.debug("rscdOCLINE = pk03 OK")

            // Retrieve CUGEX1 and depreciation rate
            DBAction query = database.table("CUGEX1").index("00").selection("F1PK04", "F1PK05", "F1N096").build()
            DBContainer CUGEX11 = query.getContainer()
            CUGEX11.set("F1CONO", currentCompany)
            CUGEX11.set("F1FILE", "DECOTE_FPR")
            CUGEX11.set("F1PK01", divi)
            CUGEX11.set("F1PK02", rscdOCLINE)
            CUGEX11.set("F1PK03", rscdMITTRA)
            if (!query.readAll(CUGEX11, 5, nbMaxRecord, outDataCUGEX11)) {
                return
            }

            logger.debug("CUGEX1 2 OK - n096 = " + n096)

            // Retrieve EXT391 and update depreciation
            DBAction queryEXT391 = database.table("EXT391").index("00").build()
            DBContainer EXT391 = queryEXT391.getContainer()
            EXT391.set("EXCONO", currentCompany)
            EXT391.set("EXWHLO", inWHLO)
            EXT391.set("EXREPN", ridn as Integer)
            EXT391.set("EXRELI", ridl)
            if (!queryEXT391.readLock(EXT391, updateCallBack)) {
                return
            }
        }

        mi.outData.put("ZDRT", n096 as String)
        mi.write()
    }
    // Retrieve CUGEX1
    Closure<?> outDataCUGEX1 = { DBContainer CUGEX1 -> pk03 = CUGEX1.get("F1PK03")
    }

    // Retrieve CUGEX1
    Closure<?> outDataCUGEX11 = { DBContainer CUGEX1 ->
        String pk04 = CUGEX1.get("F1PK04")
        String pk05 = CUGEX1.get("F1PK05")
        if ((pk04.trim() as double) <= sapr && (pk05.trim() as double) >= sapr) {
            n096 = CUGEX1.get("F1N096")
        }
    }

    // Update EXT391
    Closure<?> updateCallBack = { LockedResult lockedResult ->
        LocalDateTime timeOfCreation = LocalDateTime.now()
        int changeNumber = lockedResult.get("EXCHNO")
        lockedResult.set("EXZDC2", n096)
        lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        lockedResult.setInt("EXCHNO", changeNumber + 1)
        lockedResult.set("EXCHID", program.getUser())
        lockedResult.update()
    }
    // Execute MMS850MI.AddReclass
    private executeMMS850MIAddReclass(String PRFL, String E0PA, String E065, String WHLO, String WHSL, String ITNO, String NITN, String STAS, String QLQT) {
        Map<String, String> parameters = ["PRFL": PRFL, "E0PA": E0PA, "E065": E065, "WHLO": WHLO, "WHSL": WHSL, "ITNO": ITNO, "NITN": NITN, "STAS": STAS, "QLQT": QLQT]
        Closure<?> handler = { Map<String, String> response -> if (response.error != null) {} else {} }
        miCaller.call("MMS850MI", "AddReclass", parameters, handler)
    }
    // Reclassification
    public void reclassification() {
        // Is the reason code consigned ?
        DBAction queryCUGEX1 = database.table("CUGEX1").index("00").selection("F1A030").build()
        DBContainer CUGEX1 = queryCUGEX1.getContainer()
        CUGEX1.set("F1CONO", currentCompany)
        CUGEX1.set("F1FILE", "CSYTAB")
        CUGEX1.set("F1PK01", "")
        CUGEX1.set("F1PK02", "RSCD")
        CUGEX1.set("F1PK03", rscdOCLINE)
        CUGEX1.set("F1PK04", "")
        if (queryCUGEX1.read(CUGEX1)) {
            String a030 = CUGEX1.get("F1A030")
            if (a030.trim() == "CN") {
                consignedReasonCode = true
            }
        }

        // If the item and reason code are of the consignment type, it is necessary to reclassify
        logger.debug("consignedItem = " + consignedItem)
        logger.debug("consignedReasonCode = " + consignedReasonCode)
        logger.debug("inWHLO = " + inWHLO)
        logger.debug("whsl = " + whsl)
        logger.debug("inITNO = " + inITNO)
        logger.debug("cri1 = " + cri1)
        logger.debug("stas = " + stas)
        if (consignedItem && consignedReasonCode) {
            logger.debug("executeMMS850MIAddReclass")
            executeMMS850MIAddReclass("*EXE", "WS", "WMS", inWHLO, whsl, inITNO, cri1, stas, trqt)
        }
    }
}
