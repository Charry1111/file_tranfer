package com.firstdata.dwh.compress;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Hashtable;
import java.util.Properties;

import org.apache.log4j.Logger;


public class Init {
    private static File   configFile;
    private static long configLastModified;
    
    private static String fromCEB;
    private static String received;
    private static String out;
    private static String password;
    private static String ftp;
    private static String batFilePathMF;
    private static String batTemplatePathMF;
    private static Long   interval;
    private static String eventID;
    private static String reportPath;
    private static String backupPath;
//    private static String logPath;
    
    private static int maxConcurrentUnZip;
    private static long concurrentUnZipThreshold;
    
    private static Hashtable<String,String> JCLMapping = new Hashtable<String,String> ();
   
    private static Logger log = Logger.getLogger(Init.class);

    private static long unZip_Check_Interval = 30000;
//    private static String UNZIP_TEMP_PATTERN = "_Z......";
    
//    static {
//        try {
//            InputStream in = FileTransfer.class.getClassLoader()
//                .getResourceAsStream(
//                    "com/firstdata/dwh/ceb/FileTransfer.properties"
//                );
//            Properties p = new Properties();
//            p.load(in);
//            String s = (String)p.get("UNZIP_CHECK_INTERVAL");
//            UNZIP_CHECK_INTERVAL = parsePeriod( s.trim() );
//            log.debug("UNZIP_CHECK_INTERVAL: " + UNZIP_CHECK_INTERVAL + ".");
//            
//            s = (String)p.get("UNZIP_TEMP_PATTERN");
//            UNZIP_TEMP_PATTERN = s.trim();
//            log.debug("UNZIP_TEMP_PATTERN: " + UNZIP_TEMP_PATTERN + ".");
//        } catch (Exception e) {
//            log.debug("Reading FileTransfer.properties error.", e);
//        }
//    }
    
    public static void init() throws Exception {
        URL configURL =Init.class.getClassLoader().getResource("config.properties");
        try {
            configFile = new File(configURL.toURI());
            log.info("Config file: " + configFile);
            configLastModified = configFile.lastModified();
            InputStream in = new FileInputStream(configFile);
            Properties p = new Properties();
            p.load(in);
            
            fromCEB = p.getProperty("fromCEB").trim();
            log.info("Receiving files from CEB path: " + fromCEB);
            received = p.getProperty("received").trim();
            log.info("received Path: " + received);
            out = p.getProperty("out").trim();
            log.info("out Path: " + out);
			// password = p.getProperty("password").trim();
			// log.info("Password : " + password);
            ftp = p.getProperty("ftp").trim();
            log.info("ftp : " + ftp);
            interval = parsePeriod((p.getProperty("interval").trim()));
            log.info("interval : " + interval);
            
            batFilePathMF = p.getProperty("batFilePathMF").trim();
            log.info("batFilePathMF : " + batFilePathMF);
            batTemplatePathMF = p.getProperty("batTemplatePathMF").trim();
            log.info("batTemplatePathMF : " + batTemplatePathMF);
            eventID = p.getProperty("eventID").trim();
            log.info("eventID : " + eventID);
            reportPath = p.getProperty("reportPath").trim();
            log.info("reportPath : " + reportPath);
            backupPath = p.getProperty("backupPath").trim();
            log.info("backupPath : " + backupPath);
            maxConcurrentUnZip = Integer.parseInt( p.getProperty("maxConcurrentUnZip").trim() );
            log.info("maxConcurrentUnZip: " + maxConcurrentUnZip);
            concurrentUnZipThreshold = parseSize( p.getProperty("concurrentUnZipThreshold").trim() );
            log.info("concurrentUnZipThreshold: " + concurrentUnZipThreshold);
            unZip_Check_Interval = parsePeriod( p.getProperty("unZip_Check_Interval") .trim());
            log.info("unZip_Check_Interval: " + unZip_Check_Interval);
            
            in.close();
            
            URL mappingURL =
                Init.class.getClassLoader().getResource("mapping.properties");
            
            FileInputStream  fis = new FileInputStream(new File(mappingURL.toURI()));
            InputStreamReader isr = new InputStreamReader(fis);
            BufferedReader br = new BufferedReader(isr);
            String line = br.readLine();
            while (null != line) {
                line.trim();
                log.info("Readed line:"+line);
                JCLMapping.put(
                    line.substring( 0                  , line.indexOf(",") ), 
                    line.substring( line.indexOf(",")+1, line.length()     )
                );
                line = br.readLine();
            }
            br.close();
        } catch (Exception e) {
            log.error("Error when reading the config file!", e);
            throw e;
        }
    }
    

   
    public static Hashtable<String, String> getJCLMapping() {
        return JCLMapping;
    }


    public static void refreshConfig() throws Exception {
        long m = configFile.lastModified();
        if (m == configLastModified) {
            return;
        }
        
        configLastModified = m;
        try {
            InputStream in = new FileInputStream(configFile);
            Properties p = new Properties();
            p.load(in);
            
            int i = Integer.parseInt( p.getProperty("maxConcurrentUnZip") );
            if (i != maxConcurrentUnZip) {
                maxConcurrentUnZip = i;
                log.info("maxConcurrentUnZip: " + maxConcurrentUnZip);
            }
            
            long monitorInterval= parsePeriod(p.getProperty("interval").trim());
            if(monitorInterval !=interval){
                interval = monitorInterval;
                log.info("interval: "+ interval);
            }
            
            String ftpMF = p.getProperty("ftp").trim();
            if(!ftpMF.equalsIgnoreCase(ftp)){
                ftp = ftpMF;
                log.info("ftp: "+ ftp);
            }
            
            long unZipCheck = parsePeriod( p.getProperty("unZip_Check_Interval") .trim());
            if(unZipCheck != unZip_Check_Interval){
                unZip_Check_Interval = unZipCheck;
                log.info("unZip_Check_Interval: "+ unZip_Check_Interval);
            }
            
            long l = parseSize( p.getProperty("concurrentUnZipThreshold") );
            if (l != concurrentUnZipThreshold) {
                concurrentUnZipThreshold = l;
                log.info("concurrentUnZipThreshold: " + concurrentUnZipThreshold);
            }
            
            in.close();
        } catch (Exception e) {
            log.error("Refreshing config failed.", e);
            throw e;
        }
    }
    

    public static long parsePeriod(String s)
    throws IllegalArgumentException {
        // To ignore the cases.
        s = s.toUpperCase();
        
        long coefficient = 1;
        if ( s.endsWith("MS") ) {
            s = s.substring(0, s.length() - 2);
        }
        else if ( s.endsWith("S") ) {
            coefficient = 1000;
            s = s.substring(0, s.length() - 1);
        }
        else if ( s.endsWith("M") ) {
            coefficient = 1000 * 60;
            s = s.substring(0, s.length() - 1);
        }
        else if ( s.endsWith("H") ) {
            coefficient = 1000 * 60 * 60;
            s = s.substring(0, s.length() - 1);
        }
        else if ( s.endsWith("D") ) {
            coefficient = 1000 * 60 * 60 * 24;
            s = s.substring(0, s.length() - 1);
        }
        
        long number = Long.parseLong(s);
        return number * coefficient;
    }
    
    public static long parseSize(String s)
    throws IllegalArgumentException {
        // To ignore the cases.
        s = s.toUpperCase();
        // To ignore the 'B' at the end.
        if ( s.endsWith("B") ) {
            s = s.substring(0, s.length() - 1);
        }
        
        long coefficient = 1;
        if ( s.endsWith("K") ) {
            coefficient = 1024;
            s = s.substring(0, s.length() - 1);
        }
        else if ( s.endsWith("M") ) {
            coefficient = 1024 * 1024;
            s = s.substring(0, s.length() - 1);
        }
        else if ( s.endsWith("G") ) {
            coefficient = 1024 * 1024 * 1024;
            s = s.substring(0, s.length() - 1);
        }
        
        long number = Long.parseLong(s);
        return number * coefficient;
    }
    
    public static long getUnZip_Check_Interval() {
        return unZip_Check_Interval;
    }

    public static int getMaxConcurrentUnZip() {
        return maxConcurrentUnZip;
    }

    public static long getConcurrentUnZipThreshold() {
        return concurrentUnZipThreshold;
    }


    public static String getBackupPath() {
        return backupPath;
    }

    public static String getReportPath() {
        return reportPath;
    }

    public static File getConfigFile() {
        return configFile;
    }

    public static String getFromCEB() {
        return fromCEB;
    }

    public static String getReceived() {
        return received;
    }

 
    public static String getOut() {
        return out;
    }

    public static String getPassword() {
        return password;
    }

    public static String getFtp() {
        return ftp;
    }

    public static String getBatFilePathMF() {
        return batFilePathMF;
    }

    public static String getBatTemplatePathMF() {
        return batTemplatePathMF;
    }

    public static Long getInterval() {
        return interval;
    }

    public static String getEventID() {
        return eventID;
    }

}
