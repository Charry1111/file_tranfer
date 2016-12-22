package com.firstdata.dwh.compress;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.RollingFileAppender;

public class FileLateMonitor {
    
    public static void main(String [] a){
        try{ 
            start();
        }catch(Exception e){
            log.error(" unexpected error happened ",e);
            singleFileAlarm(" unexpected error happened ",eventID);
            System.exit(1);
        }
    }
    
    private static void start() throws Exception{
        configInit();
        log = createNewLogger("monitor");
        fileMap = readFileTimeConfig();
        initHoliday();
        while(true){
            run();
            Thread.sleep(monitorSleepTime);
        }
    }
    
    private static void configInit() throws Exception{
        URL configURL = CommonUtil.class.getClassLoader().getResource("firstdata/dwh/config.properties");
        
        Properties properties = new Properties();
        
        try {
            properties.load( configURL.openStream());
        } catch (IOException e) {
            log.error(e, e);
            throw e;
        }
        sendPath = properties.getProperty("sendPathF");
        logFilePath = properties.getProperty("logFilePath");
        eventID = properties.getProperty("eventID");
        judgeStartT = Integer.parseInt(properties.getProperty("judgeHolidayStartT"));
        judgeEndT = Integer.parseInt(properties.getProperty("judgeHolidayEndT"));
        monitorSleepTime = Integer.parseInt(properties.getProperty("monitorSleepTime"));
        endMonitorTime = Integer.parseInt(properties.getProperty("endMonitorTime"));

    }
    
    public static void run() throws Exception {
        try {
            cal = java.util.GregorianCalendar.getInstance();
            now = cal.getTime();
            nowTime = toHHmm(now);
            log.debug(" ifAlert value  0:no, 1:yes-----" + ifAlert);
            
            // when to judge the current date is holiday or not. Now before v+ tram, during the given time, judge it.
            if(nowTime<judgeEndT && nowTime >= judgeStartT){
                judgeHoliday();
            }
            
           

/*            if(nowTime <=2200 && nowTime >= 600){
                
            }else{*/
                //check file
               
            if(ifAlert==0){
                
            }else{
                renewFileTimeConfig(fileMap);
                init();

                Enumeration<String> enu = fileMap.keys();
                List<String> ncbcGetFileList = getNCBCFileList();
                while( enu.hasMoreElements() ) {
                    String filename = enu.nextElement();
                    FileArriveTime arrive = fileMap.get(filename);
                    
                   
                        if ( isSameDay( arrive.getLastArriveDate(), now ) ) {
                            //log.debug(filename + " arrived.");
                            //System.out.println("arrived  "+filename);
                        }
                        else {
                            log.debug(filename + " not arrived.");
                            
                            //check if late. create the latest monitor time. avoid alert afternoon or judge after the current tram.
                            if (nowTime - arrive.getShouldArriveTime() >= 0 && nowTime <= endMonitorTime 
                                || nowTime >= arrive.getShouldArriveTime() && ncbcGetFileList.contains(filename) ) {

                                //if alert once, not again same day
                                if ( !isSameDay( now, arrive.getLastAlarmDate() ) ) {
                                    log.debug(filename + " later than threshold.Creating alert.");
                                    singleFileAlarm(filename, eventID);
                                    arrive.setLastAlarmDate(now);
                                }
                            }
                        } //if     
                }//while
            }//if
      /*  }
*/        } catch (Exception e) {
            log.error(e, e);
            throw e;
        }
    }
    
   
    /**
     * judge  the file arrived or not
     * @throws Exception
     */
    
    public static void init() throws Exception {
        log.debug("Listing zip files.");
        File dir = new File(sendPath);
        File[] files = dir.listFiles( new FileFilter() {
            public boolean accept(File f) {
                if(f.isDirectory()){
                    return false;
                }else{
                    String name = f.getName();
                    String path = f.getAbsolutePath();

                    //System.out.println(name.substring(name.lastIndexOf("_")+1,name.lastIndexOf("_")+7));
                    //System.out.println(name.substring(name.lastIndexOf(".")+1,name.lastIndexOf(".")+5));
                    //System.out.println(new File(path + ".000K").toString());
                    //System.out.println(new File(path + ".000K").exists());
                    //System.out.println(nowDate.getDateYYMMDD());
                    if(name.substring(name.lastIndexOf("_")+1,name.lastIndexOf("_")+7).equals(nowDate.getDateYYMMDD()) &&
                        !name.contains("000K") && new File(path + ".000K").exists() ){
                        
                        return true;
                    }else{
                        return false;
                    }
                }
            }
        } );
        log.debug(files.length + " files found.");
        
        //judge the file if arrived
        for (File f: files) {
            log.debug( "Filename \"" + f.getName() + "\" found." );
            String keyword = getFilenameKeyword( f.getName() );
            //log.debug( "Filename keyword: \"" + keyword + "\"." );
            
            FileArriveTime arrive = fileMap.get(keyword);
            if ( null != arrive && !isSameDay( arrive.getLastArriveDate(), now )) {
                log.debug("Updating arrive time.");
                arrive.setLastArriveDate(now);
            }
        }
    }
    
    /**
     * 
     */
    private static String getFilenameKeyword(String filename) {
        //System.out.println(filename.substring(0,filename.lastIndexOf("_")));
        return filename.substring(0,filename.lastIndexOf("_"));
        
        
    }
    
    private static void singleFileAlarm(String fileName, String eventID){
        //System.out.println("-----------------"+fileName);
        String message = "File \"" + fileName + "\" not arrived on time.";
        try{
            String cmd = "eventcreate" +
                         " -l application" +
                         " -so NCBCFILETransfer" +
                         " -t error" +
                         " -d \"" + message + "\"" +
                         " -id " + eventID;
            
            Process p = Runtime.getRuntime().exec(cmd);
            
            drainStream( p.getErrorStream() );
            drainStream( p.getInputStream() );
            
            
            int returnCode = p.waitFor();
            if (returnCode == 0) {
                log.info("Creating Windows error event successful.");
            }
            else {
                log.error("Creating Windows error event failed." +
                          " Return code: " + returnCode + ".");
            }
        }catch (Exception e) {
            log.error("Creating Windows event failed.", e);
            
        }
    }
    
    /*private static void allFilesAlarm(String eventID, int quantity) {
        int count = 114 - quantity;
        String message = count + " file(s) not arrived until 14:00 PM.";
        try{
            String cmd = "eventcreate" +
                         " -l application" +
                         " -so CEBDRFILETransfer" +
                         " -t error" +
                         " -d \"" + message + "\"" +
                         " -id " + eventID;
            Process p = Runtime.getRuntime().exec(cmd);
            drainStream( p.getErrorStream() );
            drainStream( p.getInputStream() );
            int returnCode = p.waitFor();
            if (returnCode == 0) {
                log.info("Creating Windows error event successful.");
            }
            else {
                log.error("Creating Windows error event failed." +
                          " Return code: " + returnCode + ".");
            }
        } catch (Exception e) {
            log.error("Creating Windows event failed.", e);
        }
    }
    */
    private static boolean isSameDay(Date a, Date b) {
        Calendar aa = Calendar.getInstance();
        aa.setTime(a);
        Calendar bb = Calendar.getInstance();
        bb.setTime(b);
        return aa.get(Calendar.DAY_OF_YEAR) == bb.get(Calendar.DAY_OF_YEAR);
    }
    
    /**
     * Converts a Date to integer "HHmm". E.g., 12:45 is converted to 1245.
     */
    private static int toHHmm(Date d) {
        SimpleDateFormat f = new SimpleDateFormat("HHmm");
        String s = f.format(d);
        return Integer.parseInt(s);
    }
    
    private static void drainStream(InputStream in) throws Exception {
        try {
            byte[] b = new byte[16 * 1024];
            int n = in.read(b);
            while (n >= 0) {
                n = in.read(b);
            }
        } catch (Exception e) {
            log.error(e, e);
            throw e;
        }
    }
    
    private static Hashtable <String ,FileArriveTime> readFileTimeConfig() throws Exception {
        Hashtable<String, FileArriveTime> arriveMap =
            new Hashtable<String, FileArriveTime>();
        
        try {
            InputStream in = FileLateMonitor.class.getClassLoader()
                             .getResourceAsStream("firstdata/dwh/checkFileReport.properties");
            Properties p = new Properties();
            p.load(in);
            in.close();
            
            Set<?> keys = p.keySet();
            for (Object key: keys) {
                String filename = (String)key;
                String value = ( (String)p.get(filename) ).trim();
                if(value==null || value.trim().length()<=0){
                    value="2500";//never alert
                }
                FileArriveTime f = new FileArriveTime();
                
                f.setShouldArriveTime( Integer.parseInt(value) );
                log.info("Filename: " + filename + ", " + "should-arrive-time: " + f.getShouldArriveTime() + ".");
                
                Date epoch = new Date(0);
                f.setLastArriveDate(epoch); // Assuming no file has come yet
                f.setLastAlarmDate(epoch);
                
                arriveMap.put(filename, f);
            }
        } catch (Exception e) {
            log.error("Reading config error.", e);
            throw e;
        }
        return arriveMap;
    }
    /**
     * can update the monitor time for every file in time
     * @param config
     * @throws Exception
     */
    private static void renewFileTimeConfig(Hashtable <String ,FileArriveTime> config) throws Exception {
        try {
            InputStream in = FileLateMonitor.class.getClassLoader()
                             .getResourceAsStream("firstdata/dwh/checkFileReport.properties");
            Properties p = new Properties();
            p.load(in);
            in.close();
            
            Set<?> keys = p.keySet();
            for (Object key: keys) {
                String filename = (String)key;
                String value = ( (String)p.get(filename) ).trim();
                //if not give the time, never alert
                if(value==null || value.trim().length()<=0){
                    value="2500";
                }

                int newValue = Integer.parseInt(value);
                FileArriveTime f = config.get(filename);
                int oldValue = f.getShouldArriveTime();
                if (newValue != oldValue) {
                    log.info("Filename " + filename +
                             " should-arrive-time changed." +
                             " Old value: " + oldValue + "," +
                             " new value: " + newValue + ".");
                    f.setShouldArriveTime(newValue);
                }
            }
        } catch (Exception e) {
            log.error("Reading config error.", e);
            throw e;
        }
    }
    
    private static void judgeHoliday(){
        log.debug(" judge once ");
        String nowDate = simpleDateFormat.format(now);
        if(holidayList == null || holidayList.isEmpty()){
            ifAlert = 0;
        }else{
            if(holidayList.contains(nowDate)==true)
            {
               //do not alert  //0: not,1: yes
                ifAlert = 0;
            }else{
                ifAlert = 1;
            }
        }
       
    }
    
    /**
     * read holiday list from config. Now it init only once when the app run.
     * if change config. please run it again.
     * @throws Exception
     */
    private static void initHoliday() throws Exception{
        URL input = FileLateMonitor.class.getClassLoader().getResource("firstdata/dwh/judgeHoliday.properties");

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(input.getFile()));
        } catch (FileNotFoundException e) {
            log.error(e);
            throw e;
        }
        String line = null ;
        String [] holiday = null;
        try {
            line = br.readLine();
            while(br.readLine() !=null){ 
                line = line +br.readLine();
            }
        } catch (IOException e) {
            log.error(e);
            throw e;
        }finally{
            try {
                br.close();
            } catch (IOException e) {
                log.error(e);
                throw e;
            } 
        }
        if(line != null){
            holiday = line.split(",");
        }else{
            holiday = null;
        }
        
        holidayList = new ArrayList<String>();
        holidayList = Arrays.asList(holiday);
    }
    
    
    public static Logger createNewLogger(String logFileName) throws IOException {
        URL logURL = FileLateMonitor.class.getClassLoader().getResource(LOG_CONFIG_FILE);
        PropertyConfigurator.configure(logURL);
        Logger genericLogger = Logger.getLogger("GenericLogger");
        Logger logger = Logger.getLogger(logFileName);
        Enumeration<?> appenders = genericLogger.getAllAppenders();
        while (appenders.hasMoreElements()){
            RollingFileAppender appender = (RollingFileAppender) appenders.nextElement();
            String fileName = logFilePath + "/" + logFileName +  "/" + appender.getFile();

            
            Layout layout = appender.getLayout();
            RollingFileAppender newAppender = null;
            Priority threshold = appender.getThreshold();
            try{
                newAppender = new RollingFileAppender(layout, fileName);
            } catch (IOException e) {
                log.error("createNewLogger", e);
                throw e;
            }
            
            newAppender.setFile(fileName);
            newAppender.setName(appender.getName());
            newAppender.setThreshold(threshold);
            newAppender.setMaxFileSize(String.valueOf(appender.getMaximumFileSize()));
            newAppender.setMaxBackupIndex(appender.getMaxBackupIndex());
            logger.addAppender(newAppender);
        }
        return logger;
    }
    
    /**
     * change the proper into hashmap
     * @throws IOException 
     */
    public static List<String> getNCBCFileList() throws IOException{
        log.debug(" get fileMap properties ");
        URL fileMapConfigURL = GetFilesFromServer.class.getClassLoader().getResource(NCBCFILEMAP_CONFIG_FILE);
        Properties fileMapProperties = new Properties();
        try {
            fileMapProperties.load( fileMapConfigURL.openStream());
        } catch (IOException e) {
            log.error("", e);
            throw e;
        }
        Set<Entry<Object, Object>> set = fileMapProperties.entrySet();  

        Iterator<Map.Entry<Object, Object>> it = set.iterator();  
        String key = null;  
 
        //HashMap map = new HashMap();
       List<String> list = new ArrayList<String>();
        while (it.hasNext()) {  
            Entry<Object, Object> entry = it.next();  
            key = String.valueOf(entry.getKey());  
            key = key == null ? key : key.trim();  
            list.add(key);
        }  
        return list;
    }
    

    private static final String NCBCFILEMAP_CONFIG_FILE = "firstdata/dwh/fileMap.properties";
    
    private static java.util.Calendar cal = null;
    
    private static int judgeStartT ;
    private static int judgeEndT ;
    private static int monitorSleepTime ;
    
    private static int endMonitorTime ;
    
    private static String logFilePath;
    private static final String LOG_CONFIG_FILE = "log4jproperties/log4j.properties";
    private static int nowTime = 0;
    
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
    private static List<String> holidayList;
    private static int ifAlert = 0;  
    private static String sendPath;
    
    
    //private boolean alarmedToday = false;
    private static Date now;
    
    private static Hashtable<String, FileArriveTime> fileMap = new Hashtable<String, FileArriveTime>();
    
    private static String eventID ;
    private static Logger log ;
    

    
}
