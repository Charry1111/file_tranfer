package com.firstdata.dwh.compress;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class AutoReSend {
    
    private static Logger log ;
    
    private static String errDir ;
    private static String commandFileAbsolutionPath;
    private static String readyFlagFilePath ;
    private static String separator = System.getProperty("line.separator");
    private static HashMap<String, String> ncbcFileMap;
    private static final String checkFileReport = "firstdata/dwh/checkFileReport.properties";
    /**
     * @param args
     * @throws IOException 
     */
    private static void createFlagFile(String readyFlagFilePath,File compressedFile) {
        log.debug(" Begin to create flag file ");
        String name = compressedFile.getName().substring(0, compressedFile.getName().lastIndexOf("."));
        //获得文件目录的 最后一个.之前的东西如果
        File flagFile = new File(readyFlagFilePath + "\\" + name + ".ZIP.000K");
        FileOutputStream fos;
        try{
            fos = new FileOutputStream(flagFile, true);
            OutputStreamWriter osw = new OutputStreamWriter(fos);
            long size = compressedFile.length();
            osw.write(String.valueOf(size));
            osw.flush();
            osw.close();
            fos.close();
            log.info(" End: successfully create flag file " + flagFile);   
        }  catch (Exception e) {
            log.error("Create transferring-finished flag " + name + " failed! ",e);
            String message = "Create transferring-finished flag " + name + " failed! ";
            log.info(message);
            //Compress.writeWinEventErrLog(message, eventID);
        }
    }
    
    public static void main(String[] args) throws Exception {
        CommonUtil.propertyConfigureInit("Resend");

        log = CommonUtil.getLog();
        readyFlagFilePath = CommonUtil.getReadyFlagFilePath();
        errDir = CommonUtil.getErrDir();
        
        String sendPath = CommonUtil.getSendPath();
        
        
        File[] fileList = new File(sendPath + "/Failure").listFiles();
        
        File sendFile = null ;
        for (File f: fileList) {
            try{
                //
               // log.debug(" Begin to copy file " + toSendFile.getName() + " from directory failure. elapsedTime: " + elapsedTime);
                sendFile = new File(sendPath + "\\" + f.getName()); 
                moveFile(f,sendFile);
                
            }catch (Exception e) {
                String message = "copy file has trouble";
                log.error(message, e);
              //  Compress.writeWinEventErrLog(message, eventID);
            }
            String name = f.getName().substring(0, f.getName().lastIndexOf("."));

            createFlagFile(readyFlagFilePath, sendFile);
            
            File flagFile = new File(readyFlagFilePath + "\\" + name + ".ZIP.000K");

        
            boolean successflag =  sendFile(f.getName(), sendFile);
            CommandFile.deleteCommandFile(commandFileAbsolutionPath,log);
             
             
             if(successflag==true){
                 //move flag file
                 File oriFlagFile = new File(readyFlagFilePath + "\\" + flagFile.getName());
                 File sendFlagFile = new File(sendPath + "\\" + flagFile.getName());
                 
                 moveFile(oriFlagFile,sendFlagFile);
                 //send flag file
                 successflag = sendFile(f.getName(), sendFlagFile);
                 
                 if(successflag == true){
                     writeReport(f.getName().substring(0,f.getName().lastIndexOf("_")));
                 }
                 
                 CommandFile.deleteCommandFile(commandFileAbsolutionPath,log);
                 System.out.println("finish");
             }  

        }

        
//        File oriFlagFile = new File(sendPath + "\\Failure/ATPT_120726.ZIP144652" );
//        File sendFlagFile = new File(sendPath + "\\ATIA_120726.ZIP" );
//        FileUtils.copyFile(oriFlagFile,sendFlagFile);
        
        //uncompress
        
        //FileUtils.forceDelete(oriFlagFile);
        
    }
    
    private static boolean sendFile(String originalFileName, File toSendFile) {
        String fileName = toSendFile.getName();
        String exeFile = CommonUtil.getExeFile();
        String id_rsaPath = CommonUtil.getId_rsaPath();
        String commandPath = CommonUtil.getCommandPath();
        String commandFile = CommonUtil.getCommandFile();
        String userID = CommonUtil.getUserID();
        String hostAddress = CommonUtil.getHostAddress();
        String hostPort = CommonUtil.getHostPort();
        //String eventID = CommonUtil.getEventID();
        String sendPath = CommonUtil.getSendPath();
        try {
            log.debug("Begin to send the file " + fileName + " .");
            
            CommandFile.createCommmandFile(originalFileName, fileName,log);
            String command = "cmd /c " + exeFile + " -i \"" + id_rsaPath + "\" -b \"" 
            + commandPath + "/" + fileName+ commandFile + "\" -oPort=" + hostPort + " " + userID + "@" + hostAddress;
            commandFileAbsolutionPath = commandPath + "/" + fileName + commandFile;
            log.debug("transafering command" + command );
            log.debug("commandFileAbsolutionPath" + command );
            Process process = Runtime.getRuntime().exec(command);
            StreamRedirectThread errstr =
                redirectStream( process.getErrorStream(), "sftpErrInfo.txt", false );
            StreamRedirectThread infostr =
                redirectStream( process.getInputStream(), "sftpInputInfo.txt", false );
            if( errstr.hasFailed()){
                String message =
                    fileName + " sent to Server failed because of finding 'failed' in sftp errorstream !";
                log.error(message);
                //CommonUtil.writeWinEventErrLog(message, eventID);
                return false;
            }
            if( infostr.hasFailed() ){
                String message = fileName + " cannot send to Server because of finding 'failed' in sftp inputstream ! ";
                log.error(message);
                //CommonUtil.writeWinEventErrLog(message, eventID);
                return false; 
            }
            clearProcessStream(process.getErrorStream());
            clearProcessStream(process.getInputStream());
            int returnCode = process.waitFor();
            //success
            if (returnCode == 0) {
                //Calendar cal = GregorianCalendar.getInstance();
                log.info(" End:Send file " + fileName + " successful.");
                //move file to directory success
                File oriFlagFile = new File(sendPath + "\\" + fileName);
                File sendFlagFile = new File(sendPath + "\\" + "Success" + "\\" + fileName 
                   /* + cal.get(Calendar.HOUR_OF_DAY)
                    + cal.get(Calendar.MINUTE)
                    + cal.get(Calendar.SECOND)*/);
                
                moveFile(oriFlagFile,sendFlagFile);
                return true;
            }
            else {
                log.info(" Send file " + fileName + " failed.");
                
                File oriFlagFile = new File(sendPath + "\\" + fileName);
                File sendFlagFile = new File(sendPath + "\\" + "Failure" + "\\" + fileName);
                moveFile(oriFlagFile,sendFlagFile);
                return false;
                
            }
        } catch (Exception e){
            //e.printStackTrace();
            log.error("The process used to send file " + fileName + " is failed.",e);
        }
        return true;
    }
    private static void clearProcessStream(InputStream inputStream)
        throws IOException {
        InputStreamReader isr = new InputStreamReader(inputStream);
        BufferedReader br = new BufferedReader(isr);
        String line = br.readLine();
        while (line != null) {
            line = br.readLine();
        }
    }
    public static StreamRedirectThread redirectStream(InputStream in, String errfilename, boolean isBinary) {
        StreamRedirectThread t =
            new StreamRedirectThread( in, new File(errDir, errfilename), isBinary );
        t.writeFile();
        return t;
    }
    
    public static void writeReport(String originalFileName) {
       
    	String clientName = CommonUtil.getClientName();
        Date dateTime = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        String endTime = simpleDateFormat.format(dateTime);
        String reportPath = CommonUtil.getReportPath();
        File reportFile = new File(reportPath + "\\" + "Report" + nowDate.getDateYYMMDD() + ".txt");
        //File CheckNCBCFile = new File(reportPath + "\\" + "CheckNCBCFile" + nowDate.getDateYYMMDD() + ".txt");
        File CheckClientFile = new File(reportPath + "\\" + "Check"+clientName+"File" + nowDate.getDateYYMMDD() + ".txt");
        FileOutputStream fos;
        try{
            getNCBCFileHashMap();
            log.debug(" Begin to writereport " + originalFileName);
            if(ncbcFileMap != null && ncbcFileMap.containsKey(originalFileName) ){           
                fos = new FileOutputStream(CheckClientFile, true);
                OutputStreamWriter osw = new OutputStreamWriter(fos);
                osw.write( endTime + "-" + endTime + "--P--" + originalFileName + separator);
                osw.flush();
                osw.close();
                fos.close();
            }else{
                fos = new FileOutputStream(reportFile, true);
                OutputStreamWriter osw = new OutputStreamWriter(fos);
                osw.write( endTime + "-" + endTime + "--P--" + originalFileName + separator);
                osw.flush();
                osw.close();
                fos.close();
                log.debug(" End: Write "
                    + originalFileName
                    + " file transmission startTime and endTime to report successfully!");
            }
        } catch (Exception e) {
            log.error("Write " + originalFileName
                + " write file transmission  report failed!",e);

        }
    }
    
    public static void  getNCBCFileHashMap() throws IOException{
        log.debug(" get fileMap properties ");
        URL fileMapConfigURL = GetFilesFromServer.class.getClassLoader().getResource(checkFileReport);

        Properties fileMapProperties = new Properties();
        try {
            fileMapProperties.load( fileMapConfigURL.openStream());
        } catch (IOException e) {
            log.error("", e);
            throw e;
        }
        Set<Entry<Object, Object>> set = fileMapProperties.entrySet();  

        Iterator<Map.Entry<Object, Object>> it = set.iterator();  
        String key = null, value = null;  
 
        ncbcFileMap = new HashMap<String, String>();
        
        while (it.hasNext()) {  
            Entry<Object, Object> entry = it.next();  
            key = String.valueOf(entry.getKey());  
            value = String.valueOf(entry.getValue());  
            key = key == null ? key : key.trim();  
            value = value == null ? value : value.trim();  
            ncbcFileMap.put(key, value);  
        }  
    }
    
    private static void moveFile(File src,File dst) throws Exception {
        log.info("Moving file " + src + " to " + dst);
        boolean moved = false;
        // Try 10 times.
        for (int i = 0; !moved && i < 9; i++) {
            try {
                if ( dst.exists() ) {
                    log.info("Destination file " + dst + " exists. Deleting.");
                    FileUtils.forceDelete(dst);
                    log.info("Deleted.");
                }
                FileUtils.moveFile(src, dst);
                moved = true;
            } catch (Exception e) {
                log.debug("Moving failed.", e);
                try { // Wait for a while before retry.
                    Thread.sleep(60 * 1000);
                    //当前线程将会暂停60秒后持续运行。
                } catch (Exception e1) {}
            }
        }
        if (!moved) {
            // Last try. If it fails, let it throw Exception.
            if ( dst.exists() ) {
                log.info("Destination file " + dst + " exists. Deleting.");
                FileUtils.forceDelete(dst);
                log.info("Deleted.");
            }
            FileUtils.moveFile(src, dst);
        }
        log.info("Moving finished.");
    }
    
}
