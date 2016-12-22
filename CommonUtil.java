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
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.Properties;

import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.RollingFileAppender;

import com.firstdata.common.ReportClient;

public class CommonUtil {

	private static String clientName;
	
	private static String fileName;

	private static String compressPath;

	private static String receivePath;

	private static String sendPath;

	private static String logFilePath;

	private static String reportPath;

	private static String dataFilePath;

	private static String userID;

	private static String hostAddress;

	private static String hostPort;

	private static String exeFile;

	private static String readyFlagFilePath;

	private static String fileSizePath;

	private static String commandFile;

	private static String commandPath;

	private static String commandFileAbsolutionPath;

	private static String WZZIPPath;

	private static String startTime;

	private static String endTime;

	private static Logger log;

	private static String id_rsaPath;

	private static String eventID;

	private static File flagFile;

	private static String errDir;

	private static String separator = System.getProperty("line.separator");//换行

	private static final String CONFIG_FILE = "firstdata/dwh/config.properties";

	private static final String LOG_CONFIG_FILE = "log4jproperties/log4j.properties";

	private static long maxRepeatSend;

	private static long FileMaxWaitTimeGF;

	private static long ThreadSleepInterval;

	private static long FileMaxWaitTime;

	private static long FlagFMaxWaitTime;

	private static String WZUNZIPPath;

	private static int MaxRepeatTimeGF;

	private static String getFileRemoteAddress;
	// time to begin to get file
	private static String GetFileTime;

	private static long ThreadSleepIntervalGF;

	private static String LocalCyberfusionDirectory;
	private static String LocalClientDirectory;

	private static String BackupDerectory;

	private static String ClientFileParentDir;
	private static String GetFileTimeEnd;

	public static String getGetFileTimeEnd() {
		return GetFileTimeEnd;
	}

	public static String getClientFileParentDir() {
		return ClientFileParentDir;
	}

	public static String getBackupDerectory() {
		return BackupDerectory;
	}

	public static long getFileMaxWaitTimeGF() {
		return FileMaxWaitTimeGF;
	}

	public static long getThreadSleepInterval() {
		return ThreadSleepInterval;
	}

	public static long getFileMaxWaitTime() {
		return FileMaxWaitTime;
	}

	public static long getFlagFMaxWaitTime() {
		return FlagFMaxWaitTime;
	}

	public static long getThreadSleepIntervalGF() {
		return ThreadSleepIntervalGF;
	}

	public static Logger getLog() {
		return log;
	}

	private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
			"yyyyMMdd HH:mm:ss");

	public static String getGetFileTime() {
		return GetFileTime;
	}

	public static String getLocalCyberfusionDirectory() {
		return LocalCyberfusionDirectory;
	}

	public static String getLocalClientDirectory() {
		return LocalClientDirectory;
	}

	public static SimpleDateFormat getSimpleDateFormat() {
		return simpleDateFormat;
	}

	public static String getFileName() {
		return fileName;
	}

	public static String getCompressPath() {
		return compressPath;
	}

	public static String getReceivePath() {
		return receivePath;
	}

	public static String getSendPath() {
		return sendPath;
	}

	public static String getLogFilePath() {
		return logFilePath;
	}

	public static String getReportPath() {
		return reportPath;
	}

	public static String getDataFilePath() {
		return dataFilePath;
	}

	public static String getUserID() {
		return userID;
	}

	public static String getHostAddress() {
		return hostAddress;
	}

	public static String getHostPort() {
		return hostPort;
	}
	
	public static String getClientName() {
		return clientName;
	}/**/

	public static String getExeFile() {
		return exeFile;
	}

	public static String getReadyFlagFilePath() {
		return readyFlagFilePath;
	}

	public static String getFileSizePath() {
		return fileSizePath;
	}

	public static String getCommandFile() {
		return commandFile;
	}

	public static String getCommandPath() {
		return commandPath;
	}

	public static String getCommandFileAbsolutionPath() {
		return commandFileAbsolutionPath;
	}

	public static String getWZZIPPath() {
		return WZZIPPath;
	}

	public static String getStartTime() {
		return startTime;
	}

	public static String getEndTime() {
		return endTime;
	}

	public static String getId_rsaPath() {
		return id_rsaPath;
	}

	public static String getEventID() {
		return eventID;
	}

	public static File getFlagFile() {
		return flagFile;
	}

	public static int getMaxRepeatTimeGF() {
		return MaxRepeatTimeGF;
	}

	public static String getErrDir() {
		return errDir;
	}

	public static String getSeparator() {
		return separator;
	}

	public static String getConfigFile() {
		return CONFIG_FILE;
	}

	public static String getLogConfigFile() {
		return LOG_CONFIG_FILE;
	}

	public static long getFilemaxwaittimegf() {
		return FileMaxWaitTimeGF;
	}

	public static long getFilemaxwaittime() {
		return FileMaxWaitTime;
	}

	public static long getFlagfmaxwaittime() {
		return FlagFMaxWaitTime;
	}

	public static String getWZUNZIPPath() {
		return WZUNZIPPath;
	}

	public static String getGetFileRemoteAddress() {
		return getFileRemoteAddress;
	}

	public static long getMaxRepeatSend() {
		return maxRepeatSend;
	}

	public static void propertyConfigureInit(String logName) {

		URL configURL = CommonUtil.class.getClassLoader().getResource(CONFIG_FILE);

		Properties properties = new Properties();

		try {
			properties.load(configURL.openStream());

		} catch (IOException e) {
			log.error("", e);
		}

		LocalCyberfusionDirectory = properties.getProperty("LocalCyberfusionDirectory");
	    LocalClientDirectory = properties.getProperty("LocalClientDirectory");
		reportPath = properties.getProperty("reportPath");
		eventID = properties.getProperty("eventID");
		logFilePath = properties.getProperty("logFilePath");
		errDir = properties.getProperty("errDir");
		id_rsaPath = properties.getProperty("id_rsaPath");

		exeFile = properties.getProperty("exeFile");
		WZUNZIPPath = properties.getProperty("WZUNZIPPath");

		receivePath = properties.getProperty("receivePath");
		compressPath = properties.getProperty("compressPath");
		sendPath = properties.getProperty("sendPath");
		logFilePath = properties.getProperty("logFilePath");
		reportPath = properties.getProperty("reportPath");
		dataFilePath = properties.getProperty("dataFilePath");
		readyFlagFilePath = properties.getProperty("readyFlagFilePath");
		fileSizePath = properties.getProperty("fileSizePath");
		eventID = properties.getProperty("eventID");
		maxRepeatSend = Integer.parseInt(properties.getProperty("maxRepeatSend"));
		MaxRepeatTimeGF = Integer.parseInt(properties.getProperty("MaxRepeatTimeGF"));
		userID = properties.getProperty("userID");
		hostAddress = properties.getProperty("hostAddress");
		hostPort = properties.getProperty("hostPort");
		// exeFile = properties.getProperty("exeFile");
		clientName = properties.getProperty("clientName");
		commandPath = properties.getProperty("commandPath");
		commandFile = properties.getProperty("commandFile");
		errDir = properties.getProperty("errDir");
		id_rsaPath = properties.getProperty("id_rsaPath");

		commandFileAbsolutionPath = properties.getProperty("commandFileAbsolutionPath");

		getFileRemoteAddress = properties.getProperty("getFileRemoteAddress");

		GetFileTime = properties.getProperty("GetFileTime");

		FileMaxWaitTime = Long.parseLong(properties.getProperty("FileMaxWaitTime"));

		FlagFMaxWaitTime = Long.parseLong(properties.getProperty("FlagFMaxWaitTime"));

		ThreadSleepInterval = Long.parseLong(properties.getProperty("ThreadSleepInterval"));

		FileMaxWaitTimeGF = Long.parseLong(properties.getProperty("FileMaxWaitTimeGF"));

		ThreadSleepIntervalGF = Long.parseLong(properties.getProperty("ThreadSleepIntervalGF"));

		BackupDerectory = properties.getProperty("BackupDerectory");

		ClientFileParentDir = properties.getProperty("ClientFileParentDir");

		GetFileTimeEnd = properties.getProperty("GetFileTimeEnd");

		log = createNewLogger(logName);
		log.debug(" config info init finished ");

		Calendar cal = GregorianCalendar.getInstance();
		startTime = simpleDateFormat.format(cal.getTime());

	}

	public static Logger createNewLogger(String logFileName) {
		URL logURL = CommonUtil.class.getClassLoader().getResource(
				LOG_CONFIG_FILE);
		PropertyConfigurator.configure(logURL);//读取配置文件
		
		Logger genericLogger = Logger.getLogger("GenericLogger");
		Logger logger = Logger.getLogger(logFileName);
        Enumeration<?> appenders = genericLogger.getAllAppenders();
        
		String fileName = null;
		//判断是否有下一个元素，有则输出
		while (appenders.hasMoreElements()) {
			RollingFileAppender appender = (RollingFileAppender) appenders
					.nextElement();

			// if(logType.equals(1)){
			// fileName = logFilePath + "/" + logFileName +
			// nowDate.getDateYYMMDD()+ "/" + appender.getFile();
			// }else{
			fileName = logFilePath + "/" + logFileName + "/"
					+ appender.getFile();
			// }

			Layout layout = appender.getLayout();
			RollingFileAppender newAppender = null;
			Priority threshold = appender.getThreshold();//过滤  等级
			try {
				newAppender = new RollingFileAppender(layout, fileName);
			} catch (IOException e) {
				log.error("createNewLogger", e);
			}

			newAppender.setFile(fileName);
			newAppender.setName(appender.getName());
			newAppender.setThreshold(threshold);
			newAppender.setMaxFileSize(String.valueOf(appender
					.getMaximumFileSize()));
			newAppender.setMaxBackupIndex(appender.getMaxBackupIndex());
			logger.addAppender(newAppender);
		}
		return logger;
	}

	public static void writeReport(String FileName) {
		log.debug("Writing report. Filename: " + FileName + ".");
		String YYMMDD = nowDate.getDateYYMMDD();
		File reportFile = new File(reportPath, "Report" + YYMMDD + ".txt");
		String message = startTime + "-" + endTime + "--G--" + FileName;
		ReportClient.report(reportFile.getPath(), message, log);
	}

	public static StreamRedirectThread redirectStream(InputStream in,String errfilename, boolean isBinary)
	{
		StreamRedirectThread t = new StreamRedirectThread(in, new File(errDir,errfilename), isBinary);
		t.writeFile();
		return t;
	}

	public static void clearProcessStream(InputStream inputStream)throws IOException {
		InputStreamReader isr = new InputStreamReader(inputStream);
		BufferedReader br = new BufferedReader(isr);
		String line = br.readLine();
		while (line != null) {
			line = br.readLine();
		}
	}

	/**
	 * write the file name and file size into a appointed file
	 * 
	 * @param fileName
	 */
	public static void fileSizeWrite(String fileName, String filePath) {
		File receiveFile = new File(filePath + "\\" + fileName);
		Date dateTime = new Date();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
		String fileTrnTime = simpleDateFormat.format(dateTime);
		String reportName = "Report" + fileTrnTime + ".txt";
		File reportFile = new File(fileSizePath + "\\" + reportName);
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(reportFile, true);
			OutputStreamWriter osw = new OutputStreamWriter(fos);
			osw.write(receiveFile.getName() + "," + receiveFile.length()
					+ separator);
			osw.flush();
			osw.close();
			fos.close();
			log.debug(" Write " + fileName
					+ " size to tempreport successfully!");

		} catch (Exception e) {
			log.error("Write " + fileName + " size to report failed!", e);
		}
	}

	/**
	 * write into windows eventlog when some errore are catched
	 * 
	 * @param message
	 * @param eventID
	 */
	public static void writeWinEventErrLog(String message, String eventID) {
		String cmd = "eventcreate -l application -so dwhcompress -t error -d \""
				+ message + "\"  -id " + eventID;
		Process process;
		try {
			process = Runtime.getRuntime().exec(cmd);
			clearProcessStream(process.getErrorStream());
			clearProcessStream(process.getInputStream());
			int returnCode = process.waitFor();
			if (returnCode == 0) {
				log.debug("Write windows error event log successful.");
			} else {
				log.debug("The process used to write window error event log is failed. Return code: "
						+ returnCode);
				return;
			}
		} catch (IOException e) {
			log.error(
					"The process used to write window error event log is failed.",
					e);
		} catch (InterruptedException e) {
			log.error(
					"The process used to write window error event log is failed.",
					e);
		} catch (Exception e) {
			log.error(
					"The process used to write window error event log is failed X.",
					e);
		}
	}

	public static void main(String[] arg) throws Exception {
		// batch send files
		File filePath = new File("D:\\NCBC\\SIT\\FM");
		File[] fileList = filePath.listFiles();
		for (File filetemp : fileList) {
			if (filetemp.isFile()) {

				// String command =
				// "cmd /c start D:/workspace/file-compress-transfering/run.bat "
				// + filetemp;
				String[] arg1 = new String[1];
				arg1[0] = filetemp.getName();
				Compress.main(arg1);
			}
		}
	}

}
