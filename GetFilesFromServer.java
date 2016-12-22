package com.firstdata.dwh.compress;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class GetFilesFromServer {

	private static final String NCBCFILEMAP_CONFIG_FILE = "firstdata/dwh/fileMap.properties";
	private static Logger log;
	private static String currentTime;
	private static int ableGetFileFlag = 1; // 1 able to get file , 2 not get
											// file, 3 have write window event
	private static Properties fileMapProperties;
	private static int MaxRepeatTimes = CommonUtil.getMaxRepeatTimeGF();
	private static int repeatTimes = 0;
	// private static SimpleDateFormat dateFormat = new
	// SimpleDateFormat("yyyyMMdd");
	private static Calendar cal = null;

	private static List<String> holidayList;
	// private static int ifAlert = 0; //do not alert //0: not,1: yes
	private static Date now;
	private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
			"yyyyMMdd");
	private static Map<String, Integer> fileNumMarkMap = new HashMap<String, Integer>();
	// private static Map fileGetMarkMap = new HashMap();
	private static List<String> fileGetMarkList = new ArrayList<String>();

	/**
	 * read holiday list from config. Now it init only once when the app run. if
	 * change config. please run it again.
	 * 
	 * @throws Exception
	 */
	public static void initHoliday() throws Exception {
		log.debug("init the holiday info");
		URL input = FileLateMonitor.class.getClassLoader().getResource(
				"firstdata/dwh/judgeHoliday.properties");

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(input.getFile()));
		} catch (FileNotFoundException e) {
			log.error(e);
			throw e;
		}
		String line = null;
		String[] holiday = null;
		try {
			line = br.readLine();
			while (br.readLine() != null) {
				line = line + br.readLine();
			}
		} catch (IOException e) {
			log.error(e);
			throw e;
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				log.error(e);
				throw e;
			}
		}
		if (line != null) {
			holiday = line.split(",");
		} else {
			holiday = null;
		}

		holidayList = new ArrayList<String>();
		holidayList = Arrays.asList(holiday);
	}

	public static void main(String[] args) {
		start();
	}

	public static void start() {
		// log and config init
		try {
			CommonUtil.propertyConfigureInit("GetFile");
			log = CommonUtil.getLog();
			checkGetFilesTime();

		} catch (Exception e) {
			log.error(e, e);
			String eventID = CommonUtil.getEventID();
			CommonUtil.writeWinEventErrLog(" getting file Exception ", eventID);
			System.exit(1);
		}

	}

	public static void judgeHoliday() {
		log.debug(" judge once ");
		String nowDate = simpleDateFormat.format(now);
		if (holidayList == null || holidayList.isEmpty()) {
			ableGetFileFlag = 2;
		} else {
			if (holidayList.contains(nowDate) == true) {
				// do not alert //0: not,1: yes
				ableGetFileFlag = 2;
			} else {
				ableGetFileFlag = 1;
			}
		}

	}

	private static void checkGetFilesTime() throws Exception {
		log.debug(" check if the getting file time  ");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HHmmss");

		initHoliday();

		long ThreadSleepIntervalGF = CommonUtil.getThreadSleepIntervalGF();
		String getFileTime = CommonUtil.getGetFileTime();
		String getFileTimeEnd = CommonUtil.getGetFileTimeEnd();
		while (true) {
			cal = GregorianCalendar.getInstance();
			currentTime = simpleDateFormat.format(cal.getTime());

			now = cal.getTime();

			log.debug("the current hour_of_day: "
					+ cal.get(Calendar.HOUR_OF_DAY) + cal.get(Calendar.MINUTE));
			log.debug("the beginning getfiletime hour_of_day: "
					+ getFileTime.substring(0, 4) + " endtime "
					+ getFileTimeEnd.substring(0, 4));
			// if the hour is older than the getflletime then flag change to 1
			// means wait for new files at new day
			if (currentTime.compareToIgnoreCase(getFileTime) < 0) {
				// ableGetFileFlag = 1;
				judgeHoliday();
				fileGetMarkList.clear();

			}

			// judge holiday

			// log.debug(" ifAlert " + ifAlert);
			log.debug("ableGetFileFlag: 1 yes,2 no,3 error ---"
					+ ableGetFileFlag);
			try {
				/*
				 * if(ifAlert==0){ //holiday, no alert }else{
				 */
				if (currentTime.compareToIgnoreCase(getFileTime) >= 0
						&& ableGetFileFlag == 1
						&& currentTime.compareToIgnoreCase(getFileTimeEnd) < 0) {
					// create command to sftp get files every day
					// log = CommonUtil.createNewLogger("GetFile");

					/* Enumeration NCBCFileMap = getNCBCFileEnum(); */
					readyToGetFiles();
				}
				// if beyond the deadline, write event. so please don't run the
				// app after the getFileTimeEnd
				if (currentTime.compareToIgnoreCase(getFileTimeEnd) >= 0 /*
																		 * &&
																		 * ableGetFileFlag
																		 * ==1
																		 */) {
					log.info(" getting files fail, beyond the deadline ");
					// String eventID = CommonUtil.getEventID();
					// CommonUtil.writeWinEventErrLog("getting files fail, beyond the deadline",
					// eventID);
					// ableGetFileFlag = 3;
					ableGetFileFlag = 2;// do not get files again today.
				}
				/*
				 * }//if
				 */Thread.sleep(ThreadSleepIntervalGF);
			} catch (Exception e) {
				e.printStackTrace();
				log.error(e, e);
				throw e;
			}

		}// while
	}

	static class Filefilter implements FilenameFilter {
		@Override
		public boolean accept(File dir, String name) {

			if (new File(dir + "/" + name).isDirectory()) {
				return false;
			} else {
				log.debug(name.substring(name.indexOf("_") + 1,
						name.indexOf("_") + 9));
				if (name.substring(name.indexOf("_") + 1, name.indexOf("_") + 9)
						.equals(simpleDateFormat.format(cal.getTime()))) {
					return true;
				} else {
					return false;
				}
			}

		}
	}

	private static void readyToGetFiles() throws Exception {
		boolean flag = getFileFromClientBatch(new File("ncbcGetFilesBatch"));
		CommandFile.deleteCommandFile("ncbcGetFilesBatch");
		handleFiles(flag);
	}

	/**
	 * if seccess move file else try again beyond the max times write windows
	 * event
	 * 
	 * @param b
	 * @throws Exception
	 */
	private static void handleFiles(boolean b) throws Exception {
		long FileMaxWaitTimeGF = 0;
		// String eventID = CommonUtil.getEventID();
		if (b == true) {
			// move file
			String NCBCFileParentDir = CommonUtil.getClientFileParentDir()
					.trim();
			File fileList = new File(CommonUtil.getLocalClientDirectory() + "/"
					+ NCBCFileParentDir);
			File[] fileArray = fileList.listFiles(new Filefilter());
			/* File [] fileArray = fileList.listFiles(); */
			Map<String, String> configList = getNCBCFileHashMap();
			int fileNumMark = 0;
			//String fileGetMark;
			for (int i = 0; i < fileArray.length; i++) {
				String fileNameNCBC = fileArray[i].getName();
				String fileNamePrefix = fileNameNCBC.substring(0,
						fileNameNCBC.indexOf("_"));
				String filePath = (String) configList.get(fileNamePrefix
						.toUpperCase());

				// cxd
				fileNumMark = Integer.parseInt(fileNumMarkMap.get(filePath)
						.toString());
				fileNumMarkMap.put(filePath, fileNumMark + 1);
				if (fileNumMark > 0) {
					//
					Thread.sleep(600000);
					// System.out.println("---------");
				}
				//
				// String nowDate = simpleDateFormat.format(now);
				// fileGetMark = fileGetMarkMap.get(filePath).toString();
				String sendPath = CommonUtil.getSendPath();
				if (!fileGetMarkList.contains(fileNameNCBC)) {
					fileGetMarkList.add(fileNameNCBC);

					String cyberfusionDirectory = CommonUtil
							.getLocalCyberfusionDirectory();
					File fileNCBC = fileArray[i];
					File fileFD = new File(cyberfusionDirectory + "/"
							+ filePath + "/" + fileNameNCBC);

					String temp = fileNameNCBC.substring(0,
							fileNameNCBC.lastIndexOf("_")).concat(
							fileNameNCBC.substring(
									fileNameNCBC.lastIndexOf("_") + 1,
									fileNameNCBC.length()));
					temp = temp.substring(0, temp.lastIndexOf("_") + 1).concat(
							temp.substring(temp.lastIndexOf("_") + 3,
									temp.length()));

					File fileFDBackup = new File(sendPath + "/success/" + temp
							+ ".ZIP");

					File fileFDBackup000K = new File(sendPath + "/success/"
							+ temp + ".ZIP.000K");
					// move file
					FileUtils.copyFile(fileNCBC, fileFDBackup);
					FileUtils.copyFile(fileNCBC, fileFDBackup000K);
					moveFile(fileNCBC, fileFD);
					log.info(" End: move file success");
					CommonUtil.writeReport(fileNamePrefix);
				} else {

				}

			}
		} else {
			// get again
			if (repeatTimes >= MaxRepeatTimes) {
				log.info(" beyond the MaxRepeatTimes ");
			} else {
				repeatTimes++;
				FileMaxWaitTimeGF = CommonUtil.getFileMaxWaitTimeGF();
				try {
					Thread.sleep(FileMaxWaitTimeGF);
					readyToGetFiles();
				} catch (InterruptedException e) {
					log.error("", e);
					throw e;
				}
			}
		}
	}

	private static void moveFile(File src, File dst) throws Exception {
		log.info("Moving file " + src + " to " + dst);
		boolean moved = false;
		// Try 10 times.
		for (int i = 0; !moved && i < 9; i++) {
			try {
				if (dst.exists()) {
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
				} catch (Exception e1) {
				}
			}
		}
		if (!moved) {
			// Last try. If it fails, let it throw Exception.
			if (dst.exists()) {
				log.info("Destination file " + dst + " exists. Deleting.");
				FileUtils.forceDelete(dst);
				log.info("Deleted.");
			}
			FileUtils.moveFile(src, dst);
		}
		log.info("Moving finished.");
	}

	/**
	 * fire the command to get files
	 * 
	 * @param toGetFile
	 * @return
	 * @throws Exception
	 */
	private static boolean getFileFromClientBatch(File toGetFile)
			throws Exception {
		String fileName = toGetFile.getName();
		CommandFile.createCommandGetFile(new File(fileName));
		String message = null;
		// String eventID = CommonUtil.getEventID();
		String exeFile = CommonUtil.getExeFile();
		String id_rsaPath = CommonUtil.getId_rsaPath();
		String commandPath = CommonUtil.getCommandPath();
		String commandFile = CommonUtil.getCommandFile();
		String hostAddress = CommonUtil.getHostAddress();
		String hostPort = CommonUtil.getHostPort();
		String userID = CommonUtil.getUserID();

		try {
			log.debug(" Begin to get the file " + fileName);
			String command = exeFile + " -i \"" + id_rsaPath + "\" -b \""
					+ commandPath + "/" + fileName + commandFile + "\" -oPort="
					+ hostPort + " " + userID + "@" + hostAddress;

			log.debug(" transmit command " + command);

			Process process = Runtime.getRuntime().exec(command);

			StreamRedirectThread errstr = CommonUtil.redirectStream(
					process.getErrorStream(), "sftpErrInfo.txt", false);
			StreamRedirectThread infostr = CommonUtil.redirectStream(
					process.getInputStream(), "sftpInputInfo.txt", false);
			if (errstr.hasFailed()) {
				message = fileName
						+ " get from  Server failed because of sftp errstream has word 'failed'!";
				log.info(message);
				// CommonUtil.writeWinEventErrLog(message, eventID);
				return false;
			}
			if (infostr.hasFailed()) {
				message = fileName
						+ "get from  Server failed because of sftp infostream has word 'failed'!";
				log.info(message);
				// CommonUtil.writeWinEventErrLog(message, eventID);
				return false;
			}

			CommonUtil.clearProcessStream(process.getErrorStream());
			CommonUtil.clearProcessStream(process.getInputStream());
			int returnCode = process.waitFor();
			// get file success
			if (returnCode == 0) {
				log.info(" End: get file " + fileName
						+ " successful.Now begin to count file Num");
				return checkFileList();
				/* return true; */

			} else {
				log.info(" the ProcessStream return value is not 0 ");
				// message = " get ncbc files failed ";
				// CommonUtil.writeWinEventErrLog(message, eventID);
				return false;

			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("", e);
			throw e;
		}
	}

	/**
	 * check the file name is matched with configuration.and whether file num is
	 * right
	 * 
	 * @return
	 * @throws IOException
	 */
	private static boolean checkFileList() throws IOException {
		String NCBCFileParentDir = CommonUtil.getClientFileParentDir().trim();
		File fileList = new File(CommonUtil.getLocalClientDirectory() + "/"
				+ NCBCFileParentDir);
		File[] fileArray = fileList.listFiles(new Filefilter());
		Map<String, String> configList = getNCBCFileHashMap();
		int fileCount = 0;

		for (int i = 0; i < fileArray.length; i++) {
			String fileNameNCBC = fileArray[i].getName();
			String fileNamePrefix = fileNameNCBC.substring(0,
					fileNameNCBC.indexOf("_"));
			String filePath = (String) configList.get(fileNamePrefix
					.toUpperCase());
			if (filePath == null) {
				log.info(" the file name is not matched " + fileNameNCBC);
				continue;
			} else {
				if (filePath.trim().length() > 0) {
					log.info(" successfully get file " + fileNameNCBC);
					fileCount++;

					continue;
				} else {
					log.info(" fail to get file " + fileNameNCBC);
				}
			}
		}
		// judge the file count
		// log.info(" get the files, file count " + fileCount);
		/*
		 * if(fileCount==configList.size()){ //the file number is right //if
		 * have files ableGetFileFlag = 0; return true; }else{ return false; }
		 */

		// if have one file return true
		log.info(" get the files, file count " + fileCount);
		if (fileCount > 0) {
			// if have files
			// ableGetFileFlag = 0;
			return true;
		} else {
			return false;
		}

	}

	/**
	 * change the proper into hashmap
	 * 
	 * @throws IOException
	 */
	public static Map<String, String> getNCBCFileHashMap() throws IOException {
		log.debug(" get fileMap properties ");
		URL fileMapConfigURL = GetFilesFromServer.class.getClassLoader()
				.getResource(NCBCFILEMAP_CONFIG_FILE);

		fileMapProperties = new Properties();
		try {
			fileMapProperties.load(fileMapConfigURL.openStream());
		} catch (IOException e) {
			log.error("", e);
			throw e;
		}
		Set<Entry<Object, Object>> set = fileMapProperties.entrySet();

		Iterator<Map.Entry<Object, Object>> it = set.iterator();
		String key = null, value = null;

		HashMap<String, String> map = new HashMap<String, String>();

		while (it.hasNext()) {
			Entry<Object, Object> entry = it.next();
			key = String.valueOf(entry.getKey());
			value = String.valueOf(entry.getValue());
			key = key == null ? key : key.trim();
			value = value == null ? value : value.trim();
			map.put(key, value);
			fileNumMarkMap.put(key, 0);
		}
		return map;
	}
}
