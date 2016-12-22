package com.firstdata.dwh.compress;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

public class CommandFile {
	/**
	 * create command file contained the sftp command about sending file
	 * 
	 * @param fileName
	 * @param Log
	 */
	private static String CONFIG_FILE = "firstdata/dwh/config.properties";
	
	
	/**
	 *  The file is used to configure target remote address mapping for send files. 
	 */
	private static final String TARGET_REMOTE_ADDRESS_PATH = "firstdata/dwh/targetRemoteAddressMap.properties";
	//FOR HKT_15018
	public static void createCommmandFile(String originalFileName,String fileName, Logger Log, boolean HKT_15018, String file_Name) {

		FileInputStream fis = null;
		URL configURL = CommandFile.class.getClassLoader().getResource(
				CommonUtil.getConfigFile());
		Properties prop;
		try {

			File configFile = null;
			configFile = new File(configURL.toURI());
			Log.info("Start to create the command file .");
			fis = new FileInputStream(configFile);
			prop = new Properties();
			prop.load(fis);
			fis.close();

			String commandPath = prop.getProperty("commandPath");
			String commandFile = prop.getProperty("commandFile");
			String remoteAddress="";
			if (!HKT_15018) {
				remoteAddress = prop.getProperty("remoteAddress");
			} else {
				remoteAddress = prop.getProperty(file_Name + ".remoteAddress");
			}
			HashMap<String, String> targetRemoteAddressMap = getTargetRemoteAddressMap(Log);
			
			//targetRemoteAddress can be configured in file.

			String sendFileName = "";
			String uncompressFileList = Compress.getUncompressFileList();
			boolean uncompressFlag = false;
			if (uncompressFileList != null && !"".equals(uncompressFileList.trim())) {
                String uncompressFiles[] = uncompressFileList.split(",");
                for (int i = 0; i < uncompressFiles.length; i++) {
                    if (fileName.startsWith(uncompressFiles[i])) {
                        uncompressFlag = true;
                        sendFileName = uncompressFiles[i];
                    }
                }
            }
			if (!uncompressFlag) {
				if (Compress.isRenameAndNoCompress(originalFileName)) {
					sendFileName = fileName;
				} else {
					sendFileName = fileName.substring(0,
							fileName.lastIndexOf("_"));
				}
			}
			
//			String sendFileName = fileName.substring(0, fileName.lastIndexOf("_"));
			String sendPath = prop.getProperty("sendPath");
			BufferedWriter osw = null;
			String commandFilen = commandPath + "\\" + fileName + commandFile;
			File commandCheckPath = new File(commandPath);
			if (!commandCheckPath.exists()) {
				commandCheckPath.mkdirs();
			}
			Log.debug(commandFilen);
			osw = new BufferedWriter(new FileWriter(commandFilen, false));
			String remoteAddresses[] = null;
			if (targetRemoteAddressMap != null && targetRemoteAddressMap.get(sendFileName + ".TargetRemoteAddress") != null ) {
				remoteAddresses = targetRemoteAddressMap.get(sendFileName + ".TargetRemoteAddress").split("\\|");
			}
			if (remoteAddresses != null && remoteAddresses.length > 0) {
				for (int i = 0; i < remoteAddresses.length; i++) {
					String content = "-put \"" + sendPath + "/" + fileName + "\" "
							+ remoteAddresses[i];
					Log.debug(content);
					osw.write(content);
					osw.newLine();
					osw.flush();
				}
				osw.close();
			}
			else {
				String content = "-put \"" + sendPath + "/" + fileName + "\" "
						+ remoteAddress;
				Log.debug(content);
				osw.write(content);
				osw.newLine();
				osw.flush();
				osw.close();
			}
			Log.info(" End: Success create the command file .");
		} catch (Exception e) {
			Log.error("Fail create the command file .  ", e);
		}
	}
	
	public static void createCommmandFile(String originalFileName,String fileName, Logger Log) {

		FileInputStream fis = null;
		URL configURL = CommandFile.class.getClassLoader().getResource(CommonUtil.getConfigFile());
		Properties prop;
		try {

			File configFile = null;
			configFile = new File(configURL.toURI());
			Log.info("Start to create the command file .");
			fis = new FileInputStream(configFile);
			prop = new Properties();
			prop.load(fis);
			fis.close();

			String commandPath = prop.getProperty("commandPath");
			String commandFile = prop.getProperty("commandFile");
			
			String remoteAddress = prop.getProperty("remoteAddress");
			
			HashMap<String, String> targetRemoteAddressMap = getTargetRemoteAddressMap(Log);
			
			//targetRemoteAddress can be configured in file.

			String sendFileName = "";
			String uncompressFileList = Compress.getUncompressFileList();
			boolean uncompressFlag = false;
			if (uncompressFileList != null && !"".equals(uncompressFileList.trim())) {
                String uncompressFiles[] = uncompressFileList.split(",");
                for (int i = 0; i < uncompressFiles.length; i++) {
                    if (fileName.startsWith(uncompressFiles[i])) {
                        uncompressFlag = true;
                        sendFileName = uncompressFiles[i];
                    }
                }
            }
			if (!uncompressFlag) {
				if (Compress.isRenameAndNoCompress(originalFileName)) {
					sendFileName = fileName;
				} else {
					sendFileName = fileName.substring(0,
							fileName.lastIndexOf("_"));
				}
			}
			
//			String sendFileName = fileName.substring(0, fileName.lastIndexOf("_"));
			String sendPath = prop.getProperty("sendPath");
			BufferedWriter osw = null;
			String commandFilen = commandPath + "\\" + fileName + commandFile;
			File commandCheckPath = new File(commandPath);
			if (!commandCheckPath.exists()) {
				commandCheckPath.mkdirs();
			}
			Log.debug(commandFilen);
			osw = new BufferedWriter(new FileWriter(commandFilen, false));
			String remoteAddresses[] = null;
			if (targetRemoteAddressMap != null && targetRemoteAddressMap.get(sendFileName + ".TargetRemoteAddress") != null ) {
				remoteAddresses = targetRemoteAddressMap.get(sendFileName + ".TargetRemoteAddress").split("\\|");
			}
			if (remoteAddresses != null && remoteAddresses.length > 0) {
				for (int i = 0; i < remoteAddresses.length; i++) {
					String content = "-put \"" + sendPath + "/" + fileName + "\" "
							+ remoteAddresses[i];
					Log.debug(content);
					osw.write(content);
					osw.newLine();
					osw.flush();
				}
				osw.close();
			}
			else {
				String content = "-put \"" + sendPath + "/" + fileName + "\" "
						+ remoteAddress;
				Log.debug(content);
				osw.write(content);
				osw.newLine();
				osw.flush();
				osw.close();
			}
			Log.info(" End: Success create the command file .");
		} catch (Exception e) {
			Log.error("Fail create the command file .  ", e);
		}
	}

	/**
	 * get target remote address mapping from the configuration file.
	 * If the file is configured in this file, the file will be upload to target remote address 
	 * which is configured in this file.
	 * @param log
	 * @return targetRemoteAddressMap
	 * @throws IOException
	 */
	private static HashMap<String, String> getTargetRemoteAddressMap(Logger log) throws IOException {
		log.debug(" get targetRemoteAddressMap properties ");
		URL configURL = CommandFile.class.getClassLoader().getResource(
				TARGET_REMOTE_ADDRESS_PATH);
		Properties fileMapProperties = new Properties();
		try {
			fileMapProperties.load(configURL.openStream());
		} catch (IOException e) {
			log.error("The file targetRemoteAddressMap.properties is not found! ", e);
			throw e;
		}
		Set<Entry<Object, Object>> set = fileMapProperties.entrySet();

		Iterator<Map.Entry<Object, Object>> it = set.iterator();
		String key = null, value = null;

		HashMap<String, String> targetRemoteAddressMap = new HashMap<String,String>();

		while (it.hasNext()) {
			Entry<Object, Object> entry = it.next();
			key = String.valueOf(entry.getKey());
			value = String.valueOf(entry.getValue());
			key = key == null ? key : key.trim();
			value = value == null ? value : value.trim();
			targetRemoteAddressMap.put(key, value);
		}
		return targetRemoteAddressMap;
	}

	/**
	 * 
	 * @param fileClient
	 */
	public static void createCommandGetFile(File fileClient) {
		Logger log = CommonUtil.getLog();
		FileInputStream fis = null;
		URL configURL = CommandFile.class.getClassLoader().getResource(
				CONFIG_FILE);
		Properties prop;
		try {

			File configFile = null;
			configFile = new File(configURL.toURI());
			log.debug(" begin to create the command get file ");

			fis = new FileInputStream(configFile);
			prop = new Properties();
			prop.load(fis);
			fis.close();

			String commandPath = prop.getProperty("commandPath");
			String commandFile = prop.getProperty("commandFile");
			String getFileRemoteAddress = prop
					.getProperty("getFileRemoteAddress");
			String localClientDirectory = prop
					.getProperty("LocalClientDirectory");
			String content = null;

			if (fileClient.getName().equalsIgnoreCase("clientGetFilesBatch")) {
				content = "-get -r " + getFileRemoteAddress + " "
						+ localClientDirectory;
			} else {
				content = "-get " + getFileRemoteAddress + "/"
						+ fileClient.getName() + " " + localClientDirectory;
			}

			BufferedWriter osw = null;
			String commandFilen = commandPath + "/" + fileClient.getName()
					+ commandFile;
			// String commandFilen = commandPath + "/" + commandFile;
			log.info(content);
			log.info(commandFilen);

			osw = new BufferedWriter(new FileWriter(commandFilen, false));
			osw.write(content);
			osw.newLine();
			osw.flush();
			osw.close();
			log.debug(" End: Success create the command get file . ");
		} catch (Exception e) {
			log.error("Fail create the command get file .  ", e);
		}
	}

	/**
	 * every file have its own command file, so delete them when it's useless
	 * after the sending
	 * 
	 * @param fileName
	 * @param Log
	 */
	public static void deleteCommandFile(String fileName, Logger Log) {
		File command = new File(fileName);
		boolean deleteSuccess = command.delete();
		// return flag;
		if (deleteSuccess) {
			Log.info("Delete command file succeed!");
		} else {
			Log.info("Delete command file failed!");
		}
	}

	public static void deleteCommandFile(String fileName) {
		Logger log = CommonUtil.getLog();
		String commandPath = CommonUtil.getCommandPath();
		String commandFile = CommonUtil.getCommandFile();
		String commandFileAbsolutionPath = commandPath + "/" + fileName
				+ commandFile;
		File command = new File(commandFileAbsolutionPath);
		boolean deleteSuccess = command.delete();
		// return flag;
		if (deleteSuccess) {
			log.debug(" Delete command file succeed!");
		} else {
			log.debug(" Delete command file failed!");
		}
	}

	public static void main(String[] a) {
		CommandFile.createCommandGetFile(new File("AMTX"));
	}

}
