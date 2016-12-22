package com.firstdata.dwh.compress;

import java.io.BufferedReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.RollingFileAppender;

import com.firstdata.common.ReportClient;

/**
 * This class is use to compress the target file, then create some log files and
 * flag files
 * 
 * @author Cao Zhen
 * @modified by cxd
 * @version 1.0
 * 
 *          1 modified the way that read configuration file 2 modified the bug
 *          about during the maxwaiting time repeat sending file. promt file not
 *          found 3 changed the method renameto into FileUtils.copy.. method 4
 *          add some logs 5 add some annotation 6 change the structure of some
 *          code
 * **/

public class Compress {
	private static boolean HKT_15018;

	private static String file_Name;

	private static String nextDoFile;

	private static String notChangeDate;

	private static String nextDoFileTo;

	private static String compressPath;

	private static String receivePath;

	private static String sendPath;

	private static String logFilePath;

	private static String reportPath;

	private static String dataFilePath;

	private static String userID;

	private static String hostAddress;

	private static String hostPort;

	private static String userIDByFile;

	private static String hostAddressByFile;

	private static String hostPortByFile;

	private static String exeFile;

	private static String readyFlagFilePath;

	private static String fileSizePath;

	private static String commandFile;

	private static String commandPath;

	private static String commandFileAbsolutionPath;

	private static String WZZIPPath;

	private static String startTime;

	private static String endTime;

	// private static String reportTime;

	private static Logger log;

	private static String id_rsaPath;

	// private static Logger genericLogger;

	private static String eventID;

	private static File flagFile;

	private static int maxRepeatSend;

	private static String errDir;

	private static String separator = System.getProperty("line.separator");

	private static final String CONFIG_FILE = "firstdata/dwh/config.properties";

	private static final String LOG_PATH = "log4jproperties/log4j.properties";

	private static final String checkFileReport = "firstdata/dwh/checkFileReport.properties";

	private static long FileMaxWaitTime;

	private static long FlagFMaxWaitTime;

	private static long ThreadSleepInterval;

	// private static List checkFileList;

	private static HashMap<String, String> clientFileMap;

	private static boolean compressFlag;

	private static String clientName;

	private static String specialDealFolder;

	private static String specialDeal;

	private static boolean isSpecialDealPCA;

	private static boolean compressPasswordFlag;

	private static String WZZIPPassword;

	private static String beforeEncryptPath;

	private static String afterEncryptPath;

	private static String encryptCommand;

	private static String uncompressFileList;

	private static String bankCode;

	private static boolean encryptFlag;

	private static String PINMAILER_F1_NAME;

	private static String PINMAILER_F2_NAME;

	private static String PINMAILER_F_NAME;

	private static int CPF_RECORD_LENGTH;

	private static int CAF_RECORD_LENGTH;

	private static int PINMAILER_RECORD_LENGTH;

	// for GPG Encrypt
	private static boolean gpgEncryptFlag;
	private static String gpgPath;
	private static String gpgKeyRing;
	private static String gpgUserId;
	private static File renameFileName;

	// ///
	private static int CPF_CUSTOM_NBR_START;
	private static int CPF_CUSTOM_NBR_LENGTH;
	private static int CAF_CUSTOM_NBR_START;
	private static int CAF_CUSTOM_NBR_LENGTH;

	private static int CAF_CUSTOM_ADDRESS_START;
	// private static int PINMAILER_CUSTOM_ADDRESS_START;
	private static int CUSTOM_ADDRESS_LENGTH;

	private static int CAF_PCHN_START;
	private static int PINMAILER_PCHN_START;
	private static int PCHN_LENGTH;

	private static int ADDRESS_START;
	private static int CPF_AFTER_ADDRESS_START;
	private static int PINMAILER_AFTER_ADDRESS_START;
	private static int AFTER_ADDRESS_LENGTH;

	// PCL 16003 , append card logo and card face.
	private static int CPF_EMBOSS_SEQ_START;
	private static int CAF_EMBOSS_SEQ_START;
	private static int EMBOSS_SEQ_LENGTH;

	private static int CAF_CARD_LOGO_START;
	private static int PINMAILER_CARD_LOGO_START;
	private static int CARD_LOGO_LENGTH;
	private static int CAF_CARD_FACE_START;
	private static int PINMAILER_CARD_FACE_START;
	private static int CARD_FACE_LENGTH;

	// /// Regular Expression
	private static String REGEX_VALUE;
	// ///

	private static HashMap<String, String> mapCAF = new HashMap<String, String>();

	private static void writeLog(String[] args) throws Exception {
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter("TransferFile.log", true));

			String logStr = new Date() + ":[" + Arrays.toString(args) + "]";
			bw.write(logStr);
			bw.newLine();
		} catch (Exception e) {
			bw.write(e.getMessage());
			bw.newLine();
			e.printStackTrace();

		} finally {
			if (null != bw) {
				bw.close();
			}
		}
	}

	public static void main1(String[] args) throws Exception {

		// String dateValue = null;
		// // RH20160217903
		// byte[] dataByteValue = "RH20160217903".getBytes("ASCII");
		//
		// dateValue = new String(dataByteValue, 4, 6, "ASCII");
		//
		// System.out.println(dateValue);
		
		File a = new File("D:/", "ATB.TXT");
		System.out.println(a.getAbsolutePath());
		System.out.println(a.getPath());
		System.out.println(a.getName());
		System.out.println(a.getParent());

	}

	public static void main(String[] args) throws Exception {

		writeLog(args);

		file_Name = args[0];

		try {
			start();
		} catch (Exception e) {
			if (null == log) {
				System.out.println("File transfered error!");
				e.printStackTrace();

			} else {
				log.error("File transfered error!", e);
				Compress.writeWinEventErrLog(e.toString(), eventID);
			}
			System.exit(1);
		}
	}

	public static void getClientFileHashMap() throws Exception {
		log.debug(" get fileMap properties ");
		URL fileMapConfigURL = GetFilesFromServer.class.getClassLoader().getResource(checkFileReport);
		FileInputStream fis = null;
		Properties fileMapProperties = new Properties();
		try {

			File configFile = null;
			configFile = new File(fileMapConfigURL.toURI());
			fis = new FileInputStream(configFile);
			fileMapProperties.load(fis);

		} catch (Exception e) {

			log.error("getClientFileHashMap error", e);
			throw e;
		} finally {
			if (null != fis) {
				fis.close();
			}
		}
		Set<Entry<Object, Object>> set = fileMapProperties.entrySet();

		Iterator<Map.Entry<Object, Object>> it = set.iterator();
		String key = null, value = null;

		clientFileMap = new HashMap<String, String>();

		while (it.hasNext()) {
			Entry<Object, Object> entry = it.next();
			key = String.valueOf(entry.getKey());
			value = String.valueOf(entry.getValue());
			key = key == null ? key : key.trim();
			value = value == null ? value : value.trim();
			clientFileMap.put(key, value);
		}
		// System.out.println();
		// return map;
	}

	private static void init(String fileName) throws Exception {
		FileInputStream fis = null;
		Properties prop = new Properties();

		Date dateTime = new Date();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
		startTime = simpleDateFormat.format(dateTime);

		URL configURL = Compress.class.getClassLoader().getResource(CONFIG_FILE);

		try {
			File configFile = null;
			configFile = new File(configURL.toURI());
			fis = new FileInputStream(configFile);
			prop.load(fis);

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Parameter initialition failed! load config file error");
			String message = "Parameter initialition failed! load config file error";
			Compress.writeWinEventErrLog(message, eventID);
		} finally {
			if (null != fis) {
				fis.close();
			}
		}
		try {

			String s_HKT_15018 = prop.getProperty("HKT_15018");
			if ((null == s_HKT_15018)
					|| (!("true".equalsIgnoreCase(s_HKT_15018.trim())))) {
				HKT_15018 = false;
			} else {
				HKT_15018 = true;
			}

			if (HKT_15018) {
				userIDByFile = prop.getProperty(fileName + ".userID");
				hostAddressByFile = prop.getProperty(fileName + ".hostAddress");
				hostPortByFile = prop.getProperty(fileName + ".hostPort");
			}

			notChangeDate = prop.getProperty(fileName + ".notChangeDate");
			if (null == notChangeDate) {
				notChangeDate = "FASLE";
			} else {
				notChangeDate = notChangeDate.trim().toUpperCase();
			}

			logFilePath = GetCheckLog(prop, "logFilePath");
			log = Compress.createNewLogger(fileName);
			log.debug("Log4j initialition was finished! [" + logFilePath + "]");
			receivePath = GetCheckLog(prop, "receivePath");
			compressPath = GetCheckLog(prop, "compressPath");
			uncompressFileList = prop.getProperty("uncompressFileList");
			bankCode = prop.getProperty("bankCode");

			sendPath = GetCheckLog(prop, "sendPath");

			reportPath = GetCheckLog(prop, "reportPath");
			dataFilePath = GetCheckLog(prop, "dataFilePath");
			readyFlagFilePath = GetCheckLog(prop, "readyFlagFilePath");
			fileSizePath = GetCheckLog(prop, "fileSizePath");
			eventID = GetCheckLog(prop, "eventID");
			maxRepeatSend = Integer
					.parseInt(GetCheckLog(prop, "maxRepeatSend"));
			userID = GetCheckLog(prop, "userID");
			hostAddress = GetCheckLog(prop, "hostAddress");
			hostPort = GetCheckLog(prop, "hostPort");

			exeFile = GetCheckLog(prop, "exeFile");
			commandPath = GetCheckLog(prop, "commandPath");
			commandFile = GetCheckLog(prop, "commandFile");
			errDir = GetCheckLog(prop, "errDir");
			id_rsaPath = GetCheckLog(prop, "id_rsaPath");
			FileMaxWaitTime = Long.parseLong(GetCheckLog(prop,
					"FileMaxWaitTime"));
			FlagFMaxWaitTime = Long.parseLong(GetCheckLog(prop,
					"FlagFMaxWaitTime"));
			ThreadSleepInterval = Long.parseLong(GetCheckLog(prop,
					"ThreadSleepInterval"));
			clientName = GetCheckLog(prop, "clientName");

			if ("PCL".equalsIgnoreCase(clientName)) {
				System.out.println("PCL");
				specialDeal = GetCheckLog(prop, "SpecialDeal");
				if (null != specialDeal) {
					String[] specialList = specialDeal.split(",");
					for (int i = 0; i < specialList.length; i++) {
						if ("PINMAILER_CUSTOM_ADDRESS".equalsIgnoreCase(specialList[i])) {
							String specialDealPCAString = prop.getProperty("PINMAILER_CUSTOM_ADDRESS");
							specialDealPCAString = (null == specialDealPCAString || (!specialDealPCAString
									.trim().equalsIgnoreCase("true"))) ? "false"
									: "true";
							isSpecialDealPCA = Boolean.parseBoolean(specialDealPCAString);

							if (isSpecialDealPCA) {
								System.out.println("isSpecialDealPCA true");
								specialDealFolder = GetCheckLog(prop,
										"SpecialDealFolder");
								PINMAILER_F1_NAME = GetCheckLog(prop,
										"PINMAILER_F1_NAME");
								PINMAILER_F2_NAME = GetCheckLog(prop,
										"PINMAILER_F2_NAME");
								PINMAILER_F_NAME = GetCheckLog(prop,
										"PINMAILER_F_NAME");

								CPF_RECORD_LENGTH = Integer
										.parseInt(GetCheckLog(prop,
												"CPF_RECORD_LENGTH"));
								CAF_RECORD_LENGTH = Integer
										.parseInt(GetCheckLog(prop,
												"CAF_RECORD_LENGTH"));
								PINMAILER_RECORD_LENGTH = Integer
										.parseInt(GetCheckLog(prop,
												"PINMAILER_RECORD_LENGTH"));

								CPF_CUSTOM_NBR_START = Integer
										.parseInt(GetCheckLog(prop,
												"CPF_CUSTOM_NBR_START"));
								CPF_CUSTOM_NBR_LENGTH = Integer
										.parseInt(GetCheckLog(prop,
												"CPF_CUSTOM_NBR_LENGTH"));
								CAF_CUSTOM_NBR_START = Integer
										.parseInt(GetCheckLog(prop,
												"CAF_CUSTOM_NBR_START"));
								CAF_CUSTOM_NBR_LENGTH = Integer
										.parseInt(GetCheckLog(prop,
												"CAF_CUSTOM_NBR_LENGTH"));

								CAF_CUSTOM_ADDRESS_START = Integer
										.parseInt(GetCheckLog(prop,
												"CAF_CUSTOM_ADDRESS_START"));
								// PINMAILER_CUSTOM_ADDRESS_START = Integer
								// .parseInt(GetCheckLog(prop,
								// "PINMAILER_CUSTOM_ADDRESS_START"));
								CUSTOM_ADDRESS_LENGTH = Integer
										.parseInt(GetCheckLog(prop,
												"CUSTOM_ADDRESS_LENGTH"));

								CAF_PCHN_START = Integer.parseInt(GetCheckLog(
										prop, "CAF_PCHN_START"));
								PINMAILER_PCHN_START = Integer
										.parseInt(GetCheckLog(prop,
												"PINMAILER_PCHN_START"));
								PCHN_LENGTH = Integer.parseInt(GetCheckLog(
										prop, "PCHN_LENGTH"));

								ADDRESS_START = Integer.parseInt(GetCheckLog(
										prop, "ADDRESS_START"));

								CPF_AFTER_ADDRESS_START = Integer
										.parseInt(GetCheckLog(prop,
												"CPF_AFTER_ADDRESS_START"));
								PINMAILER_AFTER_ADDRESS_START = Integer
										.parseInt(GetCheckLog(prop,
												"PINMAILER_AFTER_ADDRESS_START"));
								AFTER_ADDRESS_LENGTH = Integer
										.parseInt(GetCheckLog(prop,
												"AFTER_ADDRESS_LENGTH"));

								// PCL 16003 , append card logo and card face.
								CPF_EMBOSS_SEQ_START = Integer
										.parseInt(GetCheckLog(prop,
												"CPF_EMBOSS_SEQ_START"));
								CAF_EMBOSS_SEQ_START = Integer
										.parseInt(GetCheckLog(prop,
												"CAF_EMBOSS_SEQ_START"));
								EMBOSS_SEQ_LENGTH = Integer
										.parseInt(GetCheckLog(prop,
												"EMBOSS_SEQ_LENGTH"));

								CAF_CARD_LOGO_START = Integer
										.parseInt(GetCheckLog(prop,
												"CAF_CARD_LOGO_START"));
								PINMAILER_CARD_LOGO_START = Integer
										.parseInt(GetCheckLog(prop,
												"PINMAILER_CARD_LOGO_START"));
								CARD_LOGO_LENGTH = Integer
										.parseInt(GetCheckLog(prop,
												"CARD_LOGO_LENGTH"));
								CAF_CARD_FACE_START = Integer
										.parseInt(GetCheckLog(prop,
												"CAF_CARD_FACE_START"));
								PINMAILER_CARD_FACE_START = Integer
										.parseInt(GetCheckLog(prop,
												"PINMAILER_CARD_FACE_START"));
								CARD_FACE_LENGTH = Integer
										.parseInt(GetCheckLog(prop,
												"CARD_FACE_LENGTH"));

							}

						}
					}
				}
			}
			// if compress start
			String compressFlagString = prop.getProperty("compressFlag");
			compressFlagString = (null == compressFlagString || (!compressFlagString
					.trim().equalsIgnoreCase("true"))) ? "false" : "true";
			compressFlag = Boolean.parseBoolean(compressFlagString);
			log.debug("compressFlag: " + compressFlag);
			if (compressFlag) {

				WZZIPPath = GetCheckLog(prop, "WZZIPPath");

				// if compress with password start
				String compressPasswordFlagString = prop
						.getProperty("compressPasswordFlag");
				compressPasswordFlagString = (null == compressPasswordFlagString || (!compressPasswordFlagString
						.trim().equalsIgnoreCase("true"))) ? "false" : "true";
				compressPasswordFlag = Boolean
						.parseBoolean(compressPasswordFlagString);
				log.debug("compressPasswordFlag: " + compressPasswordFlag);
				if (compressPasswordFlag) {
					WZZIPPassword = GetCheckLog(prop, "WZZIPPassword");
				}
				// if compress with password end

			}

			// if compress end
			// if encrypt start

			String gpgEncryptFlagString = prop.getProperty(fileName
					+ ".gpgEncryptFlag");
			gpgEncryptFlagString = (null == gpgEncryptFlagString || (!gpgEncryptFlagString
					.trim().equalsIgnoreCase("true"))) ? "false" : "true";
			gpgEncryptFlag = Boolean.parseBoolean(gpgEncryptFlagString);

			String encryptFlagString = prop.getProperty(fileName
					+ ".encryptFlag");
			encryptFlagString = (null == encryptFlagString || (!encryptFlagString
					.trim().equalsIgnoreCase("true"))) ? "false" : "true";

			encryptFlag = Boolean.parseBoolean(encryptFlagString);
			log.debug("encryptFlag: " + encryptFlag);
			if (encryptFlag || gpgEncryptFlag) {
				String beforeEncryptPathString = GetCheckLog(prop, fileName
						+ ".encryptPath");

				beforeEncryptPath = beforeEncryptPathString;
				log.debug("beforeEncryptPath : " + beforeEncryptPath);

				String afterEncryptPathString = GetCheckLog(prop, fileName
						+ ".afterEncryptPath");

				afterEncryptPath = afterEncryptPathString;
				log.debug("afterEncryptPath : " + afterEncryptPath);

				if (gpgEncryptFlag) {
					// compressFlag = false;
					encryptFlag = false;

					gpgPath = GetCheckLog(prop, "gpgPath");
					log.debug("gpgPath: " + gpgPath);

					gpgKeyRing = GetCheckLog(prop, fileName + ".gpgKeyRing");
					log.debug("gpgKeyRing: " + gpgKeyRing);

					gpgUserId = GetCheckLog(prop, fileName + ".gpgUserId");
					log.debug("gpgUserId: " + gpgUserId);
				}

				String encryptCommandString = GetCheckLog(prop, fileName
						+ ".encryptCommand");

				encryptCommand = encryptCommandString;
				log.debug("encryptCommand: " + encryptCommand);
			}

			// if encrypt end

			REGEX_VALUE = prop
					.getProperty("REGEX_VALUE_FOR_DONOT_RENAME_OR_COMPRESS");
			if (null != REGEX_VALUE && "" != REGEX_VALUE.trim()) {
				REGEX_VALUE = REGEX_VALUE.trim();
			} else {
				REGEX_VALUE = "";
			}

			getClientFileHashMap();
		} catch (Exception e) {
			if (null == log) {
				System.out.println("error in init:[" + e + "]");
			} else {
				log.error(e);
			}
			String message = "Parameter initialition failed!";
			Compress.writeWinEventErrLog(message, eventID);
			throw e;
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
	 * send files and flagfiles.Repeat to send the files according to the the
	 * param maxRepeatSend if the file can't be sent successfully once
	 * 
	 * 
	 * @param sendFile
	 * @param fileName
	 * @param maxWaitTime
	 */
	private boolean ReadyToSendFiles(String originalFileName, File sendFile,
			File fileName, long maxWaitTime) {
		boolean resultFileSend = this.statusCheck(originalFileName, sendFile,
				maxWaitTime);
		if (!resultFileSend) {
			int repeatSend = 0;
			File failFile = new File(sendPath + "\\Failure\\"
					+ fileName.getName());
			while (repeatSend < maxRepeatSend) { // repeat to send file
				log.debug("Begin to copy file " + failFile
						+ " repeatSend times " + repeatSend);
				try {
					moveFile(failFile, sendFile);
				} catch (Exception e) {
					log.error("Moving file failed.", e);
				}
				log.debug(" End: success copy file ");
				resultFileSend = this.statusCheck(originalFileName, sendFile,
						maxWaitTime);
				if (!resultFileSend) {
					repeatSend++;
				} else {
					return true;
				}
			}
			if (repeatSend == maxRepeatSend) {
				String message = " File transferring to client error! beyond the maxRepeatSend times. File name: "
						+ fileName;
				log.debug(message);
				Compress.writeWinEventErrLog(message, eventID);
			}
			return false;
		} else {
			return true;
		}

	}

	public static int encryptPart(String command, String fileName) {
		if ((null == command || command.trim().equals(""))
				|| (null == fileName || fileName.trim().equals(""))) {
			log.error("input parameter ");
			return -1;
		}

		String cmd = command.trim() + " " + fileName.trim() + " " + "0";
		log.debug("[" + cmd + "]");
		Process process;
		try {
			process = Runtime.getRuntime().exec(cmd);
			clearProcessStream(process.getErrorStream());
			clearProcessStream(process.getInputStream());
			int returnCode = process.waitFor();
			if (returnCode == 0) {
				log.debug("encryptPart successful.");
				return 0;
			} else {
				log.debug("encryptPart failed. Return code: " + returnCode);
				return 1;
			}
		} catch (IOException e) {
			log.error("The process used to encryptPart is failed.", e);
		} catch (InterruptedException e) {
			log.error("The process used to encryptPart is failed.", e);
		} catch (Exception e) {
			log.error("The process used to encryptPart is failed X.", e);
		}
		return 2;
	}

	private static int moveAndEncrypt(File sourceFile) throws Exception {
		int isEncryptSuccess = -1;

		File beforeEncryptFile = new File(beforeEncryptPath).getCanonicalFile();
		File afterEncryptFile = new File(afterEncryptPath).getCanonicalFile();

		moveFile(sourceFile, beforeEncryptFile);

		isEncryptSuccess = encryptPart(encryptCommand,
				beforeEncryptFile.getName());
		if (isEncryptSuccess == 0) {
			log.info("Encrypt file " + file_Name + " finished.");
			copyFile(afterEncryptFile, sourceFile);
		} else {
			log.error("Encrypt file " + file_Name + " failed.");
			return -1;
		}

		return 0;
	}

	private static String fixLength(int lostLength) {
		String fixString = "";
		for (int i = lostLength; i > 0; i--) {
			fixString += " ";
		}

		return fixString;
	}

	private static void doSpecialDealPCA() throws Exception {

		byte[] itemICAF = new byte[CAF_RECORD_LENGTH];
		byte[] itemICPF = new byte[CPF_RECORD_LENGTH];

		byte[] header = new byte[PINMAILER_RECORD_LENGTH];
		byte[] trailer = new byte[PINMAILER_RECORD_LENGTH];

		ArrayList<byte[]> outList = new ArrayList<byte[]>();

		InputStream fiCAF = null;
		InputStream fiCPF = null;
		OutputStream foPINMAILER = null;
		try {

			fiCAF = new FileInputStream(new File(specialDealFolder,
					PINMAILER_F1_NAME));
			fiCPF = new FileInputStream(new File(specialDealFolder,
					PINMAILER_F2_NAME));
			foPINMAILER = new FileOutputStream(new File(specialDealFolder,
					PINMAILER_F_NAME));
			mapCAF.clear();
			// get CAF.
			while (fiCAF.read(itemICAF, 0, CAF_RECORD_LENGTH) != -1) {
				String customNbrCAF = new String(itemICAF,
						CAF_CUSTOM_NBR_START - 1, CAF_CUSTOM_NBR_LENGTH,
						"ASCII");
				// 从CAF中获取Customer_NBR

				String customAddressCAF = new String(itemICAF,
						CAF_CUSTOM_ADDRESS_START - 1, CUSTOM_ADDRESS_LENGTH
								+ PCHN_LENGTH, "ASCII");
				// 从CAF中获取200位字节包括客户地址和客户姓名

				String embossSeqCAF = new String(itemICAF,
						CAF_EMBOSS_SEQ_START - 1, EMBOSS_SEQ_LENGTH, "ASCII");
				// 从CAF中获取EMBOSS_SEQ

				String cardLogoCAF = new String(itemICAF,
						CAF_CARD_LOGO_START - 1, CARD_LOGO_LENGTH, "ASCII");
				// 从CAF中获取Card_Logo

				String cardFaceCAF = new String(itemICAF,
						CAF_CARD_FACE_START - 1, CARD_FACE_LENGTH, "ASCII");
				// 从CAF中获取Card_Face

				if (null == mapCAF.get(customNbrCAF + "|" + embossSeqCAF)) {
					log.info("NICE Custom Number and Emboss Sequence.["
							+ customNbrCAF + "|" + embossSeqCAF + "]["
							+ customAddressCAF + cardLogoCAF + cardFaceCAF
							+ "]");
					mapCAF.put(customNbrCAF + "|" + embossSeqCAF,
							customAddressCAF + cardLogoCAF + cardFaceCAF);
				} else {
					log.info("Duplicate Custom Number and Emboss Sequence.["
							+ customNbrCAF + "|" + embossSeqCAF + "]["
							+ customAddressCAF + cardLogoCAF + cardFaceCAF
							+ "]");
					// 将CustomerNbr和EmbossSeq作为域名方便匹配
					// 将Address,Name,Card_logo,Card_face作为值方便提取
					mapCAF.put(customNbrCAF + "|" + embossSeqCAF,
							customAddressCAF + cardLogoCAF + cardFaceCAF);
				}
			}

			while (fiCPF.read(itemICPF, 0, CPF_RECORD_LENGTH) != -1) {
				// read header.
				if (new String(itemICPF, "ASCII").startsWith("H")) {
					log.info("NICE header0.[" + new String(itemICPF, "ASCII")
							+ "]");
					System.arraycopy(itemICPF, 0, header, 0,
							ADDRESS_START - 1 + 112);

					log.info("NICE header1.[" + new String(header, "ASCII")
							+ "]");

					System.arraycopy(fixLength(48).getBytes("ASCII"), 0,
							header, ADDRESS_START + 112, 48);

					log.info("NICE header2.[" + new String(header, "ASCII")
							+ "]");

					System.arraycopy(itemICPF, CPF_AFTER_ADDRESS_START - 1,
							header, PINMAILER_AFTER_ADDRESS_START - 1,
							AFTER_ADDRESS_LENGTH);

					log.info("NICE header3.[" + new String(header, "ASCII")
							+ "]");

					System.arraycopy(fixLength(102).getBytes("ASCII"), 0,
							header, PINMAILER_AFTER_ADDRESS_START
									+ AFTER_ADDRESS_LENGTH - 1, 102);

					log.info("NICE header.[" + new String(header, "ASCII")
							+ "]");

				}
				// read trailer.
				if (new String(itemICPF, "ASCII").startsWith("T")) {
					log.info("NICE trailer0.[" + new String(itemICPF, "ASCII")
							+ "]");
					System.arraycopy(itemICPF, 0, trailer, 0,
							ADDRESS_START - 1 + 112);

					log.info("NICE trailer1.[" + new String(trailer, "ASCII")
							+ "]");

					System.arraycopy(fixLength(48).getBytes("ASCII"), 0,
							trailer, ADDRESS_START + 112, 48);

					log.info("NICE trailer2.[" + new String(trailer, "ASCII")
							+ "]");

					System.arraycopy(itemICPF, CPF_AFTER_ADDRESS_START - 1,
							trailer, PINMAILER_AFTER_ADDRESS_START - 1,
							AFTER_ADDRESS_LENGTH);

					log.info("NICE header3.[" + new String(header, "ASCII")
							+ "]");

					System.arraycopy(fixLength(102).getBytes("ASCII"), 0,
							trailer, PINMAILER_AFTER_ADDRESS_START
									+ AFTER_ADDRESS_LENGTH - 1, 102);

					log.info("NICE trailer.[" + new String(trailer, "ASCII")
							+ "]");

				}
				// read normal data.
				byte[] itemOPINMAILER = new byte[PINMAILER_RECORD_LENGTH];
				if (new String(itemICPF, "ASCII").startsWith("D")) {

					String customNbrCPF = new String(itemICPF,
							CPF_CUSTOM_NBR_START - 1, CPF_CUSTOM_NBR_LENGTH);
					// 从CPF中获取CustomerNbr

					String embossSeqCPF = new String(itemICPF,
							CPF_EMBOSS_SEQ_START - 1, EMBOSS_SEQ_LENGTH);
					// 从CPF中获取Emboss_SEQ

					String matchKey = customNbrCPF + "|" + embossSeqCPF;
					// 组成和CAF匹配的值
					log.info("NICE data custom nbr and emboss seq.[" + matchKey
							+ "]");

					if (null != mapCAF.get(matchKey)) {
						System.arraycopy(itemICPF, 0, itemOPINMAILER, 0,
								ADDRESS_START - 1);
						// 将Address前面的47位放入新文件中

						log.info("NICE itemOPINMAILER1 record.["
								+ new String(itemOPINMAILER, "ASCII") + "]");

						System.arraycopy(
								mapCAF.get(matchKey)
										.substring(0, CUSTOM_ADDRESS_LENGTH)
										.getBytes("ASCII"), 0, itemOPINMAILER,
								ADDRESS_START - 1, CUSTOM_ADDRESS_LENGTH);
						// 将Address的160位加在47位后面

						log.info("NICE itemOPINMAILER2 record.["
								+ new String(itemOPINMAILER, "ASCII") + "]");

						System.arraycopy(itemICPF, CPF_AFTER_ADDRESS_START - 1,
								itemOPINMAILER,
								PINMAILER_AFTER_ADDRESS_START - 1,
								AFTER_ADDRESS_LENGTH);
						// 在Address后面加上41位

						log.info("NICE itemOPINMAILER3 record.["
								+ new String(itemOPINMAILER, "ASCII") + "]");

						System.arraycopy(
								mapCAF.get(matchKey)
										.substring(160, 160 + PCHN_LENGTH)
										.getBytes("ASCII"), 0, itemOPINMAILER,
								PINMAILER_PCHN_START - 1, PCHN_LENGTH);
						// 将客户姓名加在之前信息之后

						log.info("sepcial itemOPINMAILER4.["
								+ new String(itemOPINMAILER, "ASCII") + "]");

						System.arraycopy(
								mapCAF.get(matchKey)
										.substring(
												CUSTOM_ADDRESS_LENGTH
														+ PCHN_LENGTH,
												CUSTOM_ADDRESS_LENGTH
														+ PCHN_LENGTH
														+ CARD_LOGO_LENGTH)
										.getBytes("ASCII"), 0, itemOPINMAILER,
								PINMAILER_CARD_LOGO_START - 1, CARD_LOGO_LENGTH);
						// 将Card_logo加在客户姓名之后

						log.info("sepcial itemOPINMAILER5.["
								+ new String(itemOPINMAILER, "ASCII") + "]");

						System.arraycopy(
								mapCAF.get(matchKey)
										.substring(
												CUSTOM_ADDRESS_LENGTH
														+ PCHN_LENGTH
														+ CARD_LOGO_LENGTH,
												CUSTOM_ADDRESS_LENGTH
														+ PCHN_LENGTH
														+ CARD_LOGO_LENGTH
														+ CARD_FACE_LENGTH)
										.getBytes("ASCII"), 0, itemOPINMAILER,
								PINMAILER_CARD_FACE_START - 1, CARD_FACE_LENGTH);
						// 将Card_Face加在Card_logo之后

						log.info("sepcial itemOPINMAILER6.["
								+ new String(itemOPINMAILER, "ASCII") + "]");

						System.arraycopy(fixLength(55).getBytes("ASCII"), 0,
								itemOPINMAILER, PINMAILER_CARD_FACE_START
										+ CARD_FACE_LENGTH - 1, 55);
						// 补55个空格

						log.info("NICE itemOPINMAILER record.["
								+ new String(itemOPINMAILER, "ASCII") + "]");

					} else {
						log.info("sepcial record0.["
								+ new String(itemICPF, "ASCII") + "]");
						System.arraycopy(itemICPF, 0, itemOPINMAILER, 0,
								ADDRESS_START - 1 + 112);

						log.info("sepcial record1.["
								+ new String(itemOPINMAILER, "ASCII") + "]");

						System.arraycopy(fixLength(48).getBytes("ASCII"), 0,
								itemOPINMAILER, ADDRESS_START + 112, 48);

						log.info("sepcial record2.["
								+ new String(itemOPINMAILER, "ASCII") + "]");

						System.arraycopy(itemICPF, CPF_AFTER_ADDRESS_START - 1,
								itemOPINMAILER,
								PINMAILER_AFTER_ADDRESS_START - 1,
								AFTER_ADDRESS_LENGTH);

						log.info("sepcial record3.["
								+ new String(itemOPINMAILER, "ASCII") + "]");

						System.arraycopy(fixLength(102).getBytes("ASCII"), 0,
								itemOPINMAILER, PINMAILER_AFTER_ADDRESS_START
										+ AFTER_ADDRESS_LENGTH - 1, 102);

						log.info("sepcial record.["
								+ new String(itemOPINMAILER, "ASCII") + "]");

						log.info("Not found Custom Number and Emboss Seq.["
								+ customNbrCPF + "|" + embossSeqCPF + "]");
					}
					outList.add(itemOPINMAILER);
				}

			}

			foPINMAILER.write(header);
			// write data.
			for (int j = 0; j < outList.size(); j++) {
				log.info("write itemOPINMAILER record.["
						+ new String(outList.get(j), "ASCII") + "]");
				foPINMAILER.write(outList.get(j));

			}
			// write trailer.
			foPINMAILER.write(trailer);
			foPINMAILER.close();
			fiCAF.close();
			fiCPF.close();

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (null != foPINMAILER) {
				foPINMAILER.close();
			}
			if (null != fiCAF) {
				fiCAF.close();
			}
			if (null != fiCPF) {
				fiCPF.close();
			}
		}

	}

	private static boolean isNextFileCome(String fileName) {
		// FOR HKT15018
		if ("HKT".equalsIgnoreCase(clientName)) {
			if (null != nextDoFile && new File(nextDoFile).exists()) {
				return true;
			} else {
				return false;
			}

		} else {
			return false;
		}

	}

	private static String getDatePatternSuffix(String pattern) {
		String suffix = "";
		int i = pattern.lastIndexOf('+');
		if (i < 0) {
			i = pattern.lastIndexOf('-');
		}
		if (i >= 0) {
			suffix = pattern.substring(i);
		}
		return suffix;
	}

	private static Date addDays(Date date, int days) {
		Calendar cal = new GregorianCalendar();
		cal.setTime(date);
		cal.add(Calendar.DAY_OF_MONTH, days);
		return cal.getTime();
	}

	private static Date getLastWorkdayBefore(Date d) {
		Date w = addDays(d, -1);
		// while (isHoliday(w)) {
		// w = addDays(w, -1);
		// }
		DateFormat f = new SimpleDateFormat("yyyyMMdd");
		log.debug(f.format(w) + " is the last workday before " + f.format(d)
				+ ".");
		return w;
	}

	private static Date getNextWorkdayAfter(Date d) {
		Date w = addDays(d, 1);
		// while (isHoliday(w)) {
		// w = addDays(w, 1);
		// }
		DateFormat f = new SimpleDateFormat("yyyyMMdd");
		log.debug(f.format(w) + " is the next workday after " + f.format(d)
				+ ".");
		return w;
	}

	private static String convertDatePattern(String pattern, Date date) {
		if (null != log) {
			log.debug("Converting date pattern: \"" + pattern + "\".");
		} else {
			System.out.println("Converting date pattern: \"" + pattern + "\".");
		}
		// Allows data pattern have suffix "+*" or "-*".
		String suffix = getDatePatternSuffix(pattern);
		if (!suffix.isEmpty()) {
			pattern = pattern.substring(0, pattern.length() - suffix.length());
			char sign = suffix.charAt(0);
			suffix = suffix.substring(1);
			if ("n".equals(suffix)) {
				if ('+' == sign) {
					date = getNextWorkdayAfter(date);
				} else { // sign is '-'
					date = getLastWorkdayBefore(date);
				}
			} else { // suffix is a number
				int dayDiff = 0;
				try {
					dayDiff = Integer.parseInt(suffix);
				} catch (NumberFormatException e) {
					// In case the number format is wrong, do nothing
				}
				if ('-' == sign) {
					dayDiff = -dayDiff;
				}
				if (null != log) {
					log.debug("Day diff: " + dayDiff + ".");
				} else {
					System.out.println("Day diff: " + dayDiff + ".");
				}
				date = addDays(date, dayDiff);
			}
		}
		if (null != log) {
			log.debug("Date: " + date + ".");
		} else {
			System.out.println("Date: " + date + ".");
		}
		DateFormat format = new SimpleDateFormat(pattern);
		String dateString = format.format(date);
		if (null != log) {
			log.debug("Converted string: \"" + dateString + "\".");
		} else {
			System.out.println("Converted string: \"" + dateString + "\".");
		}
		return dateString;
	}

	/**
	 * Converts the file path which contains date pattern to an actual path. The
	 * File path can contain ${P}, where P is a SimpleDateFormat pattern, e.g.,
	 * {@code yyyyMMdd}. ${P} will be converted to a String based on the current
	 * date.
	 * <p />
	 * There can be suffix after P. ${P+x} (x = 1, 2, 3...) means the x-th day
	 * after. ${P-x} (x = 1, 2, 3...) means the x-th day before. ${P+n} or
	 * ${P-n} means the next or the last work day.
	 * <p />
	 * Test case: path = "${yyyyMMdd}" "${yyMMdd}" "${yyMMdd" "$yyMMdd}"
	 * "${yyMMdd+0}" "${yyMMdd-0}" "${yyMMdd+1}" "${yyMMdd+3}" "${yyMMdd-1}"
	 * "${yyMMdd-3}" "${yyMMdd+zzz}" "${yyMMdd+n}" "${yyMMdd-n}"
	 * "${yyMMdd}/foo/bar" "foo/${yyMMdd}/bar" "foo/bar/${yyMMdd}"
	 * "foo/${yyMMdd}/bar/${yyMMdd+1}" "foo/bar"
	 */
	private static String convertPath(String path, Date date) {
		if (null != log) {
			log.debug("Converting path \"" + path + "\".");
		} else {
			System.out.println("Converting path \"" + path + "\".");
		}
		try {
			while (true) {
				int l = path.indexOf("${");
				if (-1 == l) {
					break;
				}
				int r = path.indexOf("}", l);
				if (-1 == r) {
					break;
				}
				String datePattern = path.substring(l + 2, r);
				if (null != log) {
					log.debug("Date pattern: \"" + datePattern + "\".");
				} else {
					System.out
							.println("Date pattern: \"" + datePattern + "\".");
				}
				String dateString = convertDatePattern(datePattern, date);
				path = path.substring(0, l) + dateString
						+ path.substring(r + 1, path.length());
				if (null != log) {
					log.debug("Converted path :\"" + path + "\".");
				} else {
					System.out.println("Converted path :\"" + path + "\".");
				}
			}
		} catch (Exception e) {
			if (null != log) {
				log.debug("Convertion failed. Returning the original path.", e);
			} else {
				e.printStackTrace();
			}
		}
		return path;
	}

	private static void start() throws Exception {
		init(file_Name);
		fileSizeWrite(file_Name);
		Compress comp = new Compress();
		File originalFile = new File(receivePath, file_Name);

		// F2[CPF] of PCL PINMAILER
		if (isSpecialDealPCA && PINMAILER_F2_NAME.equalsIgnoreCase(file_Name)) {
			File originalCAF = new File(receivePath, PINMAILER_F1_NAME);
			File fileCAF = new File(specialDealFolder, PINMAILER_F1_NAME);
			File originalCPF = new File(receivePath, PINMAILER_F2_NAME);
			File fileCPF = new File(specialDealFolder, PINMAILER_F2_NAME);

			File filePINMAILER = new File(specialDealFolder, PINMAILER_F_NAME);

			copyFile(originalCAF, fileCAF);
			copyFile(originalCPF, fileCPF);

			if (!fileCAF.exists() || !fileCPF.exists()) {
				log.error("CAF or CPF not found!");
				throw new Exception("CAF or CPF not found!");

			}

			doSpecialDealPCA();

			if (!filePINMAILER.exists()) {
				log.error("new PINMAILER not created!");
				throw new Exception("new PINMAILER not created!");

			}

			originalFile = filePINMAILER;
		}

		// encrypt

		if (gpgEncryptFlag) {
			// For hkt15018
			renameOriginalFile(originalFile);
			originalFile = renameFileName;
			removeFirstLine(originalFile);
			moveAndGPGEncrypt(originalFile);
			// For hkt15018
			originalFile = renameFileName;
		}
		if (encryptFlag) {

			moveAndEncrypt(originalFile);
		}

		File compressedFile = null;

		if (!isRenameAndNoCompress(originalFile.getName())) {
			// Try to compress 10 times.
			compressedFile = comp.compress(originalFile);
			for (int i = 0; i < 9; i++) {
				if (null == compressedFile) {
					log.info("Compressing again.");

					// Wait 1 minute. I don't know why, but it makes me feel
					// safer.
					Thread.sleep(60 * 1000);

					compressedFile = comp.compress(originalFile);
				} else {
					break;
				}
			}
			if (null == compressedFile) {
				log.info("Can't compress file " + originalFile + ".");
				writeWinEventErrLog(
						"Can't compress file " + originalFile + ".", eventID);
				return;
			}
		} else {
			String dateValue = null;
			// RH20160217903
			byte[] dataByteValue = new byte[13];
			FileInputStream octopusFIS = null;

			try {
				octopusFIS = new FileInputStream(originalFile);

				if (13 == octopusFIS.read(dataByteValue, 0, 13)) {
					dateValue = new String(dataByteValue, 4, 6, "ASCII");

				} else {
					dateValue = nowDate.getDateYYMMDD();
				}

			} catch (Exception e) {
				log.info("Can't get date in file header " + originalFile + ".");
				dateValue = nowDate.getDateYYMMDD();
			} finally {
				log.info("Get date in file header " + originalFile + "["
						+ dateValue + "].");
				if (null != octopusFIS) {
					octopusFIS.close();
				}
			}

			compressedFile = new File(receivePath, file_Name + dateValue + "."
					+ bankCode);
			copyFile(originalFile, compressedFile);

		}

		comp.createFlagFile(readyFlagFilePath, compressedFile, originalFile);

		moveFile(compressedFile,
				new File(sendPath + "\\" + compressedFile.getName()));

		File sendFile = new File(sendPath + "\\" + compressedFile.getName());
		// send file
		boolean successflag = comp.ReadyToSendFiles(originalFile.getName(),
				sendFile, compressedFile, FileMaxWaitTime);
		CommandFile.deleteCommandFile(commandFileAbsolutionPath, log);

		//
		Boolean isSuccessFileExists = new File(sendPath + "\\" + "Success"
				+ "\\" + compressedFile.getName()).exists();

		if (successflag == true && isSuccessFileExists == true) {
			// move flag file
			File oriFlagFile = new File(readyFlagFilePath + "\\"
					+ flagFile.getName());
			File sendFlagFile = new File(sendPath + "\\" + flagFile.getName());
			moveFile(oriFlagFile, sendFlagFile);

			// send flag file
			successflag = comp.ReadyToSendFiles(originalFile.getName(),
					sendFlagFile, flagFile, FlagFMaxWaitTime);

			if (successflag == true) {
				Compress.writeReport(file_Name);

				File bkFile = new File(dataFilePath, file_Name);
				if (bkFile.exists()) {
					log.info("BackUp file " + bkFile + " exists. Deleting.");
					FileUtils.forceDelete(bkFile);
					log.info("Deleted.");
				}
			}

			CommandFile.deleteCommandFile(commandFileAbsolutionPath, log);

		}
	}

	private static void removeFirstLine(File originalFileName) throws Exception {
		BufferedReader originalBR = null;
		BufferedWriter originalBW = null;
		String str = "";
		try {

			originalBR = new BufferedReader(new FileReader(originalFileName));
			originalBW = new BufferedWriter(new FileWriter(new File(
					receivePath, originalFileName.getName() + ".temp")));
			str = originalBR.readLine();
			log.info("remove first line start: " + originalFileName + "[" + str
					+ "].");
			while (null != (str = originalBR.readLine())) {
				originalBW.write(str);
				originalBW.newLine();
				originalBW.flush();
				if (str.startsWith("T")) {
					break;
				}
			}
			if (null != originalBR) {
				originalBR.close();
			}
			if (null != originalBW) {
				originalBW.close();
			}
			FileUtils.forceDelete(originalFileName);
			copyFile(
					new File(receivePath, originalFileName.getName() + ".temp"),
					originalFileName);

		} catch (Exception e) {
			log.error("remove first line error: " + originalFileName + ".", e);
		} finally {
			log.info("remove first line end: " + originalFileName + ".");
			if (null != originalBR) {
				originalBR.close();
			}
			if (null != originalBW) {
				originalBW.close();
			}
		}
	}

	private static void renameOriginalFile(File originalFileName)
			throws Exception {
		// For hkt15018
		String indexValue = null;
		// 01
		byte[] dataByteValue = new byte[3];
		FileInputStream originalFIS = null;

		try {
			originalFIS = new FileInputStream(originalFileName);

			if (2 == originalFIS.read(dataByteValue, 0, 2)) {
				indexValue = new String(dataByteValue, 0, 2, "ASCII");

			} else {
				indexValue = "99";
			}

		} catch (Exception e) {
			log.info("Can't get index in file header " + originalFileName + ".");
			indexValue = "999";
		} finally {
			log.info("Get date in file header " + originalFileName + "["
					+ indexValue + "].");
			if (null != originalFIS) {
				originalFIS.close();
			}
		}

		renameFileName = new File(receivePath, originalFileName.getName() + "_"
				+ indexValue);
		copyFile(originalFileName, renameFileName);
	}

	/**
	 * for GPG Encrypt
	 * 
	 * @param sourceFile
	 * @return
	 * @throws Exception
	 */
	private static int moveAndGPGEncrypt(File sourceFile) throws Exception {
		int isEncryptSuccess = -1;
		// For hkt15018
		getSpecialProp(sourceFile.getName());
		// For hkt15018
		Date today = new Date();
		if (null != nextDoFile) {
			nextDoFile = convertPath(nextDoFile, today);
			log.info("nextDoFile: " + nextDoFile);
		}

		if (null != nextDoFileTo) {
			nextDoFileTo = convertPath(nextDoFileTo, today);
			log.info("nextDoFileTo: " + nextDoFileTo);
		}

		if (isNextFileCome(sourceFile.getName())) {
			log.debug("update the next file [" + nextDoFile + "] to ["
					+ nextDoFileTo + "].");
			copyFile(new File(nextDoFile), new File(nextDoFileTo));
		} else {
			log.debug("the next file [" + nextDoFile + "]is not arrival.");
		}

		File beforeEncryptFile = new File(convertPath(beforeEncryptPath, today));

		File afterEncryptFile = new File(convertPath(afterEncryptPath, today));

		moveFile(sourceFile, beforeEncryptFile);

		isEncryptSuccess = gpgEncrypt(beforeEncryptFile, afterEncryptFile);
		// For hkt15018
		renameFileName = new File(sourceFile.getParent(),
				afterEncryptFile.getName());
		log.debug("gpgResultFile[" + renameFileName.getPath() + "]");
		if (isEncryptSuccess == 0) {
			log.info("GPG Encrypt file " + renameFileName.getName()
					+ " finished.");
			copyFile(afterEncryptFile, renameFileName);
		} else {
			log.error("GPG Encrypt file " + renameFileName.getName()
					+ " failed.");
			return -1;
		}

		return 0;

	}

	private static int gpgEncrypt(File beforeEncryptFile, File afterEncryptFile)
			throws Exception {
		String gpgEncryptCommand = gpgPath + " --homedir " + gpgKeyRing
				+ " -r " + gpgUserId + " -o " + afterEncryptFile.getPath()
				+ " -ea " + beforeEncryptFile.getPath();

		log.debug("[" + gpgEncryptCommand + "]");
		Process process = null;
		try {

			if (afterEncryptFile.exists()) {
				FileUtils.forceDelete(afterEncryptFile);
			}

			process = Runtime.getRuntime().exec(gpgEncryptCommand);

			redirectStream(process.getErrorStream(), "gpgErrInfo.txt", false);
			redirectStream(process.getInputStream(), "gpgInputInfo.txt", false);

			clearProcessStream(process.getErrorStream());
			clearProcessStream(process.getInputStream());
			int returnCode = process.waitFor();
			if (0 == returnCode) {
				log.debug("gpgEncrypt successful.");
				return 0;
			} else {
				log.debug("gpgEncrypt failed. Return code: " + returnCode);
				return 1;
			}
		} catch (IOException e) {
			log.error("The process used to gpgEncrypt is failed.", e);
		} catch (InterruptedException e) {
			log.error("The process used to gpgEncrypt is failed.", e);
		} catch (Exception e) {
			log.error("The process used to gpgEncrypt is failed X.", e);
		} finally {

		}
		return 2;
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
				if (null != log) {
					log.debug("Write windows error event log successful.");
				} else {

				}
			} else {
				if (null != log) {
					log.debug("The process used to write window error event log is failed. Return code: "
							+ returnCode);
				} else {

				}
				return;
			}
		} catch (IOException e) {
			if (null != log) {
				log.error(
						"The process used to write window error event log is failed.",
						e);
			} else {

			}
		} catch (InterruptedException e) {
			if (null != log) {
				log.error(
						"The process used to write window error event log is failed.",
						e);
			} else {

			}

		} catch (Exception e) {
			if (null != log) {
				log.error(
						"The process used to write window error event log is failed X.",
						e);
			} else {

			}
		}
	}

	/**
	 * create unique log directory and corresponding file for every sending file
	 * 
	 * @param logFileName
	 * @throws Exception
	 */
	public static Logger createNewLogger(String logFileName) throws Exception {
		URL logURL = Compress.class.getClassLoader().getResource(LOG_PATH);
		PropertyConfigurator.configure(logURL);
		Logger genericLogger = Logger.getLogger("GenericLogger");
		Logger logger = Logger.getLogger(logFileName);
		@SuppressWarnings("rawtypes")
		Enumeration appenders = genericLogger.getAllAppenders();
		while (appenders.hasMoreElements()) {
			RollingFileAppender appender = (RollingFileAppender) appenders
					.nextElement();
			String fileName = logFilePath + "\\" + logFileName
					+ nowDate.getDateYYMMDD() + "\\" + appender.getFile();
			Layout layout = appender.getLayout();
			RollingFileAppender newAppender = null;
			Priority threshold = appender.getThreshold();
			try {
				newAppender = new RollingFileAppender(layout, fileName);
			} catch (IOException e) {
				if (null == log) {
					System.out.println("createNewLogger: " + e);
				} else {
					log.error("createNewLogger", e);
				}
				throw e;

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

	/**
	 * write the file name and file size into a appointed file
	 * 
	 * @param fileName
	 */
	private static void fileSizeWrite(String fileName) {
		File receiveFile = new File(receivePath + "\\" + fileName);
		Date dateTime = new Date();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
		String fileTrnTime = simpleDateFormat.format(dateTime);
		String reportName = "Report" + fileTrnTime + ".txt";
		File file = new File(fileSizePath + "\\");
		if (!file.exists()) {
			file.mkdirs();
		}
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
			log.info("Write " + fileName + " size to tempreport successfully!");

		} catch (Exception e) {
			log.error("Write " + fileName + " size to report failed!", e);
		}
	}

	/**
	 * find time file. if file is not there alert else get the time. delete the
	 * temp file until the next day batch begin.
	 * 
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	// private String findTimeFile(String fileName) throws Exception {
	// if (fileName.equals("ATCRU_D")) {
	// BufferedReader reader = new BufferedReader(new FileReader(
	// receivePath + "/" + fileName));
	// String batchTime = reader.readLine();
	// batchTime = batchTime.substring(1, batchTime.length());
	// new File(dataFilePath + "/" + batchTime).createNewFile();
	// // BufferedWriter writer = new BufferedWriter(new
	// // FileWriter(errDir+"/temp"));
	// return batchTime.substring(1, batchTime.length());
	// } else {
	// if (new File(dataFilePath).length() == 0) {
	// return null;
	// } else {
	// return new File(dataFilePath).list()[0];
	// }
	//
	// }
	// // before the batch begin, delete the time file.
	// /*
	// * if(0==0){ new File(dataFilePath).delete(); new
	// * File(dataFilePath).createNewFile(); }
	// */
	// }

	/**
	 * compress file
	 */
	private File compress(File f) {
		log.debug("Start compressing file " + file_Name + ".");
		if (!fileExists(f)) {
			log.error("Source file " + f + " doesn't exist."
					+ " Compressing failed.");
			return null;
		}

		String yyMMdd = null;
		// For hkt15018
		if ("TRUE".equals(notChangeDate)) {
			yyMMdd = nowDate.getDateYYMMDD(0);
		} else {
			yyMMdd = nowDate.getDateYYMMDD();
		}

		String targetFilename = "";
		File targetFile = null;
		try {
			log.info("compressFlag " + compressFlag);
			if (uncompressFileList != null
					&& !uncompressFileList.trim().equals("")) {
				String uncompressFiles[] = uncompressFileList.split(",");
				boolean uncompressFlag = false;
				for (int i = 0; i < uncompressFiles.length; i++) {
					if (f.getName().equals(uncompressFiles[i].trim())) {
						uncompressFlag = true;
					}
				}
				if (uncompressFlag == true) {
					if (bankCode != null) {
						targetFilename = f.getName() + yyMMdd + "." + bankCode;
					} else {
						targetFilename = f.getName() + yyMMdd;
					}
					targetFile = new File(compressPath, targetFilename);
					if (targetFile.exists()) {
						FileUtils.deleteQuietly(targetFile);
					}
					FileUtils.moveFile(f, targetFile);
					log.info("Move and not compress file to "
							+ targetFile.getName() + " successfully!");
					return targetFile;
				}
			}
			if (compressFlag) {
				targetFilename = f.getName() + "_" + yyMMdd + ".ZIP";
				targetFile = new File(compressPath, targetFilename);
				String targetPath = targetFile.getPath();

				// String command = WZZIPPath + " " + targetPath + " " +
				// f.getPath();
				// if need compressing with password. the password is configured
				// in config.properties.
				// the define in config.properties is below
				// ###passWord must have at least 8 digits. exp."12345678"
				// ###if passWord is not defined, it will compress without
				// password by winzip.
				// passWord = "12345678"
				File renameF = new File(f.getAbsolutePath());
				if (gpgEncryptFlag) {
					// For hkt15018
					renameF = new File(f.getAbsolutePath() + ".pgp");
					copyFile(f, renameF);
				} else {

				}

				String command = WZZIPPath + " ";
				if (compressPasswordFlag) {
					if (null == WZZIPPassword || WZZIPPassword.isEmpty()) {
						WZZIPPassword = "12345678";
					}
					// command = command + "-s" + WZZIPPassword + " " +
					// targetPath + " " + f.getPath();
					command = command + "-s" + WZZIPPassword + " " + targetPath
							+ " " + renameF.getPath();
				} else {
					// command = command + targetPath + " " + f.getPath();
					command = command + targetPath + " " + renameF.getPath();
				}

				log.debug("Compress command: " + command);
				targetPath = targetPath.substring(0,
						targetPath.lastIndexOf(File.separator));
				File filePathTarget = new File(targetPath);
				if (!filePathTarget.exists()) {
					filePathTarget.mkdirs();
				}
				try {
					Process process = Runtime.getRuntime().exec(command);
					redirectStream(process.getErrorStream(),
							"winzipErrInfo.txt", false);
					redirectStream(process.getInputStream(),
							"winzipInputInfo.txt", false);
					clearProcessStream(process.getErrorStream());
					clearProcessStream(process.getInputStream());

					int returnCode = process.waitFor();
					if (0 == returnCode) {
						if (!fileExists(targetFile)) {
							log.error("Target file " + targetFile
									+ " doesn't exist."
									+ " Compressing failed.");
							return null;
						}
						log.info("Compressed to " + targetPath + ".");
						moveFile(f, new File(dataFilePath, f.getName()));
						return targetFile;
					} else {
						log.info("Compressing " + f.getName() + " failed."
								+ " Return code: " + returnCode + ".");
						return null;
					}
				} catch (Exception e) {
					String message = "Compressing " + f.getName() + " failed.";
					log.error(message, e);
					return null;
				}
			} else {
				if (!gpgEncryptFlag) {
					targetFilename = f.getName() + "_" + yyMMdd;
				} else {
					targetFilename = f.getName();
				}
				targetFile = new File(compressPath, targetFilename);
				if (targetFile.exists()) {
					FileUtils.deleteQuietly(targetFile);
				}
				FileUtils.moveFile(f, targetFile);
				log.info("Move and not compress file to "
						+ targetFile.getName() + " successfully!");
				return targetFile;
			}
		} catch (Exception e) {
			String message = "Prepare DWH file " + targetFile.getName()
					+ " failed.";
			log.error(message, e);
			return null;
		}
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

	/**
	 * create flagfile for every needed file according the specification
	 * 
	 * @param readyFlagFilePath
	 * @param compressedFile
	 */
	private void createFlagFile(String readyFlagFilePath, File compressedFile,
			File originalFile) {
		log.debug(" Begin to create flag file =" + compressedFile.getName());
		String name = "";
		File flagPath = new File(readyFlagFilePath);

		if (!flagPath.exists()) {
			flagPath.mkdirs();
		}

		if (!isRenameAndNoCompress(originalFile.getName())) {
			if (compressedFile.getName().lastIndexOf(".") > 0) {
				name = compressedFile.getName().substring(0,
						compressedFile.getName().lastIndexOf("."));
			} else {
				name = compressedFile.getName();
			}

			if (compressFlag
					&& (uncompressFileList == null || ""
							.equals(uncompressFileList.trim()))) {
				flagFile = new File(readyFlagFilePath + "\\" + name
						+ ".ZIP.000K");
			} else {
				flagFile = new File(readyFlagFilePath + "\\" + name + ".000K");
			}
		} else {
			name = compressedFile.getName();
			flagFile = new File(readyFlagFilePath + "\\" + name + ".000K");
		}

		FileOutputStream fos;
		try {
			fos = new FileOutputStream(flagFile, true);
			OutputStreamWriter osw = new OutputStreamWriter(fos);
			long size = compressedFile.length();
			osw.write(String.valueOf(size));
			osw.flush();
			osw.close();
			fos.close();
			log.info(" End: successfully create flag file " + flagFile);
		} catch (Exception e) {
			log.error("Create transferring-finished flag " + file_Name
					+ " failed! ", e);
			String message = "Create transferring-finished flag " + file_Name
					+ " failed! ";
			log.info(message);
			Compress.writeWinEventErrLog(message, eventID);
		}
	}

	/**
	 * invoke the sftp.exe command in dos. send the file to client. copy the
	 * errorStream and InputStream to a file named ... If the transfer is
	 * successful.move the compressed file to directory success. if fail, move
	 * to failure.About the conditions we use to judge the failure. The return
	 * code and the Stream content are useful.
	 * 
	 * @param toSendFile
	 * @return
	 */
	private static boolean sendFile(String originalFileName, File toSendFile) {
		String fileName = toSendFile.getName();
		int returnCode = 1;
		try {
			log.debug("Begin to send the file " + fileName + " .");
			if (!fileExists(toSendFile)) {
				log.error("File " + toSendFile + " doesn't exist."
						+ " Sending failed.");
			}

			CommandFile.createCommmandFile(originalFileName, fileName, log,
					HKT_15018, file_Name);
			String command = "";
			if (!HKT_15018) {
				command = exeFile + " -i \"" + id_rsaPath + "\" -b \""
						+ commandPath + "/" + fileName + commandFile
						+ "\" -oPort=" + hostPort + " " + userID + "@"
						+ hostAddress;
			} else {
				command = exeFile + " -i \"" + id_rsaPath + "\" -b \""
						+ commandPath + "/" + fileName + commandFile
						+ "\" -oPort=" + hostPortByFile + " " + userIDByFile
						+ "@" + hostAddressByFile;
			}
			commandFileAbsolutionPath = commandPath + "/" + fileName
					+ commandFile;
			log.debug("transafering command" + command);
			log.debug("commandFileAbsolutionPath" + command);
			log.debug("HKT_15018 = " + HKT_15018);
			log.debug("fileName = " + fileName);

			if (!(HKT_15018 && fileName.endsWith(".000K"))) {

				Process process = Runtime.getRuntime().exec(command);
				StreamRedirectThread errstr = redirectStream(
						process.getErrorStream(), "sftpErrInfo.txt", false);
				StreamRedirectThread infostr = redirectStream(
						process.getInputStream(), "sftpInputInfo.txt", false);
				if (errstr.hasFailed()) {
					String message = fileName
							+ " sent to Server failed because of finding 'failed' in sftp errorstream !";
					log.error(message);
					return false;
				}
				if (infostr.hasFailed()) {
					String message = fileName
							+ " cannot send to Server because of finding 'failed' in sftp inputstream ! ";
					log.error(message);
					return false;
				}
				clearProcessStream(process.getErrorStream());
				clearProcessStream(process.getInputStream());
				returnCode = process.waitFor();
			} else {
				returnCode = 0;
			}
			// success
			if (returnCode == 0) {
				// Calendar cal = GregorianCalendar.getInstance();
				log.info("End:Send file " + fileName + " successful.");
				// move file to directory success
				File oriFlagFile = new File(sendPath + "\\" + fileName);
				File sendFlagFile = new File(sendPath + "\\" + "Success" + "\\"
						+ fileName
				/*
				 * + cal.get(Calendar.HOUR_OF_DAY) + cal.get(Calendar.MINUTE) +
				 * cal.get(Calendar.SECOND)
				 */);

				moveFile(oriFlagFile, sendFlagFile);
				return true;
			} else {
				log.info("Send file " + fileName + " failed.");

				File oriFlagFile = new File(sendPath + "\\" + fileName);
				File sendFlagFile = new File(sendPath + "\\" + "Failure" + "\\"
						+ fileName);
				moveFile(oriFlagFile, sendFlagFile);
				return false;
			}
		} catch (IOException e) {
			log.error("The process used to send file " + fileName
					+ " is failed.", e);
		} catch (InterruptedException e) {
			log.error("The process used to send file " + fileName
					+ " is failed.", e);
		} catch (Exception e) {
			log.error("The process used to send file " + fileName
					+ " is failed.", e);
		}
		return true;
	}

	public static StreamRedirectThread redirectStream(InputStream in,
			String errfilename, boolean isBinary) {
		StreamRedirectThread t = new StreamRedirectThread(in, new File(errDir,
				errfilename), isBinary);
		t.writeFile();
		return t;
	}

	private static boolean copyFile(File src, File dst) {
		log.debug("Copying file " + src + " to " + dst);
		boolean copied = false;
		for (int i = 0; !copied && i < 2; i++) {
			try {
				FileUtils.copyFile(src, dst);
				copied = true;
			} catch (Exception e) {
				log.error("Error during copying.", e);
			}
			if (!copied) {
				try {
					Thread.sleep(1000 * 60);
				} catch (Exception e) {
				}
			}
		}
		if (copied) {
			log.info("File " + src + " copied to " + dst);
		} else {
			log.info("Copying file " + src + " to " + dst + " failed.");
		}
		return copied;
	}

	/**
	 * check the status about the waiting time. According to the param
	 * maxWaiting, it can try to send the files again and again, if it's not
	 * beyond the maxwaiting time.Every trial sleep about some seconds.
	 * 
	 * @param toSendFile
	 * @param maxWaiting
	 * @return
	 */
	private boolean statusCheck(String originalFileName, File toSendFile,
			long maxWaiting) {
		String fileName = toSendFile.getName();
		log.debug("Start to check status file  " + fileName);
		long startTime = System.currentTimeMillis();
		long elapsedTime = 0;
		// send file
		boolean flag = Compress.sendFile(originalFileName, toSendFile);
		// log.info("Waiting for file transferring! It is transferring file " +
		// fileName + " to Client...");
		int count = 0;
		File failFile = new File(sendPath + "\\Failure\\"
				+ toSendFile.getName());
		log.debug("flag value " + flag + " elapsedTime " + elapsedTime);
		while (flag == false && elapsedTime < maxWaiting) {
			try {
				Thread.sleep(ThreadSleepInterval);// 60 seconds

				log.debug("Resending. elapsedTime: " + elapsedTime);
				moveFile(failFile, toSendFile);
			} catch (InterruptedException e) {
				String message = "copy file has trouble";
				log.error(message, e);
			} catch (IOException e) {
				String message = "copy file has trouble";
				log.error(message, e);
			} catch (Exception e) {
				String message = "copy file has trouble";
				log.error(message, e);
			}

			flag = Compress.sendFile(originalFileName, toSendFile);
			count++;
			log.debug("Waited " + count + " minute(s)!");
			elapsedTime = System.currentTimeMillis() - startTime;
		}
		if (flag == false) {
			String message = "File transferring to client error! Please check copssh. File name: "
					+ fileName;
			// log.error(message);
			log.info(message);
			return false;
			/*
			 * } else if (elapsedTime > maxWaiting) { String message =
			 * "File transferring to client error! It used more than maxWaiting. File name: "
			 * + fileName; //log.error(message); log.info(message);
			 * Compress.writeWinEventErrLog(message, eventID); return false;
			 */
		} else {
			String message = "File " + fileName
					+ " transferring to client successfully!";
			log.info(message);
			Date dateTime = new Date();
			SimpleDateFormat fileTranferTime = new SimpleDateFormat(
					"yyyyMMdd HH:mm:ss");
			endTime = fileTranferTime.format(dateTime);
			return true;
		}
	}

	/**
	 * Writes report about a file, including the start time, end time.
	 */
	public static void writeReport(String fileName) {
		log.debug("Writing report. Filename: " + fileName + ".");
		String YYMMDD = nowDate.getDateYYMMDD();

		// for PCL OCTOPUS
		if ("AB".equalsIgnoreCase(fileName) || "DB".equalsIgnoreCase(fileName)) {
			YYMMDD = nowDate.getDateYYMMDD(0);
		}

		// For hkt15018
		if ("TRUE".equalsIgnoreCase(notChangeDate)) {
			YYMMDD = nowDate.getDateYYMMDD(0);
		}

		File reportFile = new File(reportPath, "Report" + YYMMDD + ".txt");
		File CheckClientFile = new File(reportPath, "Check" + clientName
				+ "File" + YYMMDD + ".txt");
		String message = Compress.startTime + "-" + endTime + "--P--"
				+ fileName;
		if (null != clientFileMap && clientFileMap.containsKey(fileName)) {
			ReportClient.report(CheckClientFile.getPath(), message, log);
		} else {
			ReportClient.report(reportFile.getPath(), message, log);
		}
	}

	private static boolean fileExists(File f) {
		for (int i = 0; i < 5; i++) {
			if (f.exists()) {
				return true;
			}
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
			}
		}
		return f.exists();
	}

	private static String GetCheckLog(Properties prop, String tag) {

		String tagValue = prop.getProperty(tag);
		if (null == log) {
			System.out.println(tag + "=" + tagValue);
		} else {
			log.debug(tag + "=" + tagValue);
		}
		if (null == tagValue || tagValue.trim() == "") {
			if (null == log) {
				System.out.println("invalid value =[" + tag + "]");

			} else {
				log.error("invalid value =[" + tag + "]");
			}

			errorFinalDeal();
			return null;

		} else {
			return tagValue.trim();
		}
	}

	public static boolean isRenameAndNoCompress(String originalFileName) {
		// REGEX_VALUE_FOR_DONOT_RENAME_OR_COMPRESS=(DR|LT|AS|AB|DB)\\d{6}\\.\\d{3}
		// REGEX_VALUE_FOR_DONOT_RENAME_OR_COMPRESS=(DR|LT|AS|AB|DB)
		log.debug("in isNoRenameAndNoCompress: [" + originalFileName + "]");
		if ("".equals(REGEX_VALUE)) {
			log.debug("in isNoRenameAndNoCompress regex_value: [" + REGEX_VALUE
					+ "]");
			return false;
		}
		Pattern pattern = Pattern
				.compile(REGEX_VALUE, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(originalFileName);

		return matcher.matches();
	}

	public static String getUncompressFileList() {
		return uncompressFileList;
	}

	public static void errorFinalDeal() {
		System.out.println("error and exit");
		System.exit(-2);
	}

	public static void getSpecialProp(String SpecailName) throws Exception {
		FileInputStream fis = null;
		Properties prop = new Properties();
		URL configURL = Compress.class.getClassLoader()
				.getResource(CONFIG_FILE);

		try {
			File configFile = null;
			configFile = new File(configURL.toURI());
			fis = new FileInputStream(configFile);
			prop.load(fis);

		} catch (Exception e) {
			e.printStackTrace();
			System.out
					.println("Parameter initialition failed! load config file error");
			String message = "Parameter initialition failed! load config file error";
			Compress.writeWinEventErrLog(message, eventID);
		} finally {
			if (null != fis) {
				fis.close();
			}
		}
		try {
			nextDoFile = prop.getProperty(SpecailName + ".nextDoFile");
			if (null != nextDoFile || "".equals(nextDoFile.trim())) {
				nextDoFile = nextDoFile.trim();
			} else {
				nextDoFile = null;
			}
			nextDoFileTo = prop.getProperty(SpecailName + ".nextDoFileTo");
			if (null != nextDoFileTo || "".equals(nextDoFileTo.trim())) {
				nextDoFileTo = nextDoFileTo.trim();
			} else {
				nextDoFileTo = null;
			}

			beforeEncryptPath = prop.getProperty(SpecailName + ".encryptPath");
			afterEncryptPath = prop.getProperty(SpecailName
					+ ".afterEncryptPath");

		} catch (Exception e) {

			if (null == log) {
				System.out.println("reading prop error:[" + e + "]");
			} else {
				log.error("reading prop error", e);
			}
			String message = "reading prop error!";
			Compress.writeWinEventErrLog(message, eventID);
			throw e;
		}
	}

}
