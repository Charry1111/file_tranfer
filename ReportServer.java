package com.firstdata.common;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class ReportServer {
    
    private static Logger log;
    private static int port;
    private static Charset charset;
    private static final byte[] HEADER = { // The header is "REPORTSERVER"
        0x52, 0x45, 0x50, 0x4F, 0x52, 0x54,
        0x53, 0x45, 0x52, 0x56, 0x45, 0x52
    };
    
    static {
        try {
            Class<?> c = ReportServer.class;
            
            URL log4jConfigURL = c.getClassLoader().getResource("log4j.properties");
            File log4jConfigFile = new File( log4jConfigURL.toURI() );
            PropertyConfigurator.configureAndWatch( log4jConfigFile.getCanonicalPath() );
            log = Logger.getLogger(c);
            
            InputStream in = c.getClassLoader()
                              .getResourceAsStream("report-server.properties");
            Properties p = new Properties();
            p.load(in);
            
            port = Integer.parseInt( p.getProperty("port").trim() );
            log.info("Port: " + port + ".");
            
            String charsetName = p.getProperty("charset").trim();
            charset = Charset.forName(charsetName);
            log.info("Charset: " + charset.name() + ".");
            
            in.close();
        } catch (Exception e) {
            log.debug("Reading config failed.", e);
        }
    }
    
    public static void main(String[] args) throws IOException {
        ServerSocket ss = new ServerSocket(port);
        while (true) {
            try {
                Socket s = ss.accept();
                log.debug("Accepted: " + s + ".");
                receiveReport(s);
            } catch (Exception e) {
                log.error(e, e);
            }
        }
    }
    
    /**
     * Receive a report request. The request format is:
     * <ol>
     * <li>12-byte header {@code REPORTSERVER}, in ASCII encoding.</li>
     * <li>2-byte length of file path, binary, big-endian.</li>
     * <li>file path, as the length above, encoded as the {@code charset}
     * property.</li>
     * <li>2-byte length of message, binary, big-endian.</li>
     * <li>the message, as the length above, encoded as the {@code charset}
     * property.</li>
     * </ol>
     */
    private static void receiveReport(Socket s) {
        try {
            InputStream in = s.getInputStream();
            OutputStream out = s.getOutputStream();
            
            boolean correct = readHeader(in);
            if (!correct) {
                return;
            }
            
            log.debug("Reading file path.");
            String path = readString(in);
            if (null == path) {
                log.debug("Reading file path failed.");
                return;
            }
            log.debug("File path: \"" + path + "\"");
            
            log.debug("Reading message.");
            String m = readString(in);
            if (null == m) {
                log.debug("Reading message failed.");
                return;
            }
            log.debug("Message: \"" + m + "\"");
            
            log.debug("Sending response.");
            out.write(HEADER);
            log.debug("Response sent.");
            
            PrintStream fout = null;
            try {
                File f = new File(path);
                File dir = f.getCanonicalFile().getParentFile();
                if ( !dir.exists() ) {
                    log.debug("Creating nonexisting directory " + dir + ".");
                    boolean created = dir.mkdirs();
                    if (created) {
                        log.error("Directory " + dir + " created.");
                    } else {
                        log.error("Creating directory " + dir + " failed.");
                    }
                }
                fout = new PrintStream( new FileOutputStream(f, true) );
                fout.println(m);
            } finally {
                if (null != fout) {
                    try {
                        fout.close();
                    } catch (Exception e) {}
                }
            }
        } catch (Exception e) {
            log.error(e, e);
        }
    }
    
    private static boolean readHeader(InputStream in) throws Exception {
        log.debug("Reading header.");
        byte[] b = new byte[HEADER.length];
        readBytes(in, b);
        if ( !Arrays.equals(HEADER, b) ) {
            log.debug("Invalid header.");
            return false;
        }
        log.debug("Correct header.");
        return true;
    }
    
    private static String readString(InputStream in) throws IOException {
        log.trace("Reading string length.");
        short len = readShort(in);
        log.trace("String length: " + len + ".");
        if (len < 0) {
            log.debug("Invalid string length: " + len + ".");
            return null;
        }
        byte[] b = new byte[len];
        log.trace("Reading String.");
        readBytes(in, b);
        String s = new String(b, charset);
        log.trace("Reading string finished.");
        return s;
    }
    
    private static short readShort(InputStream in) throws IOException {
        byte[] b = new byte[2];
        readBytes(in, b);
        int n = ( ( b[0] & 0xFF ) <<  8) | ( b[1] & 0xFF );
        return (short)n;
    }
    
    private static void readBytes(InputStream in, byte[] b)
    throws IOException {
        log.trace(b.length + " byte(s) to read.");
        int s = 0;
        while (s < b.length) {
            int n = in.read(b, s, b.length - s);
            if (n < 0) {
                throw new EOFException(
                    "EOF at " + s + " byte(s) but " +
                    b.length + " byte(s) expected."
                );
            }
            s += n;
        }
        log.trace("Reading finished.");
    }
    
}
