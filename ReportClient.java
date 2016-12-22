package com.firstdata.common;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.Properties;
import org.apache.log4j.Logger;

public class ReportClient {
     
    public static void main(String[] args) throws Exception {
        report( args[0], args[1], log );
    }
    
    private static Logger log;
    private static String address;
    private static int port;
    private static Charset charset;
    private static final byte[] HEADER = { // The header is "REPORTSERVER"
        0x52, 0x45, 0x50, 0x4F, 0x52, 0x54,
        0x53, 0x45, 0x52, 0x56, 0x45, 0x52
    };
    
    static {
        try {
            log = Logger.getLogger(ReportClient.class);//ReportClient这个类注册到log，然后就能调用log4j里面的配置进行日志输出
            InputStream in = ReportClient.class.getClassLoader().getResourceAsStream("report-client.properties");
            Properties p = new Properties();
            p.load(in);
            
            address = p.getProperty("address").trim();
            log.info("Address: " + address + ".");
            
            port = Integer.parseInt( p.getProperty("port").trim() );
            log.info("Port: " + port + ".");
            
            String charsetName = p.getProperty("charset").trim();
            charset = Charset.forName(charsetName);
            log.info("Charset: " + charset.name() + ".");
            
            in.close();
        } catch (Exception e) {
            log.error("Reading config failed.", e);
        }
    }
    
    synchronized
    public static void report(
        String filePath, String message, Logger logger
    ) {
        log = logger;
        log.debug( "Sending report: \"" + message +
                   "\" to remote file \""+ filePath + "\"." );
        Socket s = null;
        try {
            s = new Socket(address, port);
            OutputStream out = s.getOutputStream();
            InputStream in = s.getInputStream();
            send(out, in, filePath, message);
            log.debug("Sending finished.");
        } catch (Exception e) {
            log.error("Sending failed.", e);
            disconnect(s);
            log.debug("Retrying.");
            try {
                s = new Socket(address, port);
                OutputStream out = s.getOutputStream();
                InputStream in = s.getInputStream();
                send(out, in, filePath, message);
                log.debug("Sending finished.");
            } catch(Exception e2) {
                log.error("Sending failed.", e2);
                return;
            }
        } finally {
            disconnect(s);
        }
    }
    
    private static void send(
        OutputStream out, InputStream in, String f, String m
    ) throws Exception {
        log.debug("Sending header.");
        out.write(HEADER);
        log.debug("Header sent.");
        
        log.debug("Sending file path.");
        sendString(out, f);
        log.debug("File path sent. Sending message.");
        sendString(out, m);
        log.debug("Message sent.");
        
        log.debug("Receiving response.");
        byte[] b = new byte[ HEADER.length ];
        readBytes(in, b);
        log.debug("Response received.");
    }
    
    private static void sendString(OutputStream out, String s)
    throws IOException {
        byte[] b = s.getBytes(charset);
        log.trace("Sending string length: " + b.length + ".");
        byte[] len = { (byte)(b.length >> 8), (byte)b.length };
        out.write(len);
        log.trace("String length sent. Sending string.");
        out.write(b);
        log.trace("String sent.");
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
    
    private static void disconnect(Socket s) {
        log.trace("Disconnecting.");
        try {
            if (null != s) {
                s.close();
            }
            log.trace("Disconnected.");
        } catch (Exception e) {
            log.debug("Disconnecting failed.", e);
        }
    }
    
}
