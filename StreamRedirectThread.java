package com.firstdata.dwh.compress;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class StreamRedirectThread {
    
    private InputStream in;
    private File f;
    private long size = 0;
    private boolean failed = false;
    private boolean isBinary;
    
    public StreamRedirectThread(InputStream in, File f, boolean isBinary) {
        this.in = in;
        this.f = f;
        this.isBinary = isBinary;
    }
    
    public void writeFile() {
        if (this.isBinary) {
            binaryRedirect();
        }
        else {
            lineRedirect();
        }
    }
    
    public long getSize() {
        return this.size;
    }
    
    public boolean hasFailed() {
        return this.failed;
    }
    
    private void lineRedirect() {
        try {
        	//第一步：实例化BufferedReader 对象，其中this.in获取的是控制台输入的字节流，InputStreamReader()则是把这个字节流转化成字符流；
            BufferedReader reader =new BufferedReader( new InputStreamReader(this.in) );
            PrintWriter out = new PrintWriter( new FileOutputStream(f, true) );
            //BufferedReader 对象reader 获取控制台输入
            String line = reader.readLine();
            while (null != line) {
                this.size += line.length();
                if ( line.contains("failed")) {
                    this.failed = true;
                }
                out.println(line);
                line = reader.readLine();
            }
            out.close();
        } catch (Exception e) {}
    }
    
    private void binaryRedirect() {
        try {
            FileOutputStream out = new FileOutputStream(this.f, true);
            byte[] b = new byte[16 * 1024];
            int n = this.in.read(b);
            while (n >= 0) {
                this.size += n;
                out.write(b, 0, n);
                n = this.in.read(b);
            }
            out.close();
        } catch (Exception e) {}
    }
    
}
