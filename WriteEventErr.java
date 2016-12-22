package com.firstdata.dwh.compress;

public class WriteEventErr {

   /**
    * when cyberfusion transmit fail, write error.
    * @param args
    */
    
    public static void main(String[] args) {
        CommonUtil.propertyConfigureInit("monitor");
        CommonUtil.writeWinEventErrLog(" Cyberfusion transmit error ",CommonUtil.getEventID());       
    }
    
}
