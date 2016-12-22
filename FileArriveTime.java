package com.firstdata.dwh.compress;

import java.util.Date;

public class FileArriveTime {
    
    /**
     * The integer means HHmm. E.g., 1345 means 13:45; 925 means 09:25.
     */
    private int shouldArriveTime;
    private Date lastArriveDate;
    private Date lastAlarmDate;
    
    public Date getLastArriveDate() {
        return lastArriveDate;
    }
    public void setLastArriveDate(Date lastArriveDate) {
        this.lastArriveDate = lastArriveDate;
    }

    public Date getLastAlarmDate() {
        return lastAlarmDate;
    }
    public void setLastAlarmDate(Date lastAlarmDate) {
        this.lastAlarmDate = lastAlarmDate;
    }
    
    public int getShouldArriveTime() {
        return shouldArriveTime;
    }
    public void setShouldArriveTime(int shouldArriveTime) {
        this.shouldArriveTime = shouldArriveTime;
    }
    
}
