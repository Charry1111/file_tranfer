package com.firstdata.dwh.compress;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class nowDate {
	
	
	public static String getDateYYMMDD(int days){
	    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
		Calendar a=Calendar.getInstance();
		    a.add(Calendar.DAY_OF_MONTH, days);
            return  simpleDateFormat.format(a.getTime()).substring(2, 8); 
        }
	
	/**
	 * utility
	 * @author Cao Zhen
	 */
	
	public static String getDateYYMMDD(){
	    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
		Calendar a=Calendar.getInstance();
		
		int hour_of_day = a.get(Calendar.HOUR_OF_DAY);
		
		if(hour_of_day>=0 && hour_of_day<12 ){
            a.add(Calendar.DAY_OF_MONTH, -1);
            return  simpleDateFormat.format(a.getTime()).substring(2, 8); 
        }else{
            return  simpleDateFormat.format(a.getTime()).substring(2, 8); 
        }
	    /*Calendar a=Calendar.getInstance();
		String date = null;
		String year=new Integer(a.get(Calendar.YEAR)).toString();
		int month=new Integer(a.get(Calendar.MONTH)).intValue();
		int day=new Integer(a.get(Calendar.DATE)).intValue();
		int hour_of_day = a.get(Calendar.HOUR_OF_DAY);
		
		
		if(hour_of_day>=0 && hour_of_day<12 ){
		    //day=day - 1;
		}else{
		}
		
		if(month < 9 && day < 10) {
			
			date = year + "0" + (month + 1) + "0" + day;
		
		} else if(month < 9 && day >= 10) {
			
			date = year + "0" + (month + 1) + day;
		
		} else if(month >= 9 && day >= 10) {
			
			date = year + (month + 1) + day;
			
		} else {
			
			date = year + (month + 1) + "0" + day;
			
		}
		date = date.substring(2, 8);
		return date;*/
	}
	
	public static void main(String[] agrs){
	    System.out.print(nowDate.getDateYYMMDD());
	}
}