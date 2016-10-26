package com.comm.basedSearch.utils;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class DateUtil
{
	final static Map<String,String> monthMap=new HashMap<>();
	static{
		monthMap.put("January", "01");
		monthMap.put("February", "02");

		monthMap.put("March", "03");

		monthMap.put("April", "04");

		monthMap.put("May", "05");

		monthMap.put("June", "06");

		monthMap.put("July", "07");

		monthMap.put("August", "08");

		monthMap.put("September", "09");

		monthMap.put("October", "10");
		monthMap.put("November", "11");

		monthMap.put("December", "12");


	}

	
//2014-06-04T00:00:00.000Z
        public static final String DATE_TIME_Z = "yyyy-MM-ddTHH:mm:ss.000Z";
        public static final String DATE_TIME_TZ="yyyy-MM-dd'T'HH:mm:ss'Z'";
	public static final String DATE_TIME = "yyyy-MM-dd HH:mm:ss";
	public static final String DATE_TIME_INFO = "yyyyMMddHHmmss";
	//一天的毫秒数
	public static final long dayTime = 86400000L;
    
	public static final String TIME = "HH:mm:ss";

	public static final String DAY = "yyyy-MM-dd";
	
	public static final String MONTH = "yyyyMM";
	
	public static final String MONTH_CH = "yyyy年MM月";
	
	public static final String DAY_CH = "yyyy年MM月dd日";
	
	public static final String DAY_SLASH = "yyyy/MM/dd";
	
	//1970-1-1的毫秒数
	public static final long NUMBER1970_1_1 = -28800000;

	private static final Log log = LogFactory.getLog(DateUtil.class);

	private static SimpleDateFormat getFmt(String fmt)
	{
		return new SimpleDateFormat(fmt);
	}

	
	/**
	 * 按照指定的格式返回日期的字符串格式
	 * @param date
	 * @param format
	 * @return
	 */
	public static String getDateString(Date date, String format)
	{
		if(date != null)
		{
		SimpleDateFormat fmt = getFmt(format);
		return fmt.format(date);
		}else
		{
			return "";
		}
	}

	/**
	 * 按照指定的格式返回日期的字符串格式
	 * @param date
	 * @param format
	 * @return
	 */
	public static String getDateString(Date date)
	{
		SimpleDateFormat fmt = getFmt(DATE_TIME);
		return fmt.format(date);
	}
	
	/**
	 * 按照指定的格式返回日期时间组合的字符串格式
	 * @param date
	 * @param format
	 * @return
	 */
	public static String getDateInfoString(Date date)
	{
		SimpleDateFormat fmt = getFmt(DATE_TIME_INFO);
		return fmt.format(date);
	}
	
	/**
	 * 获取当前时间的默认格式字符串
	 * @param date
	 * @param format
	 * @return
	 */
	public static String getCurrentDateString()
	{
		SimpleDateFormat fmt = getFmt(DATE_TIME);
		return fmt.format(new Date());
	}
	
	/**
	 * 获取当前时间的指定格式字符串
	 * @param date
	 * @param format
	 * @return
	 */
	public static String getCurrentDateString(String format)
	{
		SimpleDateFormat fmt = getFmt(format);
		return fmt.format(new Date());
	}
	
	/**
	 * 默认格式的返回日期类型
	 * @param datet
	 * @return
	 */
	public static Date getDate(String datet)
	{
		SimpleDateFormat fmt = getFmt(DATE_TIME);
		Date date = null;
		try
		{
			date = fmt.parse(datet);
		} catch (ParseException e)
		{
			
			log.debug("字符串转换成日期错误！ " + e.getMessage());
		}
		return date;
	}

	
	
	/**
	 * 指定格式返回日期类型
	 * @param datet
	 * @param format
	 * @return
	 */
	public static Date getDate(String datet, String format)
	{
		SimpleDateFormat fmt = getFmt(format);
		Date date = null;
		try
		{
			date = fmt.parse(datet);
		} catch (ParseException e)
		{
			
			log.debug("字符串转换成日期错误！ " + e.getMessage());
		}
		return date;
	}

	
	public static void main(String[] args)
	{
	 Date date=new Date();
         String str=DateUtil.getCurrentDateString(DateUtil.DATE_TIME_TZ);
         
         System.out.print(str);
          
	  
    
	
	
		
	}
	
	public static Date getDate(Date datet, String format)
	{
		SimpleDateFormat fmt = getFmt(format);
		Date result = null;
		try
		{
			result = fmt.parse(fmt.format(datet));
			
		} catch (ParseException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	

	
	public static Date getStartTimeOfDate(Date value){
		Calendar cal1=Calendar.getInstance();
		cal1.setTime(value);
		cal1.set(cal1.get(Calendar.YEAR), cal1.get(Calendar.MONTH), cal1.get(Calendar.DAY_OF_MONTH),0,0,0);
		return cal1.getTime();
	}
	
	public static Date getEndTimeOfDate(Date value){
		Calendar cal1=Calendar.getInstance();
		cal1.setTime(value);
		cal1.set(cal1.get(Calendar.YEAR), cal1.get(Calendar.MONTH), cal1.get(Calendar.DAY_OF_MONTH),23,59,59);
		return cal1.getTime();
	}
	
	public static String getMonthByStr(String str){
		String month=null;
		for(String s:monthMap.keySet()){
			if(s.toLowerCase().contains(str.toLowerCase())){
				month=monthMap.get(s);
				break;
			}
		}
		return month;
	}
   
}
