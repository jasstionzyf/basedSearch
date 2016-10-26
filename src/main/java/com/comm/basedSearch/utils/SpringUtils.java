package com.comm.basedSearch.utils;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SpringUtils {
	private static ApplicationContext appContext=null;
	//classpath*:conf/appContext.xml
	public static String defaultSpringConfigPath="classpath:spring/applicationContext.xml";
	public static Object getBeanFromBeanContainer(String name){
		return getAppContext().getBean(name);
		
	}
	public static Object getBeanFromBeanContainer(Class c){
		return getAppContext().getBean(c);
		
	}//初始化各种类型连接处理对象
	public static Object getBeanFromBeanContainer(String name,String appConfigFile){
		return getAppContext(appConfigFile).getBean(name);
		
	}
	public static Object getBeanFromBeanContainer(Class c,String appConfigFile){
		return getAppContext(appConfigFile).getBean(c);
		
	}
	private static ApplicationContext getAppContext(){
		appContext=appContext== null?new ClassPathXmlApplicationContext(defaultSpringConfigPath):appContext;
		return appContext;
		
		
	}
	private static ApplicationContext getAppContext(String appConfigFile){
		appContext=appContext== null?new ClassPathXmlApplicationContext(appConfigFile):appContext;
		return appContext;
		
		
	}
	
	
	
}
