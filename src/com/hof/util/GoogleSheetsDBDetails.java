package com.hof.util;

import java.util.Calendar;

import org.json.JSONObject;

public class GoogleSheetsDBDetails {
	
	private String worksheetName;
	private String dbKey;
	private long lastSaveTime;
	
	public GoogleSheetsDBDetails()
	{
		worksheetName="";
		dbKey="";
		lastSaveTime=0L;
	}
	
	public GoogleSheetsDBDetails(String wName)
	{
		worksheetName=wName;
		
		Calendar cal=Calendar.getInstance();
		String arr[]=wName.split(" - ");
		
		if(arr.length==2)
		{
			dbKey=arr[0].substring(0, 3)+arr[1].substring(0, 3)+cal.get(Calendar.YEAR)+(cal.get(Calendar.MONTH)+1)+cal.get(Calendar.DAY_OF_MONTH)+cal.get(Calendar.HOUR_OF_DAY)+cal.get(Calendar.MINUTE)+cal.get(Calendar.SECOND);
		}
		else
		{
			dbKey=wName+cal.get(Calendar.YEAR)+(cal.get(Calendar.MONTH)+1)+cal.get(Calendar.DAY_OF_MONTH)+cal.get(Calendar.HOUR_OF_DAY)+cal.get(Calendar.MINUTE)+cal.get(Calendar.SECOND);
		}
		
		lastSaveTime=cal.getTimeInMillis();
		
	}
	
	public GoogleSheetsDBDetails(String wName, String key)
	{
		worksheetName=wName;
		dbKey=key;
		
		Calendar cal=Calendar.getInstance();
		lastSaveTime=cal.getTimeInMillis();
	}
	
	public GoogleSheetsDBDetails(JSONObject dbDetails)
	{
		if (dbDetails.has("worksheetName"))
		{
			worksheetName=dbDetails.getString("worksheetName");
		}
		else worksheetName="";
		
		if (dbDetails.has("dbKey"))
		{
			dbKey=dbDetails.getString("dbKey");
		}
		else dbKey="";
		
		if (dbDetails.has("lastSaveTime"))
		{
			lastSaveTime=dbDetails.getLong("lastSaveTime");
		}
		else lastSaveTime=0L;
				
	}
	
	public JSONObject getJSON()
	{
		JSONObject obj=new JSONObject();
		
		obj.put("worksheetName", worksheetName);
		obj.put("dbKey", dbKey);
		obj.put("lastSaveTime", lastSaveTime);
		
		return obj;
	}
	
	public String getWorksheetName()
	{
		return worksheetName;
	}
	
	public String getDbKey()
	{
		return dbKey;
	}
	
	public long getLastSaveTime()
	{
		return lastSaveTime;
	}
	
	public void setLastSaveTime(long time)
	{
		lastSaveTime=time;
	}

}
