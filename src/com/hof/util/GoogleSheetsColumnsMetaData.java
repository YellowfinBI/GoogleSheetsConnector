package com.hof.util;

import org.json.JSONObject;

import com.hof.mi.thirdparty.interfaces.DataType;

public class GoogleSheetsColumnsMetaData {
	
	private String columnName;
	private DataType dataType;
	private int columnNumber;
	
	public GoogleSheetsColumnsMetaData()
	{
		columnName="";
		dataType=null;
		columnNumber=0;
	}
	
	public GoogleSheetsColumnsMetaData(String name, DataType type, int number)
	{
		columnName=name;
		dataType=type;
		columnNumber=number;
	}
	
	public GoogleSheetsColumnsMetaData(JSONObject column)
	{
		if (column.has("columnName"))
		{
			columnName=column.getString("columnName");
		}
		else columnName="";
		
		if (column.has("dataType"))
		{
			if (column.getString("dataType").equals("NUMERIC"))
				dataType=DataType.NUMERIC;
			else if (column.getString("dataType").equals("DATE"))
				dataType=DataType.DATE;
			else if (column.getString("dataType").equals("TIMESTAMP"))
				dataType=DataType.TIMESTAMP;
			else dataType=DataType.TEXT;
		}
		else dataType=null;
		
		if (column.has("columnNumber"))
		{
			columnNumber=column.getInt("columnNumber");
		}
		else columnNumber=0;
	}
	
	public JSONObject getJSON()
	{
		JSONObject obj=new JSONObject();
		
		obj.put("columnName", columnName);
		obj.put("dataType", dataType);
		obj.put("columnNumber", columnNumber);
		
		return obj;
	}
	
	public String getColumnName()
	{
		return columnName;
	}
	
	public int getColumnNumber()
	{
		return columnNumber;
	}
	
	public DataType getDataType()
	{
		return dataType;
	}

}
