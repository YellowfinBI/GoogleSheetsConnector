package com.hof.util;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.WorksheetEntry;


public class GoogleSheetsWorksheetMetaData {
	
	private String spreadsheetTitle;
	private String worksheetTitle;
	URL worksheetListFeedURL;
	URL worksheetCellFeedURL;
	private final static Logger log = Logger.getLogger(GoogleSheetsWorksheetMetaData.class);
	
	public GoogleSheetsWorksheetMetaData()
	{
		spreadsheetTitle="";
		worksheetTitle="";
		worksheetListFeedURL=null;
		worksheetCellFeedURL=null;
	}
	
	public GoogleSheetsWorksheetMetaData(SpreadsheetEntry s, WorksheetEntry w)
	{
		spreadsheetTitle=s.getTitle().getPlainText();
		worksheetTitle=w.getTitle().getPlainText();
		worksheetListFeedURL=w.getListFeedUrl();
		worksheetCellFeedURL=w.getCellFeedUrl();
	}
	
	public GoogleSheetsWorksheetMetaData(JSONObject md)
	{
		if (md.has("spreadsheetTitle"))
		{
			spreadsheetTitle=md.getString("spreadsheetTitle");
		}
		else spreadsheetTitle="";
		
		if (md.has("worksheetTitle"))
		{
			worksheetTitle=md.getString("worksheetTitle");
		}
		else worksheetTitle="";
		
		if (md.has("worksheetTitle"))
		{
			worksheetTitle=md.getString("worksheetTitle");
		}
		else worksheetTitle="";
		
		if (md.has("worksheetListFeedURL"))
		{
			try {
				worksheetListFeedURL=new URL(md.getString("worksheetListFeedURL"));
			} catch (MalformedURLException e) {
				log.error(e.getMessage());
			} catch (JSONException e) {
				log.error(e.getMessage());
			}
		}
		else worksheetListFeedURL=null;
		
		if (md.has("worksheetCellFeedURL"))
		{
			try {
				worksheetCellFeedURL=new URL(md.getString("worksheetCellFeedURL"));
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				log.error(e.getMessage());
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				log.error(e.getMessage());
			}
		}
		else worksheetCellFeedURL=null;
	}
	
	public String getSpreadsheetTitle()
	{
		return spreadsheetTitle;
	}
	public String getWorksheetTitle()
	{
		return worksheetTitle;
	}
	public URL getWorksheetListFeedURL()
	{
		return worksheetListFeedURL;
	}
	public URL getWorksheetCellFeedURL()
	{
		return worksheetCellFeedURL;
	}
	
	public JSONObject getJSON()
	{
		JSONObject obj=new JSONObject();
		
		obj.put("spreadsheetTitle", spreadsheetTitle);
		obj.put("worksheetTitle", worksheetTitle);
		obj.put("worksheetListFeedURL", worksheetListFeedURL.toString());
		obj.put("worksheetCellFeedURL", worksheetCellFeedURL.toString());
				
		return obj;
	}
	
	

}
