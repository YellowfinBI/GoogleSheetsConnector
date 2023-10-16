package com.hof.imp;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.SpreadsheetFeed;
import com.google.gdata.data.spreadsheet.WorksheetEntry;
import com.google.gdata.util.ServiceException;
import com.hof.jdbc.metadata.GoogleSheetsMetaData;
import com.hof.mi.thirdparty.interfaces.AbstractDataSet;
import com.hof.mi.thirdparty.interfaces.AbstractDataSource;
import com.hof.mi.thirdparty.interfaces.ScheduleDefinition;
import com.hof.mi.thirdparty.interfaces.ScheduleDefinition.FrequencyTypeCode;
import com.hof.pool.JDBCMetaData;
import com.hof.util.GoogleSheetsDataZoom;
import com.hof.util.GoogleSheetsWorksheet;
import com.hof.util.GoogleSheetsWorksheetMetaData;

public class GoogleSheetsDataSource extends AbstractDataSource {
	private final static Logger log = Logger.getLogger(GoogleSheetsDataSource.class);
	private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();
	
	
	
	@Override
	public String getDataSourceName() {
		
		return GoogleSheetsDataZoom.getText("Google Sheets", "mi.text.gs.datasource.name");
		
	}
	
	
	@Override
	public ScheduleDefinition getScheduleDefinition() { 
		return new ScheduleDefinition(FrequencyTypeCode.MINUTES, null, 1); 
	};
	
	
	@Override
	public ArrayList<AbstractDataSet> getDataSets() {
		/*AbstractDataSet simpleDataSet = new AbstractDataSet() {
			
			public ArrayList<FilterMetaData> getFilters() {
				
				ArrayList<FilterMetaData> fm = new ArrayList<FilterMetaData>();
				
				fm.add(new FilterMetaData("FilterA", DataType.INTEGER, true));
				
				return fm;
				
			}
			
			
			public String getDataSetName() {
				
				return "TestData";
								
			}
			
			public ArrayList<ColumnMetaData> getColumns() {
				
				ArrayList<ColumnMetaData> cm = new ArrayList<ColumnMetaData>();
				
				cm.add(new ColumnMetaData("ColumnA", DataType.TEXT));
				cm.add(new ColumnMetaData("ColumnB", DataType.TEXT));
				cm.add(new ColumnMetaData("ColumnC", DataType.TEXT));
				cm.add(new ColumnMetaData("ColumnD", DataType.INTEGER));
				
				return cm;
			}
			
			
			public Object[][] execute(List<ColumnMetaData> columns, List<FilterData> filters) {
				
				FilterData filter = findFilter("FilterA", filters);
				
				Integer p = 0; 
				if (filter!=null && filter.getFilterValue()!=null) {
					if (filter.getFilterValue() instanceof Integer) {
						p = (Integer) filter.getFilterValue();
					} else {
						try { 
							p = Integer.parseInt((String) filter.getFilterValue());
						} catch (Exception e) {}
					}
				}
				
				String date = null;
				byte[] b = loadBlob("LASTRUN");
				if (b!=null) date = new String(b); 
				
				return new Object[][] { 
						new Object[] { "a", "b", getAttribute("PARAMETER1"), p * 1 }, 
						new Object[] { "d", "e", date , p * 2 } };
			}


			@Override
			public boolean getAllowsDuplicateColumns() {
				return false;
			}


			@Override
			public boolean getAllowsAggregateColumns() {
				return false;
			}


			
		};*/
		
		//System.out.println("Entered getDatasets() funtion!!!");
		ArrayList<AbstractDataSet> dataSets = new ArrayList<AbstractDataSet>();
		SpreadsheetService service = new SpreadsheetService("Yellowfin");
		service.setAuthSubToken(getAccessToken());
		
		//System.out.println("Defined service");
		
		JSONArray jsonTables;
		int i;
		
		if (areBlobsAvailable())
		{
			//System.out.println("Blobs were available, so started caching");
			cacheTables();
			//System.out.println("Caching ended, started retrieving datasets from storage");
			jsonTables=new JSONArray(new String(loadBlob("AVAILABLE_TABLES")));
			//System.out.println("Datasets retrieved from storage and the size is: "+jsonTables.length());
			
		}
		else
		{
			//System.out.println("Blobs were not available, so trying to show tables live");
			jsonTables=new JSONArray(getTablesJSON());
			//System.out.println("Finished downloading the tables and the size of array is: "+jsonTables.length());
		}
		
		
		//System.out.println("Creating datasetes from json array");
		for (i=0; i<jsonTables.length(); i++)
		{
			
			
			JSONObject obj=jsonTables.getJSONObject(i);
			GoogleSheetsWorksheetMetaData md=new GoogleSheetsWorksheetMetaData(obj);
			//System.out.println(md.getSpreadsheetTitle()+" - "+md.getWorksheetTitle());
			dataSets.add(new GoogleSheetsWorksheet(md.getSpreadsheetTitle()+" - "+md.getWorksheetTitle(), md.getWorksheetListFeedURL(), md.getWorksheetCellFeedURL(), service, (String)getAttribute("DATACONVERSION"), Integer.valueOf((String)getAttribute("LINESQUANTITY")), this));
		}
		
		//System.out.println("Just before returning datasets. The size of datasets is: "+dataSets.size());
		
		return dataSets;
		
	}

	public void saveData(String key, byte[] data ) 
	{
		saveBlob(key, data);
	}
	
	public byte[] getData(String key)
	{
		return loadBlob(key); 
	}
	private void cacheTables()
	{
		
			//System.out.println("Entered cacheTables");
			java.util.Date curDt=new java.util.Date();
			java.util.Date lastSave;
			if (loadBlob("AVAILABLE_TABLES_SAVE_TIME")!=null)
			{
				lastSave=new java.util.Date(Long.valueOf(new String(loadBlob("AVAILABLE_TABLES_SAVE_TIME"))));
			}
			else lastSave=new Date(0L);
			
			long diff = curDt.getTime() - lastSave.getTime();
			long secsDiff=diff / (1000);
			
			/*for (int i=0;i<10;i++)
			{
				System.out.println(secsDiff);
			}*/
			if (secsDiff>=60)
			{
				/*for (int i=0;i<10;i++)
				{
					System.out.println("It was longer than 60 sec");
				}*/
				String arr=getTablesJSON();
			   //System.out.println(arr);
			   //saveBlob("AVAILABLE_TABLES", "[]".getBytes());
			   saveBlob("AVAILABLE_TABLES", arr.getBytes());
			   saveBlob("AVAILABLE_TABLES_SAVE_TIME", String.valueOf(new java.util.Date().getTime()).getBytes());
			}
		
		
	}
	
	private String getTablesJSON()
	{
		try
		{
			
			GoogleCredential cr;
			cr = new GoogleCredential.Builder()
			.setTransport(HTTP_TRANSPORT)
			.setJsonFactory(JSON_FACTORY)
			.setClientSecrets(new String(com.hof.util.Base64.decode(GoogleSheetsDataZoom.getData())), new String(com.hof.util.Base64.decode(GoogleSheetsDataZoom.getZoom())))
			.build()
			//.setAccessToken(getAccessToken())
			.setRefreshToken(getRefreshToken());
			
			String aToken;
			if(cr.refreshToken())
			{
				aToken=cr.getAccessToken();
				//System.out.println("Token successfully refreshed in getTables()");
			}
			else 
			{
				//System.out.println("Could not update token in getTables(), so used the existing one");
				aToken=getAccessToken();
			}
			
			SpreadsheetService service = new SpreadsheetService("Yellowfin");
			URL SPREADSHEET_FEED_URL;
			
			SPREADSHEET_FEED_URL = new URL("https://spreadsheets.google.com/feeds/spreadsheets/private/full");
			
			service.setAuthSubToken(aToken);
			SpreadsheetFeed feed = service.getFeed(SPREADSHEET_FEED_URL, SpreadsheetFeed.class);
			List<SpreadsheetEntry> spreadsheets = feed.getEntries();
		    String arr="[";
		    for (SpreadsheetEntry spreadsheet : spreadsheets) 
		    {
		        List<WorksheetEntry> worksheets = spreadsheet.getWorksheets();
		        for (WorksheetEntry w:worksheets)
		        {
		        	GoogleSheetsWorksheetMetaData md=new GoogleSheetsWorksheetMetaData(spreadsheet, w);
		        	arr=arr+md.getJSON()+",";
		        }
		    }
		   arr=arr+"]";
		   return arr;
	} catch (MalformedURLException e) {
	log.error(e.getMessage());
} catch (IOException e) {
	
	log.error(e.getMessage());
} catch (ServiceException e) {
	
	log.error(e.getMessage());
}
		return "[]";
	}
	
	private String getAccessToken() {
		String accessToken;
		if (loadBlob("ACCESS_TOKEN")!=null)
		{
			//System.out.println("ACCESS_TOKEN was available from repository");
			accessToken=new String(loadBlob("ACCESS_TOKEN"));
		}
		
		else 
		{
			//System.out.println("Seems that value at ACCESS_TOKEN in blobs is null");
			accessToken=(String) getAttribute("ACCESS_TOKEN");
		}
		return accessToken;
	}
	
	private String getRefreshToken() {
		String refreshToken;
		if (loadBlob("REFRESH_TOKEN")!=null)
		{
			refreshToken=new String(loadBlob("REFRESH_TOKEN"));
		}
		
		else 
		{
			refreshToken=(String) getAttribute("REFRESH_TOKEN");
		}
		return refreshToken;
	}


	@Override
	public JDBCMetaData getDataSourceMetaData() {
		return new GoogleSheetsMetaData();
	}


	@Override
	public boolean authenticate() 
	{
		return true;
	}
	
	@Override
	public void disconnect(){
		
		
	}
	
	

	@Override
	public Map<String,Object> testConnection(){
		try
		{
			Map<String,Object> p = new HashMap<String, Object>();
			SpreadsheetService service = new SpreadsheetService("Yellowfin");
			URL SPREADSHEET_FEED_URL;
			
			SPREADSHEET_FEED_URL = new URL("https://spreadsheets.google.com/feeds/spreadsheets/private/full");
			
			service.setAuthSubToken(getAccessToken());
			SpreadsheetFeed feed = service.getFeed(SPREADSHEET_FEED_URL, SpreadsheetFeed.class);
			List<SpreadsheetEntry> spreadsheets = feed.getEntries();
			p.put(GoogleSheetsDataZoom.getText("Service Version", "mi.text.gs.testconnection.parameters.serviceversion"), service.getServiceVersion());
			p.put(GoogleSheetsDataZoom.getText("Number of Spreadsheets", "mi.text.gs.testconnection.parameters.numberofspreadsheets"), spreadsheets.size());
			//p.put("All good", "All good");
		  
		   return p;
		   }
		
catch (MalformedURLException e) {
	Map<String,Object> p =new HashMap<String, Object>();
	p.put("ERROR", e.getMessage());
	return p;
} catch (IOException e) {
	Map<String,Object> p =new HashMap<String, Object>();
	p.put("ERROR", e.getMessage());
	return p;
} catch (ServiceException e) {
	Map<String,Object> p =new HashMap<String, Object>();
	p.put("ERROR", e.getMessage());
	return p;
}
	}
	
	
	
	@Override
	public boolean autoRun(){	
		cacheTables();
		cacheTablesMetadata();
		if (loadBlob("LASTRUN")!=null)
		{
			java.util.Date curDt=new java.util.Date();
			java.util.Date lastrun=new java.util.Date(Long.valueOf(new String(loadBlob("LASTRUN"))));
			long diff = curDt.getTime() - lastrun.getTime();
			double minsDiff=diff / (60 * 1000);
			//System.out.println("minsDiff: "+minsDiff);
			if (minsDiff>=50)
			{
				//System.out.println("Access Token: "+getAccessToken());
				//System.out.println("Refresh Token: "+getRefreshToken());
				try {
					//System.out.println("Trying to update tokens");
					//System.out.println("Access Token before update: "+getAccessToken());
					//System.out.println("Refresh Token before update: : "+getRefreshToken());
					GoogleCredential cr;
					cr = new GoogleCredential.Builder()
					.setTransport(HTTP_TRANSPORT)
					.setJsonFactory(JSON_FACTORY)
					.setClientSecrets(new String(com.hof.util.Base64.decode(GoogleSheetsDataZoom.getData())), new String(com.hof.util.Base64.decode(GoogleSheetsDataZoom.getZoom())))
					.build()
					.setAccessToken(getAccessToken())
					.setRefreshToken(getRefreshToken());
					
					
					
					if (cr.refreshToken())
					{
						saveBlob("ACCESS_TOKEN", cr.getAccessToken().getBytes());
						saveBlob("REFRESH_TOKEN", cr.getRefreshToken().getBytes());
						saveBlob("LASTRUN", String.valueOf(new java.util.Date().getTime()).getBytes());
						//System.out.println("Access Token Updated Successfully");
						//System.out.println("Access Token after update: "+getAccessToken());
						//System.out.println("Refresh Token after update: : "+getRefreshToken());
						log.info("Access Token Updated Successfully");
						return true;
					}
					
					else
					{
						//System.out.println("Could not update token!");
						log.error("Could not update token!");
						return false;
					}
					
				}
				
				catch (IOException e) 
				{
					log.error("Could not update token!");
					return false;
				}
				
				
				
			}
			
			
			return true;
		}
		
		else
		{
			saveBlob("LASTRUN", String.valueOf(new java.util.Date().getTime()).getBytes());
			return true;
		}
	}


	private void cacheTablesMetadata() 
	{
		
	}
}
