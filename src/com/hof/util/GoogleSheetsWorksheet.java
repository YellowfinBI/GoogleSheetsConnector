package com.hof.util;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import org.apache.log4j.Logger;
import org.json.JSONArray;

import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.data.spreadsheet.CellEntry;
import com.google.gdata.data.spreadsheet.CellFeed;
import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.data.spreadsheet.ListFeed;
import com.google.gdata.util.ServiceException;
import com.hof.imp.GoogleSheetsDataSource;
import com.hof.mi.thirdparty.interfaces.AbstractDataSet;
import com.hof.mi.thirdparty.interfaces.AggregationType;
import com.hof.mi.thirdparty.interfaces.ColumnMetaData;
import com.hof.mi.thirdparty.interfaces.DataType;
import com.hof.mi.thirdparty.interfaces.FilterData;
import com.hof.mi.thirdparty.interfaces.FilterMetaData;
import com.hof.mi.thirdparty.interfaces.FilterOperator;
import com.hof.mi.thirdparty.interfaces.ThirdPartyException;


public class GoogleSheetsWorksheet extends AbstractDataSet{

	private String name;
	private URL listFeedURL;
	private URL cellFeedURL;
	private SpreadsheetService service;
	private String dataCon;
	private Integer linesQ;
	private GoogleSheetsDataSource datasource;
	private final static Logger log = Logger.getLogger(GoogleSheetsWorksheet.class);
	public GoogleSheetsWorksheet(String nm, URL lurl, URL curl, SpreadsheetService ser, String dataConversionType, Integer linesQuantity, GoogleSheetsDataSource source)
	{
		name=nm;
		listFeedURL=lurl;
		cellFeedURL=curl;
		service=ser;
		dataCon=dataConversionType;
		linesQ=linesQuantity;
		datasource=source;
		/*System.out.println("-----------------------------------------------------");
		System.out.println("Name: "+name);
		System.out.println("lfeed: "+listFeedURL);
		System.out.println("cfeed: "+cellFeedURL);
		System.out.println("-----------------------------------------------------");*/
	}
	
	@Override
	public Object[][] execute(List<ColumnMetaData> columns, List<FilterData> filters) {
		try 
		{
			Object[][] data=new Object[1][1];
			HashMap<String, Integer> columnMapping=new HashMap<String, Integer>();
			if (datasource.getData(getDBKey(name))==null)
			{
				return null;
			}
			
			JSONArray columnsMetaData=new JSONArray(new String(datasource.getData(getDBKey(name))));
			
			int i;
			for (i=0; i<columnsMetaData.length(); i++)
			{
				GoogleSheetsColumnsMetaData cmd=new GoogleSheetsColumnsMetaData(columnsMetaData.getJSONObject(i));
				columnMapping.put(cmd.getColumnName(), cmd.getColumnNumber());
			}
			ArrayList<String> filtersList=getSheetsFilters(filters);
			
			String filterString="";
						
			if (filtersList.size()>0)
			{
				filterString=filterString+"("+filtersList.get(0)+")";
				
				for (i=1; i<filtersList.size(); i++)
				{
					filterString=filterString+" and ("+filtersList.get(i)+")";
				}
			}
			
			filterString="?sq=("+URLEncoder.encode(filterString, "UTF-8")+")";
			
			/*for (i=0; i<10; i++)
			{
				System.out.println("Filters Size: "+filters.size());
				System.out.println("Filters String: "+filterString);
			}*/
			
			if (!filterString.equals("?sq=()"))
			{
				try {
					/*for(i=0; i<10; i++)
					{
						System.out.println(listFeedURL.toString()+filterString);
					}*/
					listFeedURL = new URI(listFeedURL.toString()
						    + filterString).toURL();
				} catch (URISyntaxException e1) {
					throw new ThirdPartyException(e1.getMessage());
				}
			}
			
			ListFeed listFeed = service.getFeed(listFeedURL, ListFeed.class);
            ArrayList<Object[]>table=new ArrayList<Object[]>();
            
            for (ListEntry row : listFeed.getEntries())
            {
            	Object[] r=new Object[row.getCustomElements().getTags().size()];
            	i=0;
               	for (String tag : row.getCustomElements().getTags()) 
            	{
            		r[i]=row.getCustomElements().getValue(tag);
            		i++;
            		//System.out.print(row.getCustomElements().getValue(tag) + "\t");
            	}
               	table.add(r);
            }
            
            int j;
            data=new Object[table.size()][columns.size()];
			for (i=0; i<table.size(); i++)
			{
				for (j=0; j<columns.size(); j++)
				{
					int index;
					if (columnMapping.containsKey(columns.get(j).getColumnName()))
					{
						index=columnMapping.get(columns.get(j).getColumnName())-1;
					}
					
					else index=0;
					
					if (columns.get(j).getColumnType().equals(DataType.NUMERIC))
					{
						try
						{
							data[i][j]=Double.valueOf((String)table.get(i)[index]);
						}
						
						catch(Exception e)
						{
							data[i][j]=null;
						}
						
						
					}
					
					/*if (columns.get(j).getColumnType().equals(DataType.INTEGER))
					{
						try
						{
							data[i][j]=Integer.valueOf((String)table.get(i)[index]);
						}
						
						catch(Exception e)
						{
							data[i][j]=null;
						}
						
						
					}*/
					
					
					else if (columns.get(j).getColumnType().equals(DataType.TIMESTAMP))
					{
						try {
							String dtFormat=getTimestampFormat((String)table.get(i)[index]);
							if (dtFormat!=null)
							{
								DateFormat df= new SimpleDateFormat(dtFormat, Locale.ENGLISH);
								java.util.Date dt;
								dt = df.parse((String)table.get(i)[index]);
								data[i][j]=new java.sql.Timestamp(dt.getTime());
							}
							
							else data[i][j]=null;
							
						} catch (ParseException e) {
							data[i][j]=null;
						}
					}
					
					else if (columns.get(j).getColumnType().equals(DataType.DATE))
					{
						try {
							String dtFormat=getDateFormat((String)table.get(i)[index]);
							
							if (dtFormat!=null)
							{
								DateFormat df= new SimpleDateFormat(dtFormat, Locale.ENGLISH);
								java.util.Date dt;
								dt = df.parse((String)table.get(i)[index]);
								data[i][j]=new java.sql.Date(dt.getTime());
							}
							
							else data[i][j]=null;
							
						} catch (ParseException e) {
							data[i][j]=null;
						}
					}
					
					else if (columns.get(j).getColumnType().equals(DataType.TEXT))
					{
						data[i][j]=table.get(i)[index];
					}
					
				}
			}
			
			return data;
			
        
		} catch (IOException e) {
			log.error(e.getMessage());
		} catch (ServiceException e) {
			log.error(e.getMessage());
		}
		
          return null;
	}

	@Override
	public boolean getAllowsAggregateColumns() {
		return false;
	}

	@Override
	public boolean getAllowsDuplicateColumns() {
		return false;
	}

	@Override
	public List<ColumnMetaData> getColumns() {
		
		AggregationType[] aggT;
		aggT=new AggregationType[2];
		aggT[0]=AggregationType.COUNT;
		aggT[1]=AggregationType.COUNTDISTINCT;
		
		AggregationType[] agg;
		agg=new AggregationType[6];
		agg[0]=AggregationType.COUNT;
		agg[1]=AggregationType.COUNTDISTINCT;
		agg[2]=AggregationType.MAX;
		agg[3]=AggregationType.MIN;
		agg[4]=AggregationType.AVG;
		agg[5]=AggregationType.SUM;
		
		FilterOperator fo[]={FilterOperator.EQUAL
							, FilterOperator.BETWEEN
							, FilterOperator.GREATER
							, FilterOperator.GREATEREQUAL
							, FilterOperator.INLIST
							, FilterOperator.ISNOTNULL
							, FilterOperator.ISNULL
							, FilterOperator.LESS
							, FilterOperator.LESSEQUAL
							, FilterOperator.NOTEQUAL
							, FilterOperator.NOTINLIST};
		
		FilterOperator foT[]={FilterOperator.BETWEEN
							, FilterOperator.CONTAINS
							, FilterOperator.ENDSWITH
							, FilterOperator.EQUAL
							, FilterOperator.GREATER
							, FilterOperator.GREATEREQUAL
							, FilterOperator.INLIST
							, FilterOperator.ISEMPTYSTRING
							, FilterOperator.ISNOTEMPTYSTRING
							, FilterOperator.ISNOTNULL
							, FilterOperator.ISNULL
							, FilterOperator.LESS
							, FilterOperator.LESSEQUAL
							, FilterOperator.NOTCONTAINS
							, FilterOperator.NOTENDSWITH
							, FilterOperator.NOTEQUAL
							, FilterOperator.NOTINLIST
							, FilterOperator.NOTSTARTSWITH
							, FilterOperator.STARTSWITH};
			
		
		cacheColumns(name);
		
		
		JSONArray columnsMetaData=new JSONArray(new String(datasource.getData(getDBKey(name))));
		
		ArrayList<ColumnMetaData> cm = new ArrayList<ColumnMetaData>();
		int i;
		for (i=0; i<columnsMetaData.length(); i++)
		{
			GoogleSheetsColumnsMetaData cmd=new GoogleSheetsColumnsMetaData(columnsMetaData.getJSONObject(i));
			if (cmd.getDataType().equals(DataType.DATE) || cmd.getDataType().equals(DataType.TIMESTAMP) || cmd.getDataType().equals(DataType.TEXT))
			{
				cm.add(new ColumnMetaData(cmd.getColumnName(),cmd.getDataType(), null, aggT, foT));
			}
			else if (cmd.getDataType().equals(DataType.NUMERIC))
			{
				cm.add(new ColumnMetaData(cmd.getColumnName(),cmd.getDataType(), null, agg, fo));
			}
		}
		
		return cm;
        		
	}	
	
	private void cacheColumns(String wName)
	{
		try 
		{
			java.util.Date curDt=new java.util.Date();
			long lastSave;
			lastSave=getLastSaveTime(wName);
			
			long diff = curDt.getTime() - lastSave;
			long secsDiff=diff / (1000);
			
			if (secsDiff>=120L)
			{
				URL tmpCellFeedUrl = new URI(cellFeedURL.toString()+"?min-row=1&max-row=1").toURL();
				CellFeed cellFeed = service.getFeed(tmpCellFeedUrl, CellFeed.class);
				
				int i=1;
				String arr="[";
				for (CellEntry cell : cellFeed.getEntries())
				{
					GoogleSheetsColumnsMetaData cmd=new GoogleSheetsColumnsMetaData(cell.getCell().getValue(), GetDataType(i), i);
					arr=arr+cmd.getJSON()+",";
					i++;
				}
				
				arr=arr+"]";
				
				if (getDBKey(wName)==null)
				{
					updateSaveTime(wName);
				}
				datasource.saveData(getDBKey(wName), arr.getBytes());
				updateSaveTime(wName);			
				
				ArrayList<GoogleSheetsDBDetails> db=getDBDetails();
				/*for (GoogleSheetsDBDetails d:db)
				{
					System.out.println(d.getWorksheetName()+"\t"+d.getDbKey()+"\t"+d.getLastSaveTime());
				}*/
			}
    
		} catch (IOException e) {
			log.error(e.getMessage());
		} catch (ServiceException e) {
			log.error(e.getMessage());
		} catch (URISyntaxException e) {
			log.error(e.getMessage());
		}
		//return null;
	}
	
	public void updateSaveTime(String worksheetName)
	{
		boolean worksheetWasInList=false;
		ArrayList<GoogleSheetsDBDetails> dbDetails=getDBDetails();
		ArrayList<GoogleSheetsDBDetails> dbDetailsUpdated=new ArrayList<GoogleSheetsDBDetails>();
		
		if(dbDetails!=null)
		{
			for (GoogleSheetsDBDetails dbDet:dbDetails)
			{
				if (dbDet.getWorksheetName().equals(worksheetName))
				{
					Calendar cal=Calendar.getInstance();
					dbDet.setLastSaveTime(cal.getTimeInMillis());
					dbDetailsUpdated.add(dbDet);
					worksheetWasInList=true;
				}
				else dbDetailsUpdated.add(dbDet);
			}
			
			if(!worksheetWasInList)
			{
				dbDetailsUpdated.add(new GoogleSheetsDBDetails(worksheetName));
			}
			setDBDetails(dbDetailsUpdated);
		}
		else
		{
			dbDetailsUpdated.add(new GoogleSheetsDBDetails(worksheetName));
			setDBDetails(dbDetailsUpdated);
		}
		
	}
	
	public long getLastSaveTime(String worksheetName)
	{
		ArrayList<GoogleSheetsDBDetails> dbDetails=getDBDetails();
		
		if(dbDetails!=null)
		{
			for (GoogleSheetsDBDetails dbDet:dbDetails)
			{
				if (dbDet.getWorksheetName().equals(worksheetName))
				{
					return dbDet.getLastSaveTime();
				}
			}
		}
		else
		{
			return 0L;
		}
		
		return 0L;
	}
	
	public String getDBKey(String worksheetName)
	{
		ArrayList<GoogleSheetsDBDetails> dbDetails=getDBDetails();
		
		if(dbDetails!=null)
		{
			for (GoogleSheetsDBDetails dbDet:dbDetails)
			{
				if (dbDet.getWorksheetName().equals(worksheetName))
				{
					return dbDet.getDbKey();
				}
			}
		}
		else
		{
			return null;
		}
		
		return null;
	}

	public void setDBDetails(ArrayList<GoogleSheetsDBDetails> dbDetails)
	{
		String jArr="[";
		for (GoogleSheetsDBDetails det:dbDetails)
		{
			jArr=jArr+det.getJSON()+",";
		}
		jArr=jArr+"]";
		datasource.saveData("WORKSHEETS_DB_DETAILS", jArr.getBytes());		
	}

	public ArrayList<GoogleSheetsDBDetails> getDBDetails() 
	{
		if (datasource.getData("WORKSHEETS_DB_DETAILS")!=null)
		{
			ArrayList<GoogleSheetsDBDetails> details=new ArrayList<GoogleSheetsDBDetails>();
			JSONArray arr=new JSONArray(new String(datasource.getData("WORKSHEETS_DB_DETAILS")));
			int i;
			for (i=0; i<arr.length(); i++)
			{
				details.add(new GoogleSheetsDBDetails(arr.getJSONObject(i)));
			}
			
			return details;
		}
		
		else return null;
	}

	private DataType GetDataType(int col) 
	{
		try 
		{
			URL tmpCellFeedUrl;
			HashMap<DataType, Integer> dataTypeStat=new HashMap<DataType, Integer>();
			if (linesQ>0)
			{
				tmpCellFeedUrl = new URI(cellFeedURL.toString()+"?min-row=2&max-row="+linesQ+"&min-col="+col+"&max-col="+col).toURL();
			}
			else
			{
				tmpCellFeedUrl = new URI(cellFeedURL.toString()+"?min-row=2&min-col="+col+"&max-col="+col).toURL();
			}
			
			CellFeed cellFeed = service.getFeed(tmpCellFeedUrl, CellFeed.class);

			
			for (CellEntry cell : cellFeed.getEntries())
			{
				DataType tp=getType(cell.getCell().getValue());
				if (dataTypeStat.containsKey(tp))
				{
					dataTypeStat.put(tp, dataTypeStat.get(tp)+1);
				}
				
				else dataTypeStat.put(tp, 1);
				
				
			}
			
			
			if (dataCon.equals("YES"))
			{
				if (dataTypeStat.keySet().size()>1)
				{
					DataType ret=(DataType)dataTypeStat.keySet().toArray()[0];
					int q=dataTypeStat.get(ret);
					
					for (DataType t:dataTypeStat.keySet())
					{
						if (dataTypeStat.get(t)>q)
						{
							ret=t;
							q=dataTypeStat.get(t);
						}
					}
					
					return ret;
				}
				
				else if (dataTypeStat.keySet().size()==1) return (DataType)dataTypeStat.keySet().toArray()[0];
				
				else return DataType.TEXT;
			}
			
			else if (dataCon.equals("NO"))
			{
				if (dataTypeStat.keySet().size()>1)
				{
					return DataType.TEXT;
				}
				
				else if (dataTypeStat.keySet().size()==1) return (DataType)dataTypeStat.keySet().toArray()[0];
				
				else return DataType.TEXT;
			}
		
			return DataType.TEXT;
		
		} catch (MalformedURLException e) {
			log.error(e.getMessage());
		} catch (URISyntaxException e) {
			log.error(e.getMessage());
		} catch (IOException e) {
			log.error(e.getMessage());
		} catch (ServiceException e) {
			log.error(e.getMessage());
		}
		
		return DataType.TEXT;

	}
	
	
	private DataType getType(String val)
	{
		if (getTimestampFormat(val)!=null)
		{
			return DataType.TIMESTAMP;
		}
		
		else if (getDateFormat(val)!=null)
		{
			return DataType.DATE;
		}
		
		try
		{
			Double vl=Double.valueOf(val);
			return DataType.NUMERIC;			
		}
		
		catch(Exception e)
		{
			
		}
		
			
		
		return DataType.TEXT;
		
	}

	private String getTimestampFormat(String val) {
		String [] templates=
			{
                /*"dd/MM HH:mm"
				, "HH:mm"
				, "H:mm:ss a"
				, "H:mm a"
				, "dd/MM HH:mm"
				, "dd.MMMM.yyyy HH:mm:ss"
				, "dd/MMMM/yyyy HH:mm:ss"
				, "dd-MMMM-yyyy HH:mm:ss"
				, "dd MMMM yyyy HH:mm:ss"
				, "MMMM dd yyyy HH:mm:ss"
				//, "EEEE, MMMM d, yyyy at H:mm:ss a"
				, "EEEE, MMMM d, yyyy, H:mm:ss a"*/
				"yyyy-MM-dd HH:mm:ss"
				/*, "yyyy/MM/dd HH:mm:ss"
				, "dd.MM.yyyy HH:mm:ss"
				, "dd/MM/yyyy HH:mm:ss"
				, "dd-MM-yyyy HH:mm:ss"
				, "yyyy-MMMM-dd HH:mm:ss"
				, "yyyy/MMMM/dd HH:mm:ss"
				
				
				, "yyyy-MM-dd H:mm:ss a"
				, "yyyy/MM/dd H:mm:ss a"
				, "dd.MM.yyyy H:mm:ss a"
				, "dd/MM/yyyy H:mm:ss a"
				, "dd-MM-yyyy H:mm:ss a"
				, "yyyy-MMMM-dd H:mm:ss a"
				, "yyyy/MMMM/dd H:mm:ss a"
				, "dd.MMMM.yyyy H:mm:ss a"
				, "dd/MMMM/yyyy H:mm:ss a"
				, "dd-MMMM-yyyy H:mm:ss a"
				, "dd MMMM yyyy H:mm:ss a"
				, "MMMM dd yyyy H:mm:ss a"*/
				
		};
		int i;
		for (i=0;i<templates.length;i++)
		{
			DateFormat df= new SimpleDateFormat(templates[i], Locale.ENGLISH);
		    try {
				java.util.Date result=df.parse(val);
				return templates[i];
			} catch (ParseException e) {
				
			}

		}
				
		return null;
	}
	
	
	private String getDateFormat(String val) {
		String [] templates=
			{ 
                /*"dd/MM"
				, "EEEE, MMMM d, yyyy"*/
				"yyyy-MM-dd"
				/*, "yyyy/MM/dd"
				, "dd.MMMM.yyyy"
				, "dd/MMMM/yyyy"
				, "dd-MMMM-yyyy"
				, "dd MMMM yyyy"
				, "MMMM dd yyyy"				
				, "dd.MM.yyyy"
				, "dd/MM/yyyy"
				, "dd-MM-yyyy"
				, "yyyy-MMMM-dd"
				, "yyyy/MMMM/dd"*/
				
		};
		int i;
		if(val==null || val.equals(""))
		{
			return null;
		}
		for (i=0;i<templates.length;i++)
		{
			DateFormat df= new SimpleDateFormat(templates[i], Locale.ENGLISH);
		    try {
				java.util.Date result=df.parse(val);
				return templates[i];
			} catch (ParseException e) {
				
			}

		}
				
		return null;
	}


	@Override
	public String getDataSetName() {
		return name;
	}

	@Override
	public List<FilterMetaData> getFilters() {
		return null;
	}
	
	private String formatColumnName(String columnName)
	{
		columnName=columnName.replaceAll("[^A-Za-z0-9]", "");
		columnName=columnName.toLowerCase();
		
		return columnName;
	}
	
	private ArrayList<String> getSheetsFilters(List<FilterData> filters) 
	{
		ArrayList<String> filtersSt=new ArrayList<String>();
		int i;
		for (i = 0; i < filters.size(); i++) 
		{
			FilterData fData=filters.get(i);
			String columnName="";
			
			columnName=fData.getFilterName();
			
			filtersSt.add(getQueryForFilter(fData, formatColumnName(columnName)));
			
								
		}
						
		
		return filtersSt;
	}
	
	private String getQueryForFilter(FilterData fData, String columnName)
	{
		String q="";
		
		if (fData.getFilterOperator().equals(FilterOperator.EQUAL))
		{
			if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
			{
				q=columnName+"=\""+fData.getFilterValue()+"\"";
			}
			
			else if(fData.getFilterMetaData().getFilterType().equals(DataType.DATE))
			{
				Date dt;
				dt=(Date)fData.getFilterValue();
				Calendar cal=Calendar.getInstance();
				cal.setTime(dt);				
				q=columnName+"="+fData.getFilterValue();
				//q=columnName+"=DATE("+cal.get(Calendar.YEAR)+","+(cal.get(Calendar.MONTH)+1)+","+cal.get(Calendar.DAY_OF_MONTH)+")";
				//q=columnName+"="+cal.get(Calendar.YEAR)+"-"+(cal.get(Calendar.MONTH)+1)+"-"+cal.get(Calendar.DAY_OF_MONTH);
			}
			
			else q=columnName+"="+fData.getFilterValue();
			
			int i;
			/*for (i=0;i<10;i++)
				System.out.println(q);*/
			return q;
		}
		
		if (fData.getFilterOperator().equals(FilterOperator.NOTEQUAL))
		{
			if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
			{
				q=columnName+"!=\""+fData.getFilterValue()+"\"";
			}
			
			else q=columnName+"!="+fData.getFilterValue();
			
			return q;
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.BETWEEN))
		{
			List<Object> values;
			try
			{
				if (fData.getFilterValue() instanceof List<?>)
				{
					values=(List<Object>)fData.getFilterValue();
				}
				else throw new ThirdPartyException(GoogleSheetsDataZoom.getText(GoogleSheetsDataZoom.getText("Bad filter values received", "mi.text.gs.datasets.error.message1"), "mi.text.gs.datasets.error.message1"));
				
			
				Object MIN, MAX;
				
				MIN=values.get(0);
				MAX=values.get(1);
				
				//System.out.println(MIN+"\t"+MAX);
				
				if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
				{
					q=columnName+">=\""+MIN+"\" and "+columnName+"<=\""+MAX+"\"";
				}
				
				else q=columnName+">="+MIN+" and "+columnName+"<="+MAX;
				
				return q;
			}
			catch(Exception e)
			{
				throw new ThirdPartyException(GoogleSheetsDataZoom.getText("Bad filter values received", "mi.text.gs.datasets.error.message1"));
			}
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.INLIST))
		{
			List<Object> values;
			if (fData.getFilterValue() instanceof List<?>)
			{
				values=(List<Object>)fData.getFilterValue();
			}
			else throw new ThirdPartyException(GoogleSheetsDataZoom.getText("Bad filter values received", "mi.text.gs.datasets.error.message1"));
			String subQ="";
			if (values.size()>0)
			{
				if(fData.getFilterMetaData().getFilterType().equals(DataType.DATE))
				{
					DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
					Date dt;
					try {
						dt = format.parse((String)values.get(0));
					} catch (ParseException e) {
						throw new ThirdPartyException(GoogleSheetsDataZoom.getText("Bad filter format", "mi.text.gs.datasets.error.message2"));
					}
					
					
					int j;
					Calendar cal=Calendar.getInstance();
					cal.setTime(dt);
					subQ=subQ+columnName+"="+cal.get(Calendar.YEAR)+"-"+(cal.get(Calendar.MONTH)+1)+"-"+cal.get(Calendar.DAY_OF_MONTH);
					
					for(j=1; j<values.size(); j++)
					{
						format = new SimpleDateFormat("yyyy-MM-dd");
						try {
							dt = format.parse((String)values.get(j));
						} catch (ParseException e) {
							throw new ThirdPartyException(GoogleSheetsDataZoom.getText("Bad filter format", "mi.text.gs.datasets.error.message2"));
						}
						
						cal.setTime(dt);
						subQ=subQ+" or "+columnName+"="+cal.get(Calendar.YEAR)+"-"+(cal.get(Calendar.MONTH)+1)+"-"+cal.get(Calendar.DAY_OF_MONTH);
					}
				}
				
				else if (fData.getFilterMetaData().getFilterType().equals(DataType.TEXT)) 
				{
					int j;
					subQ=subQ+columnName+"=\""+values.get(0)+"\"";
					
					for(j=1; j<values.size(); j++)
					{
						subQ=subQ+" or "+columnName+"=\""+values.get(j)+"\"";
					}
					
				}
				
				else 
				{
					int j;
					subQ=subQ+columnName+"="+values.get(0);
					
					for(j=1; j<values.size(); j++)
					{
						subQ=subQ+" or "+columnName+"="+values.get(j);
					}
					
				}
				q=subQ;
				return q;
			}
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.NOTINLIST))
		{
			List<Object> values;
			if (fData.getFilterValue() instanceof List<?>)
			{
				values=(List<Object>)fData.getFilterValue();
			}
			else throw new ThirdPartyException(GoogleSheetsDataZoom.getText("Bad filter values received", "mi.text.gs.datasets.error.message1"));
			String subQ="";
			if (values.size()>0)
			{
				if(fData.getFilterMetaData().getFilterType().equals(DataType.DATE))
				{
					DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
					Date dt;
					try {
						dt = format.parse((String)values.get(0));
					} catch (ParseException e) {
						throw new ThirdPartyException(GoogleSheetsDataZoom.getText("Bad filter format", "mi.text.gs.datasets.error.message2"));
					}
					
										
					int j;
					Calendar cal=Calendar.getInstance();
					cal.setTime(dt);
					subQ=subQ+columnName+"!="+cal.get(Calendar.YEAR)+"-"+(cal.get(Calendar.MONTH)+1)+"-"+cal.get(Calendar.DAY_OF_MONTH);
					
					for(j=1; j<values.size(); j++)
					{
						format = new SimpleDateFormat("yyyy-MM-dd");
						try {
							dt = format.parse((String)values.get(j));
						} catch (ParseException e) {
							throw new ThirdPartyException(GoogleSheetsDataZoom.getText("Bad filter format", "mi.text.gs.datasets.error.message2"));
						}
						
						cal.setTime(dt);
						subQ=subQ+" and "+columnName+"!="+cal.get(Calendar.YEAR)+"-"+(cal.get(Calendar.MONTH)+1)+"-"+cal.get(Calendar.DAY_OF_MONTH);
					}
				}
				
				else if (fData.getFilterMetaData().getFilterType().equals(DataType.TEXT)) 
				{
					int j;
					subQ=subQ+columnName+"!=\""+values.get(0)+"\"";
					
					for(j=1; j<values.size(); j++)
					{
						subQ=subQ+" and "+columnName+"!=\""+values.get(j)+"\"";
					}
					
				}
				
				else 
				{
					int j;
					subQ=subQ+columnName+"!="+values.get(0);
					
					for(j=1; j<values.size(); j++)
					{
						subQ=subQ+" and "+columnName+"!="+values.get(j);
					}
					
				}
				q=subQ;
				return q;
			}
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.GREATER))
		{
			Object value=fData.getFilterValue();
				
			if (fData.getFilterMetaData().getFilterType().equals(DataType.TEXT)) 
			{
				q=columnName+">\""+value+"\"";
			}
			else q=columnName+">"+value;
			
			return q;
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.LESS))
		{
			Object value=fData.getFilterValue();
								
			if (fData.getFilterMetaData().getFilterType().equals(DataType.TEXT)) 
			{
				q=columnName+"<\""+value+"\"";
			}
			else q=columnName+"<"+value;
			
			return q;
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.GREATEREQUAL))
		{
			Object value=fData.getFilterValue();
								
			if (fData.getFilterMetaData().getFilterType().equals(DataType.TEXT)) 
			{
				q=columnName+">=\""+value+"\"";
			}
			else q=columnName+">="+value;
			
			return q;
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.LESSEQUAL))
		{
			Object value=fData.getFilterValue();
								
			if (fData.getFilterMetaData().getFilterType().equals(DataType.TEXT)) 
			{
				q=columnName+"<=\""+value+"\"";
			}
			else q=columnName+"<="+value;
			
			return q;
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.NOTCONTAINS))
		{
			/*Object value=fData.getFilterValue();
								
			if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
			{
				q="not ("+columnName+" like '%"+value+"%')";
			}
			
			return q;*/
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.CONTAINS))
		{
			/*Object value=fData.getFilterValue();
								
			if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
			{
				q=columnName+" like '%"+value+"%'";
			}
			
			return q;*/
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.STARTSWITH))
		{
			/*Object value=fData.getFilterValue();
								
			if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
			{
				q=columnName+" like '"+value+"%'";
			}
			
			return q;*/
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.ENDSWITH))
		{
			/*Object value=fData.getFilterValue();
								
			if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
			{
				q=columnName+" like '%"+value+"'";
			}
			
			return q;*/
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.NOTSTARTSWITH))
		{
			/*Object value=fData.getFilterValue();
								
			if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
			{
				q="not ("+columnName+" like '"+value+"%')";
			}
			
			return q;*/
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.NOTENDSWITH))
		{
			/*Object value=fData.getFilterValue();
								
			if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
			{
				q="not ("+columnName+" like '%"+value+"')";
			}
			
			return q;*/
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.ISEMPTYSTRING))
		{
			/*if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
			{
				q=columnName+"=''";
			}
			
			return q;*/
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.ISNOTEMPTYSTRING))
		{
			/*if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
			{
				q=columnName+"!=''";
			}
			
			return q;*/
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.ISNOTNULL))
		{
			q=columnName+"!=null";
			
			return q;
		}
		
		else if (fData.getFilterOperator().equals(FilterOperator.ISNULL))
		{
			q=columnName+"=null";
			
			return q;
		}
		
		return q;
	}
	

};
