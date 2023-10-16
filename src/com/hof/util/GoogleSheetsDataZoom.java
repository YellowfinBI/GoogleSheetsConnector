package com.hof.util;


public class GoogleSheetsDataZoom {

	//private final static String API_KEY="<API KEY>";
	//private final static String API_SECRET="<API SECRET>";
	
	private final static char[] Data={'K','E','Y'};
	private final static char[] Zoom={'S', 'E', 'C', 'R', 'E', 'T'};
	
	public static String getData() {
		return new String(Data);
	}
	
	public static String getZoom() {
		return new String(Zoom);
	}
	
	public static String getText(String englishText, String translationKey) {

	    String text = UtilString.getResourceString(translationKey);
	    if (text==null) return englishText;
	    return text;

	}
	
	public static String getText(String englishText, String translationKey, String paramKey) {
		String key="";
		if(UtilString.getResourceString(paramKey)!=null)
		{
			key=UtilString.getResourceString(paramKey);
		}
		String text = UtilString.getResourceString(translationKey);
	    if (text==null) return englishText;
	    
	    else if (text.contains("{0}"))
	    {
	    	text=text.replace("{0}", "'"+key+"'");
	    }
	    else if (text.contains("{1}"))
	    {
	    	text=text.replace("{1}", "'"+key+"'");
	    }
	    
	    return text;
	}
	
	

}
