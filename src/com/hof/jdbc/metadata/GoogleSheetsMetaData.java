package com.hof.jdbc.metadata;

import java.util.Arrays;

import org.scribe.model.Token;

import com.google.api.client.auth.oauth2.AuthorizationCodeFlow;
import com.google.api.client.auth.oauth2.BearerToken;
import com.google.api.client.auth.oauth2.ClientParametersAuthentication;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.hof.pool.DBType;
import com.hof.pool.JDBCMetaData;
import com.hof.util.GoogleSheetsDataZoom;

public class GoogleSheetsMetaData extends JDBCMetaData {

	
	boolean initialised = false;
	String url;
	Token requestToken=new Token("", "");
	
	private static HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	private static JsonFactory JSON_FACTORY = new JacksonFactory();
	private static String SCOPE = "https://spreadsheets.google.com/feeds";
	private static String AUTHORIZATION_SERVER_URL = "https://accounts.google.com/o/oauth2/auth";
	private static String TOKEN_SERVER_URL="https://accounts.google.com/o/oauth2/token";
	private static AuthorizationCodeFlow flow = new AuthorizationCodeFlow.Builder(BearerToken
		      .authorizationHeaderAccessMethod(),
		      HTTP_TRANSPORT,
		      JSON_FACTORY,
		      new GenericUrl(TOKEN_SERVER_URL),
		      new ClientParametersAuthentication(
		          new String(com.hof.util.Base64.decode(GoogleSheetsDataZoom.getData())), new String(com.hof.util.Base64.decode(GoogleSheetsDataZoom.getZoom()))),
		          new String(com.hof.util.Base64.decode(GoogleSheetsDataZoom.getData())),
		      AUTHORIZATION_SERVER_URL).setScopes(Arrays.asList(SCOPE))
		      .build();
	
	public GoogleSheetsMetaData() {
		
		super();
		
		sourceName = GoogleSheetsDataZoom.getText("Google Sheets", "mi.text.gs.datasource.name");
		sourceCode = "GOOGLE_SHEETS";
		driverName = "com.hof.imp.GoogleSheetsDataSource";
		sourceType = DBType.THIRD_PARTY;
	}
	
	/*public  void initialiseParameters() {
		
		super.initialiseParameters();
		
		addParameter(new Parameter("PARAMETER1", "Example Parameter", "Example Parameter description",TYPE_TEXT, DISPLAY_TEXT_LONG,  null, true));
		addParameter(new Parameter("PARAMETER2", "Another Example Paramter", "Another Example Paramter description",TYPE_TEXT, DISPLAY_TEXT_MED,  null, true));
		

	}*/
	
	
	 @Override
	public  void initialiseParameters() 
	 {
	        super.initialiseParameters();
	        
	       if (!initialised)
			{
				String urlAddr = flow
						.newAuthorizationUrl()
						.setState("xyz")
						.setRedirectUri("urn:ietf:wg:oauth:2.0:oob").build();
				url=urlAddr;
				initialised=true;
			}
	        
	       String inst=GoogleSheetsDataZoom.getText("1. Click the 'Authorize Google Sheets' button, login, and 'Allow' access to your account.", "mi.text.gs.connection.instructions.line1", "mi.text.gs.connection.request.pin.button.text")+"<br>"+  
					GoogleSheetsDataZoom.getText("2. Copy the PIN provided and paste it into Yellowfin.", "mi.text.gs.connection.instructions.line2")+"<br>"+ 
					GoogleSheetsDataZoom.getText("3. Click the 'Validate PIN' button.", "mi.text.gs.connection.instructions.line3", "mi.text.gs.connection.validate.pin.button.text");
	       
			addParameter(new Parameter("HELP", GoogleSheetsDataZoom.getText("Connection Instructions", "mi.text.gs.connection.instructions.label"),  inst, TYPE_NUMERIC, DISPLAY_STATIC_TEXT, null, true));
	        
	        Parameter p = new Parameter("URL", GoogleSheetsDataZoom.getText("1. Request Access PIN", "mi.text.gs.connection.request.pin.button.label"), GoogleSheetsDataZoom.getText("Connect to Google Sheets to receive a PIN for data access", "mi.text.gs.connection.request.pin.button.description"),TYPE_UNKNOWN, DISPLAY_URLBUTTON,  null, true);
	        p.addOption("BUTTONTEXT", GoogleSheetsDataZoom.getText("Authorize Google Sheets", "mi.text.gs.connection.request.pin.button.text"));
	        p.addOption("BUTTONURL", url);
	        addParameter(p);
	        addParameter(new Parameter("PIN", GoogleSheetsDataZoom.getText("2. Enter PIN", "mi.text.gs.connection.request.pin.field.label"),  GoogleSheetsDataZoom.getText("Enter the PIN received from Google Sheets", "mi.text.gs.connection.request.pin.field.description"), TYPE_TEXT, DISPLAY_TEXT_MED, null, true));
	        p = new Parameter("POSTPIN", GoogleSheetsDataZoom.getText("3. Validate Pin", "mi.text.gs.connection.validate.pin.button.label"),  GoogleSheetsDataZoom.getText("Validate the PIN", "mi.text.gs.connection.validate.pin.button.description"), TYPE_TEXT, DISPLAY_BUTTON, null, true);
	        p.addOption("BUTTONTEXT", GoogleSheetsDataZoom.getText("Validate PIN", "mi.text.gs.connection.validate.pin.button.text"));
	        addParameter(p);
			
			addParameter(new Parameter("ACCESS_TOKEN", GoogleSheetsDataZoom.getText("Access Token", "mi.text.linkedin.connection.access.token.field.label"), GoogleSheetsDataZoom.getText("Access Token", "mi.text.linkedin.connection.access.token.field.description"),TYPE_TEXT, DISPLAY_PASSWORD,  null, true));
			addParameter(new Parameter("REFRESH_TOKEN", GoogleSheetsDataZoom.getText("Refresh Token", "mi.text.gs.connection.refresh.token.field.label"), GoogleSheetsDataZoom.getText("Refresh Token", "mi.text.gs.connection.refresh.token.field.description"),TYPE_TEXT, DISPLAY_PASSWORD,  null, true));
			
			Parameter select = new Parameter("DATACONVERSION", GoogleSheetsDataZoom.getText("Set incorrect data to null", "mi.text.gs.connection.dataconversionpolicy.select.label") ,  GoogleSheetsDataZoom.getText("Set incorrect data to null", "mi.text.gs.connection.dataconversionpolicy.select.description"), TYPE_TEXT, DISPLAY_SELECT, null, true);
			select.addOption("YES",GoogleSheetsDataZoom.getText("Yes", "mi.text.gs.connection.dataconversionpolicy.select.option1"));
			select.addOption("NO", GoogleSheetsDataZoom.getText("No", "mi.text.gs.connection.dataconversionpolicy.select.option2"));
	        addParameter(select);
			
	        addParameter(new Parameter("LINESQUANTITY", GoogleSheetsDataZoom.getText("Number of rows used to define the data type", "mi.text.gs.connection.numberofrows.label"), GoogleSheetsDataZoom.getText("Number of rows used to define the data type", "mi.text.gs.connection.numberofrows.description"),TYPE_NUMERIC, DISPLAY_TEXT_LONG,  "10", true));
	    }
	    
	    @Override
		public String buttonPressed(String buttonName) throws Exception {
	    
	    	if (buttonName.equals("POSTPIN") && getParameterValue("PIN")!=null)
	        {
	        	String ver=(String)getParameterValue("PIN");
	        	TokenResponse resp=flow.newTokenRequest(ver).setRedirectUri("urn:ietf:wg:oauth:2.0:oob").execute();
				
	        	setParameterValue("ACCESS_TOKEN", resp.getAccessToken());
				setParameterValue("REFRESH_TOKEN", resp.getRefreshToken());
					
	    		
	    		
	        } 
	        return null;
	        
	    }
	    
	    
	    
		@Override
		public byte[] getDatasourceIcon() {
			String str = "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";
			return str.getBytes();
		}
	    
		
		@Override
		public String getDatasourceShortDescription(){
			return GoogleSheetsDataZoom.getText("Connect to Google Sheets", "mi.text.gs.short.description");
		}

		@Override
		public String getDatasourceLongDescription(){
			return GoogleSheetsDataZoom.getText("Analyze the data in your Google Sheets.", "mi.text.gs.long.description");
		}


	
	
	
}
