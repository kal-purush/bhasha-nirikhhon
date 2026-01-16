package com.api.Actions;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.http.entity.ContentType;

import com.api.EnvVariables.EnvConstants;
import com.api.EnvVariables.EnvVariables;
import com.api.ReusableUtils.ExcelUtils;
import com.api.ReusableUtils.Reusable_CRUD_Operations;
import com.api.ReusableUtils.UserExcelReader;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.apache.poi.ddf.EscherColorRef;


public class Patient_Actions {
	
	Reusable_CRUD_Operations restUtil = new Reusable_CRUD_Operations();
	private String requestBody = "";
	//String token;
	Response response;
	int statusCode;
	String sheetName_patient= "create_patient";
	String sheetName_patient_Negative= "create_patient_Negative";
	/*
	 * String sheetName_patient_vitals= "patient_vitals"; String
	 * sheetName_patient_fileMorbidityfileId= "fileMorbidityfileId"; String
	 * sheetName_patient_FileMorbidityCondition= "patient_vitals";
	 */
	ExcelUtils excelUtils= new ExcelUtils();
	static String filePath_responseReport;
	
	String FirstName;							
	String LastName;
	String ContactNumber;
	String Email;
	String Allergy;
	String FoodPreference;
	String CuisineCategory;
	String DateOfBirth;



	
	
	public static String createJsonPayload(String... keyValuePairs) {
		JsonObject json = new JsonObject();
		for (int i = 0; i < keyValuePairs.length; i += 2) {
			json.addProperty(keyValuePairs[i], keyValuePairs[i + 1]);
		}
		Gson gson = new Gson();
		return gson.toJson(json);
	}
	
	/* ====================Building requestSpecification============================================== */
	
	public RequestSpecification buildRequest() throws FileNotFoundException {
		RequestSpecification reqSpec;
		reqSpec = restUtil.getRequestSpec();
		return reqSpec;
	}

	
	/* ====================Storing the auth token in Envvariables============================================== */

	public void storeAuthToken(Response response) {
		// System.out.println("response sending from actions
		// "+response.asPrettyString());
		String token = restUtil.extractStringFromResponse(response, "token");
		System.out.println("The token from the response is " + token);
		EnvVariables.token = token;
		System.out.println("The token stored in EnvVariables.token is " + EnvVariables.token);
	}

	
	/* ====================Create Patient by reading TestData from Excel============================================== */
	public Response createPatient(RequestSpecification reqSpec) throws org.apache.poi.openxml4j.exceptions.InvalidFormatException, IOException
	{

		File report=new File(EnvConstants.reportPath_patientModule);
		System.out.println("The token stored in EnvVariables.Diet_token is " + EnvVariables.Diet_token);
	response= restUtil.readExcel_createPatient( EnvConstants.filePath_patientModule, sheetName_patient, reqSpec, EnvVariables.Diet_token, report, EnvConstants.createpatient, "patientInfo", "file","patientId");
	
	String strResponse= response.getBody().asString();
	JsonPath jsPath= new JsonPath(strResponse);

	Object fileMorbidityKeyObj =jsPath.getMap("FileMorbidity").keySet().iterator().next();
	String fileMorbidityKey = fileMorbidityKeyObj.toString();
	EnvVariables.fileId = fileMorbidityKey;
	
	System.out.println("EnvVariables.fileId is "+ EnvVariables.fileId);
	String fileMorbidityKeyValue = jsPath.getString("FileMorbidity." + fileMorbidityKey);

	Object FileMorbidityConditionKeyObj =jsPath.getMap("FileMorbidityCondition").keySet().iterator().next();
	String FileMorbidityConditionKey = fileMorbidityKeyObj.toString();
	String fileMorbidityConditionValue = jsPath.getString("FileMorbidityCondition." + FileMorbidityConditionKey);
	System.out.println("FileMorbidityConditionkey is "+ FileMorbidityConditionKey+ " FileMorbidityCondition value is " + fileMorbidityConditionValue);
	
	
	EnvVariables.patientId =jsPath.getString("patientId");
	System.out.println("patientId is "+ EnvVariables.patientId);
	EnvVariables.patient1_Email1 =jsPath.getString("Email");
	
	//need to fetch the dietician id from the patient.
	
	
	//extracting response to check the thyroidism range
	 String t4StringValue=jsPath.getString("FileMorbidity."+fileMorbidityKey+ ".T4");
	 String tshStringValue=jsPath.getString("FileMorbidity."+fileMorbidityKey+ ".TSH");
	 String t3StringValue= jsPath.getString("FileMorbidity."+fileMorbidityKey+ ".T3");
//// Remove non-numeric characters from the string values and converting numeric string into double by parsing
	 double t4DoubleValue =Double.parseDouble(t4StringValue.replaceAll("[^\\d.]", ""));
	 double tshDoubleValue =Double.parseDouble(tshStringValue.replaceAll("[^\\d.]", ""));
	 double t3DoubleValue=Double.parseDouble(t3StringValue.replaceAll("[^\\d.]", ""));
	 System.out.println("The values are:\n" +
             "  T4: " + t4DoubleValue + "\n" +
             "  TSH: " + tshDoubleValue + "\n" +
             "  T3: " + t3DoubleValue);
	
	 
//using logical operator to find the thyroidism
	if((tshDoubleValue < 0.55 && t3DoubleValue > 1.8 && t4DoubleValue > 12))
	{
		 System.out.println("Hyperthyroidism detected!");
		 String Hyper=jsPath.get("FileMorbidityCondition."+fileMorbidityKey);
		 System.out.println("The value of FileMorbidityCondition is "+ Hyper);
	}
	
   else if (tshDoubleValue > 4.78 && t4DoubleValue < 5) {
  
   System.out.println("Hypothyroidism detected!");
   String Hypo=jsPath.get("FileMorbidityCondition."+fileMorbidityKey);
   System.out.println("The value of FileMorbidityCondition is "+ Hypo);
	
 } else {
   
   System.out.println("No thyroid condition detected.");
}
	
	
	
	
	
	
	return response;
	}
	
	/* ==================================Create Patient _Negative Scenarios============================================== */
	public String buildInValidRequestData(RequestSpecification reqSpec , String currentTag, String invalid_Data)
			throws InvalidFormatException, IOException, org.apache.poi.openxml4j.exceptions.InvalidFormatException {
		
		System.out.println("invalid data is "+invalid_Data);
		
		List<Map<String, String>> getUserData =
				 (UserExcelReader.getData(EnvConstants.filePath_patientModule, "create_patient_Negative"));
		
		/*Map<String, String> row_invalid = getUserData.get(8); // Get the first row from your Excel data (as a map)
		String invalidDataValue = row_invalid.get("invalidData");
		System.out.println("invalidDataValue is "+ invalidDataValue);*/
		
		if (getUserData == null || getUserData.isEmpty()) {
	        throw new RuntimeException("No data found in the Excel sheet.");
		}
				 Map<String, String> rowdata = getUserData.stream() .filter(row ->
				 row.get("invalidData").equals(invalid_Data)).findFirst() .orElseThrow(() ->
				 new RuntimeException("No matching data found for tag: " +invalid_Data));
				 
				 FirstName = rowdata.get("FirstName");
				 LastName = rowdata.get("LastName");
				 ContactNumber = rowdata.get("ContactNumber");
				 Email = rowdata.get("Email");
				 Allergy = rowdata.get("Allergy");
				 FoodPreference = rowdata.get("FoodPreference");
				 CuisineCategory = rowdata.get("CuisineCategory");
				 DateOfBirth = rowdata.get("DateOfBirth");
				 
				 
				 requestBody = createJsonPayload("FirstName", FirstName, "LastName", LastName, "ContactNumber", ContactNumber, "Email", Email, "Allergy", Allergy, "FoodPreference", FoodPreference, "CuisineCategory", CuisineCategory, "DateOfBirth", DateOfBirth);
				 System.out.println("Login request Body  : " + requestBody);
				return requestBody;
	
		}
	
	public Response createPatient_AllTest(RequestSpecification reqSpec, String requestData, String currentTag)
			throws InvalidFormatException, IOException {
		
		File report=new File(EnvConstants.reportPath_patientModule);
		String jsonKey= "patientInfo";
		String fileKey="file";

		String trimmedCurrentTag = currentTag.startsWith("@")? currentTag.substring(1) : currentTag ;
		
		System.out.println("trimmedCurrentTag is : " + trimmedCurrentTag);
		 
		    // sending request
		
		    switch(trimmedCurrentTag) {
		    case "using_adminToken":
		    	//response = restUtil.create(reqSpec, EnvVariables.token, requestBody,EnvConstants.createpatient);
		    	
		    	response=restUtil.createMultipart(EnvVariables.token,report, EnvConstants.createpatient,requestBody, jsonKey,fileKey);
		    	System.out.println(response.getStatusCode() +"  and  "+ response.getContentType());
		    	break;
		    
		    case "using_patientToken":
		    	response=restUtil.createMultipart(EnvVariables.Patient_token,report, EnvConstants.createpatient,requestBody, jsonKey,fileKey);
		    	
		    	break;
		    case "invalid_HTTP":
		    	response = restUtil.put(reqSpec, EnvVariables.token, requestBody,
						EnvConstants.createpatient);
		    	break;
		    	
		    case "NoAuth":
		    	response = restUtil.create_withNoAuth(reqSpec,requestBody,
						EnvConstants.createpatient);
		    	break;
		    case "invalidMandatory":
		    	
		    	response = restUtil.create(reqSpec, EnvVariables.Diet_token, requestBody,
						EnvConstants.createpatient);
		    	break;
		    case "invalid_endpoint":
		    	response = restUtil.create(reqSpec, EnvVariables.Diet_token, requestBody,
						EnvConstants.createpatient_Invalid);
		    	break;
		    	
		    case "invalidAdditional":
		    	response = restUtil.create(reqSpec, EnvVariables.Diet_token, requestBody,
						EnvConstants.createpatient);
		    	break;
		    	default :
		    		throw new RuntimeException("no matching tag :" +trimmedCurrentTag);
		    }
		    
		    //System.out.println("out switch case "+response.asPrettyString());
		 
		return response;
	}
	
	/*====================Create Patient by reading TestData from Excel_only mandatory============================*/
	public Response createPatient_mandatory(RequestSpecification reqSpec) throws org.apache.poi.openxml4j.exceptions.InvalidFormatException, IOException {
	    
	    // Read the request body from Excel
	    String requestBody = restUtil.readExcel_create_withMandatory(reqSpec, EnvVariables.Diet_token, EnvConstants.createpatient, EnvConstants.filePath_patientModule, sheetName_patient);
	    
	    // Call the createMultipart_2 method (without report and fileKey)
	    Response response = restUtil.createMultipart_2(EnvVariables.Diet_token, EnvConstants.createpatient, requestBody, "patientInfo");
	    
	    // Log the status code
	    System.out.println("Status code for valid mandatory is " + response.getStatusCode());
	    
	    // Return the response
	    return response;
	}
	
	
	/*====================Get All Kind Of requests============================*/
	
	
	public Response Get_Patient_AllTest(RequestSpecification reqSpec, String currentTag)
			throws InvalidFormatException, IOException {
		
		
		String trimmedCurrentTag = currentTag.startsWith("@")? currentTag.substring(1) : currentTag ;
		
		System.out.println("trimmedCurrentTag is : " + trimmedCurrentTag);
		 
		    // sending request
		
		    switch(trimmedCurrentTag) {
		    case "getAllPatients":
		    	response=restUtil.retrieve(reqSpec, EnvVariables.Diet_token, EnvConstants.getAll_patients);
		    	System.out.println(response.asPrettyString());
		    	break;
		    
		    case "getAll_noAuth":
		    	response=restUtil.getAllNoAuth(reqSpec, EnvConstants.getAll_patients);
		    	
		    	break;
		    	
		    case "getAll_invalidReq":
		    	response = restUtil.get_put(reqSpec, EnvVariables.token,EnvConstants.getAll_patients);
						
		    	break;
		    	
		    case "getAll_patientToken":
		    	response = restUtil.retrieve(reqSpec,EnvVariables.Patient_token,  EnvConstants.getAll_patients);
		    	break;
		    case "getAll_adminToken":
		    	
		    	response = restUtil.retrieve(reqSpec, EnvVariables.token,
						EnvConstants.createpatient);
		    	break;
		    case "getAll_invalidPoints":
		    	response = restUtil.retrieve(reqSpec, EnvVariables.Diet_token,EnvConstants.createpatient_Invalid);
		    	break;
		  /*  	
		    case "invalidAdditional":
		    	response = restUtil.create(reqSpec, EnvVariables.Diet_token, requestBody,
						EnvConstants.createpatient);
		    	break;*/
		    	default :
		    		throw new RuntimeException("no matching tag :" +trimmedCurrentTag);
		    }
		    
		    //System.out.println("out switch case "+response.asPrettyString());
		 
		return response;
	}
	
	
	public Response get_morbidity(RequestSpecification reqSpec )
	{
		response=restUtil.retrieve(reqSpec, EnvVariables.Diet_token, EnvConstants.Patients_Morbidity);
		filePath_responseReport=EnvConstants.filePath_responseReport;
		// Saved the PDF content to a file
		 try (FileOutputStream fos = new FileOutputStream(filePath_responseReport))	 
		 {
			 fos.write(response.getBody().asByteArray());
		     System.out.println("PDF saved successfully to: " + filePath_responseReport);
		 }
		 
	   catch (IOException e) {
	        e.printStackTrace();
	    }
		return response;	
	
		
	}
}