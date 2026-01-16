// package com.Inv.InventorySave.Controller;


// import java.util.Map;

// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.http.HttpStatus;
// import org.springframework.http.MediaType;
// import org.springframework.web.bind.annotation.GetMapping;
// import org.springframework.web.bind.annotation.PathVariable;
// import org.springframework.web.bind.annotation.RequestBody;
// import org.springframework.web.bind.annotation.RequestMapping;
// import org.springframework.web.bind.annotation.RequestMethod;
// import org.springframework.web.bind.annotation.ResponseBody;
// import org.springframework.web.bind.annotation.RestController;

// import com.Inv.InventorySave.Entity.MoResponse;
// import com.Inv.InventorySave.Entity.MoSave;
// import com.Inv.InventorySave.Exception.ExGenericException;
// import com.Inv.InventorySave.Repository.InvSaveCommonRepository;
// import com.Inv.InventorySave.Repository.InvSaveCommonRepository;


// //public class CoInvCommon {
// //
// //}


// @RestController
// public class CoInvSaveCommon {
// 	@Autowired		
// 	private InvSaveCommonRepository invSaveCommonRepository;
	
// 	//For All Dropdown(through api),Retrieve Screen
// 	@GetMapping(path = "/common/{sql}",
// 			produces = MediaType.APPLICATION_JSON_VALUE)		    
// 		  public MoResponse fnGetScreenJSON(@PathVariable("sql") String sql) {	        
// 		       Map<String, Object> patients = invSaveCommonRepository.fnGetScreenJSON(sql);
// 		       MoResponse moResponse = new MoResponse();
// 		        try{
// 		            moResponse.setSuccess(true);
// 		            moResponse.setMessage("SUCCESS");
// 		            moResponse.setHttpstatus(HttpStatus.OK.value());
// 		            moResponse.setData(patients);
// 		            patients = null;
// 		            System.gc();
// 		            return moResponse;
// 		        }catch (Exception e) {
// 		            throw new ExGenericException("API GenericException","API-Exception On fnGetScreenJSON  --" + e.toString());
// 		        }
// 		    }
	
// }

package com.Inv.InventorySave.Controller;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.Inv.InventorySave.Entity.MoResponse;
import com.Inv.InventorySave.Entity.MoSave;
import com.Inv.InventorySave.Exception.ExGenericException;
import com.Inv.InventorySave.Repository.InvCommonSaveRepository;
@RestController

 public class CoInvCommonSave {

	@Autowired
	private InvCommonSaveRepository invCommonSaveRepository;
	
	
	//To Save Instructions
	@RequestMapping(value = "/RnSave",method = RequestMethod.POST, 
			produces = MediaType.APPLICATION_JSON_VALUE)
	 @ResponseBody
	    public MoResponse fnGetSaveSummary(@RequestBody String moJson) {	
		    String data = moJson;
//		    System.out.println(data);
	        MoSave moSave = invCommonSaveRepository.fnGetSaveSummary(moJson);
	        MoResponse moResponse = new MoResponse();
	        try{
	        	moResponse.setSuccess(true);
	            moResponse.setMessage("SUCCESS");
	            moResponse.setHttpstatus(HttpStatus.OK.value());
	            moResponse.setData(moSave);
	            return moResponse;
	        }catch (Exception e) {
	            throw new ExGenericException("API GenericException","API-Exception On  fnGetAddInstructions  --" + e.toString());
	        }
	}	
	
}