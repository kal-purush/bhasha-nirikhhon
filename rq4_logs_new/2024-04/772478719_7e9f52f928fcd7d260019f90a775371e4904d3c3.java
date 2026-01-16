package com.donation.donationmanager.controller;

import java.time.LocalDate;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.donation.donationmanager.model.DonationType;
import com.donation.donationmanager.repository.MasterRepository;

@RestController
@RequestMapping("master/{masterId}")
public class MasterController {
	
	/*public <T> MasterController(@PathVariable("masterId") Long masterId) {
		if(masterId==1) {
			final MasterRepository<DonationType> masterrepository;
		}
	}*/
	@Autowired private MasterRepository<DonationType> masterrepository;
	
	
	@GetMapping("getallrecords")
	public List<DonationType> getAllRecords(){	
		 return masterrepository.findAll();		 
	}
	
	//@PostMapping(value="adduser",consumes="application/json")
	@PostMapping("addrecord")
	public void addRecords(@RequestBody DonationType record){		
		masterrepository.save(record);		 
	}
	
	@GetMapping("getrecord/{id}")
	public DonationType getRecord(@PathVariable("id") Long id){
		 return masterrepository.findById(id); 
	}
	
	@PutMapping("editrecord/{id}")
	public void editRecord(@PathVariable("id") Long id,@RequestBody DonationType record){
		DonationType existingRecord = masterrepository.findById(id);
        if (existingRecord != null) {
        	
        	//set values from existing record to fields which will not be updated through request data
        	record.setId(id);
        	record.setCreated(existingRecord.getCreated());
        	record.setModified(LocalDate.now());
        	
        	//rest fields will be updated from the request data
        	masterrepository.update(record);
        }
	}
	
	@PutMapping("deleterecord/{id}")
	public void deleteRecord(@PathVariable Long id){
		DonationType existingRecord = masterrepository.findById(id);
        if (existingRecord != null) {
        	masterrepository.delete(existingRecord);	 
        }
	}
}