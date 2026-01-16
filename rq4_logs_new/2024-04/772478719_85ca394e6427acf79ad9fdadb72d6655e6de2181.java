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

import com.donation.donationmanager.model.Donation;
import com.donation.donationmanager.repository.DonationRepository;

@RestController
@RequestMapping("donation")
public class DonationController {

	@Autowired private DonationRepository donationrepository;
	
	@GetMapping("getdonations")
	public List<Donation> getAllDonations(){	
		 return donationrepository.findAll();		 
	}
	
	@PostMapping("adddonation")
	public void addDonation(@RequestBody Donation donation){		
		donationrepository.save(donation);		 
	}
	
	@GetMapping("getdonation/{id}")
	public Donation getDonation(@PathVariable("id") Long id){
		 return donationrepository.findById(id); 
	}
	
	@PutMapping("editdonation/{id}")
	public void editDonation(@PathVariable("id") Long id,@RequestBody Donation donation){
		Donation existingDonation = donationrepository.findById(id);
        if (existingDonation != null) {
        	
        	//set values from existing record to fields which will not be updated through request data
        	donation.setId(id);
        	donation.setCreated(existingDonation.getCreated());
        	donation.setModified(LocalDate.now());
        	
        	//rest fields will be updated from the request data
        	donationrepository.update(donation);
        }
	}
	
	@PutMapping("deletedonation/{id}")
	public void deleteDonation(@PathVariable Long id){
		Donation existingDonation = donationrepository.findById(id);
        if (existingDonation != null) {
        	donationrepository.delete(existingDonation);	 
        }
	}
}