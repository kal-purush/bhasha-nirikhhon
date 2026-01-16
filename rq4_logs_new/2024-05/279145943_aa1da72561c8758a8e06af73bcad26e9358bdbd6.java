package com.example.personaldata.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.Valid;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

import com.example.personaldata.constants.Message;
import com.example.personaldata.exception.ResourceNotFoundException;
import com.example.personaldata.model.Address;
import com.example.personaldata.model.Person;
import com.example.personaldata.repository.AddressRepository;
import com.example.personaldata.repository.PersonRepository;

@Service
public class PersonalDataService2 {
	
	@Autowired
	private PersonRepository personRepository;
	
	@Autowired
	private AddressRepository addressRepository;
	
	private static final Logger LOGGER  = LogManager.getLogger(PersonRepository.class);
	
	public List<Person> getAllPersons() {
		return personRepository.findAll();
	}
	
	public String countAllPersons() {
		long count = personRepository.count();		
		return "total persons in system: "+count;
	}
	
	public ResponseEntity<Person> getPersonById(Long personId) throws ResourceNotFoundException{
		Person person = personRepository.findById(personId)
				.orElseThrow(() -> new ResourceNotFoundException(Message.PERSON_NOT_FOUND + personId));
		return ResponseEntity.ok().body(person);
	}
	
	public ResponseEntity<Person> createPerson(Person person) {
		Person personCreated = personRepository.save(person);
		return ResponseEntity.ok(personCreated);
	}
	
	public ResponseEntity<Person> updatePerson(Long personId,Person personDetails) throws ResourceNotFoundException {
		Person person = personRepository.findById(personId)
				.orElseThrow(() -> new ResourceNotFoundException(Message.PERSON_NOT_FOUND + personId));

		person.setLastName(personDetails.getLastName());
		person.setFirstName(personDetails.getFirstName());
		final Person updatedPerson = personRepository.save(person);
		return ResponseEntity.ok(updatedPerson);
	}
	
	public Map<String, Boolean> deletePerson(Long personId) throws ResourceNotFoundException {
		Person person = personRepository.findById(personId)
				.orElseThrow(() -> new ResourceNotFoundException(Message.PERSON_NOT_FOUND + personId));

		personRepository.delete(person);
		Map<String, Boolean> response = new HashMap<>();
		response.put(Message.PERSON_DELETED + personId, Boolean.TRUE);
		LOGGER.info("Person deleted");
		return response;
	}
	
	public ResponseEntity<Address> createAddress(Long personId, Address address)  
			                                    throws ResourceNotFoundException{
		
		Person person = personRepository.findById(personId)
				.orElseThrow(() -> new ResourceNotFoundException(Message.PERSON_NOT_FOUND + personId));
		
		Address saved = addressRepository.save(address);
		person.getPersonAddresses().add(saved);
		personRepository.save(person);
		refresh();
		return ResponseEntity.ok(address);
	}
	
	public ResponseEntity<Address> updateAddress(Long personId,Long addressId,Address addressDetails) 
			                                     throws ResourceNotFoundException {
		
		Person person = personRepository.findById(personId)
				.orElseThrow(() -> new ResourceNotFoundException(Message.PERSON_NOT_FOUND + personId));
		
		Set<Address> addresses = person.getPersonAddresses();
		
		Address address = addresses.stream().
		filter(p -> Long.valueOf(p.getAddressId()).equals(Long.valueOf(addressId))).
		findAny().orElse(null);
		
		if(null == address) {
			LOGGER.error("Address not found for this id:{}",addressId);
			throw new ResourceNotFoundException(Message.ADDRESS_NOT_FOUND + addressId);
		}
		
		address.setAddressType(addressDetails.getAddressType());
		address.setCity(addressDetails.getCity());
		address.setPhoneNumber(addressDetails.getPhoneNumber());
		address.setPostalCode(addressDetails.getPostalCode());
		address.setState(addressDetails.getState());
		address.setStreet(addressDetails.getStreet());
		final Address addressUpdated = addressRepository.save(address);
		refresh();
		return ResponseEntity.ok(addressUpdated);
	}
	
	public Map<String, Boolean> deleteAddress(@PathVariable(value = "id") Long personId,
            @PathVariable(value = "id") Long addressId)
            throws ResourceNotFoundException {
		if(!personRepository.existsById(personId)) {
			LOGGER.error("deleteAddress -> Person with id not found:{}",personId);
			throw new ResourceNotFoundException(Message.RESOURCE_ADDRESS_NOT_FOUND+personId);
		}
		
		Person person = personRepository.findById(personId)
				.orElseThrow(() -> new ResourceNotFoundException(Message.PERSON_NOT_FOUND + personId));
				
		Set<Address> addresses = person.getPersonAddresses();
		
		Address address = addresses.stream().
		filter(p -> Long.valueOf(p.getAddressId()).equals(Long.valueOf(addressId))).
		findAny().orElse(null);
		
		if(null == address) {
			LOGGER.error("Address not found for this id:{}",addressId);
			throw new ResourceNotFoundException(Message.RESOURCE_ADDRESS_NOT_FOUND + addressId);
		}
		
		boolean isRemoved = addresses.remove(address);
		refresh();

		Map<String, Boolean> response = new HashMap<>();
		response.put("address with id: " + addressId + " deleted.", isRemoved);
		return response;
	}
	
    private boolean personExistsById(Long id) {
        return personRepository.existsById(id);
    }
    
    private boolean addressExistsById(Long id) {
        return addressRepository.existsById(id);
    }

	private void refresh() {
		personRepository.flush();
		addressRepository.flush();
	}
}