package com.musala.sdcs.http;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.musala.sdcs.device.Device;
import com.musala.sdcs.repository.DeviceRepository;

@RestController
public class DeviceController {
	@Autowired
	DeviceRepository drepo;
	
	@RequestMapping("/")
	@CrossOrigin
	public List<Device> getDevices() {
		return  drepo.getAllDevices();
	}
	
	@RequestMapping(value = "/{id}", method = RequestMethod.GET)
	@CrossOrigin
	public Device getDeviceById(@PathVariable("id") int id) {
		return drepo.getDeviceById(id);
	}
	
	@RequestMapping(value = "/channel/{id}", method = RequestMethod.PUT)
	@CrossOrigin
	public boolean updateChannelCommand(@PathVariable("id") int channelId, @PathVariable("command") String newCommand) {
		
		
		return true;
	}
}