package com.ssi.devicemonitor.entity;

public class HardwareDevice extends Device {
	
    private String location;
    private String macAddress;
    
	public HardwareDevice(String name) {
        super(name,"Hardware");
    }
    
    public String getLocation() {
		return location;
	}


	public void setLocation(String location) {
		this.location = location;
	}


	public String getMacAddress() {
		return macAddress;
	}


	public void setMacAddress(String macAddress) {
		this.macAddress = macAddress;
	}



}