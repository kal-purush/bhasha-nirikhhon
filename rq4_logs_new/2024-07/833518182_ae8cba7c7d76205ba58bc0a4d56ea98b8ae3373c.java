package com.demon.casestudy2;

// This part is implemented by  Ruturaj Patil

import java.time.LocalTime;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class AirConditioner implements Device
{
	String name;
	String status;
	LocalTime timePeriod;
	
	public AirConditioner() {
		// TODO Auto-generated constructor stub
	}

	public AirConditioner(String status) {
		super();
		this.name = "AirConditioner";
		this.status = status;
		this.timePeriod = LocalTime.now();
	}
	
	public String getDeviceName()
	{
		return this.name;
	}

	public boolean deviceOn()
	{
		if(this.status.equals("On"))
		{
			return false;
		}
		else
		{
			this.status = "On";
			Duration d = Duration.between(this.timePeriod, LocalTime.now());
			System.out.println("\n Device was OFF for : " + d.get(ChronoUnit.SECONDS));
			return true;
		}
	}

	
	public boolean deviceOff()
	{
		if(this.status.equals("Off"))
		{
			return false;
		}
		else
		{
			this.status = "Off";
			Duration d = Duration.between(this.timePeriod, LocalTime.now());
			System.out.println("\n Device was ON for : " + d.get(ChronoUnit.SECONDS));
			return true;
		}
	}


	public void checkDeviceStatus()
	{
		if(this.status.equals("On"))
		{
			System.out.println("\t AIR-CONDITIONER is ON ");
		}
		else
		{
			System.out.println("\t AIR-CONDITIONER is OFF ");
		}
	}
	
}