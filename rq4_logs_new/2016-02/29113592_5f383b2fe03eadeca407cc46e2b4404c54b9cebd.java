package com.flipturnapps.kevinLibrary.sprite.physics;

import java.util.ArrayList;

import com.flipturnapps.kevinLibrary.sprite.PositionSprite;

public abstract class PhysicsSprite extends PositionSprite 
{
	private ArrayList<Force> forces = new ArrayList<Force>();
	private double netAMag;
	private double netADir;
	private double netVMag;
	private double netVDir;
	private long lastUpdate;
	private double speedMult;
	public PhysicsSprite ()
	{
		lastUpdate = System.currentTimeMillis();
	}
	private void sumForces()
	{
		double xComp = 0;
		double yComp = 0;
		for (int i = 0; i < forces.size(); i++) 
		{
			Force force = forces.get(i);
			xComp += force.getMagnitude() * Math.cos(force.getDirection());
			yComp += force.getMagnitude() * Math.sin(force.getDirection());
		}
		netAMag = Math.sqrt(xComp*xComp + yComp*yComp);
		netADir = Math.atan2(xComp, yComp);
	}
	public void update()
	{
		long millisDelta = System.currentTimeMillis() - lastUpdate;
		double timeMult = (millisDelta+0.0)/(1000+0.0);
		sumForces();
		double vXComp = netVMag * Math.cos(netVDir);
		double vYComp = netVMag * Math.sin(netVDir);
		double aXComp = netAMag * Math.cos(netADir);
		double aYComp = netAMag * Math.sin(netADir);
		vXComp += aXComp * timeMult;
		vYComp += aYComp * timeMult;
		netVMag = Math.sqrt(vXComp*vXComp + vYComp*vYComp);
		netVDir = Math.atan2(vXComp, vYComp);
		this.vectorMove(netVDir, netVMag * timeMult * getSpeedMult());
		lastUpdate = System.currentTimeMillis();
	}
	public ArrayList<Force> getForces()
	{
		return forces;
	}

	public void setForces(ArrayList<Force> forces) 
	{
		this.forces = forces;
	}
	public double getSpeedMult() {
		return speedMult;
	}
	public void setSpeedMult(double speedMult) {
		this.speedMult = speedMult;
	}

}