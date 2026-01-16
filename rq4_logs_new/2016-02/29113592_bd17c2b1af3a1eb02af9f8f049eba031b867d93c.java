package com.flipturnapps.kevinLibrary.sprite.physics;

public class GravitationalForce implements Force 
{
	private PhysicsSprite major;
	private PhysicsSprite minor;
	private double gravitationalConstant = 2;

	public GravitationalForce(PhysicsSprite major, PhysicsSprite minor) 
	{
		this.major = major;
		this.minor = minor;
	}
	@Override
	public double getMagnitude(PhysicsSprite s)
	{
		double numerator = gravitationalConstant * major.getMass() * minor.getMass();
		double x = deltaX(); 
		double y = deltaY(); 
		double denominator = x*x+y*y;
		return numerator/denominator;
	}
	private double deltaY() 
	{
		return major.getY() + major.getComY() - minor.getY() - minor.getComY();
	}
	private double deltaX()
	{
		return major.getX() + major.getComX() - minor.getX() - minor.getComX();
	}

	@Override
	public double getDirection(PhysicsSprite s)
	{
		double mod = Math.PI;
		if (s != major)
			mod += Math.PI;
		return Math.atan2(deltaY(), deltaX()) + mod;
	}
	public double getGravitationalConstant() 
	{
		return gravitationalConstant;
	}
	public void setGravitationalConstant(double gravitationalConstant) 
	{
		this.gravitationalConstant = gravitationalConstant;
	}
	

}