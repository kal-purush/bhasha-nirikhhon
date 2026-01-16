package com.endercrypt.jdarwin.bot.field;

public class ChangeValue
{
	private double value;
	private double changed;

	public ChangeValue(double value)
	{
		this.value = value;
	}

	public void add(double value)
	{
		set(this.value + value);
	}

	public void set(double value)
	{
		changed += (value - this.value);
	}

	public void resetChange()
	{
		changed = 0.0;
	}

	public double getChange()
	{
		return changed;
	}

	public double get()
	{
		return value;
	}
}