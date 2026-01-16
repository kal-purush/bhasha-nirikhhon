package org.usfirst.frc4089.Stealth2017;


import org.usfirst.frc4089.Stealth2017.commands.AutoGoForwardFiveFeet;

import edu.wpi.first.wpilibj.command.Command;

public class AutoOptions {
	public static final String[] Options = new String[] {
			"Go Forward Five Feet",
			"Cross Low Defense",
			"Portcullis",
			"Low Goal Shot",
			"High Goal Shot (Low bar)",
			"High Goal Shot (Stationary Middle)"};
	
	//get each individual command, no duplicates
	static Command getCommand(int i)
	{
		Command[] allCommands = new Command[] {
			new AutoGoForwardFiveFeet()
		};
		return allCommands[i];
	}
	
	public static Command getAutoCommandFromString(String chosen)
	{
		for(int i = 0; i < Options.length; i++)
		{
			if(Options[i].equals(chosen)){
				return getCommand(i);
			}
		}
		return null; //for now
	}
}
