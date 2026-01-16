using UnityEngine;
using System.Collections;

public static class IncreaseExperience {

	private float addPlayerXP; 
	private float leftOverXP;

	//Setup stats for new player
	public static void setNewPlayer(bool aSavedPlayer)
	{
		if (!aSavedPlayer && Global.PlayerLevel == 0) {
			
				Global.PlayerLevel = 1;
				Global.RequiredXP = 100;
			

		}
	}

	public static void addExperience()
	{
		//Add exp to player, check to see if player leveled up
		addPlayerXP = Global.PlayerLevel * 13;

		Global.ExperiencePoints = addPlayerXP;

		if(Global.ExperiencePoints >= Global.RequiredXP)
		{
			nextLevel();

		}

	}



	public static void nextLevel(){

		//Send Level up message
		//Increase Stats
		//Gain Technique points
		//Increment player level
		//Assign leftover XP
		//Reset Required XP


		Global.AttackPoints += 30;
		Global.DefensePoints += 20;
		Global.MaxHP += 500;



		Global.PlayerLevel += 1;
		leftOverXP = Global.RequiredXP - Global.ExperiencePoints; 

	
		if(leftOverXP != 0.0F)
		{
			Global.ExperiencePoints = leftOverXP;
		}

		Global.RequiredXP += 100;

	}








}
