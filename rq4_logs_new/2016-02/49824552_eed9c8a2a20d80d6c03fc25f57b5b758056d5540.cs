using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using RTS;

public class Team : MonoBehaviour {

	public string teamName;
	public Color teamColor;

	private List<Building> buildingsList = new List<Building>();
	private List<Player> playersList = new List<Player>();


	// Method to initialize the team
	public static Team InstantiateTeam(string teamName, Color teamColor) 
	{
		GameObject teamObject = (GameObject)Instantiate(Resources.Load("Team")); 
		teamObject.name = "Team " + teamName;
		Team myTeam = teamObject.GetComponent<Team>();
		myTeam.teamName = teamName;
		myTeam.teamColor = teamColor;

	 	return myTeam;
	}


	public void AddPlayerInTeam(Player player) { 
		playersList.Add(player);
		player.team = this; 
	}
	

	public void AddBuildingInList(Building building) {
    	buildingsList.Add(building);
    }

    public void RemoveBuildingInList(Building building) {
    	buildingsList.Remove(building);
    }

    public List<Building> GetBuildingList() {
    	return buildingsList;
    }
}