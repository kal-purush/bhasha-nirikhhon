using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TurnManager : MonoBehaviour {
	private int turn;
    private int players;
    public void SetPlayers(int players){
        this.players = players;
    }
	public int GetTurn(){
		return turn;
	}
	public void SetTurn(int turn){
		this.turn = turn;
	}
	public void TurnChange(){
		if (turn > players) {
			turn = 1;
		} 
		else {
			turn = +1;
		}
	}
	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
			
	}
}