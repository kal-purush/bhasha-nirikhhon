using UnityEngine;
using System.Collections;

public class Director : MonoBehaviour {

	public static Director main;

	public int currentScore;

	void Awake(){
		main = this;
	}

	public void AddScore(int score = 1){
		this.currentScore += score;
	}

}