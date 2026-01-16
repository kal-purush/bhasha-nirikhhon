using UnityEngine;
using System.Collections;

public class Food : MonoBehaviour {

	public int foodPoints = 10;
	public FoodType type;
}

public enum FoodType {
	Edible,
	Drink
}