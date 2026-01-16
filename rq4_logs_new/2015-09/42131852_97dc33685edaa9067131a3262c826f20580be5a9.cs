using UnityEngine;
using System.Collections;

// Script for holding the general character controller information
public class PlayerModel : MonoBehaviour {

	public int health;
	public float walkSpeed;
	public bool isActivePlayer;
	public Animator animator;
	
	
	public Animator Animator
	{
		get {
			return animator;
		}
		set {
			animator = value;
		}
	}
	
	public int Health
	{
		get {
			return health;
		}
		set {
			health = value;
		}
	}

	public float WalkSpeed
	{
		get {
			return walkSpeed;
		}
		set {
			walkSpeed = value;
		}
	}
	
	public bool IsActivePlayer
	{
		get {
			return isActivePlayer;
		}
		set {
			isActivePlayer = value;
		}
	}
}