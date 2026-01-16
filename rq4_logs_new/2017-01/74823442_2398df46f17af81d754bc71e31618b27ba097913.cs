using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class VillagersContainer : MonoBehaviour
{
	private HashSet<GameObject> menInside = new HashSet<GameObject>();

	public void Add(GameObject villager)
	{
		if (Object.ReferenceEquals(villager.GetComponent<VillagerScript>().Container, gameObject))
			menInside.Add(villager);
		else
			villager.GetComponent<VillagerScript>().Container = gameObject;
	}

	public void Remove(GameObject villager)
	{
		if (Object.ReferenceEquals(villager.GetComponent<VillagerScript>().Container, gameObject))
			villager.GetComponent<VillagerScript>().Container = null;
		else if (villager.GetComponent<VillagerScript>().Container == null)
			menInside.Remove(villager);
	}

	// Use this for initialization
	void Start()
	{
		
	}
	
	// Update is called once per frame
	void Update()
	{
		foreach (var man in menInside)
			man.GetComponent<VillagerScript>().AI();
	}
}