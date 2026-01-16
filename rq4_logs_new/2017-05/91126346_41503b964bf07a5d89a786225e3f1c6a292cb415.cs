using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;

public class AIControlPerson : MonoBehaviour {
	private Person person;
	private Network network;
	
	private void Awake() {
		person = GetComponent<Person>();
		network = new Network(2, new [] { 4, 4 }, 3);
	}
	
	private void Update() {
		float[] outputs = network.Feed(new [] { Random.value, Random.value });
		person.Act(
			outputs[0],
			outputs[1],
			outputs[2]);
	}
}