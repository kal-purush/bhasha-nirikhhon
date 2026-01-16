using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class RocketBuilderScript : MonoBehaviour {
	[SerializeField]
	private int rocketHeight = 7;

	enum RocketPart {
		TopEnd,
		Capsule,
		Tank,
		Engine
	}
	RocketPart[] topCombination = {RocketPart.Capsule, RocketPart.TopEnd};
	Dictionary<RocketPart, RocketPart[]> possibleTransitions = new Dictionary<RocketPart, RocketPart[]>()
	{
		{RocketPart.Tank, new RocketPart[]{ RocketPart.Tank, RocketPart.Engine }},
		{RocketPart.Engine, new RocketPart[]{ RocketPart.Tank }},
	};

	[SerializeField]
	GameObject[] topEnds;
	[SerializeField]
	GameObject[] capsules;
	[SerializeField]
	GameObject[] tanks;
	[SerializeField]
	GameObject[] engines;
	
	private RocketPart getRandomNextPart(RocketPart part) {
		RocketPart[] possibleNextParts = possibleTransitions [part];
		return possibleNextParts[Random.Range(0, possibleNextParts.Length)];
	}

	public GameObject[] createRocket() {
		List<RocketPart> rocketPlan = new List<RocketPart> ();
		rocketPlan.Add (RocketPart.Engine);
		
		while (rocketPlan.Count < rocketHeight) {
			if (rocketHeight - rocketPlan.Count == topCombination.Length + 1) {
				rocketPlan.Add(RocketPart.Tank);
				foreach(RocketPart topPart in topCombination) {
					rocketPlan.Add(topPart);
				}
			} else {
				rocketPlan.Add(getRandomNextPart(rocketPlan[rocketPlan.Count - 1]));
			}
		}

		GameObject[] rocket = new GameObject [rocketHeight];
		for (int i = 0; i < rocketHeight; i++) {
			GameObject[] possibleParts;
			switch (rocketPlan[i]) {
				case RocketPart.TopEnd:
					possibleParts = topEnds;
					break;
				case RocketPart.Capsule:
					possibleParts = capsules;
					break;
				case RocketPart.Tank:
					possibleParts = tanks;
					break;
				case RocketPart.Engine:
					possibleParts = engines;
					break;
				default:
					possibleParts = tanks;
					break;
			}
			rocket[i] = possibleParts[Random.Range(0, possibleParts.Length)];
		}

		return rocket;
	}
	
	// Update is called once per frame
	void Update () {
	
	}
}