using UnityEngine;
using System.Collections;

public class yoga : MonoBehaviour {
	string[] lines;
	int count = 0;
	int lineN = 0;
	int max_poses = 3;
	char[] delimiterChars = {' ', '(', 'R', 'L', ',', ')' };
	GameObject lSphere;
	GameObject rSphere;
	// Use this for initialization
	void Start () {
		lines = System.IO.File.ReadAllLines(@"Assets/Scenes/Yoga/yogaPoses.txt");	
		parse(0);
	}

	// Update is called once per frame
	void Update () {
		//5 second changes each
		count++;
		Debug.Log ("count: " + count);
		if (count > 60) {
			lineN++;
			count = 0;
			parse(lineN);
		} 
		if (lineN > max_poses) {
			lineN = 0;
		}
	}
		
	void parse(int lineNum) {
		string[] positions;
		positions = lines[lineNum].Split(delimiterChars);
		Debug.Log (int.Parse (positions[0]) + ',' + int.Parse(positions[1]) + ',' + int.Parse (positions[2]));
		Debug.Log (int.Parse (positions[3]) + ',' + int.Parse(positions[4]) + ',' + int.Parse (positions[5]));
		Vector3 rightV = new Vector3 (int.Parse(positions[0]), int.Parse(positions[1]), int.Parse(positions[2]));
		Vector3 leftV = new Vector3 (int.Parse(positions[3]), int.Parse(positions[4]), int.Parse(positions[5]));
		if (rSphere == null) 
			rSphere = GameObject.CreatePrimitive(PrimitiveType.Sphere);
		rSphere.transform.position = rightV;
		rSphere.GetComponent<Renderer> ().material.color = new Color (0, 255, 0);
		if (lSphere == null) 
			lSphere = GameObject.CreatePrimitive(PrimitiveType.Sphere);
		lSphere.transform.position = leftV;
		lSphere.GetComponent<Renderer> ().material.color = new Color (0, 0, 255);
	}
		
}