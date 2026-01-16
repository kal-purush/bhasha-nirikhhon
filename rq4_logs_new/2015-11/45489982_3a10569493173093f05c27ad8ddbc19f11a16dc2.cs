using UnityEngine;
using System.Collections;

public class Game : MonoBehaviour {
    public GameObject characterPrefab;
    private Vector2 ArenaPosition;
    private Vector2 ArenaSize;
    private int numOfCharacter;
    private BoidsController boidsController;
	// Use this for initialization
	void Start () {
        boidsController = new BoidsController();
	    ArenaPosition = new Vector2(-5f, -5f);
        ArenaSize = new Vector2(10f, 10f);

        numOfCharacter = 20;
        for (int i = 0; i < numOfCharacter; ++i) {
            Character character = ((GameObject)Instantiate(characterPrefab, Vector2.zero, Quaternion.identity)).GetComponent<Character>();
            // GameObject characterObj = (GameObject)Instantiate(characterPrefab, Vector2.zero, Quaternion.identity);
            
            Vector2 position = new Vector2(Random.Range(0, ArenaSize.x), Random.Range(0, ArenaSize.y));
            position += ArenaPosition;
            character.GetComponent<Transform>().position = position;
            boidsController.AddCharacter(ref character);
            
        }
        boidsController.Start();
	}
	
	// Update is called once per frame
	void Update () {
        boidsController.Update();	
	}
}