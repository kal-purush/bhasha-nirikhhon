using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class BoidsController : MonoBehaviour {

    private float neighborRadius;
    private List<Character> characters = new List<Character>();
	// Use this for initialization
	void Start () {
	
        neighborRadius = 300f;
	}
	
    public void AddCharacter(ref Character i_character) {
        characters.Add(i_character);
    }

    //
    public List<Character> FindCharactersInNeighborhood(ref Character i_character) {
        Vector2 center = (Vector2) i_character.GetComponent<Transform>().position;
        List <Character> characterInNeighborhood = new List<Character>();
        Character[] characters = FindObjectsOfType(typeof(Character)) as Character[];
        foreach (Character character in characters) {
            float distance = Vector2.Distance(center, (Vector2) character.GetComponent<Transform>().position);
            if (distance < neighborRadius) {
                characterInNeighborhood.Add(character);
            }
        }

        return characterInNeighborhood;
    }
	// Update is called once per frame
	void Update () {
	
	}
}