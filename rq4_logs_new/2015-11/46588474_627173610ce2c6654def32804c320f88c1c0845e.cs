using UnityEngine;
using System.Collections;
using System.Collections.Generic;


public enum operations
{ SUP,INF};


public struct assocMoods
{
    public string moodType;
    public operations comparison;
    public int value;
}

/* fonctionnement general d'un bloc , classe purment virtuelle Behavior et non behaviour parce que behaviour est prit par unity*/
public abstract class Behavior : MonoBehaviour {
    [SerializeField]
    protected List<assocMoods> tests;

    public bool isActive = false;

	// Use this for initialization
	void Start () {
	}
	
	// Update is called once per frame
	void Update () {
	
	}

    void OnTriggerEnter2D(Collider2D coll)
    {
        Debug.Log("collision");
        State characterState;
        if (coll.gameObject.tag == "Character" && isActive)
        {
            characterState = coll.gameObject.GetComponent<State>();
            bool result = true;
            foreach (assocMoods elt in tests)
            {
                result = result && characterState.TestValue(elt.moodType, elt.comparison, elt.value);
            }
            if (result)
            {
                Success(coll);
            }
            else
            {
                Fail(coll);
            }
        }
    }

    protected abstract void Success(Collider2D coll);

    protected abstract void Fail(Collider2D coll);



}