using UnityEngine;
using System.Collections;

public class AnimationSelector : MonoBehaviour {

    [SerializeField]
    protected string name;
    
    // Use this for initialization
    void Start () {
        gameObject.GetComponentInChildren<Animator>().SetBool(name, true);
	}
	
	// Update is called once per frame
	void Update () {
	
	}
}