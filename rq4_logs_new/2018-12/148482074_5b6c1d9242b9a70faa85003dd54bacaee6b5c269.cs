using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class gravityChangeDie : MonoBehaviour {
    [SerializeField]
    public ParticleSystem pulse;
    bool flag = false;
    // Use this for initialization
    void Start () {
        pulse = GetComponent<ParticleSystem>();
	}
	
	// Update is called once per frame
	void Update () {
        if(pulse.isPlaying){
            flag = true;
        }
        if(!pulse.isPlaying && flag == true){
            Destroy(this.gameObject);
        }
	}
}