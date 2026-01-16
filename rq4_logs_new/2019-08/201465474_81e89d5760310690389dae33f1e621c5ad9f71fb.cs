using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameGoal : MonoBehaviour {
    

    public GameObject GoalText;

    // Use this for initialization
    void Start () {

    GoalText.SetActive(false);

    }

    // Update is called once per frame
    void Update () {

    }

    private void OnTriggerEnter(Collider collider){
        if (collider.gameObject.tag == "Player"){
            Debug.Log("poti");
            GoalText.SetActive(true);
        }
    }

}