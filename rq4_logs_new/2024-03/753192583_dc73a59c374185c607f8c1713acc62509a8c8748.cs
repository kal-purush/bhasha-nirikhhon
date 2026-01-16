using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using TMPro;
public class GameController : MonoBehaviour
{

    [Header("Ink JSON")]
    [SerializeField] private TextAsset scenarioOne; 
    // Start is called before the first frame update
    void Start()
    {
        ScenarioMaster.GetInstance().StartDialogue(scenarioOne); //passes the script into the dialogue controller and starts it up.
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}