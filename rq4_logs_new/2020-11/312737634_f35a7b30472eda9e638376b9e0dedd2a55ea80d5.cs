using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class Restart : MonoBehaviour
{
    private SpeedControl speedControl;
    private bool pronto=false;



    void Start()
    {
        speedControl=GameObject.FindWithTag("speedManager").GetComponent<SpeedControl>();
        gameObject.SetActive(false);
    }

    void OnEnable()
    {
        pronto=true;
    }

    void Update(){
        if(pronto)
            speedControl.StopWorldSpeed();
    }

    public void Reestart(){
        Scene scene = SceneManager.GetActiveScene(); SceneManager.LoadScene(scene.name);
    }
}