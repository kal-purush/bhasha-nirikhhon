using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EndClear : MonoBehaviour
{
    bool end;
    public HP_ZERO hp_zero;
    public GameObject kogane; //kogane_wait
    public GameObject koganeMat;
    public Material gameOverMat;

    // Start is called before the first frame update
    void Start()
    {
        end = false;
    }

    // Update is called once per frame
    void Update()
    {
        if (end == false)
        {
            if (hp_zero.playerHP.value == 0)
            {
                kogane.GetComponent<Animator>().SetTrigger("gameover");
                koganeMat.GetComponent<Renderer>().material = gameOverMat;
            }
            else if (hp_zero.bossHP.value == 0)
            {

            }
        }
    }
}