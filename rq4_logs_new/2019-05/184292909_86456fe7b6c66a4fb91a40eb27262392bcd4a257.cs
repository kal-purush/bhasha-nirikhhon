using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ShakeController : MonoBehaviour
{
    static ShakeController _instance;

    public static ShakeController Instance
    {
        get
        {
            if (_instance == null)
            {
                _instance = FindObjectOfType<ShakeController>();
            }
            return _instance;
        }
    }

    public static void GreateShake(){
        Instance.GetComponent<Animator>().SetTrigger("Greate");
    }

    public static void SetSmallShake(bool b){
        Instance.GetComponent<Animator>().SetBool("Small",b);
    }

}