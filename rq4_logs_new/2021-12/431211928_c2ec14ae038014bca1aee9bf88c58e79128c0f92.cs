using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Back : MonoBehaviour
{
    [SerializeField]
    GameObject Logo;

    [SerializeField]
    GameObject Ma_Menu;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void Return()
    {
        Logo.SetActive(true);
        Ma_Menu.SetActive(true);
        this.gameObject.SetActive(false);
    }
}