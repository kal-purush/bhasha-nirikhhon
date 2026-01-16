using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TowerManager : Singleton<TowerManager>
{

    private TowerButton towerButtonPressed;


    public void selectedTower(TowerButton towerSelected)
    {
        towerButtonPressed = towerSelected;
        Debug.Log(" you pressed" + towerButtonPressed.gameObject);
    }


    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}