using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ManagerTester : MonoBehaviour
{
    public static ManagerTester Instance;
    
    public string mineralType;

    public int pointValue = 1;
   
    public GameManager gameManager;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    private void Awake()
    {
        Instance = this;
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetMouseButtonDown(0))
        {
            BreakMineral();
        }
    }

    public void BreakMineral()
    {
        Debug.Log("Broke Mineral 2");
      gameManager.TestMineralMined();
    }

}