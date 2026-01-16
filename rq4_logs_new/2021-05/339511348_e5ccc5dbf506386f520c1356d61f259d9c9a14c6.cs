using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class victoryscreen : MonoBehaviour
{
    [SerializeField]
    private GameObject completelevelui;
    // Start is called before the first frame update
    void Start()
    {
        BossHealth bosshealth = GetComponent<BossHealth>();
        bosshealth.OnWinning += Bosshealth_OnWinning;
    }

    private void Bosshealth_OnWinning(object sender, System.EventArgs e)
    {
        //Debug.Log("You win");
        completelevelui.SetActive(true);
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}