using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
 
public class HPGauge : MonoBehaviour
{

    public GameObject objHPGauge, objTextHPValue;
    private Image imgHPGauge;
    private Text textHP;
    private int maxHp, hp;

    // Use this for initialization
    void Start()
    {
        imgHPGauge = objHPGauge.GetComponent<Image>();
        textHP = objTextHPValue.GetComponent<Text>();
    }

    public int MaxHP
    {
        set { maxHp = value; }
    }

    public int HP
    {
        set { hp = value; }
    }

    // Update is called once per frame
    void Update()
    {
        imgHPGauge.fillAmount = (float)hp / maxHp;
        textHP.text = hp.ToString();
    }
}