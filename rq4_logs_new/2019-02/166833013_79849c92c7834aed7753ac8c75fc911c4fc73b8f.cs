using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class N_CardEvent : MonoBehaviour
{

    public int number;
    public N_CardDeckSys CDS;
    public Text energyText;
    private int energy;

    void Start()
    {
        CDS.removeMark(number);
    }

    private void OnEnable()
    {
        setEnergy();
    }

    public void setEnergy()
    {
        int CardType = CDS.getCardType(number);
        switch (CardType)
        {
            case 0:
            case 1:
            case 2:
            case 3:
            case 8:
                energyText.text = "" + 0;
                energy = 0;
                break;
            case 4:
            case 5:
                energyText.text = "" + 30;
                energy = 30;
                break;
            case 6:
                energyText.text = "" + 15;
                energy = 15;
                break;
            case 7:
                energyText.text = "" + 20;
                energy = 20;
                break;
        }
    }

    // 마우스가 눌렀다 땠을 때
    public void OnMouseUp()
    {
        // 마우스 왼쪽인 경우
        if (Input.GetMouseButtonUp(0))
        {
            if (CDS.setEnergy(energy))
            {
                CDS.clickLeftCard(number);
                setEnergy();
            }
        }
    }

    // 마우스가 들어온 순간
    public void OnMouseEnter()
    {
        CDS.setMark(number);
    }

    // 마우스가 나가는 순간
    public void OnMouseExit()
    {
        CDS.removeMark(number);
    }

}