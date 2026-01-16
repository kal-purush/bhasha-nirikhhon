using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;


public class KeyPad : MonoBehaviour
{
    [SerializeField] private TextMeshProUGUI Ans; // Text hiển thị số đã nhập



    private string Answer = "361844"; // Mã đúng để mở cửa



    public void Number(int number)
    {
        Ans.text += number.ToString();
    }

    public void Execute()
    {
        if (Ans.text == Answer)
        {
            Ans.text = "Nhập đúng";

        }
        else
        {
            Ans.text = "Nhập Sai";
        }
    }

    public void Delete()
    {
        Ans.text = "";
    }


}