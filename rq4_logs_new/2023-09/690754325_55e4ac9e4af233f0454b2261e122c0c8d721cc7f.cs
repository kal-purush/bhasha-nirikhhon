using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class InternationalText : MonoBehaviour
{
    [SerializeField] string _en;
    [SerializeField] string _ru;

    private void Start()
    {
        if (Language.Instance.currentLanguage == "en")
        {
            GetComponent<Text>().text = _en;
        }
        else if (Language.Instance.currentLanguage == "ru")
        {
            GetComponent<Text>().text = _ru;
        }
        else
        {
            GetComponent<Text>().text = _en;
        }
    }

}