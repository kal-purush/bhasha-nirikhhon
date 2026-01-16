using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;

public class OtherCompanyController : MonoBehaviour
{

    private TextMeshProUGUI companyName;
    
    // Start is called before the first frame update
    void Start()
    {
        companyName = transform.Find("OtherCompanyNameText").GetComponent<TextMeshProUGUI>();
    }

    public void SetCompanyName(string companyName)
    {
        transform.Find("OtherCompanyNameText").GetComponent<TextMeshProUGUI>().SetText(companyName);
    }


    // Update is called once per frame
    void Update()
    {
        
    }
}