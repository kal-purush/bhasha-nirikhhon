using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;


public class UICompanyController : MonoBehaviour
{

    private TextMeshProUGUI companyName;


    // Start is called before the first frame update
    void Start()
    {
        companyName = transform.Find("CompanyNameText").GetComponent<TextMeshProUGUI>();
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void SetCompanyName(string newCompanyName)
    {
        //transform.Find("CompanyNameText").GetComponent<TextMeshProUGUI>().SetText(companyName);
        if (companyName == null)
        {
            companyName = transform.Find("CompanyNameText").GetComponent<TextMeshProUGUI>();
        }
        this.companyName.SetText(newCompanyName);
    }
}