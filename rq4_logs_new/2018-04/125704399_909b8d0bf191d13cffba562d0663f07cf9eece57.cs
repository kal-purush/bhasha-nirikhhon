using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class myprocessbar : MonoBehaviour {
    public Transform myprogress;
    public Transform setText;
    public Transform loadingtext;
    private float currentAmount=0;
    private float m_speed=20;

	// Update is called once per frame
	void Update () {
        if (currentAmount < 100)
        {
            currentAmount += m_speed * Time.deltaTime;
            setText.GetComponent<Text>().text = ((int)currentAmount).ToString() + "%";
            setText.gameObject.SetActive(true);
        }
        else
        {
            loadingtext.GetComponent<Text>().text = "Done";
            setText.gameObject.SetActive(false);
        }
        myprogress.GetComponent<Image>().fillAmount = currentAmount / 100;
    }
}