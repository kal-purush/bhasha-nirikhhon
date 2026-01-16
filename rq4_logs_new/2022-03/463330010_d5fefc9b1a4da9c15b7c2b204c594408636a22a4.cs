using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ButtonModeScript : MonoBehaviour
{

    public GameObject swipeButtons;
    public GameObject dPadButtons;
    // Start is called before the first frame update


    void Awake() {

        Time.timeScale = 0f;
    }
    void Start()
    {
        Time.timeScale = 0f;
    }

    // Update is called once per frame
    void Update()
    {
        Time.timeScale = 0f;
    }



    public void swipeButton()
    {

        swipeButtons.SetActive(true);
        dPadButtons.SetActive(false);
        gameObject.SetActive(false);
        Time.timeScale = 1f;
    }

    public void dPadButton() {

        swipeButtons.SetActive(false);
        dPadButtons.SetActive(true);
        gameObject.SetActive(false);
        Time.timeScale = 1f;
    }
}