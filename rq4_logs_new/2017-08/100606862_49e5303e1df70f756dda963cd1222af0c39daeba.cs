using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Contacto : MonoBehaviour {

    public Image background;
    public Sprite newBackgroundImage;
    public Sprite commonBackgroundImage;
    public GameObject panelContacto;

    public ToggleButton mensaje;
    public GameObject panelMessage;

    public ToggleButton contacto;
    public GameObject panelContactoWriteQuery;
    public GameObject panelContactoReadMessage;



    float scaleOff = 0.9f;

    public void OnEnable()
    {
        background.sprite = newBackgroundImage;
    }
  
    private void OnDisable()
    {
        background.sprite = commonBackgroundImage;
    }


    public void BTN_Contacto()
    {
        mensaje.Toggle(false);
        mensaje.button.transform.localScale = Vector3.one * scaleOff;
        contacto.Toggle(true);
        contacto.button.transform.localScale = Vector3.one;
        panelContactoWriteQuery.SetActive(true);
        panelContactoReadMessage.SetActive(false);
    }

    public void BTN_Mensaje()
    {
        mensaje.Toggle(true);
        contacto.Toggle(false);
        mensaje.button.transform.localScale = Vector3.one;
        contacto.button.transform.localScale = Vector3.one * scaleOff;
        panelContactoWriteQuery.SetActive(false);
        panelContactoReadMessage.SetActive(true);
    }

}