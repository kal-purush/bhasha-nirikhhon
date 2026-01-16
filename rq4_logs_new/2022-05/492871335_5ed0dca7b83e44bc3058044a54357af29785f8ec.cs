using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;

public class sobreBoton : MonoBehaviour, IPointerEnterHandler, IPointerExitHandler
{
    public RectTransform boton;
    // Start is called before the first frame update
    void Start()
    {
        boton.GetComponent<Animator>().Play("Off");
    }
    public void OnPointerEnter(PointerEventData eventData)
    {
        boton.GetComponent<Animator>().Play("On");
    }
    public void OnPointerExit(PointerEventData eventData)
    {
        boton.GetComponent<Animator>().Play("Off");
    }
}