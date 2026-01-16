using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;

public class sobreBotonServer : MonoBehaviour,IPointerEnterHandler,IPointerExitHandler
{
    public RectTransform boton;
    public AudioSource source;
    public AudioClip clip;
    void Start()
    {
        boton.GetComponent<Animator>().Play("OffServer");
    }
    public void OnPointerEnter(PointerEventData eventData)
    {
        source.PlayOneShot(clip);
        boton.GetComponent<Animator>().Play("OnServer");
    }
    public void OnPointerExit(PointerEventData eventData)
    {
        boton.GetComponent<Animator>().Play("OffServer");
    }
}