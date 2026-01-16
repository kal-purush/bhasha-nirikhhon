using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class FadeManager : MonoBehaviour {

    public Collider[] col;
    public GameObject fadeImage;
    public Image negro;
    public Animator anim;

    private CanvasGroup grupoFade;
    private float velocidadFade = 0.3f;

    // Use this for initialization
    void Start()
    {
        grupoFade = FindObjectOfType<CanvasGroup>();
        grupoFade.alpha = 1f;

        col = GetComponentsInChildren<Collider>();

        fadeImage = GameObject.Find("FadeImage");

        negro = fadeImage.GetComponent<Image>();
        anim = fadeImage.GetComponent<Animator>();
    }

    // Update is called once per frame
    void Update()
    {
        PrimerFade();
    }

    public void PrimerFade()
    {
        grupoFade.alpha = 1 - Time.timeSinceLevelLoad * velocidadFade;
    }

    public void OnTriggerEnter(Collider other)
    {
        if (other.CompareTag("Player"))
        {
            //Debug.Log("Fade");
            StartCoroutine(Fading());
        }
    }

    public void OnTriggerExit(Collider other)
    {
        if (other.CompareTag("Player"))
        {
            StartCoroutine(NoFading());
            //Debug.Log("NoFade");
        }
    }

    IEnumerator Fading()
    {
        anim.SetBool("Fade", true);
        yield return new WaitUntil(() => negro.color.a == 1);
    }

    IEnumerator NoFading()
    {
        anim.SetBool("Fade", false);
        yield return new WaitUntil(() => negro.color.a == 0);
    }
}