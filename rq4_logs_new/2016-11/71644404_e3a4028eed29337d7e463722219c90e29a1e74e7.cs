using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class AffichageHist5 : MonoBehaviour
{

    public Text TextBuzz;
    public Text TextHorcan;
    private int i = 0;
    private int j = 2;
    private int k = 0;
    private int l = 0;
    private string strComplete;
    private string[] tabBuzz = { "Nous avons réussi !!! Prêt pour le voyage ?", "Oui tu vas retrouver ta maison. La première planète a exploré d'après les plans de grand-père est Atargatis.", "J'espère que l'on trouvera des réponses sur la planète d'où tu viens."};
    private string[] tabHorcan = { "Rentrer...maison?", "Atargatis.....", "Merci...Buzz!!" };
    public GameObject CanevaBuzz;
    public GameObject CanevaHorcan;
    public Text Buzz;
    public Text Horcan;
    private bool value = true;
    public AudioSource paroleBuzz1;
    public AudioSource paroleHorcan1;
    void Start()
    {
        CanevaBuzz.SetActive(value);
        CanevaHorcan.SetActive(!value);
        strComplete = tabBuzz[k];
        paroleBuzz1.Play();
        k++;
    }


    void Update()
    {
        if (j == 2)
        {
            if (i < strComplete.Length)
            {
                if (value)
                {
                    TextBuzz.text += strComplete[i++];
                }
                else
                {
                    TextHorcan.text += strComplete[i++];
                }
            }
            else
            {
                if (value)
                {
                    paroleBuzz1.Pause();
                }
                else
                {
                    paroleHorcan1.Pause();
                }
            }
            j = 0;
        }
        else
        {
            j++;
        }

        if (i == strComplete.Length && Input.GetKeyDown(KeyCode.Space))
        {
            if (tabBuzz.Length != k || tabHorcan.Length != l)
            {
                i = 0;
                value = !value;
                CanevaBuzz.SetActive(value);
                CanevaHorcan.SetActive(!value);
                if (k > l)
                {
                    strComplete = tabHorcan[l];
                    paroleHorcan1.Play();
                    l++;
                    TextBuzz.text = "";
                }
                else
                {
                    strComplete = tabBuzz[k];
                    paroleBuzz1.Play();
                    k++;
                    TextHorcan.text = "";
                }
            }

        }

    }
}