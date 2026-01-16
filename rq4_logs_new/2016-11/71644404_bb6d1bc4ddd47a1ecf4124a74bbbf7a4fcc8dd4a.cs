using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class AffichageHist3 : MonoBehaviour {

    public Text TextBuzz;
    public Text TextHorcan;
    private int i = 0;
    private int j = 2;
    private int k = 0;
    private int l = 0;
    private string strComplete;
    private string[] tabBuzz= { "Il y a quelqu'un dans le cratère...Bonjour... Qui êtes vous ?","Vous ne comprenez pas ce que je dis. Je suis désolé, moi c'est Buzz.","Vous parlez, c'est incroyable. D'où venez-vous ? C'est la première fois que je rencontre un extra-terrestre.","Oui bien sur je comprends, mon grand-père possède un vaisseau au Groënland je peux t'y conduire si tu veux.","Euh... oui bien sur je peux t'accompagner, suis moi allons-y." };
    private string[] tabHorcan = { "J... A...", "H..or..can, moi c'est Horcan.","Je...sais pas. Moi rentrer chez moi.","Besoin aide, je... conduire navette...euh... mal, toi aider moi ?","M..er..ci Buzz." };
    public GameObject CanevaBuzz;
    public GameObject CanevaHorcan;
    public Text Buzz;
    public Text Horcan;
    private bool value = true;
    public  AudioSource paroleBuzz1;
    public AudioSource paroleHorcan1;
    void Start()
    {
        CanevaBuzz.SetActive(value);
        CanevaHorcan.SetActive(!value);
        strComplete = tabBuzz[k];
        paroleBuzz1.Play();
        k++;
        Buzz.text = "Buzz";
        Horcan.text = "???";
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
                }else
                {
                    TextHorcan.text += strComplete[i++];
                }
            }else
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

        if(i == strComplete.Length && Input.GetKeyDown(KeyCode.Space)){
            if(tabBuzz.Length != k || tabHorcan.Length != l)
            {
                i = 0;
                value = !value;
                CanevaBuzz.SetActive(value);
                CanevaHorcan.SetActive(!value);
                if (k > l)
                {
                    if (l == 2) Horcan.text = "Horcan";
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