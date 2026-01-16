using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class AffichageHist4 : MonoBehaviour
{

    public Text TextBuzz;
    public Text TextHorcan;
    private int i = 0;
    private int j = 2;
    private int k = 0;
    private int l = 0;
    private string strComplete;
    private string[] tabBuzz = { "La porte est vérouillée et je ne vois pas de bouton...", "Mais bien sur, un ordinateur. On dirait bien qu'il faut faire bouger le point à travers le labyrinthe.", "Ah oui et il faut aussi faire bouger le masque pour voir où l'on va.", "Tu as raison, tu es intelligent. Il faut récupérer des indices dans le labyrinthe pour trouver le mot de passe.", "Oui allons-y, moi je bouge le point et toi le masque." };
    private string[] tabHorcan = { "Machine...Code?", "Lumière...Bouger lumière aussi?", "Récupérer indice, phrase... devinette ?", "Besoin...aide?","D'accord Buzz." };
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
