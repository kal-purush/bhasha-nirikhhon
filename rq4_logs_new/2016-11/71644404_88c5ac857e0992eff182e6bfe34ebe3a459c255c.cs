using UnityEngine;
using System.Collections;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class AffichageHist1 : MonoBehaviour {

    public Text Text;
    private int i = 0;
    private int j = 2;
    private string strComplete;
	void Start () {
        strComplete = "C’était un soir comme les autres pour tous les habitants de la planète Terre mais pas pour Buzz, passionné par les astres qui vivait dans une campagne au Nord du Canada. Il savait que ce soir vers minuit de nombreuses étoiles filantes allaient parcourir le ciel et que cet événement n’avait lieu que très rarement. Son grand-père, à qui il doit cette amour pour les étoiles, lui avait confié qu’il n’avait pu assister à un spectacle pareil qu’une seule fois dans sa longue vie et qu’il n’y avait rien de plus magnifique.";
	}
	
	
	void Update () {
        if (j == 2)
        {
            if (i < strComplete.Length)
            {
                Text.text += strComplete[i++];
            }
            j = 0;
        }else
        {
            j++;
        }

        if (Input.GetKeyDown(KeyCode.B))
        {
            SceneManager.LoadScene(1);
        }
	   
	}

}