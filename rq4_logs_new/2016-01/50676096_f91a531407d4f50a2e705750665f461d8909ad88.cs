using UnityEngine;
using System.Collections;

public class Wywolanie : MonoBehaviour {
    public GameObject jednorozec;
    public GameObject wybuch;
    public GameObject demon;
    public GameObject partner;
    float wartosc;
	// Use this for initialization
 void wywolaj()
    {
        Gra gr = partner.GetComponent<Gra>();
        wartosc = gr.stability
        if(wartosc>70)
        {
            
        }
     else
        {
            if(wartosc>40)
            {

            }
            else
            {
                if(wartosc>15)
                {

                }
                else
                {

                }
            }
        }
        
    }
}