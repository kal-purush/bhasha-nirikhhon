using UnityEngine;
using System.Collections;

public class BordeGol : MonoBehaviour {

    public ePlayer jugador;
    public Puntaje puntaje;

	void OnCollisionEnter(Collision col)
    {
        Bola bola = col.gameObject.GetComponent<Bola>();
        if (bola != null)
        {
            bola.transform.position = new Vector3(0f, (float)0.8, 0f);
            if (jugador == ePlayer.Right)
            {
                puntaje.puntaje2++;    
            }
            else if (jugador == ePlayer.Left)
            {
                puntaje.puntaje1++;
            }
        }
    }
}