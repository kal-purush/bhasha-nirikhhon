using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class mirarController : MonoBehaviour
{
    #region Delegados
    public delegate void ManejadorJugadorPerdido(GameObject jugador);
    public delegate void ManejadorJugadorEncontrado(GameObject jugador);
    #endregion
    #region Eventos
    public event ManejadorJugadorPerdido OnJugadorPerdido;
    public event ManejadorJugadorEncontrado Encontrado;
    #endregion
    private GameObject[] ojos;
    private GameObject objetivo;
    public static mirarController OjosController { get; private set; }
    private void Awake()
    {
        if (OjosController != null)
        {
            Destroy(gameObject);
        }
        OjosController = this;
        ojos = new GameObject[35];
        // arriba

        ojos[0] = transform.GetChild(0).transform.GetChild(0).transform.gameObject;
        ojos[1] = transform.GetChild(0).transform.GetChild(1).transform.gameObject;
        ojos[2] = transform.GetChild(0).transform.GetChild(2).transform.gameObject;
        ojos[3] = transform.GetChild(0).transform.GetChild(3).transform.gameObject;
        ojos[4] = transform.GetChild(0).transform.GetChild(4).transform.gameObject;
        ojos[5] = transform.GetChild(0).transform.GetChild(5).transform.gameObject;
        ojos[6] = transform.GetChild(0).transform.GetChild(6).transform.gameObject;

        // frente

        ojos[7] = transform.GetChild(1).transform.GetChild(0).transform.gameObject;
        ojos[8] = transform.GetChild(1).transform.GetChild(1).transform.gameObject;
        ojos[9] = transform.GetChild(1).transform.GetChild(2).transform.gameObject;
        ojos[10] = transform.GetChild(1).transform.GetChild(3).transform.gameObject;
        ojos[11] = transform.GetChild(1).transform.GetChild(4).transform.gameObject;
        ojos[12] = transform.GetChild(1).transform.GetChild(5).transform.gameObject;
        ojos[13] = transform.GetChild(1).transform.GetChild(6).transform.gameObject;

        // abajo

        ojos[14] = transform.GetChild(2).transform.GetChild(0).transform.gameObject;
        ojos[15] = transform.GetChild(2).transform.GetChild(1).transform.gameObject;
        ojos[16] = transform.GetChild(2).transform.GetChild(2).transform.gameObject;
        ojos[17] = transform.GetChild(2).transform.GetChild(3).transform.gameObject;
        ojos[18] = transform.GetChild(2).transform.GetChild(4).transform.gameObject;
        ojos[19] = transform.GetChild(2).transform.GetChild(5).transform.gameObject;
        ojos[20] = transform.GetChild(2).transform.GetChild(6).transform.gameObject;

    }

    private void FixedUpdate()
    {
        mirar();
    }

    private void mirar()
    {
        for (int i = 0; i < 21; i++)
        {
            RaycastHit hit;

            if (Physics.Raycast(ojos[i].transform.position, ojos[i].transform.forward, out hit, 15))
            {
                if (hit.transform.gameObject.tag == "Jugador")
                {
                    objetivo = hit.transform.gameObject;
                    Debug.DrawRay(ojos[i].transform.position, ojos[i].transform.forward * hit.distance, Color.yellow);
                    JugadorEncontrado(objetivo);
                }
                else
                {
                    Debug.DrawRay(ojos[i].transform.position, ojos[i].transform.forward * hit.distance, Color.white);
                }
            }
            else
            {
                Debug.DrawRay(ojos[i].transform.position, ojos[i].transform.forward * 15, Color.white);
            }
        }
    }
    public void JugadorEncontrado(GameObject jugador)
    {
        Encontrado?.Invoke(jugador);
    }
}