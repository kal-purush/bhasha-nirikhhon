using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Photon.Pun;
using Photon.Realtime;
using UnityEngine.UI;

public class ConectarSala : MonoBehaviourPunCallbacks
{
    public Text titulo;
    public Button boton_sala1;
    public Button boton_sala2;

    // Start is called before the first frame update
    void Start()
    {
        titulo.text = "Bienvenido " + PhotonNetwork.NickName;
    }

    public void pulsa_botonS1()
    {
        RoomOptions opciones = new RoomOptions();
        opciones.MaxPlayers = 2;
        PhotonNetwork.JoinOrCreateRoom("Circuito1", opciones, TypedLobby.Default);
    }

    public void pulsa_botonS2()
    {
        RoomOptions opciones = new RoomOptions();
        opciones.MaxPlayers = 2;
        PhotonNetwork.JoinOrCreateRoom("Circuito2", opciones, TypedLobby.Default);
    }

    private void Update()
    {
        if (PhotonNetwork.InRoom)
        {
            int jugadoresC = PhotonNetwork.CurrentRoom.PlayerCount;

            titulo.text = "Esperando jugadores..." + jugadoresC + "/2";

            if (PhotonNetwork.IsMasterClient && jugadoresC == 2)
            {
                string salaNombre = PhotonNetwork.CurrentRoom.Name;

                if (salaNombre == "Circuito1")
                {
                    PhotonNetwork.LoadLevel("Circuito1");
                }
                else if (salaNombre == "Circuito2")
                {
                    PhotonNetwork.LoadLevel("Circuito2");
                }

                Destroy(this);
            }
        }
    }
}