using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public  static class EscuchadorEventos 
{
    #region Delegados
    public delegate void ManejadorJugadorPerdido(GameObject jugador);
    public delegate void ManejadorJugadorEncontrado(GameObject jugador);
    #endregion
    #region Eventos
    public static event ManejadorJugadorPerdido OnJugadorPerdido;
    public static event ManejadorJugadorEncontrado OnJugadorEncontrado;
    #endregion
    public static void JugadorEncontrado(GameObject juga)
    {
        Debug.Log("Voy a lanzar el evento jugador encontrado");
        OnJugadorEncontrado?.Invoke(juga);
    }
}