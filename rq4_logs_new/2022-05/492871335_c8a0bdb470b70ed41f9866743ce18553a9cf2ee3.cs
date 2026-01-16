using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Ojos : MonoBehaviour
{
    public delegate void ManejadorJugadorEncontrado(GameObject jugador);
    public event ManejadorJugadorEncontrado OnJugadorEncontrado;
    public static Ojos Instancia { get; private set;}
    private void Awake()
    {
        if (Instancia!=null)
        {
            Destroy(gameObject);
        }
        Instancia = this;
    }
    private void OnTriggerEnter(Collider other)
    {
        if(other.tag=="Jugador")
        {
            OnJugadorEncontrado?.Invoke(other.gameObject);
        }
    }
}