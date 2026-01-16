using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class JugadorSelector : MonoBehaviour
{
    public Jugador jugador;
    public GameObject[] prefabs;
    // Start is called before the first frame update
    void Start()
    {
        this.Select(0);
    }
    void Update()
    {
        if (Input.GetKeyDown(KeyCode.X))
        {
            jugador.Spawn();
        }
    }
    public void Select(int index)
    {
        JugadorStorage.index = index;
        JugadorStorage.jugadorPrefab = this.prefabs[index];
    }
}