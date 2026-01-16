using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EventManager : TemporalSingleton<EventManager>
{
    [SerializeField] float duracionEvento;

    int numEvento;
    int eventoActual;

    float temporizador;


    // Start is called before the first frame update
    void Start()
    {
        temporizador = duracionEvento;
        CambiarEvento();
    }

    // Update is called once per frame
    void Update()
    {
        temporizador -= Time.deltaTime;
        if(temporizador <= 0)
        {
            CambiarEvento();
            temporizador = duracionEvento;
        }
    }

    private void CambiarEvento()
    {
        numEvento++;
        int aux = eventoActual;
        while (eventoActual == aux)
        {
            eventoActual = UnityEngine.Random.Range(0, 3);
        }
        switch (eventoActual)
        {
            case 0:
                break;
            case 1:
                FingerSpawner.Instance.SpawnFinger(numEvento);
                break;
            case 2:
                break;
        }
    }
}