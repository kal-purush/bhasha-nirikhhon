using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Score : PersistentSingleton<Score>
{
    List<float> listaPuntos;
    [SerializeField] GameObject canvasPuntuacion;
    [SerializeField] Text txt;
    float puntos;
    float tiempo;

    float segundos;
    float minutos;

    private void Start()
    {
        canvasPuntuacion.SetActive(false);
        listaPuntos = new List<float>();
    }

    public void AñadirPuntos(float p)
    {
        puntos += p;
    }

    // Update is called once per frame
    void Update()
    {
        tiempo += Time.deltaTime;
        if(tiempo >= 1)
        {
            segundos++;
            tiempo = 0;
            if(segundos >= 60)
            {
                minutos++;
                segundos = 0;
            }
        }
        txt.text = "Puntos: " + puntos;
    }

    internal void GameEnd()
    {
        listaPuntos.Add(puntos);
        Time.timeScale = 0;
        canvasPuntuacion.SetActive(true);
        //canvasPuntuacion.GetComponentInChildren<Text>().text =
    }
}