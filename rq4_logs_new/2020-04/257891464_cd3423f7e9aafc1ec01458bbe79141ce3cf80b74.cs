using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class Score : TemporalSingleton<Score>
{

    static List<float> listaPuntos;
    public GameObject canvasPuntuacion;
    public Text txt;
    Text puntosTexto;
    [SerializeField] float timeUntilRestart = 5;
    float puntos;
    float tiempo;
    bool gameEnded;
    float segundos;
    float minutos;

    private void Start()
    {

        GameStart();
        if (listaPuntos == null)
        {
            listaPuntos = new List<float>();
        }
    }

    private void GameStart()
    {
        canvasPuntuacion = GameObject.Find("FinalCanvas");
        txt = GameObject.Find("Points").GetComponent<Text>();
        puntosTexto = canvasPuntuacion.GetComponentInChildren<Text>();
        canvasPuntuacion.SetActive(false);
        gameEnded = false;

    }

    public void AñadirPuntos(float p)
    {
        if (!gameEnded)
            puntos += p;
    }

    // Update is called once per frame
    void Update()
    {
        tiempo += Time.deltaTime;
        if (tiempo >= 1)
        {
            segundos++;
            tiempo = 0;
            if (segundos >= 60)
            {
                minutos++;
                segundos = 0;
            }
        }


        txt.text = "Puntos: " + puntos;
        canvasPuntuacion.SetActive(gameEnded);

    }

    private void RestartGame()
    {
        SceneManager.LoadScene(SceneManager.GetActiveScene().buildIndex);
    }

    internal void GameEnd()
    {

        if (!gameEnded)
        {
            gameEnded = true;
            listaPuntos.Add(puntos);
            listaPuntos.Sort();
            canvasPuntuacion.SetActive(true);
            puntosTexto.text = "Puntuaciones";
            for (int i = listaPuntos.Count - 1; i >= 0; i--)
            {
                puntosTexto.text += "\n " + (listaPuntos.Count - i) + ". " + listaPuntos[i];
            }
            Invoke("RestartGame", timeUntilRestart);
        }

    }
}