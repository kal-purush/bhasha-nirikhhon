using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class SkipScene : MonoBehaviour
{
    public Image[] cinematic;
    public GameObject Empezarjuego, Continuar, Saltar;
    int cont;
    // Use this for initialization
    void Start()
    {
        cont = 0;
    }

    // Update is called once per frame
    void Update()
    {

    }

    public void CargarEscena()
    {
        SceneManager.LoadScene("Juego");
    }
    public void Continue()
    {
        cinematic[cont].gameObject.SetActive(false);
        cinematic[cont + 1].gameObject.SetActive(true);
        if (cont < cinematic.Length - 2)
        {
            cont++;
        }
        else
        {
            Saltar.gameObject.SetActive(false);
            Continuar.gameObject.SetActive(false);
            Empezarjuego.gameObject.SetActive(true);
        }
    }
}
