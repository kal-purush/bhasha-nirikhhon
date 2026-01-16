using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class Menu : MonoBehaviour
{
    public Button botonJugar;
    public Button botonCreditos;
    public Button botonSalir;

    public void pulsarBotonJugar()
    {
        botonJugar.enabled = false;
        botonCreditos.enabled = false;
        botonSalir.enabled = false;
        SceneManager.LoadScene("conexion");
    }
    public void pulsarBotonCreditos()
    {
        botonJugar.enabled = false;
        botonCreditos.enabled = false;
        botonSalir.enabled = false;
        //SceneManager.LoadScene("creditos");
    }
    public void pulsarBotonSalir()
    {
        botonJugar.enabled = false;
        botonCreditos.enabled = false;
        botonSalir.enabled = false;
        //SceneManager.LoadScene("creditos");
    }
}