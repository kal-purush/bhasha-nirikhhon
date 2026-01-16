using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Inventory : MonoBehaviour
{
    public GameObject inventario;
    public GameObject btns;
    public GameObject Volver;

   private void Start()
    {
        btns.SetActive(true);
        Volver.SetActive(false);
        inventario.SetActive(false);
    }

    public void Activar()
    {
        btns.SetActive(false);
        Volver.SetActive(true);
        inventario.SetActive(true);

    }
   public void Desactivar()
   {
        btns.SetActive(true);
        Volver.SetActive(false);
        inventario.SetActive(false);
    }
}