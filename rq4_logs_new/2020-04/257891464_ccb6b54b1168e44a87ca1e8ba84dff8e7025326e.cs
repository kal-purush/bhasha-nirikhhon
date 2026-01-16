using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;




public class Cursor : MonoBehaviour
{
    public enum Cuadrante
    {
        TopRight, TopLeft, BottomRight, BottomLeft
    }

    List<Cuadrante> cuadrantes;

    private Cuadrante cursorCuadrante = Cuadrante.TopRight;

    // Start is called before the first frame update
    void Start()
    {
        cuadrantes = new List<Cuadrante>();
    }

    // Update is called once per frame
    void Update()
    {
        ComprobarCuadrante();
        ComprobarCirculoHecho();
    }

    private void ComprobarCirculoHecho()
    {
        throw new NotImplementedException();
    }

    private void ComprobarCuadrante()
    {
        Vector3 pos = Camera.main.ScreenToWorldPoint(Input.mousePosition);
        if(pos.x >= 0 && pos.y >= 0)
        {
            cursorCuadrante = Cuadrante.TopRight;
        }
        else if(pos.x >= 0 && pos.y < 0)
        {
            cursorCuadrante = Cuadrante.BottomRight;
        }
        else if (pos.x < 0 && pos.y >= 0)
        {
            cursorCuadrante = Cuadrante.TopLeft;
        }
        else
        {
            cursorCuadrante = Cuadrante.BottomLeft;
        }
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        
    }
}