using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class JogoXml 
{

    public int turno { get; set; }

    public string vencedor { get; set; }


    public void SetTurno()
    {
        turno = turno * -1;

    }

}