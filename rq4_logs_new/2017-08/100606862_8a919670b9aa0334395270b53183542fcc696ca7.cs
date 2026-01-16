using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Premios : MonoBehaviour {

    public GameObject panelStaff;
    public GameObject panelManager;
    public GameObject panelOwner;

    public void BTN_ShowPremios(Rol rol)
    {
        switch (rol)
        {
            case Rol.STAFF:
                panelStaff.SetActive(true);
                break;
            case Rol.MANAGER:
                panelManager.SetActive(true);
                break;
            case Rol.OWNER:
                panelOwner.SetActive(true);
                break;
        }
    }
}


public enum Rol
{
    STAFF,MANAGER,OWNER
}