using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Home : MonoBehaviour {

    public GameObject panelStaff;
    public GameObject panelOwnerManager;

    private void OnEnable()
    {
        print("ASD");
    }

    public void BTN_ShowHome()
    {
        switch (User.instance.rol)
        {
            case Rol.STAFF:
                panelStaff.SetActive(true);
                panelOwnerManager.SetActive(false);
                break;
            case Rol.MANAGER:
                panelStaff.SetActive(false);
                panelOwnerManager.SetActive(true);
                break;
            case Rol.OWNER:
                panelStaff.SetActive(false);
                panelOwnerManager.SetActive(true);
                break;
        }
    }
}