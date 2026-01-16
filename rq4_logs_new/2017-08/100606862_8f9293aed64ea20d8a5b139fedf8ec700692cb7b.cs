using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Beertender : MonoBehaviour {

    public GameObject staff;
    public GameObject ownerManager;

    public GameObject panelBeertender;
    public GameObject panelTerminos;
    public GameObject panelConoceMarca;

    private void OnDisable()
    {
        staff.SetActive(false);
        ownerManager.SetActive(false);
        panelBeertender.SetActive(false);
    }

    public void BTN_ShowBeertender()
    {
        panelBeertender.SetActive(true);
        panelTerminos.SetActive(false);
        panelConoceMarca.SetActive(false);
        switch (User.instance.rol)
        {
            case Rol.STAFF:
                staff.SetActive(true);
                break;
            case Rol.MANAGER:
                ownerManager.SetActive(true);
                break;
            case Rol.OWNER:
                ownerManager.SetActive(true);
                break;
        }
    }

    public void BTN_ConoceMarca()
    {
        panelTerminos.SetActive(false);
        panelConoceMarca.SetActive(true);
        panelBeertender.SetActive(false);
    }

    public void BTN_TerminosYCondiciones()
    {
        panelTerminos.SetActive(true);
        panelConoceMarca.SetActive(false);
        panelBeertender.SetActive(false);
    }

}