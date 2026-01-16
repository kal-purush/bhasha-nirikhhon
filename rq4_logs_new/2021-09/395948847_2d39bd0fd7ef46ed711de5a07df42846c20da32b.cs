using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Saishuu_panelclose : MonoBehaviour
{
    //2021.9.7　服部
    [SerializeField]
    public GameObject role_change_canvas;
    public GameObject theme_change_canvas;
    public GameObject Saishuu_Panel;
    // Start is called before the first frame update
    // Update is called once per frame
    public void panelclose()
    {
        Saishuu_Panel.SetActive(false);
        role_change_canvas.gameObject.SetActive(true);
        theme_change_canvas.gameObject.SetActive(true);

    }
}