using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Login : MonoBehaviour {

    public InputField txtUser;
    public InputField txtPassword;

    public Text nameTopRigth;
    public Text premiosStaffPuntos;

	void Start () {
        
	}

    public void BTN_Ingresar()
    {
        PhpQuery.GetUser(int.Parse(txtUser.text), Algo);
    }

    private void Algo(string result)
    {
        User u = JsonUtility.FromJson<JsonParser<User>>(result).data;
        print(u.rol);
        nameTopRigth.text = u.firstName.ToUpper() + " " + u.lastName.ToUpper();
        premiosStaffPuntos.text = "TENÉS <size=70> "+u.total_pts+" </size> PUNTOS";
        gameObject.SetActive(false);
    }

}