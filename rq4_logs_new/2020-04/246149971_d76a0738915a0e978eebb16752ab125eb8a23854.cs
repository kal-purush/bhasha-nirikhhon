using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CasasDialogos : MonoBehaviour
{
    public GameObject Dialogo1;
    public GameObject Dialogo2;
    public GameObject Dialogo3;
    public GameObject Dialogo4;
    public GameObject Dialogo5;
    public GameObject DialogoGeral;
    public GameObject conversa;
    public int casa;
    public int dialogo;
    public int numero;


    // Start is called before the first frame update
    void Start()
    {

        Dialogo1.SetActive(false);
        Dialogo2.SetActive(false);
        Dialogo3.SetActive(false);
        Dialogo4.SetActive(false);
        Dialogo5.SetActive(false);
        DialogoGeral.SetActive(false);
        

    }

    // Update is called once per frame
    void Update()
    {
        numero = PlayerPrefs.GetInt("Casa1");
        dialogo = PlayerPrefs.GetInt("DialogoCasa");

    }
    public void OnTriggerStay2D(Collider2D collision)
    {
        if (Input.GetKeyUp(KeyCode.E))
        {
            if (casa == 1)
            {
                PlayerPrefs.SetInt("Casa1", 2);

            }
            if (casa == 2)
            {
                PlayerPrefs.SetInt("Casa2", 1);

            }
            if (casa == 3)
            {
                PlayerPrefs.SetInt("Casa3", 1);

            }
            if (casa == 4)
            {
                PlayerPrefs.SetInt("Casa4", 1);

            }
            if (casa == 5)
            {
                PlayerPrefs.SetInt("Casa5", 1);


            }
        }


            if (collision.gameObject.CompareTag("Player"))
        {
            if (Input.GetKeyDown(KeyCode.E))
            {
               
                if (casa == 1)
                {
                    if (PlayerPrefs.GetInt("Casa1") == 1 )
                    {
                        Dialogo1.SetActive(false);
                        Dialogo2.SetActive(false);
                        Dialogo3.SetActive(false);
                        Dialogo4.SetActive(false);
                        Dialogo5.SetActive(false);
                        DialogoGeral.SetActive(true);
                    }
              else  if (PlayerPrefs.GetInt("Casa1") == 0)
                    {
                        if (PlayerPrefs.GetInt("DialogoCasa") == 0)
                        {
                            Dialogo1.SetActive(true);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 1);
                        }
                        else if (PlayerPrefs.GetInt("DialogoCasa") ==3)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(true);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 2);

                        }
                        else if (PlayerPrefs.GetInt("DialogoCasa") == 2)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(true);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 3);

                        }
                        else if (PlayerPrefs.GetInt("DialogoCasa") == 3)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(true);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 4);

                        }
                        else if (PlayerPrefs.GetInt("DialogoCasa") == 4)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(true);
                            DialogoGeral.SetActive(false);

                        }
                        
                    }
                }

                if (casa == 2)
                {
                    if (PlayerPrefs.GetInt("Casa2") == 1)
                    {
                        Dialogo1.SetActive(false);
                        Dialogo2.SetActive(false);
                        Dialogo3.SetActive(false);
                        Dialogo4.SetActive(false);
                        Dialogo5.SetActive(false);
                        DialogoGeral.SetActive(true);
                    }
                    if (PlayerPrefs.GetInt("Casa2") == 0)
                    {
                        if (PlayerPrefs.GetInt("DialogoCasa") == 0)
                        {
                            Dialogo1.SetActive(true);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 1);
                        }
                        if (PlayerPrefs.GetInt("DialogoCasa") == 1)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(true);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 2);

                        }
                        if (PlayerPrefs.GetInt("DialogoCasa") == 2)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(true);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 3);

                        }
                        if (PlayerPrefs.GetInt("DialogoCasa") == 3)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(true);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 4);

                        }
                        if (PlayerPrefs.GetInt("DialogoCasa") == 4)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(true);
                            DialogoGeral.SetActive(false);

                        }

                    }
                }

                if (casa == 3)
                {
                    if (PlayerPrefs.GetInt("Casa3") == 1)
                    {
                        Dialogo1.SetActive(false);
                        Dialogo2.SetActive(false);
                        Dialogo3.SetActive(false);
                        Dialogo4.SetActive(false);
                        Dialogo5.SetActive(false);
                        DialogoGeral.SetActive(true);
                    }
                    if (PlayerPrefs.GetInt("Casa3") == 0)
                    {
                        if (PlayerPrefs.GetInt("DialogoCasa") == 0)
                        {
                            Dialogo1.SetActive(true);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 1);
                        }
                        if (PlayerPrefs.GetInt("DialogoCasa") == 1)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(true);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 2);

                        }
                        if (PlayerPrefs.GetInt("DialogoCasa") == 2)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(true);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 3);

                        }
                        if (PlayerPrefs.GetInt("DialogoCasa") == 3)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(true);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 4);

                        }
                        if (PlayerPrefs.GetInt("DialogoCasa") == 4)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(true);
                            DialogoGeral.SetActive(false);

                        }

                    }
                }

                if (casa == 4)
                {
                    if (PlayerPrefs.GetInt("Casa4") == 1)
                    {
                        Dialogo1.SetActive(false);
                        Dialogo2.SetActive(false);
                        Dialogo3.SetActive(false);
                        Dialogo4.SetActive(false);
                        Dialogo5.SetActive(false);
                        DialogoGeral.SetActive(true);
                    }
                    if (PlayerPrefs.GetInt("Casa4") == 0)
                    {
                        if (PlayerPrefs.GetInt("DialogoCasa") == 0)
                        {
                            Dialogo1.SetActive(true);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 1);
                        }
                        if (PlayerPrefs.GetInt("DialogoCasa") == 1)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(true);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 2);

                        }
                        if (PlayerPrefs.GetInt("DialogoCasa") == 2)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(true);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 3);

                        }
                        if (PlayerPrefs.GetInt("DialogoCasa") == 3)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(true);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 4);

                        }
                        if (PlayerPrefs.GetInt("DialogoCasa") == 4)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(true);
                            DialogoGeral.SetActive(false);

                        }

                    }
                }

                if (casa == 5)
                {
                    if (PlayerPrefs.GetInt("Casa5") == 1)
                    {
                        Dialogo1.SetActive(false);
                        Dialogo2.SetActive(false);
                        Dialogo3.SetActive(false);
                        Dialogo4.SetActive(false);
                        Dialogo5.SetActive(false);
                        DialogoGeral.SetActive(true);
                    }
                    if (PlayerPrefs.GetInt("Casa5") == 0)
                    {
                        if (PlayerPrefs.GetInt("DialogoCasa") == 0)
                        {
                            Dialogo1.SetActive(true);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 1);
                        }
                        if (PlayerPrefs.GetInt("DialogoCasa") == 1)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(true);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 2);

                        }
                        if (PlayerPrefs.GetInt("DialogoCasa") == 2)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(true);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 3);

                        }
                        if (PlayerPrefs.GetInt("DialogoCasa") == 3)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(true);
                            Dialogo5.SetActive(false);
                            DialogoGeral.SetActive(false);
                            PlayerPrefs.SetInt("DialogoCasa", 4);

                        }
                        if (PlayerPrefs.GetInt("DialogoCasa") == 4)
                        {
                            Dialogo1.SetActive(false);
                            Dialogo2.SetActive(false);
                            Dialogo3.SetActive(false);
                            Dialogo4.SetActive(false);
                            Dialogo5.SetActive(true);
                            DialogoGeral.SetActive(false);

                        }

                    }
                }

            }
        }

    }
}