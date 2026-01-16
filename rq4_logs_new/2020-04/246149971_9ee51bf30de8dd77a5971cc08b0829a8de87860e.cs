using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;

public class DialogoContinuo : MonoBehaviour
{
    public GameObject panelBox;
    public GameObject Conversa;
    public bool podeFalar = false;
    public bool Guilda = false;
    public bool jafoi = false;


    [SerializeField]
    private int linhaAtual;
    public TextMeshProUGUI textoMensagem;
    public string[] texto;
    public int limitText;



    //public float timer = 0;
    public static bool estaFalando = false;
    [SerializeField]
    private bool teste = false;
    [SerializeField]
    private bool jaComecaFalando;
    [SerializeField]
    // private bool[] Mike = 
    public GameObject img;
    public GameObject[] imgs;
    public int conta;
    //[SerializeField]
    //private bool[] ray;
    public bool rodaCut = false;
    void Start()
    {
        textoMensagem.text = texto[linhaAtual].ToString();
        img = imgs[linhaAtual];
        conta = 0;

    }

    // Update is called once per frame
    void Update()
    {

        if (jafoi == false)
        {

            if (podeFalar)
            {
                //img.SetActive(false);
                //img = imgs[linhaAtual];
                estaFalando = true;


                img.SetActive(false);
                img = imgs[linhaAtual];
                if (img == imgs[linhaAtual])
                {
                    img.SetActive(true);
                }


                if (Input.GetKeyDown(KeyCode.E))
                {
                    //img.SetActive(false);
                    if (linhaAtual >= limitText)
                    {
                        Time.timeScale = 1f;
                        Desabilitar();
                        podeFalar = false;
                        linhaAtual = 0;

                    }
                    linhaAtual++;
                    textoMensagem.text = texto[linhaAtual].ToString();
                    // img.SetActive(true);

                }
               
            }
            teste = estaFalando;
        }
    }


    public void OnTriggerStay2D(Collider2D collision)
    {



        if (collision.gameObject.CompareTag("Player"))
        {
            if (jaComecaFalando)
            {
                Time.timeScale = 0f;

                podeFalar = true;
                Habilitar();
            }
            else if (!jaComecaFalando && Input.GetKeyUp(KeyCode.E))
            {
                Time.timeScale = 0f;

                podeFalar = true;
                Habilitar();

            }
        }

    }

    void Habilitar()
    {
        panelBox.SetActive(true);

        //estaFalando = true;
        img.SetActive(true);
    }
    void Desabilitar()
    {
        img.SetActive(false);

        panelBox.SetActive(false);
        estaFalando = false;
        jafoi = true;
        Conversa.SetActive(false);


    }
    public void ativaSprite()
    {

    }
}