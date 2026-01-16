using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class IntroCenario : MonoBehaviour
{
     
    SpriteRenderer m_SpriteRenderer;
    Color m_NewColor;  // Retorno da cor em float m_r, m_g, m_b
    public ParticleSystem fogo;
    Color corFinal = new Color(0f ,0f, 0f, 0f);
    private GameObject[] arvores;


    void Awake()
    {  
       // Pegar <Component> Cor
       // m_SpriteRenderer = GetComponent<SpriteRenderer>();
       // SpriteRenderer spriteRenderer = GetComponent<SpriteRenderer>();
       // Color spriteColor = gameObject.GetComponent<SpriteRenderer>().color;

    } 

    void Start()
    {
        if (arvores == null)
          arvores = GameObject.FindGameObjectsWithTag("Trees");

        foreach (GameObject arvore in arvores) // Mudar a cor das arvores && Iniciar particula de fogo
        {
	// m_SpriteRenderer.color = Color.blue;
        // spriteColor = Color.blue;
        // spriteColor = corFinal;
        }
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}