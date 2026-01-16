using System.Collections;
using UnityEngine.UI;
using UnityEngine;

public class OpenEyes : MonoBehaviour
{
    public float speed = 0.09f;
    public Image eyes;
    public float alfa = 0;
    public bool close = false;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        if(close)
        {
            Opem();
        }
        else
        {
            alfa = 1;
        }
    }

    void Opem()
    {
        alfa -= speed * Time.deltaTime;
        Color _color = new Color(0, 0, 0, alfa);
        eyes.color = _color;
        if (alfa < 0)
            close = false;
    }
}