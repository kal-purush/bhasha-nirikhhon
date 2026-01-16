using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class Playermovement : MonoBehaviour
{

    // Höfundur: Daníel Þór Ingólfsson
    private Rigidbody leikmadur; // Rigidbody fyrir leikmann
    public float hradi = 0.5f; // hraða breyta leikmanns
    public float hlidarhradi = 0.25f; // hliðarhraða breyta leikmanns
    public float hoppa = 0.25f; // hopp stærð leikmanns
    public static int count; // count breyta fyrir stig
    public Text countText; // texti fyrir stigin

    void Start()
    {
        leikmadur = GetComponent<Rigidbody>(); // tengir leikmadur breytuna við player
    }

    void FixedUpdate()
    {
        if (Input.GetKey(KeyCode.UpArrow)) // ef ýtt er á upp örvatakka, færir áfram
        {
            transform.position += transform.forward * hradi ;
        }
        if (Input.GetKey(KeyCode.DownArrow)) // ef ýtt er á niður örvatakka, færir aftur
        {
            transform.position -= transform.forward * hradi ;
        }
        if (Input.GetKey(KeyCode.RightArrow)) // ef ýtt er á hægri örvatakka, færir til hægri
        {
            transform.position += transform.right * hlidarhradi ;
        }
        if (Input.GetKey(KeyCode.LeftArrow)) // ef ýtt er á vinstri örvatakka, færir til vinstri
        {
            transform.position -= transform.right * hlidarhradi ;
        }
        if (Input.GetKey(KeyCode.Space)) // ef ýtt er á space takkann, hoppar
        {
            transform.position += transform.up * hoppa;
        }
        if (transform.position.y <= -1) // ef player dettur út af, endurræsir leikinn
        {
            Endurraesa();
        }
    }
    private void OnCollisionEnter(Collision collision) // fyrir árekstra leikmanns
    {
        if (collision.collider.tag == "hlutur") // ef tag er hlutur, þá rekst leikmaður
                                                // á venjulegan pening, 1 stig
        {
            collision.collider.gameObject.SetActive(false);
            count = count + 1;
            Debug.Log("Nú er ég komin með " + count);
            SetCountText(); // kallar á fallið Setcounttext
        }
        if (collision.collider.tag == "bonus") // bonus er 3 stig, betri peningar
        {
            collision.collider.gameObject.SetActive(false);
            count = count + 3;
            Debug.Log("Nú er ég komin með " + count);
            SetCountText();//kallar á fallið Setcounttext
        }
        if (collision.collider.tag == "hindrun") //hindrun er hindrun, -1 á stig leikmanns
        {
            collision.collider.gameObject.SetActive(false);
            count = count - 1;
            Debug.Log("Nú er ég komin með " + count);
            SetCountText();//kallar á fallið Setcounttext
        }
    }
    void SetCountText() // breytir console texta yfir í UI enableraðan streng
    {
        countText.text = "Stig: " + count.ToString();

        if (count <= 0) // ef stig verða 0 eða minna, þá endurræsist leikur
        {
            this.enabled = false;//kemur í veg fyrir að leikmaður geti hreyfst áfram eftir dauðan
            countText.text = "dauður"; // gefur í skyn hvað gerðist

            StartCoroutine(Bida()); // byrjar rútínuna Bíða

        }
    }
    IEnumerator Bida() // bíður í eina sek og kallar í fallið endurræsa
    {
        yield return new WaitForSeconds(1);
        Endurraesa();
    }
    public void Endurraesa() // endurræsir í senu 1, sem er fyrra borðið
    {
        SceneManager.LoadScene(1);
    }
}