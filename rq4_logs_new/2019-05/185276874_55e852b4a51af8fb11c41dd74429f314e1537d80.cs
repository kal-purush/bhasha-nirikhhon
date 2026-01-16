using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Kretanje : MonoBehaviour
{
    public float brzinaKretanja = 5;
    public GameObject sideCamera;
    public GameObject ThirdPersonCamera;
    bool sideViewActive= true;
    bool delayActive = false;
    float delayTime = 0;

    private void Update()
    {
        if (sideViewActive == true && delayActive==false) //Ako smo u modu SideViewa pokreni 2-D kontrole
        {
            MovementSide();
        }

        if (sideViewActive == false) //Ako smo u third person modu pokreni 3-D kontrole
        {
            if(delayActive==false)
            {
                MovementThird();
            }
        }

        if(delayActive==true) //Delay za promjenu kontrola
        {
            delayTime += Time.deltaTime;
            if(delayTime>0.3)
            {
                delayActive = false;
            }
        }
    }
    void MovementSide() //2-D kontrole - zasad lijevo i desno
    {
        if (Input.GetKey(KeyCode.D))
        {
            transform.Translate(Vector3.right * brzinaKretanja * Time.deltaTime); //LIJEVO
        }
        if (Input.GetKey(KeyCode.A))
        {
            transform.Translate(Vector3.left * brzinaKretanja * Time.deltaTime); //DESNO
        }
        if(Input.GetKeyDown(KeyCode.Space))
        {
            GetComponent<Rigidbody>().velocity += Vector3.up * 7; //SKOK dodaje rigidbodyiju velocity prema gore za 7 jedinica
        }
    }
    void MovementThird() // 3-D kontrole - zasad gore dolje lijevo i desno bez skoka
    {
        if (Input.GetKey(KeyCode.W))
        {
            transform.Translate(Vector3.right * brzinaKretanja * Time.deltaTime); //UNAPRIJED
        }
        if (Input.GetKey(KeyCode.A))
        {
            transform.Translate(Vector3.forward * brzinaKretanja * Time.deltaTime); //LIJEVO
        }
        if (Input.GetKey(KeyCode.D))
        {
            transform.Translate(Vector3.back * brzinaKretanja * Time.deltaTime); //DESNO
        }
        if (Input.GetKey(KeyCode.S))
        {
            transform.Translate(Vector3.left * brzinaKretanja * Time.deltaTime); // UNAZAD
        }
    }
    private void OnTriggerEnter(Collider other) // promjena perspektive pri prolazu kroz triger
    {
        if(other.gameObject.tag =="CameraSwitch3rd") //Promjena u pticju perspektivu
        {
            sideCamera.transform.gameObject.SetActive(false);
            ThirdPersonCamera.transform.gameObject.SetActive(true);
            sideViewActive = false;
            delayActive = true;
        }
        if (other.gameObject.tag == "CameraSwitchSide") //Promjena u 2D
        {
            ThirdPersonCamera.transform.gameObject.SetActive(false);
            sideCamera.transform.gameObject.SetActive(true);
            float x = gameObject.transform.position.x;
            float y = gameObject.transform.position.y;
            gameObject.transform.position = new Vector3(x, y, 0);
            sideViewActive = true;
            delayActive = true;
        }
    }
}