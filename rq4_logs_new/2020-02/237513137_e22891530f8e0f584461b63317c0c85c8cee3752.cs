using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Burner : MonoBehaviour
{
    private Camera Camera;
    private Baloon Balloon;
    [SerializeField] private ParticleSystem particleSystem;
    [SerializeField] private ParticleSystem airParticleSystem;

    private float tempIncreaseRate = 5;

    void Start()
    {
        Camera = GetComponentInChildren<Camera>();
        Balloon = GameObject.FindGameObjectWithTag("Baloon").GetComponentInParent<Baloon>();

    }
    void Update()
    {
        RaycastHit rayHit;

        bool hitBurner = false;
        bool hitFlap = false;

        if (Input.GetAxis("Fire1") > 0.1f)
        {
            //if (Physics.Raycast(Camera.ScreenPointToRay(Input.mousePosition), out rayHit))
            if (Physics.Raycast(Camera.transform.position, Camera.transform.forward, out rayHit))
            {
                if (rayHit.transform.CompareTag("Burner"))
                {
                    hitBurner = true;
                    Balloon.AddTemp(tempIncreaseRate * Time.deltaTime);

                    particleSystem.gameObject.SetActive(true);
                }
                else if (rayHit.transform.CompareTag("Flap"))
                {
                    hitFlap = true;
                    Balloon.AddTemp(-tempIncreaseRate * 2f * Time.deltaTime );
                    airParticleSystem.gameObject.SetActive(true);
                }
            }

        }
        else
        {
            particleSystem.gameObject.SetActive(false);
            airParticleSystem.gameObject.SetActive(false);
        }

        
    }
}