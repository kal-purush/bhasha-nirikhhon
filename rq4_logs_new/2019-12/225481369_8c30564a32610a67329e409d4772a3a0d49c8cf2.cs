using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class EnemyIA : MonoBehaviour
{
    [SerializeField] private float sighRange = 0;
    [SerializeField] private Transform PlayerTransform;
    [SerializeField] private GameObject warning = null;
    [SerializeField] private GameObject detect = null;
    private Transform enemitransform;
    float vel = 1.5f;
    public Text perdi;
    

    void Awake()
    {
        enemitransform = transform;
    }

    void Start()
    {
        StartCoroutine(Cambio());
    }

    IEnumerator Cambio()
    {
        while (true)
        {
            transform.Rotate(0,180,0);
            yield return new WaitForSeconds(10);
        }
        
        
    }
    void Update()
    {
        IsLookinThePlayer(PlayerTransform.position);
        transform.position+=transform.forward *(vel * Time.deltaTime);
    }

    private float timeOnsight;

    private void Detecplayer(bool IsLookinThePlayer)
    {
        warning.SetActive(true);
        detect.SetActive(false);
        if(timeOnsight<=3)
        {
            timeOnsight += Time.deltaTime;

            if (!(timeOnsight >= 3)) return;
            warning.SetActive(false);
            detect.SetActive(true);
            Debug.Log("fin");
            SceneManager.LoadScene("escenario1");
            
        }
       

        else
        {
            if(timeOnsight > 0)
            {
                timeOnsight -= Time.deltaTime;
            }
            if (!(timeOnsight <= 0)) return;
            warning.SetActive(false);
            detect.SetActive(false);
        }
        
    }

    private bool IsLookinThePlayer(Vector3 playerPosition)
    {
        var displacement = playerPosition - enemitransform.position;
        var distacetoPlayer = displacement.magnitude;
        if (distacetoPlayer <= sighRange)
        {
            var dot = Vector3.Dot(enemitransform.forward, displacement.normalized);
            if (!(dot >= 0.5)) return false;

            var layerMask = 1 << 2;

            layerMask = ~layerMask;

            if(Physics.Raycast(enemitransform.position,displacement.normalized,out var hit , sighRange, layerMask))
            {
                Debug.DrawRay(enemitransform.position, displacement.normalized * hit.distance, Color.red);

                Debug.Log("Did hit");

               if (!hit.collider.GetComponent<Player>()) return false;

                Debug.DrawRay(enemitransform.position, displacement.normalized * hit.distance, Color.green);
               Invoke("Scen",2);
                perdi.text = "Perditessss Gonorr";
                Invoke("Men",2);
                Debug.Log("Te pille caremirla");

               
            }
        }
        return false;
    }
    void Scen()
    {
         SceneManager.LoadScene("Gonorr");
    }
    void Men()
    {
        perdi.enabled = false;
    }
}