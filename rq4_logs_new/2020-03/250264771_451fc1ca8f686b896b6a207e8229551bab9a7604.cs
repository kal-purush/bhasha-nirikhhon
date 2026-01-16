using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Silah : MonoBehaviour  {
   
    public bool basla1;
    
    public int mermi;
	public bool ates;
    public int puan;
    public Text puanyazi;
	RaycastHit hit;
    public float ateszamanı,sıradakiAtes;
    ParticleEmitter muzzle;

    AudioSource ses,ses2;

    public AudioClip atessesi;
    public AudioClip patlamasesi;
    


    void Start () {

        
        puan = 0;
        muzzle = GetComponentInChildren<ParticleEmitter> ();
        ses = GetComponent<AudioSource>();
        ses2 = GetComponent<AudioSource>();
      
	} 
	
	// Update is called once per frame
	void Update () {
		if (Input.GetMouseButton(0) && mermi>0 && Time.time>sıradakiAtes) {
			ates = true;
            muzzle.emit =true;
            ses.PlayOneShot(atessesi);
            sıradakiAtes = Time.time + ateszamanı;
            mermi--;

            

            if (mermi == 0)
            {
                mermi = 30;
            }
		}
        if (Input.GetMouseButtonUp(0))
             {
            muzzle.emit = false;
            }
        
        puanyazi.text = "Puan : "+puan;
		}
	void FixedUpdate(){
        
        if (ates == true) {
			
			if(Physics.Raycast(Camera.main.transform.position,Camera.main.transform.forward,out hit)){
                if (hit.transform.tag == "Fıcı")
                {
                    ses2.PlayOneShot(patlamasesi);
                    Debug.Log("fıcıya değdi");
                    puan = puan + 5;
                    
                    hit.transform.gameObject.active = false;
                    Invoke("gorun", 1.5f);
                    //  Destroy(hit.transform.gameObject);
                }
                
				
			}
            ates = false;
        }
        if (Input.GetKey(KeyCode.E))
        {
            Application.LoadLevel(0);
        }
	}
    void gorun()
    {
        hit.transform.gameObject.active = true;
    }
}