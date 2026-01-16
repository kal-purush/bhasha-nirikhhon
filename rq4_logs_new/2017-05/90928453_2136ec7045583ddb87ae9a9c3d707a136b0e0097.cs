using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AKM : MonoBehaviour {
    [SerializeField]
    private AudioClip _ShotSound;

    [SerializeField]
    private float _Range;

    private EllipsoidParticleEmitter _Sparks;
	// Use this for initialization
	void Start () {
        _Sparks = gameObject.GetComponentInChildren<EllipsoidParticleEmitter>();
        _Sparks.emit = false;
	}
	
	// Update is called once per frame
	void Update () {
        Vector3 fwd = transform.TransformDirection(Vector3.forward);
        RaycastHit hit;

        if (Input.GetButtonDown("Fire1"))
        {
            _Sparks.Emit();
            //audio.Play(); TODO

            if (Physics.Raycast(transform.position, fwd, out hit))
            {
                if (hit.transform.tag == "Enemy" && hit.distance < _Range)
                {
                    Debug.Log("Trafiony przeciwnik");

                }
                else if (hit.distance < _Range)
                {
                    Debug.Log("Trafiona Sciana");
                }
            }
        }
    }
}