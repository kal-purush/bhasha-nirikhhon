using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[System.Serializable]
public class Leash : MonoBehaviour
{
    [SerializeField][Range(0.01f, 0.2f)]
    public float MX_DIST_TO_HAND = .1f;
    [SerializeField] private Transform leash;

    public bool is_leashed = true;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    public void Update()
    {
	if(is_leashed){
        // Leash Target back to hand
	var delta = transform.position-leash.position;
	if(delta.magnitude > MX_DIST_TO_HAND)
	{transform.position = leash.position + delta.normalized * MX_DIST_TO_HAND;}
	}
    }
}