using UnityEngine;
using System.Collections;

public class InFlight : MonoBehaviour
{
    [SerializeField]GameObject rig;
    [SerializeField]GameObject head;

    [SerializeField]Vector3 landPosForRig;
    Vector3 headpos;
    [SerializeField]
    TextWindow dialouge;
	
    void Start()
    {
        headpos = head.transform.localPosition;
    }

	void Update ()
    {
        head.transform.position = headpos;
	}

    public void KickOut()
    {
        rig.transform.parent = null;
        rig.transform.position = landPosForRig;
        dialouge.PushText("Lets walk aroud and see whats going on.");
        Destroy(gameObject.GetComponent<InFlight>());
    }
}