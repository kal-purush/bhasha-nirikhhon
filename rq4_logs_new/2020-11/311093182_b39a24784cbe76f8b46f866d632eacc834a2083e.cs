using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CartHandle : MonoBehaviour
{
    const float HANDLE_RADIUS = 0.65f;
    Transform hand_L;
    Transform hand_R;

    // Start is called before the first frame update
    void Start()
    {
        
    }
   

    private void FixedUpdate()
    {
        if (hand_L != null)
        {
            pull(hand_L);
        }
        if (hand_R != null)
        {
            pull(hand_R);
        }
    }

    private Vector3 calculateHandleRoot(bool right_hand)
    {
        float mod = 1;
        if (!right_hand)
            mod *= -1;
        Vector3 offset = new Vector3(mod * HANDLE_RADIUS, 0, 0);
        offset = Quaternion.AngleAxis(-transform.localRotation.eulerAngles.x, Vector3.forward) * offset;
        return offset;
    }

    public void grab(Transform grabber, int hand_id)
    {
        switch (hand_id)
        {
            case 0:
                hand_L = grabber;
                break;
            case 1:
                hand_R = grabber;
                break;
        }
    }
    
    void pull(Transform hand)
    {
        Vector3 projection = new Vector3(
            hand.GetComponent<RectTransform>().position.x,
            hand.GetComponent<RectTransform>().position.y,
            gameObject.transform.position.z - Camera.main.transform.position.z);

        Vector3 handle_root = calculateHandleRoot(false);

        Vector3 diff = Camera.main.ScreenToWorldPoint(projection) - handle_root;
        GetComponent<Rigidbody>().AddForceAtPosition(diff, handle_root, ForceMode.Impulse);

        Debug.DrawRay(gameObject.transform.position, diff, Color.white, Time.fixedDeltaTime);
    }
}