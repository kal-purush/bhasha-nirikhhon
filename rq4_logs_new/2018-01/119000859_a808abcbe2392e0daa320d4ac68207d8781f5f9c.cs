using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Chaser : MonoBehaviour {

    public Transform m_target;
    public float m_moveSpeed;

    private Rigidbody2D m_rigidbody2d;

	// Use this for initialization
	void Start () {
        m_rigidbody2d = GetComponent<Rigidbody2D>();
	}
	
	// Update is called once per frame
	void Update () {

        Vector3 dirToTarget = m_target.position - transform.position;
        Vector3 normalizedDirToTarget = Vector3.Normalize(dirToTarget);

        // Rotate to face the target
        float angle = Mathf.Atan2(dirToTarget.y, dirToTarget.x) * Mathf.Rad2Deg;
        transform.rotation = Quaternion.AngleAxis(angle, Vector3.forward);
        
        m_rigidbody2d.velocity = new Vector2(normalizedDirToTarget.x * m_moveSpeed, normalizedDirToTarget.y * m_moveSpeed);

	}
}