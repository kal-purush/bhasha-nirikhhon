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

        Vector3 dirToTarget = Vector3.Normalize(m_target.position - transform.position);

        m_rigidbody2d.velocity = new Vector2(dirToTarget.x * m_moveSpeed, dirToTarget.y * m_moveSpeed);

	}
}