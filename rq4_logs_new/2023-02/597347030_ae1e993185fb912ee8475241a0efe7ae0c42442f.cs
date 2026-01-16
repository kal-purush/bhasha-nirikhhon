using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

[RequireComponent(typeof(NavMeshAgent))]
public class MoveSelectedObject : MonoBehaviour
{
    NavMeshAgent m_Agent;
    UintSelected stockedObject;
    RaycastHit m_HitInfo = new RaycastHit();

    void Start()
    {
        stockedObject = this.gameObject.GetComponent<UintSelected>();
        // m_Agent = GetComponent<NavMeshAgent>();
    }

    void Update()
    {
        if (Input.GetKey(KeyCode.Escape)) {
            stockedObject.setObject(null);
            return;
        }
        if (!stockedObject.getObject()) {
            return;
        }
        if (Input.GetMouseButtonDown(1) && !Input.GetKey(KeyCode.LeftShift))
        {
            Debug.Log(m_Agent);
            m_Agent = stockedObject.getObject().GetComponent<NavMeshAgent>();
            var ray = Camera.main.ScreenPointToRay(Input.mousePosition);
            if (Physics.Raycast(ray.origin, ray.direction, out m_HitInfo))
                m_Agent.destination = m_HitInfo.point;
        }   
    }

}