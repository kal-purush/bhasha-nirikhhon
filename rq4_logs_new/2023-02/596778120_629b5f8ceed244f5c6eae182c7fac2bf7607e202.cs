using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class AgentController : MonoBehaviour
{
    private NavMeshAgent _navMeshAgent;
    private bool _interactMouse = false;
    
    void Start()
    {
        InputManager.OnInteractionMouse += () => _interactMouse = true;
        if (TryGetComponent(out NavMeshAgent _na_navMeshAgent))
        {
            _navMeshAgent = _na_navMeshAgent;
        }
    }

    // Update is called once per frame
    void Update()
    {
        if(_interactMouse)
        {
            _interactMouse = false;
            RaycastHit hit;
            if (Physics.Raycast(Camera.main.ScreenPointToRay(Input.mousePosition), out hit, Mathf.Infinity, LayerMask.GetMask("Ground")))
            {
                Vector3 mousePos = hit.point;
                _navMeshAgent.SetDestination(mousePos);
            }
        }
    }
    private void OnDisable()
    {
        InputManager.OnInteractionMouse -= () => _interactMouse = false;
    }
}