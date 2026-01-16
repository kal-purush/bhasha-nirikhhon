using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class Director : MonoBehaviour {
    #region Parameter
    public NavMeshAgent[] _agent;
    private Transform g_Camera;
    public float _edgeOfCursor = 100f;

    #endregion

    #region UnityFunction
    // Use this for initialization
    void Start () {
        g_Camera = Camera.main.transform;
        _agent = new NavMeshAgent[6];
    }
	
	// Update is called once per frame
	void Update () {
        Ray ray = Camera.main.ScreenPointToRay(Input.mousePosition);
        RaycastHit hit;
        if (Input.GetKeyDown(KeyCode.Mouse0) && Physics.Raycast(ray, out hit, _edgeOfCursor))
        {


            if (hit.transform.CompareTag("Agent"))
            {
                for (int i = 0; i < _agent.Length; i++)
                {
                    if (_agent[i] != null)
                        if (hit.transform.position == _agent[i].transform.position)
                        {
                            break;
                        }
                    if (_agent[i] == null)
                    {
                        _agent[i] = hit.transform.GetComponent<NavMeshAgent>();

                        g_Camera.LookAt(_agent[i].transform);
                        break;

                    }
                }
            }
           
            else
            {
                foreach (NavMeshAgent a in _agent)
                {
                    if (a != null)
                    {
                        a.destination = hit.point;
                        //g_Camera.eulerAngles = Vector3.Lerp(g_Camera.eulerAngles,new Vector3(20f, 0f, 0f), 0.5f);
                    }
                }
            }

        }
    }
    #endregion
}