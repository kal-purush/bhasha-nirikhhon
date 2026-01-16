using UnityEngine;
using System.Collections;

public class ObjectFollower : MonoBehaviour
{
    [SerializeField]
    private Transform other;
    public Transform Other
    {
        get
        {
            return other;
        }
        set
        {
            other = value;
            oriThis = transform.position;
            oriOther = other.transform.position;
        }
    }

    private Vector2 oriThis;
    private Vector2 oriOther;

	// Use this for initialization
	void Start ()
    {
        oriThis = transform.position;
        oriOther = other.transform.position;
    }

	void Update()
    {
        Vector2 dif = oriOther - (Vector2)other.transform.position;
        transform.position = oriThis + dif;
    }
}