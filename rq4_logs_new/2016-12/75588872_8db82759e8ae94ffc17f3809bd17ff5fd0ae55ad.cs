using UnityEngine;
using System.Collections;

public class CameraBoundObject : MonoBehaviour {

    private new Transform transform;

    [System.Serializable]
    public struct Bound {
        public float left, right;
        public float top, bottom;
    };

    [SerializeField]
    private Bound offset;

    void Awake() {
        transform = GetComponent<Transform>();
    }

    public Bound getBound
    {
        get
        {
            Bound bound = new Bound();
            bound.left = transform.position.x + offset.left;
            bound.right = transform.position.x + offset.right;
            bound.top = transform.position.z + offset.top;
            bound.bottom = transform.position.z + offset.bottom;

            return bound;
        }
    }
}