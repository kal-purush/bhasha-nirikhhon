using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;

namespace Girigiri
{
    [RequireComponent(typeof(Rigidbody2D))]
    public class Chip : MonoBehaviour
    {
        public float Size { get; private set; } = 1.0f;
        public bool IsStop
        {
            get
            {
                return body?.IsSleeping() ?? true;
            }
        }
        private Rigidbody2D body;
        [SerializeField]
        private List<GameObject> OnDropChips;
        public ChipFactory ChipFactory { get; set; }
        // Use this for initialization
        void Start()
        {
            body = GetComponent<Rigidbody2D>();
            if (body == null)
            {
                Debug.Log("Rigidbody2d is null");
                Destroy(gameObject);
            }
        }
        // Update is called once per frame
        void Update()
        {

        }

        void OnCollisionEnter2D(Collision2D collision)
        {
            if (collision.gameObject.GetComponent<Ground>() != null)
            {
                Destroy(gameObject);
            }
        }
        void OnDestroy()
        {
            ChipFactory.Remove(this);
        }
    }
}