using System;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

namespace _Scripts.PlayerScripts
{
    public class TrackablePlayerCircle : MonoBehaviour
    {
        [SerializeField] private GameObject track;
        [SerializeField] private List<Color> circles;
        private int _active;
        private Image _image;
        
        private void Start()
        {
            _image = GetComponentInChildren<Image>();
            _image.color = circles[_active];
        }

        private void Update()
        {
            Vector3 pos = track.transform.position;
            Transform transform1 = transform;
            transform1.position = new Vector3(pos.x, transform1.position.y, pos.z);
            Quaternion transform1Rotation = transform1.rotation;
            transform1Rotation.eulerAngles = new Vector3(0, track.transform.rotation.eulerAngles.y + 180, 0);
            transform1.rotation = transform1Rotation;
        }

        public void SetActive(int index)
        {
            _active = index;
        }
    }
}