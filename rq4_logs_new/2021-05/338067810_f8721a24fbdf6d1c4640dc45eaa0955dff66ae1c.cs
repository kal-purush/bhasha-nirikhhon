using UnityEngine;
using System;
using System.Timers;

namespace Assets.Scripts.Obstacle
{
    [RequireComponent(typeof(BoxCollider2D))]
    class DissapearableBlockObstacle : MonoBehaviour
    {
        [Tooltip("The block that dissapears when player steps in the collision area")]
        [SerializeField] private GameObject _block;

        [Tooltip("The time it takes for a block to reappear after it has dissapeared")]
        private double _appearTime = 3000;
        [Tooltip("The time it takes for a block to dissapear after player steps on it")]
        private double _dissapearTime = 800;

        private bool _active = true;

        private Timer _timer = new Timer();

        public void FixedUpdate()
        {
            if (_active != _block.activeSelf)
                _block.SetActive(_active);
        }

        public void OnTriggerEnter2D(Collider2D collision)
        {
            if (collision.CompareTag("Player") && !_timer.Enabled && _block.activeSelf)
                NewTimer(_dissapearTime, OnBlockDissapear);
        }

        private void OnBlockDissapear(System.Object source, ElapsedEventArgs e)
        {
            _active = false;
            NewTimer(_appearTime, OnBlockAppear);
        }

        private void OnBlockAppear(System.Object source, ElapsedEventArgs e)
        {
            _active = true;
        }

        private void NewTimer(double time, Action<System.Object, ElapsedEventArgs> func)
        {
            Debug.Log(time);
            _timer = new Timer(time);
            _timer.Elapsed += (sender, e) => func(sender, e);
            _timer.AutoReset = false;

            _timer.Start();
        }
    }
}