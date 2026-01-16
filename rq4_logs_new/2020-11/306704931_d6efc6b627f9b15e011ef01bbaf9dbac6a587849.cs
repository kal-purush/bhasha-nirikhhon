using System;
using UnityEngine;
using UnityEngine.AI;
using UnityEngine.UIElements;
using Random = UnityEngine.Random;

namespace JevLogin
{
    public class Ghost : InteractiveObject
    {
        private NavMeshAgent _navMeshAgent;
        private Transform[] _waypoints;

        private int _currentWaypointIndex;

        public Ghost()
        {
            if (_waypoints == null)
            {
                _waypoints = new[] { transform };
            }
            Transform tempTransform = transform;

            tempTransform.position = new Vector3(transform.position.x + Random.Range(2, 5), transform.position.y, transform.position.z + Random.Range(2, 5));

            Array.Resize(ref _waypoints, _waypoints.Length + 1);
            _waypoints[_waypoints.Length - 1] = tempTransform;
        }

        private void Start()
        {
            _navMeshAgent.SetDestination(_waypoints[0].position);
        }

        public override void Execute()
        {
            throw new System.NotImplementedException();
        }

        protected override void Interaction()
        {
            throw new System.NotImplementedException();
        }
    }
}