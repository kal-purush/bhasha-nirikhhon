using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

namespace JevLogin
{
    public sealed class Radar : MonoBehaviour
    {
        private Transform _playerPosition;
        private readonly float _mapScale = 2.0f;
        public static List<RadarObject> RadObjects = new List<RadarObject>();

        private void Start()
        {
            _playerPosition = Camera.main.transform;
        }
        public static void RegisterRadarObject(GameObject obj, Image i)
        {
            Image image = Instantiate(i);
            RadObjects.Add(new RadarObject { Owner = obj, Icon = image });
        }

        public static void RemoveRadarObject(GameObject o)
        {
            List<RadarObject> newList = new List<RadarObject>();
            foreach (RadarObject t in RadObjects)
            {
                if (t.Owner == o)
                {
                    Destroy(t.Icon);
                    continue;
                }
                newList.Add(t);
            }
            RadObjects.RemoveRange(0, RadObjects.Count);
            RadObjects.AddRange(newList);
        }

        private void DrawRadarDots()
        {
            foreach (RadarObject radarObject in RadObjects)
            {
                Vector3 radarPosition = radarObject.Owner.transform.position - _playerPosition.position;
                float distToObject = Vector3.Distance(_playerPosition.position, radarObject.Owner.transform.position) * _mapScale;
                float deltaY = Mathf.Atan2(radarPosition.x, radarPosition.z) * Mathf.Rad2Deg - 270 - _playerPosition.eulerAngles.y;
                radarPosition.x = distToObject * Mathf.Cos(deltaY * Mathf.Deg2Rad) * -1.0f;
                radarPosition.z = distToObject * Mathf.Sin(deltaY * Mathf.Deg2Rad);
                radarObject.Icon.transform.SetParent(transform);
                radarObject.Icon.transform.position = new Vector3(radarPosition.x, radarPosition.z, 0.0f) + transform.position;
            }
        }

        private void Update()
        {
            if (Time.frameCount % 2 == 0)
            {
                DrawRadarDots();
            }
        }
    }
}