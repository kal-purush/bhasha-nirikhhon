using UnityEngine;

namespace Earthquake
{
    //Attribute [MonoController]
    public class CameraTravelController : MonoBehaviour
    {
        [SerializeField] private Camera m_Camera;
        [SerializeField] private float travelDelta;
        private Vector3 cameraOriginalPosition;
        private Vector3 cameraFinishTravelPosition;
        bool isTraveling, isMoveLeft;

        private void OnEnable()
        {
            cameraOriginalPosition = m_Camera.transform.position;
            cameraFinishTravelPosition = new Vector3(cameraOriginalPosition.x - travelDelta, cameraOriginalPosition.y, cameraOriginalPosition.z);
            StopTravel();
        }
        public void ChangeCameraMode()
        {
            if (isTraveling)
            {
                StopTravel();
            }
            else
            {
                StartTravel();
            }
        }

        public void StartTravel()
        {
            isTraveling = true;
            isMoveLeft = true;
        }

        public void StopTravel()
        {
            isTraveling = false;

            m_Camera.transform.position = cameraOriginalPosition;
            isMoveLeft = true;
        }

        private void Travel()
        {
            if (isMoveLeft)
            {
                m_Camera.transform.position = new Vector3(m_Camera.transform.position.x - 0.05f, m_Camera.transform.position.y, m_Camera.transform.position.z);
                if (m_Camera.transform.position.x < cameraFinishTravelPosition.x)
                {
                    isMoveLeft = false;
                }
            }
            else
            {
                m_Camera.transform.position = new Vector3(m_Camera.transform.position.x + 0.05f, m_Camera.transform.position.y, m_Camera.transform.position.z);
                if (m_Camera.transform.position.x > cameraOriginalPosition.x)
                {
                    isMoveLeft = true;
                }
            }
        }

        private void FixedUpdate()
        {
            if (!isTraveling) { return; }
            Travel();
        }
    }
}