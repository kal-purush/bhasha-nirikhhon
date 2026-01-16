using System.Collections;
using Cinemachine;
using UnityEngine;

namespace Puzzle.PuzzlePieces
{
    public class ChangeCamera : MonoBehaviour
    {
        public void SetActiveCam(CinemachineVirtualCamera cam)
        {
            CameraManagerScript.CurrentActiveCamera = cam;
        }

        public void SetActiveCamWithDelay(CinemachineVirtualCamera cam)
        {
            StartCoroutine(DelayCamSwap(cam));
        }

        private IEnumerator DelayCamSwap(CinemachineVirtualCamera cam)
        {
            yield return new WaitForSeconds(3f);
            CameraManagerScript.CurrentActiveCamera = cam;
        }
    }
}