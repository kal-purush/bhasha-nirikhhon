using UnityEngine;
using System.Collections;

public class CameraMovement : MonoBehaviour
{
    [SerializeField]
    GameObject cameraHolder;
    [SerializeField]
    GameObject defaultCameraPos;
    [SerializeField]
    GameObject groundPos;
    [SerializeField]
    PlayerManager playerManager;

	// Use this for initialization
	void Start ()
    {
	

	}
	
	// Update is called once per frame
	void FixedUpdate ()
    {
        groundPos.transform.position = new Vector3(cameraHolder.transform.position.x, 0.0f, cameraHolder.transform.position.z);

	    if(playerManager.totalPlayers.Count == 2)
        {
            gameObject.transform.position = Vector3.MoveTowards(gameObject.transform.position, Vector3.Lerp(playerManager.totalPlayers[0].transform.position, playerManager.totalPlayers[1].transform.position, 0.5f), 5.0f * Time.deltaTime);

            float dist = Vector3.Distance(playerManager.totalPlayers[0].transform.position, playerManager.totalPlayers[1].transform.position);

            Debug.Log(Vector3.Distance(groundPos.transform.position, playerManager.totalPlayers[0].transform.position));
            float player1Dist = Vector3.Distance(groundPos.transform.position, playerManager.totalPlayers[0].transform.position);
            float player2Dist = Vector3.Distance(groundPos.transform.position, playerManager.totalPlayers[1].transform.position);

            Vector3 zoomPos = Vector3.zero;

            if (player1Dist < 30.0f || player2Dist < 30.0f)
            {
                zoomPos = defaultCameraPos.transform.position + -(gameObject.transform.position - defaultCameraPos.transform.position) * dist / 70.0f;
            }
            else
            {
                zoomPos = defaultCameraPos.transform.position + -(gameObject.transform.position - defaultCameraPos.transform.position) * dist / 100.0f;
            }

            cameraHolder.transform.position = Vector3.MoveTowards(cameraHolder.transform.position, zoomPos, 5.0f * Time.deltaTime);
        }
	}
}