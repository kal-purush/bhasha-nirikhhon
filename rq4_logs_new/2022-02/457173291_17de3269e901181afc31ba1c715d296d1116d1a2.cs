using UnityEngine;
using UnityEngine.SceneManagement;

public class GameManager : MonoBehaviour
{
    float checkpointDelay = 3f;
    float playerX = 0f;
    float playerZ = 0f;
    public GameObject player;

    public void Respawn(float xPos, float zPos){

        playerX = xPos;
        playerZ = zPos;
        Invoke("FromCheckpoint", checkpointDelay);
        
    }

    private void FromCheckpoint(){
        //for player death particle effect
        //particalSystem.gameObject.SetActive(false);

        Vector3 respawnPosition;
        
        // if (playerX <= 370f){
        //     respawnPosition = new Vector3(0,0,0);
        // } else if (playerX <= 715f){
        //     respawnPosition = new Vector3(2.25f, 0, 367.71f);
        // } else if (playerX <= 927f){
        //     respawnPosition = new Vector3(-0.82f, 0, 715.37f);
        // } else if (playerX <= 1036f){
        //     respawnPosition = new Vector3(0, 0, 930f);
        // } else {
        //     respawnPosition = new Vector3(0, 0, 1030f);
        // }

        respawnPosition = new Vector3(-6f, 1f, -21f);

        player.transform.position = respawnPosition;
        player.SetActive(true);
    }
    private void Update() {
        if(Input.GetKeyUp("escape")){
            Invoke("Restart", 0.5f);
        }
    }
    void Restart() {
        SceneManager.LoadScene(0);
    }



}