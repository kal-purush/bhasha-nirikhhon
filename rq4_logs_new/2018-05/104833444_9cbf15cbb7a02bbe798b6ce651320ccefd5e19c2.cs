using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class MiniGameSceneSwitchController : MonoBehaviour {

    public bool save;

    public string scene_name;

    private bool oldSave;
    private bool upToDate;

    private string exitState;

    public MinigameController mc;
    public GameObject mc_go;

    public MonoBehaviour passingObject;
    public GameObject passingGameObject;

    private GameObject parentGO;

    public StarSpawnController spawnController;

    // Use this for initialization
    void Start() {
        save = false;
        upToDate = true;

        mc = null;

        parentGO = new GameObject("Inactive Scene");
        parentGO.transform.parent = this.transform;
        parentGO.SetActive(false);

    }

    // Update is called once per frame
    void Update() {


        if(mc != null) {
            if(mc.state == "post-game") {

                save = false;
                if(mc.exitState == "win") {
                    Debug.Log("You won the thing!");
                }
                Destroy(mc_go);
            }
        }

        if(save && !oldSave) {
            upToDate = false;
        }

        if(!save && oldSave) {
            upToDate = false;
        }

        if(!upToDate) {
            Debug.Log("Updating");
            updateData();
            upToDate = true;
        }

        oldSave = save;
    }

    public void passObject(MonoBehaviour mb) {
        passingObject = mb;
    }


    void updateData() {
        if(save) {
            DontDestroyOnLoad(this.gameObject);
            List<GameObject> rootObjects = new List<GameObject>();
            Scene scene = SceneManager.GetActiveScene();
            scene.GetRootGameObjects(rootObjects);

            foreach(GameObject go in rootObjects) {
                if(go != this.gameObject) {
                    go.transform.parent = parentGO.transform;
                }
            }

            GameController.LoadScene(scene_name);
        } else {
            List<GameObject> rootObjects = new List<GameObject>();
            Scene scene = SceneManager.GetActiveScene();
            scene.GetRootGameObjects(rootObjects);

            foreach(GameObject go in rootObjects) {
                if(go != this.gameObject) {
                    Destroy(go);
                }
            }

            List<Transform> tList = new List<Transform>();
            foreach(Transform t in parentGO.transform) {
                tList.Add(t);
            }

            foreach(Transform child in tList) {
                child.parent = null;
            }

            //Handle star dropping here.
            spawnController.DropStar();
        }
    }
}