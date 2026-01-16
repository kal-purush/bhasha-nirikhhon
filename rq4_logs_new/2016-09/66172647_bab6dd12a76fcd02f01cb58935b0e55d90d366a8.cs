using UnityEngine;
using System.Collections;
using UnityEngine.SceneManagement;

public class LevelManagerScript : MonoBehaviour {

    public GameObject[] pieces;
    private int i = 0;
    private GameObject catapult;
    private GameObject cam;
	
    void Awake()
    {
        catapult = GameObject.FindGameObjectWithTag("catapult");
        cam = GameObject.FindWithTag("MainCamera");
    }

    void Update ()
    {
        if(Input.GetKeyDown("r"))
        {
            SceneManager.LoadScene(1);
        }
    }

	void Start ()
    {
        nextPiece();
	}
    public void nextPieceDelayed()
    {
        Invoke("nextPiece", 3f);
    }
    public void nextPiece()
    {
        Debug.Log("nextpieced");
        Instantiate(pieces[i], catapult.transform.position, catapult.transform.rotation);
        i++;
    }
}