using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AlienNest : MonoBehaviour
{
    #region Variables
    public GameObject alienSmall;
    public GameObject alienQueen;
    public int nbAlienNeed;
    public int spawnTimer = 3;
    // Start is called before the first frame update
    #endregion


    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown("f"))
        {
            StartCoroutine(WaitSpawn());
        }
    }

    private IEnumerator WaitSpawn()
    {
        while (enabled)
        {
            yield return new WaitForSeconds(spawnTimer);
            SpawnSmall();
        }
    }

    private void SpawnSmall()
    {

        //Instantiate the small aliens at the same position we are at
        GameObject alienSClone = (GameObject)Instantiate(alienSmall, transform.position, Quaternion.identity);
        //Carry over some varriable to Alien script?
        //alienSClone.GetComponent<AlienNest>().someVariable = GetComponent<AlienNest>().someVariable;
    }
}