using UnityEngine;
using System.Collections;

public class TransporterController : MonoBehaviour
{
    public GameObject player;
    public GameObject[] transporters;

    private int currIndex;
    private static int pass;

    void Start()
    {
        currIndex = 0;
        pass = 0;
    }

    void Update()
    {
        StartCoroutine(CheckDistance());
    }

    IEnumerator CheckDistance()
    {
        float distance = Vector3.Distance(player.transform.localPosition, transporters[currIndex].transform.localPosition);
        // Mathf.Approximately(player.transform.localPosition.x, transporters[currIndex].transform.localPosition.x)

        if (distance < 0.5)
        {
            yield return new WaitForSeconds(0.5f);

            player.transform.localPosition = transporters[currIndex + 1].transform.localPosition;
            pass++;
            Debug.Log(pass);
        }
    }
}